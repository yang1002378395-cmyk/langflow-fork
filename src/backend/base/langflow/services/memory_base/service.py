"""MemoryBase service – business logic for CRUD and ingestion orchestration.

Edge cases handled:
- Deletion during sync: cancels active tasks before DB deletion.
- Concurrent task prevention: returns 409 if a job is already IN_PROGRESS.
- Threshold updates: deferred; does not re-evaluate pending count immediately.
- FS / Vector DB mismatch: detects and surfaces a warning flag.
- Regenerate: resets all session cursors to None and re-triggers ingestion.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from lfx.log.logger import logger
from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from langflow.api.utils.kb_helpers import KBAnalysisHelper, KBStorageHelper
from langflow.services.database.models.jobs.model import Job, JobStatus, JobType
from langflow.services.database.models.memory_base.model import (
    MemoryBase,
    MemoryBaseCreate,
    MemoryBaseSession,
    MemoryBaseSessionRead,
    MemoryBaseUpdate,
)
from langflow.services.database.models.message.model import MessageTable
from langflow.services.deps import get_job_service, get_task_service, session_scope
from langflow.services.memory_base.task import ingest_memory_task


class MemoryBaseService:
    """Service layer for MemoryBase CRUD and ingestion orchestration."""

    # ------------------------------------------------------------------ #
    #  CRUD                                                                 #
    # ------------------------------------------------------------------ #

    async def create(self, payload: MemoryBaseCreate, user_id: uuid.UUID) -> MemoryBase:
        async with session_scope() as db:
            mb = MemoryBase(**payload.model_dump(), user_id=user_id)
            db.add(mb)
            await db.commit()
            await db.refresh(mb)

        # Stamp is_memory_base: true on the KB metadata file immediately so that
        # the Knowledge Retrieval component filters it out from the first moment
        # this KB is designated as a Memory Base, before any sync has occurred.
        await self._stamp_memory_base_flag(mb)

        return mb

    async def _stamp_memory_base_flag(self, mb: MemoryBase) -> None:
        """Write is_memory_base: true into the KB's embedding_metadata.json.

        Best-effort: a missing or unwritable KB path is logged but not fatal —
        the flag will be set during the first successful ingestion sync instead.
        """
        import json

        try:
            kb_username = await self._resolve_kb_username_by_user_id(mb.user_id)
            kb_root = KBStorageHelper.get_root_path()
            if not kb_root:
                return
            kb_path = kb_root / kb_username / mb.kb_name
            if not kb_path.exists():
                return
            metadata = KBAnalysisHelper.get_metadata(kb_path, fast=True)
            if metadata.get("is_memory_base") is True:
                return  # Already stamped
            metadata["is_memory_base"] = True
            (kb_path / "embedding_metadata.json").write_text(json.dumps(metadata, indent=2))
        except Exception:
            await logger.awarning(
                "Could not stamp is_memory_base on KB '%s' — will be set at first sync.", mb.kb_name, exc_info=True
            )

    async def list_for_user(self, user_id: uuid.UUID) -> list[MemoryBase]:
        async with session_scope() as db:
            stmt = select(MemoryBase).where(MemoryBase.user_id == user_id)
            result = await db.exec(stmt)
            return list(result.all())

    async def get(self, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> MemoryBase | None:
        async with session_scope() as db:
            stmt = select(MemoryBase).where(MemoryBase.id == memory_base_id).where(MemoryBase.user_id == user_id)
            result = await db.exec(stmt)
            return result.first()

    async def update(
        self,
        memory_base_id: uuid.UUID,
        user_id: uuid.UUID,
        patch: MemoryBaseUpdate,
    ) -> MemoryBase | None:
        """Update mutable fields.

        Threshold changes take effect on the NEXT auto-capture trigger; any
        already-running ingestion task ignores the change (immutable args).
        """
        async with session_scope() as db:
            stmt = select(MemoryBase).where(MemoryBase.id == memory_base_id).where(MemoryBase.user_id == user_id)
            result = await db.exec(stmt)
            mb = result.first()
            if mb is None:
                return None
            for field, value in patch.model_dump(exclude_unset=True).items():
                setattr(mb, field, value)
            db.add(mb)
            await db.commit()
            await db.refresh(mb)
            return mb

    async def delete(self, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Delete a MemoryBase.

        Edge case: If a sync task is active, cancel it BEFORE committing the
        database deletion to avoid dangling background work.
        """
        async with session_scope() as db:
            stmt = select(MemoryBase).where(MemoryBase.id == memory_base_id).where(MemoryBase.user_id == user_id)
            result = await db.exec(stmt)
            mb = result.first()
            if mb is None:
                return False

            # Cancel any active ingestion jobs for this memory base
            await self._cancel_active_jobs(memory_base_id=memory_base_id, db=db)

            await db.delete(mb)
            await db.commit()
            return True

    # ------------------------------------------------------------------ #
    #  Sessions                                                             #
    # ------------------------------------------------------------------ #

    async def get_sessions(self, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> list[MemoryBaseSessionRead]:
        """Return all tracked sessions with pending message counts."""
        async with session_scope() as db:
            # Verify ownership
            mb = await self._get_mb_or_raise(db, memory_base_id, user_id)

            stmt = select(MemoryBaseSession).where(MemoryBaseSession.memory_base_id == memory_base_id)
            result = await db.exec(stmt)
            sessions = list(result.all())

            output: list[MemoryBaseSessionRead] = []
            for s in sessions:
                pending = await self._count_pending(db, mb, s)
                read = MemoryBaseSessionRead(
                    id=s.id,
                    memory_base_id=s.memory_base_id,
                    session_id=s.session_id,
                    cursor_id=s.cursor_id,
                    total_processed=s.total_processed,
                    last_sync_at=s.last_sync_at,
                    pending_count=pending,
                )
                output.append(read)
            return output

    # ------------------------------------------------------------------ #
    #  Ingestion                                                            #
    # ------------------------------------------------------------------ #

    async def trigger_ingestion(
        self,
        memory_base_id: uuid.UUID,
        user_id: uuid.UUID,
        session_id: str,
    ) -> str:
        """Manually trigger (or auto-trigger) an ingestion sync.

        Returns:
            job_id string for the newly created job.

        Raises:
            ValueError: If MemoryBase not found.
            RuntimeError: If a job is already active (caller should return 409).
        """
        async with session_scope() as db:
            mb = await self._get_mb_or_raise(db, memory_base_id, user_id)

            # Concurrent task prevention – one active job per (memory_base_id, session_id)
            if await self._has_active_job(db, memory_base_id, session_id):
                msg = f"Ingestion already in progress for memory_base={memory_base_id} session={session_id}"
                raise RuntimeError(msg)

            # Ensure a session record exists
            mbs = await self._get_or_create_session(db, memory_base_id, session_id)

            # Snapshot the cursor NOW (immutable arg for the task)
            cursor_id_snapshot = mbs.cursor_id

            kb_username = await self._resolve_kb_username(db, mb.user_id)
            embedding_provider, embedding_model = self._resolve_embedding(mb.kb_name, kb_username)

        # Create tracking job
        job_service = get_job_service()
        job_id = uuid.uuid4()
        await job_service.create_job(
            job_id=job_id,
            flow_id=mb.flow_id,
            job_type=JobType.INGESTION,
            asset_id=memory_base_id,
            asset_type="memory_base",
        )

        task_service = get_task_service()
        await task_service.fire_and_forget_task(
            job_service.execute_with_status,
            job_id=job_id,
            run_coro_func=ingest_memory_task,
            memory_base_id=memory_base_id,
            session_id=session_id,
            flow_id=mb.flow_id,
            kb_name=mb.kb_name,
            kb_username=kb_username,
            user_id=mb.user_id,
            embedding_provider=embedding_provider,
            embedding_model=embedding_model,
            cursor_id=cursor_id_snapshot,
            task_job_id=job_id,
            job_service=job_service,
        )

        return str(job_id)

    # ------------------------------------------------------------------ #
    #  Auto-capture hook (called from flow execution engine)               #
    # ------------------------------------------------------------------ #

    async def on_flow_output(
        self,
        flow_id: uuid.UUID,
        session_id: str,
        run_id: uuid.UUID | None,
    ) -> None:
        """Called after flow output messages are persisted.

        For every MemoryBase watching this flow with auto_capture=True:
        1. Ensure a MemoryBaseSession exists.
        2. Count pending output messages for the session.
        3. If count >= threshold, fire ingestion task.
        """
        async with session_scope() as db:
            stmt = (
                select(MemoryBase).where(MemoryBase.flow_id == flow_id).where(MemoryBase.auto_capture == True)  # noqa: E712
            )
            result = await db.exec(stmt)
            memory_bases = list(result.all())

        for mb in memory_bases:
            try:
                await self._maybe_trigger(mb=mb, session_id=session_id)
            except Exception:
                await logger.aerror(
                    "Auto-capture failed for memory_base=%s session=%s", mb.id, session_id, exc_info=True
                )

    async def _maybe_trigger(self, *, mb: MemoryBase, session_id: str) -> None:
        async with session_scope() as db:
            mbs = await self._get_or_create_session(db, mb.id, session_id)
            pending = await self._count_pending(db, mb, mbs)

            if pending < mb.threshold:
                return

            if await self._has_active_job(db, mb.id, session_id):
                await logger.adebug(
                    "Auto-capture: job already active for memory_base=%s session=%s – skipping.", mb.id, session_id
                )
                return

            cursor_id_snapshot = mbs.cursor_id
            kb_username = await self._resolve_kb_username(db, mb.user_id)

        embedding_provider, embedding_model = self._resolve_embedding(mb.kb_name, kb_username)

        job_service = get_job_service()
        job_id = uuid.uuid4()
        await job_service.create_job(
            job_id=job_id,
            flow_id=mb.flow_id,
            job_type=JobType.INGESTION,
            asset_id=mb.id,
            asset_type="memory_base",
        )

        task_service = get_task_service()
        await task_service.fire_and_forget_task(
            job_service.execute_with_status,
            job_id=job_id,
            run_coro_func=ingest_memory_task,
            memory_base_id=mb.id,
            session_id=session_id,
            flow_id=mb.flow_id,
            kb_name=mb.kb_name,
            kb_username=kb_username,
            embedding_provider=embedding_provider,
            embedding_model=embedding_model,
            cursor_id=cursor_id_snapshot,
            task_job_id=job_id,
            job_service=job_service,
        )

    # ------------------------------------------------------------------ #
    #  FS / Vector DB mismatch detection                                   #
    # ------------------------------------------------------------------ #

    async def check_mismatch(self, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Return True if metadata claims processed rows but vector store is empty.

        The UI should surface a "Mismatch Detected" warning and offer Regenerate.
        """
        async with session_scope() as db:
            mb = await self._get_mb_or_raise(db, memory_base_id, user_id)
            stmt = select(func.sum(MemoryBaseSession.total_processed)).where(
                MemoryBaseSession.memory_base_id == memory_base_id
            )
            result = await db.exec(stmt)
            total_processed: int = result.first() or 0

        if total_processed == 0:
            return False

        kb_username = await self._resolve_kb_username_by_user_id(user_id)
        kb_root = KBStorageHelper.get_root_path()
        if not kb_root:
            return False
        kb_path = kb_root / kb_username / mb.kb_name
        if not kb_path.exists():
            return True

        metadata = KBAnalysisHelper.get_metadata(kb_path, fast=True)
        return int(metadata.get("chunks", 0)) == 0

    async def regenerate(self, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> list[str]:
        """Reset all session cursors to None and re-trigger ingestion per session.

        Used to recover from FS / Vector DB mismatch (Chroma dir deleted externally).
        Returns list of newly created job IDs.
        """
        async with session_scope() as db:
            mb = await self._get_mb_or_raise(db, memory_base_id, user_id)

            stmt = select(MemoryBaseSession).where(MemoryBaseSession.memory_base_id == memory_base_id)
            result = await db.exec(stmt)
            sessions = list(result.all())

            for s in sessions:
                s.cursor_id = None
                db.add(s)
            await db.commit()

        job_ids: list[str] = []
        for s in sessions:
            try:
                jid = await self.trigger_ingestion(memory_base_id, user_id, s.session_id)
                job_ids.append(jid)
            except RuntimeError:
                await logger.awarning(
                    "Regenerate: active job exists for session %s – reset cursor but skipped trigger.", s.session_id
                )
        return job_ids

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _get_mb_or_raise(self, db: AsyncSession, memory_base_id: uuid.UUID, user_id: uuid.UUID) -> MemoryBase:
        stmt = select(MemoryBase).where(MemoryBase.id == memory_base_id).where(MemoryBase.user_id == user_id)
        result = await db.exec(stmt)
        mb = result.first()
        if mb is None:
            msg = f"MemoryBase {memory_base_id} not found"
            raise ValueError(msg)
        return mb

    async def _get_or_create_session(
        self, db: AsyncSession, memory_base_id: uuid.UUID, session_id: str
    ) -> MemoryBaseSession:
        stmt = (
            select(MemoryBaseSession)
            .where(MemoryBaseSession.memory_base_id == memory_base_id)
            .where(MemoryBaseSession.session_id == session_id)
        )
        result = await db.exec(stmt)
        mbs = result.first()
        if mbs is None:
            mbs = MemoryBaseSession(memory_base_id=memory_base_id, session_id=session_id)
            db.add(mbs)
            await db.commit()
            await db.refresh(mbs)
        return mbs

    async def _count_pending(self, db: AsyncSession, mb: MemoryBase, mbs: MemoryBaseSession) -> int:
        """Count is_output messages for this session that come after the cursor."""
        stmt = (
            select(func.count())
            .select_from(MessageTable)
            .where(MessageTable.flow_id == mb.flow_id)
            .where(MessageTable.session_id == mbs.session_id)
            .where(MessageTable.is_output == True)  # noqa: E712
        )
        if mbs.cursor_id is not None:
            cursor_stmt = select(MessageTable.timestamp).where(MessageTable.id == mbs.cursor_id)
            cursor_result = await db.exec(cursor_stmt)
            cursor_ts = cursor_result.first()
            if cursor_ts:
                stmt = stmt.where(col(MessageTable.timestamp) > cursor_ts)

        result = await db.exec(stmt)
        return result.one()

    async def _has_active_job(self, db: AsyncSession, memory_base_id: uuid.UUID, session_id: str) -> bool:
        """Check whether an ingestion job is already IN_PROGRESS for this (mb, session)."""
        # We store jobs with asset_id=memory_base_id and asset_type="memory_base".
        # session_id granularity is not tracked in the Job table; we use memory_base_id
        # as the granularity guard (one active job per MB per session pair is handled
        # by checking any active job for the asset_id).
        stmt = (
            select(func.count())
            .select_from(Job)
            .where(Job.asset_id == memory_base_id)
            .where(Job.asset_type == "memory_base")
            .where(Job.status == JobStatus.IN_PROGRESS)
        )
        result = await db.exec(stmt)
        return result.one() > 0

    async def _cancel_active_jobs(self, *, memory_base_id: uuid.UUID, db: AsyncSession) -> None:
        """Cancel all IN_PROGRESS or QUEUED jobs for this memory base."""
        stmt = (
            select(Job)
            .where(Job.asset_id == memory_base_id)
            .where(Job.asset_type == "memory_base")
            .where(col(Job.status).in_([JobStatus.IN_PROGRESS, JobStatus.QUEUED]))
        )
        result = await db.exec(stmt)
        active_jobs = list(result.all())

        task_service = get_task_service()
        job_service = get_job_service()
        for job in active_jobs:
            try:
                await task_service.revoke_task(job.job_id)
                await job_service.update_job_status(job.job_id, JobStatus.CANCELLED)
                await logger.ainfo("Cancelled job %s for memory_base %s", job.job_id, memory_base_id)
            except Exception:
                await logger.awarning(
                    "Could not cancel job %s for memory_base %s", job.job_id, memory_base_id, exc_info=True
                )

    async def _resolve_kb_username(self, db: AsyncSession, user_id: uuid.UUID) -> str:
        from langflow.services.database.models.user.model import User

        stmt = select(User.username).where(User.id == user_id)
        result = await db.exec(stmt)
        username = result.first()
        if not username:
            msg = f"User {user_id} not found"
            raise ValueError(msg)
        return username

    async def _resolve_kb_username_by_user_id(self, user_id: uuid.UUID) -> str:
        async with session_scope() as db:
            return await self._resolve_kb_username(db, user_id)

    def _resolve_embedding(self, kb_name: str, kb_username: str) -> tuple[str, str]:
        """Read embedding provider/model from KB metadata.json, with sane defaults."""
        kb_root = KBStorageHelper.get_root_path()
        if not kb_root:
            return "OpenAI", "text-embedding-3-small"
        kb_path: Path = kb_root / kb_username / kb_name
        metadata = KBAnalysisHelper.get_metadata(kb_path, fast=True)
        provider = metadata.get("embedding_provider") or "OpenAI"
        model = metadata.get("embedding_model") or "text-embedding-3-small"
        return provider, model
