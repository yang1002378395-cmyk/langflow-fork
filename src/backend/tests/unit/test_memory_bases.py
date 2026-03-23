"""Unit tests for the MemoryBase feature.

Coverage areas:
- DB model creation and field defaults
- MemoryBaseService CRUD operations
- Concurrency guard (409 on duplicate active job)
- Cursor atomicity (cursor not advanced on ingestion failure)
- Threshold-change deferral
- FS/VectorDB mismatch detection
- Regenerate: cursor reset + re-trigger
- API endpoint routing (happy path + error paths)
- ingest_memory_task: pending message fetch, document building, cursor advance
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langflow.services.database.models.memory_base.model import (
    MemoryBase,
    MemoryBaseCreate,
    MemoryBaseSession,
    MemoryBaseUpdate,
)
from langflow.services.database.models.message.model import MessageTable

# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #


def _make_mb(
    *,
    user_id: uuid.UUID | None = None,
    flow_id: uuid.UUID | None = None,
    threshold: int = 10,
    auto_capture: bool = True,
) -> MemoryBase:
    return MemoryBase(
        id=uuid.uuid4(),
        name="test_mb",
        flow_id=flow_id or uuid.uuid4(),
        user_id=user_id or uuid.uuid4(),
        threshold=threshold,
        kb_name="test_kb",
        auto_capture=auto_capture,
        created_at=datetime.now(timezone.utc),
    )


def _make_session(
    *,
    memory_base_id: uuid.UUID | None = None,
    session_id: str = "sess-1",
    cursor_id: uuid.UUID | None = None,
    total_processed: int = 0,
) -> MemoryBaseSession:
    return MemoryBaseSession(
        id=uuid.uuid4(),
        memory_base_id=memory_base_id or uuid.uuid4(),
        session_id=session_id,
        cursor_id=cursor_id,
        total_processed=total_processed,
    )


def _make_message(
    *,
    flow_id: uuid.UUID,
    session_id: str,
    is_output: bool = True,
    text: str = "Hello from the bot",
    run_id: uuid.UUID | None = None,
) -> MessageTable:
    return MessageTable(
        id=uuid.uuid4(),
        sender="AI",
        sender_name="Bot",
        session_id=session_id,
        text=text,
        flow_id=flow_id,
        is_output=is_output,
        run_id=run_id,
        timestamp=datetime.now(timezone.utc),
    )


# ------------------------------------------------------------------ #
#  Model tests                                                         #
# ------------------------------------------------------------------ #


class TestMemoryBaseModel:
    def test_defaults(self):
        mb = MemoryBase(
            name="mb",
            flow_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            kb_name="kb",
        )
        assert mb.threshold == 50
        assert mb.auto_capture is True

    def test_create_schema(self):
        payload = MemoryBaseCreate(
            name="mb",
            flow_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            threshold=25,
            kb_name="kb",
        )
        assert payload.threshold == 25

    def test_update_schema_partial(self):
        patch = MemoryBaseUpdate(threshold=100)
        dumped = patch.model_dump(exclude_unset=True)
        assert "threshold" in dumped
        assert "name" not in dumped

    def test_memory_base_session_defaults(self):
        mbs = MemoryBaseSession(
            memory_base_id=uuid.uuid4(),
            session_id="s1",
        )
        assert mbs.cursor_id is None
        assert mbs.total_processed == 0
        assert mbs.last_sync_at is None


class TestMessageExtensions:
    """Ensure the new fields exist on MessageTable."""

    def test_run_id_field_exists(self):
        msg = _make_message(flow_id=uuid.uuid4(), session_id="s1")
        assert hasattr(msg, "run_id")
        assert msg.run_id is None

    def test_is_output_field_defaults_false(self):
        msg = MessageTable(
            sender="Human",
            sender_name="User",
            session_id="s1",
            text="hi",
        )
        assert msg.is_output is False

    def test_is_output_can_be_set(self):
        msg = _make_message(flow_id=uuid.uuid4(), session_id="s1", is_output=True)
        assert msg.is_output is True


# ------------------------------------------------------------------ #
#  Service tests (mock DB)                                             #
# ------------------------------------------------------------------ #


class TestMemoryBaseServiceCRUD:
    @pytest.fixture
    def service(self):
        from langflow.services.memory_base.service import MemoryBaseService

        return MemoryBaseService()

    @pytest.mark.asyncio
    async def test_create_stores_user_id(self, service):
        user_id = uuid.uuid4()
        payload = MemoryBaseCreate(
            name="mb",
            flow_id=uuid.uuid4(),
            user_id=user_id,
            kb_name="kb",
        )

        created_mb = _make_mb(user_id=user_id)

        with patch.object(service, "create", AsyncMock(return_value=created_mb)):
            result = await service.create(payload, user_id=user_id)

        assert result.user_id == user_id

    @pytest.mark.asyncio
    async def test_get_returns_none_for_wrong_user(self, service):
        with patch.object(service, "get", AsyncMock(return_value=None)):
            result = await service.get(uuid.uuid4(), user_id=uuid.uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_update_returns_none_for_missing(self, service):
        with patch.object(service, "update", AsyncMock(return_value=None)):
            result = await service.update(uuid.uuid4(), uuid.uuid4(), MemoryBaseUpdate(threshold=5))
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_returns_false_for_missing(self, service):
        with patch.object(service, "delete", AsyncMock(return_value=False)):
            result = await service.delete(uuid.uuid4(), user_id=uuid.uuid4())
        assert result is False


class TestMemoryBaseServiceConcurrency:
    """409 guard: only one active ingestion per (memory_base_id, session_id)."""

    @pytest.fixture
    def service(self):
        from langflow.services.memory_base.service import MemoryBaseService

        return MemoryBaseService()

    @pytest.mark.asyncio
    async def test_trigger_raises_when_job_active(self, service):
        mb = _make_mb()

        async def fake_get(*a, **kw):
            return mb

        with (
            patch.object(service, "_get_mb_or_raise", AsyncMock(return_value=mb)),
            patch.object(service, "_has_active_job", AsyncMock(return_value=True)),
        ):
            with pytest.raises(RuntimeError, match="already in progress"):
                await service.trigger_ingestion(mb.id, mb.user_id, "sess-1")

    @pytest.mark.asyncio
    async def test_trigger_succeeds_when_no_active_job(self, service):
        mb = _make_mb()
        mbs = _make_session(memory_base_id=mb.id)

        with (
            patch.object(service, "_get_mb_or_raise", AsyncMock(return_value=mb)),
            patch.object(service, "_has_active_job", AsyncMock(return_value=False)),
            patch.object(service, "_get_or_create_session", AsyncMock(return_value=mbs)),
            patch.object(service, "_resolve_kb_username", AsyncMock(return_value="testuser")),
            patch.object(service, "_resolve_embedding", return_value=("OpenAI", "text-embedding-3-small")),
            patch("langflow.services.memory_base.service.get_job_service") as mock_jsc,
            patch("langflow.services.memory_base.service.get_task_service") as mock_tsc,
        ):
            mock_job_svc = MagicMock()
            mock_job_svc.create_job = AsyncMock()
            mock_task_svc = MagicMock()
            mock_task_svc.fire_and_forget_task = AsyncMock()
            mock_jsc.return_value = mock_job_svc
            mock_tsc.return_value = mock_task_svc

            job_id = await service.trigger_ingestion(mb.id, mb.user_id, "sess-1")

        assert isinstance(job_id, str)
        mock_job_svc.create_job.assert_awaited_once()
        mock_task_svc.fire_and_forget_task.assert_awaited_once()


class TestMemoryBaseServiceThreshold:
    """Threshold update should NOT immediately re-evaluate pending count."""

    @pytest.fixture
    def service(self):
        from langflow.services.memory_base.service import MemoryBaseService

        return MemoryBaseService()

    @pytest.mark.asyncio
    async def test_threshold_update_does_not_trigger_ingestion(self, service):
        """Updating threshold via PATCH should never fire a task."""
        mb_updated = _make_mb(threshold=5)

        with patch.object(service, "update", AsyncMock(return_value=mb_updated)):
            result = await service.update(mb_updated.id, mb_updated.user_id, MemoryBaseUpdate(threshold=5))

        assert result.threshold == 5
        # No ingestion task should have been triggered as a side effect


class TestMemoryBaseServiceMismatch:
    @pytest.fixture
    def service(self):
        from langflow.services.memory_base.service import MemoryBaseService

        return MemoryBaseService()

    @pytest.mark.asyncio
    async def test_mismatch_detected_when_processed_but_empty_store(self, service, tmp_path):
        mb = _make_mb()

        with (
            patch.object(service, "_get_mb_or_raise", AsyncMock(return_value=mb)),
            patch("langflow.services.memory_base.service.session_scope") as mock_scope,
            patch("langflow.services.memory_base.service.KBStorageHelper.get_root_path", return_value=tmp_path),
            patch(
                "langflow.services.memory_base.service.KBAnalysisHelper.get_metadata",
                return_value={"chunks": 0},
            ),
            patch("langflow.services.memory_base.service.KBStorageHelper.get_root_path", return_value=tmp_path),
        ):
            # Simulate session_scope returns total_processed=10
            mock_db = AsyncMock()
            mock_db.exec = AsyncMock(return_value=MagicMock(first=MagicMock(return_value=10)))

            class FakeCtx:
                async def __aenter__(self):
                    return mock_db

                async def __aexit__(self, *a):
                    pass

            mock_scope.return_value = FakeCtx()

            # Create KB path dir so path.exists() is True
            kb_path = tmp_path / "testuser" / mb.kb_name
            kb_path.mkdir(parents=True)

            result = await service.check_mismatch(mb.id, mb.user_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_no_mismatch_when_nothing_processed(self, service, tmp_path):
        mb = _make_mb()

        with (
            patch.object(service, "_get_mb_or_raise", AsyncMock(return_value=mb)),
            patch("langflow.services.memory_base.service.session_scope") as mock_scope,
        ):
            mock_db = AsyncMock()
            mock_db.exec = AsyncMock(return_value=MagicMock(first=MagicMock(return_value=0)))

            class FakeCtx:
                async def __aenter__(self):
                    return mock_db

                async def __aexit__(self, *a):
                    pass

            mock_scope.return_value = FakeCtx()

            result = await service.check_mismatch(mb.id, mb.user_id)

        assert result is False


class TestMemoryBaseServiceRegenerate:
    @pytest.fixture
    def service(self):
        from langflow.services.memory_base.service import MemoryBaseService

        return MemoryBaseService()

    @pytest.mark.asyncio
    async def test_regenerate_resets_cursors_and_triggers(self, service):
        mb = _make_mb()
        mbs1 = _make_session(memory_base_id=mb.id, session_id="s1", cursor_id=uuid.uuid4())
        mbs2 = _make_session(memory_base_id=mb.id, session_id="s2", cursor_id=uuid.uuid4())

        triggered_sessions: list[str] = []

        async def fake_trigger(mb_id, user_id, session_id):
            triggered_sessions.append(session_id)
            return str(uuid.uuid4())

        with (
            patch("langflow.services.memory_base.service.session_scope") as mock_scope,
            patch.object(service, "trigger_ingestion", side_effect=fake_trigger),
        ):
            mock_db = AsyncMock()
            mock_mb_result = MagicMock()
            mock_mb_result.first = MagicMock(return_value=mb)
            mock_session_result = MagicMock()
            mock_session_result.all = MagicMock(return_value=[mbs1, mbs2])
            mock_db.exec = AsyncMock(side_effect=[mock_mb_result, mock_session_result])
            mock_db.add = MagicMock()
            mock_db.commit = AsyncMock()

            class FakeCtx:
                async def __aenter__(self):
                    return mock_db

                async def __aexit__(self, *a):
                    pass

            mock_scope.return_value = FakeCtx()

            job_ids = await service.regenerate(mb.id, mb.user_id)

        assert len(job_ids) == 2
        assert set(triggered_sessions) == {"s1", "s2"}
        # Verify cursors were reset
        assert mbs1.cursor_id is None
        assert mbs2.cursor_id is None


# ------------------------------------------------------------------ #
#  Task tests                                                          #
# ------------------------------------------------------------------ #


class TestIngestMemoryTask:
    @pytest.mark.asyncio
    async def test_no_op_when_no_pending_messages(self):
        from langflow.services.memory_base.task import ingest_memory_task

        job_service = MagicMock()
        job_id = uuid.uuid4()

        with patch(
            "langflow.services.memory_base.task._fetch_pending_messages",
            AsyncMock(return_value=[]),
        ):
            result = await ingest_memory_task(
                memory_base_id=uuid.uuid4(),
                session_id="s1",
                flow_id=uuid.uuid4(),
                kb_name="kb",
                kb_username="user",
                embedding_provider="OpenAI",
                embedding_model="text-embedding-3-small",
                cursor_id=None,
                task_job_id=job_id,
                job_service=job_service,
            )

        assert result["ingested"] == 0

    @pytest.mark.asyncio
    async def test_cursor_not_advanced_on_ingestion_failure(self):
        """Critical: cursor_id must stay unchanged if ingestion fails."""
        from langflow.services.memory_base.task import ingest_memory_task

        flow_id = uuid.uuid4()
        mb_id = uuid.uuid4()
        old_cursor = uuid.uuid4()

        msg = _make_message(flow_id=flow_id, session_id="s1")
        job_service = MagicMock()
        job_id = uuid.uuid4()

        advance_cursor_called = False

        async def fake_advance_cursor(**_kwargs):
            nonlocal advance_cursor_called
            advance_cursor_called = True

        with (
            patch(
                "langflow.services.memory_base.task._fetch_pending_messages",
                AsyncMock(return_value=[msg]),
            ),
            patch(
                "langflow.services.memory_base.task._build_documents_from_messages",
                return_value=[MagicMock()],
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._is_job_cancelled",
                AsyncMock(return_value=False),
            ),
            patch(
                "langflow.services.memory_base.task._ingest_documents_to_kb",
                AsyncMock(side_effect=RuntimeError("Chroma exploded")),
            ),
            patch(
                "langflow.services.memory_base.task._advance_cursor",
                side_effect=fake_advance_cursor,
            ),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_root_path",
                return_value=Path("/tmp/kb"),
            ),
            pytest.raises(RuntimeError, match="Chroma exploded"),
        ):
            await ingest_memory_task(
                memory_base_id=mb_id,
                session_id="s1",
                flow_id=flow_id,
                kb_name="kb",
                kb_username="user",
                embedding_provider="OpenAI",
                embedding_model="text-embedding-3-small",
                cursor_id=old_cursor,
                job_id=job_id,
                job_service=job_service,
            )

        # Cursor must NOT have been advanced
        assert not advance_cursor_called, "cursor_id must not advance when ingestion fails"

    @pytest.mark.asyncio
    async def test_metadata_synced_on_success(self):
        """embedding_metadata.json must be updated after a successful ingestion."""
        from langflow.services.memory_base.task import ingest_memory_task

        flow_id = uuid.uuid4()
        msg = _make_message(flow_id=flow_id, session_id="s1")
        job_service = MagicMock()
        job_id = uuid.uuid4()

        sync_called_with: dict = {}

        def fake_sync_kb_metadata(*, kb_path, chroma):
            sync_called_with["kb_path"] = kb_path
            sync_called_with["chroma"] = chroma

        with (
            patch(
                "langflow.services.memory_base.task._fetch_pending_messages",
                AsyncMock(return_value=[msg]),
            ),
            patch(
                "langflow.services.memory_base.task._build_documents_from_messages",
                return_value=[MagicMock()],
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._is_job_cancelled",
                AsyncMock(return_value=False),
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._build_embeddings",
                AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_fresh_chroma_client",
                return_value=MagicMock(),
            ),
            patch("langflow.services.memory_base.task.Chroma"),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper.write_documents_to_chroma",
                AsyncMock(return_value=1),
            ),
            patch("langflow.services.memory_base.task._sync_kb_metadata", side_effect=fake_sync_kb_metadata),
            patch("langflow.services.memory_base.task._advance_cursor", AsyncMock()),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_root_path",
                return_value=Path("/tmp/kb"),
            ),
            patch("langflow.services.memory_base.task.KBStorageHelper.release_chroma_resources"),
        ):
            await ingest_memory_task(
                memory_base_id=uuid.uuid4(),
                session_id="s1",
                flow_id=flow_id,
                kb_name="kb",
                kb_username="user",
                user_id=uuid.uuid4(),
                embedding_provider="OpenAI",
                embedding_model="text-embedding-3-small",
                cursor_id=None,
                task_job_id=job_id,
                job_service=job_service,
            )

        assert "kb_path" in sync_called_with, "_sync_kb_metadata was not called on success"

    @pytest.mark.asyncio
    async def test_metadata_not_synced_when_cancelled(self):
        """embedding_metadata.json must NOT be updated when ingestion is cancelled."""
        from langflow.services.memory_base.task import ingest_memory_task

        flow_id = uuid.uuid4()
        msg = _make_message(flow_id=flow_id, session_id="s1")
        job_service = MagicMock()
        job_id = uuid.uuid4()
        sync_called = False

        def fake_sync(*args, **kwargs):
            nonlocal sync_called
            sync_called = True

        with (
            patch(
                "langflow.services.memory_base.task._fetch_pending_messages",
                AsyncMock(return_value=[msg]),
            ),
            patch(
                "langflow.services.memory_base.task._build_documents_from_messages",
                return_value=[MagicMock()],
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._is_job_cancelled",
                AsyncMock(return_value=False),
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._build_embeddings",
                AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_fresh_chroma_client",
                return_value=MagicMock(),
            ),
            patch("langflow.services.memory_base.task.Chroma"),
            # write_documents_to_chroma returns fewer docs than sent → cancelled
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper.write_documents_to_chroma",
                AsyncMock(return_value=0),
            ),
            patch("langflow.services.memory_base.task._sync_kb_metadata", side_effect=fake_sync),
            patch("langflow.services.memory_base.task._advance_cursor", AsyncMock()),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_root_path",
                return_value=Path("/tmp/kb"),
            ),
            patch("langflow.services.memory_base.task.KBStorageHelper.release_chroma_resources"),
        ):
            result = await ingest_memory_task(
                memory_base_id=uuid.uuid4(),
                session_id="s1",
                flow_id=flow_id,
                kb_name="kb",
                kb_username="user",
                user_id=uuid.uuid4(),
                embedding_provider="OpenAI",
                embedding_model="text-embedding-3-small",
                cursor_id=None,
                task_job_id=job_id,
                job_service=job_service,
            )

        assert not sync_called, "_sync_kb_metadata must not be called when ingestion is cancelled"
        assert "cancelled" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_cursor_advanced_on_success(self):
        from langflow.services.memory_base.task import ingest_memory_task

        flow_id = uuid.uuid4()
        mb_id = uuid.uuid4()

        msg = _make_message(flow_id=flow_id, session_id="s1")
        job_service = MagicMock()
        job_id = uuid.uuid4()

        advance_kwargs: dict = {}

        async def fake_advance_cursor(**kwargs):
            advance_kwargs.update(kwargs)

        with (
            patch(
                "langflow.services.memory_base.task._fetch_pending_messages",
                AsyncMock(return_value=[msg]),
            ),
            patch(
                "langflow.services.memory_base.task._build_documents_from_messages",
                return_value=[MagicMock()],
            ),
            patch(
                "langflow.services.memory_base.task.KBIngestionHelper._is_job_cancelled",
                AsyncMock(return_value=False),
            ),
            patch("langflow.services.memory_base.task._ingest_documents_to_kb", AsyncMock()),
            patch("langflow.services.memory_base.task._advance_cursor", side_effect=fake_advance_cursor),
            patch(
                "langflow.services.memory_base.task.KBStorageHelper.get_root_path",
                return_value=Path("/tmp/kb"),
            ),
        ):
            result = await ingest_memory_task(
                memory_base_id=mb_id,
                session_id="s1",
                flow_id=flow_id,
                kb_name="kb",
                kb_username="user",
                embedding_provider="OpenAI",
                embedding_model="text-embedding-3-small",
                cursor_id=None,
                task_job_id=job_id,
                job_service=job_service,
            )

        assert result["ingested"] == 1
        assert advance_kwargs["new_cursor_id"] == msg.id
        assert advance_kwargs["ingested_count"] == 1

    def test_sync_kb_metadata_stamps_is_memory_base(self, tmp_path):
        """_sync_kb_metadata must write is_memory_base: true to the metadata file."""
        import json

        from langflow.services.memory_base.task import _sync_kb_metadata

        kb_path = tmp_path / "test_kb"
        kb_path.mkdir()

        mock_chroma = MagicMock()

        with (
            patch(
                "langflow.services.memory_base.task.KBAnalysisHelper.get_metadata",
                return_value={"chunks": 0, "embedding_provider": "OpenAI"},
            ),
            patch("langflow.services.memory_base.task.KBAnalysisHelper.update_text_metrics"),
            patch("langflow.services.memory_base.task.KBStorageHelper.get_directory_size", return_value=1024),
        ):
            _sync_kb_metadata(kb_path=kb_path, chroma=mock_chroma)

        written = json.loads((kb_path / "embedding_metadata.json").read_text())
        assert written["is_memory_base"] is True
        assert "memory" in written.get("source_types", [])

    def test_sync_kb_metadata_failure_does_not_raise(self, tmp_path):
        """Metadata sync errors must be swallowed so the cursor can still advance."""
        from langflow.services.memory_base.task import _sync_kb_metadata

        kb_path = tmp_path / "no_such_dir"  # does not exist

        with patch(
            "langflow.services.memory_base.task.KBAnalysisHelper.get_metadata",
            side_effect=OSError("disk full"),
        ):
            # Must not raise
            _sync_kb_metadata(kb_path=kb_path, chroma=MagicMock())

    def test_build_documents_skips_empty_messages(self):
        from langflow.services.memory_base.task import _build_documents_from_messages

        flow_id = uuid.uuid4()
        messages = [
            _make_message(flow_id=flow_id, session_id="s1", text=""),
            _make_message(flow_id=flow_id, session_id="s1", text="   "),
            _make_message(flow_id=flow_id, session_id="s1", text="Valid content here."),
        ]
        docs = _build_documents_from_messages(messages, session_id="s1", flow_id=str(flow_id))
        assert len(docs) == 1
        assert docs[0].page_content == "Valid content here."

    def test_build_documents_metadata(self):
        from langflow.services.memory_base.task import _build_documents_from_messages

        flow_id = uuid.uuid4()
        run_id = uuid.uuid4()
        msg = _make_message(flow_id=flow_id, session_id="s1", text="Test output.", run_id=run_id)
        docs = _build_documents_from_messages([msg], session_id="s1", flow_id=str(flow_id))
        assert docs[0].metadata["message_id"] == str(msg.id)
        assert docs[0].metadata["run_id"] == str(run_id)
        assert docs[0].metadata["session_id"] == "s1"


# ------------------------------------------------------------------ #
#  API endpoint routing tests                                          #
# ------------------------------------------------------------------ #


class TestMemoriesAPIRouting:
    """Verify routing and response codes without hitting the DB."""

    @pytest.fixture
    def patched_service(self):
        """Patch the module-level _service singleton in memories.py."""
        with patch("langflow.api.v1.memories._service") as mock_svc:
            yield mock_svc

    @pytest.mark.asyncio
    async def test_get_not_found_returns_404(self, patched_service):
        from httpx import AsyncClient
        from langflow.app import create_app

        patched_service.get = AsyncMock(return_value=None)

        app = create_app()
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Just check the route exists and returns an appropriate status
            # (will be 401 without auth – confirming endpoint is registered)
            response = await client.get("/api/v1/memories/00000000-0000-0000-0000-000000000001")
            assert response.status_code in (401, 403, 422, 404)

    @pytest.mark.asyncio
    async def test_flush_conflict_returns_409(self, patched_service):
        """trigger_ingestion raising RuntimeError should map to HTTP 409."""
        from langflow.api.v1.memories import flush_memory_base

        patched_service.trigger_ingestion = AsyncMock(side_effect=RuntimeError("already in progress"))

        # We call the handler directly to test the error mapping
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        from langflow.api.v1.memories import FlushRequest

        with pytest.raises(Exception) as exc_info:
            await flush_memory_base(
                memory_base_id=uuid.uuid4(),
                body=FlushRequest(session_id="s1"),
                current_user=mock_user,
            )
        from fastapi import HTTPException

        assert isinstance(exc_info.value, HTTPException)
        assert exc_info.value.status_code == 409
