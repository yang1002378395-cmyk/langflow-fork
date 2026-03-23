from datetime import datetime, timezone
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, ForeignKey, Index, UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel


class MemoryBaseBase(SQLModel):
    name: str = Field(index=False)
    flow_id: UUID = Field(index=True)
    user_id: UUID = Field(index=True)
    threshold: int = Field(default=50)
    kb_name: str
    auto_capture: bool = Field(default=True)


class MemoryBase(MemoryBaseBase, table=True):  # type: ignore[call-arg]
    __tablename__ = "memory_base"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    sessions: list["MemoryBaseSession"] = Relationship(
        back_populates="memory_base",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class MemoryBaseCreate(MemoryBaseBase):
    pass


class MemoryBaseUpdate(SQLModel):
    name: str | None = None
    threshold: int | None = None
    kb_name: str | None = None
    auto_capture: bool | None = None


class MemoryBaseRead(MemoryBaseBase):
    id: UUID
    created_at: datetime


class MemoryBaseSessionBase(SQLModel):
    """Fields shared between the table class and response schemas."""

    session_id: str = Field(index=True)
    cursor_id: UUID | None = Field(default=None)
    total_processed: int = Field(default=0)
    last_sync_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )


class MemoryBaseSession(MemoryBaseSessionBase, table=True):  # type: ignore[call-arg]
    __tablename__ = "memory_base_session"

    __table_args__ = (
        UniqueConstraint("memory_base_id", "session_id", name="uq_memory_base_session"),
        Index("ix_memory_base_session_lookup", "memory_base_id", "session_id"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)

    # FK defined via sa_column so Alembic sees the same shape as the migration:
    # inline ForeignKey on the column with ondelete="CASCADE".
    # This matches the pattern used by the File model (ForeignKey on sa_column).
    memory_base_id: UUID = Field(
        sa_column=Column(
            sa.Uuid(),
            ForeignKey("memory_base.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )

    memory_base: MemoryBase = Relationship(back_populates="sessions")


class MemoryBaseSessionRead(MemoryBaseSessionBase):
    id: UUID
    memory_base_id: UUID  # Explicit — not in base to keep base free of DB-layer FK
    pending_count: int = Field(default=0)
