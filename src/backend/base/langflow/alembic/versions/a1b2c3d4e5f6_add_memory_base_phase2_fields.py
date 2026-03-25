"""add_memory_base_phase2_fields

Adds Phase II fields to the memory_base table:
  - embedding_model  (VARCHAR, not null, default "")
  - preprocessing    (BOOLEAN, not null, default false)
  - preproc_model    (VARCHAR, nullable)
  - preproc_instructions (VARCHAR, nullable)

Revision ID: a1b2c3d4e5f6
Revises: f6e5d4c3b2a1
Create Date: 2026-03-25 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from langflow.utils import migration

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "f6e5d4c3b2a1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    conn = op.get_bind()

    with op.batch_alter_table("memory_base", schema=None) as batch_op:
        if not migration.column_exists("memory_base", "embedding_model", conn):
            batch_op.add_column(
                sa.Column("embedding_model", sa.String(), nullable=False, server_default=sa.text("''"))
            )
        if not migration.column_exists("memory_base", "preprocessing", conn):
            batch_op.add_column(
                sa.Column("preprocessing", sa.Boolean(), nullable=False, server_default=sa.text("false"))
            )
        if not migration.column_exists("memory_base", "preproc_model", conn):
            batch_op.add_column(sa.Column("preproc_model", sa.String(), nullable=True))
        if not migration.column_exists("memory_base", "preproc_instructions", conn):
            batch_op.add_column(sa.Column("preproc_instructions", sa.String(), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()

    with op.batch_alter_table("memory_base", schema=None) as batch_op:
        if migration.column_exists("memory_base", "preproc_instructions", conn):
            batch_op.drop_column("preproc_instructions")
        if migration.column_exists("memory_base", "preproc_model", conn):
            batch_op.drop_column("preproc_model")
        if migration.column_exists("memory_base", "preprocessing", conn):
            batch_op.drop_column("preprocessing")
        if migration.column_exists("memory_base", "embedding_model", conn):
            batch_op.drop_column("embedding_model")
