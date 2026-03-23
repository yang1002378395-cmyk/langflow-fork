"""merge_heads_for_memory_base

Revision ID: e1f2a3b4c5d6
Revises: 1cb603706752, 0e6138e7a0c2, d9a6ea21edcd, 2a5defa5ddc0, 4e5980a44eaa, d37bc4322900
Create Date: 2026-03-23 00:00:00.000000

Phase: EXPAND
"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: str | Sequence[str] | None = (
    "1cb603706752",
    "0e6138e7a0c2",
    "d9a6ea21edcd",
    "2a5defa5ddc0",
    "4e5980a44eaa",
    "d37bc4322900",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
