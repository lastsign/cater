"""unique content_hash

Revision ID: a1b2c3d4e5f6
Revises: 27bd2384675c
Create Date: 2026-05-26 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "27bd2384675c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("ix_content_content_hash", table_name="content")
    op.create_index(
        "ix_content_content_hash",
        "content",
        ["content_hash"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_content_content_hash", table_name="content")
    op.create_index(
        "ix_content_content_hash",
        "content",
        ["content_hash"],
        unique=False,
    )
