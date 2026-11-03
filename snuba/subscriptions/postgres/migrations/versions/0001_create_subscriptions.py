"""Create the subscriptions table.

Revision ID: 0001
Revises:
Create Date: 2026-10-16

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column("entity", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("data", postgresql.JSONB(), nullable=False),
    )
    op.create_index(
        "subscriptions_entity_partition_idx",
        "subscriptions",
        ["entity", "partition"],
    )


def downgrade() -> None:
    op.drop_index("subscriptions_entity_partition_idx", table_name="subscriptions")
    op.drop_table("subscriptions")
