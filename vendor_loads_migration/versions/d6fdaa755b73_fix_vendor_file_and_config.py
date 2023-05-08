"""fix-vendor-file-and-config

Revision ID: d6fdaa755b73
Revises: 3327b26c27ad, 5d7df5e8f5d7
Create Date: 2023-05-08 17:52:18.196614

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd6fdaa755b73'
down_revision = ('3327b26c27ad', '5d7df5e8f5d7')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
