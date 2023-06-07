"""Merge heads

Revision ID: c20d54b0d230
Revises: 310b31225b9d, 6877e5af64dd
Create Date: 2023-06-05 17:33:03.747657

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c20d54b0d230'
down_revision = ('310b31225b9d', '6877e5af64dd')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
