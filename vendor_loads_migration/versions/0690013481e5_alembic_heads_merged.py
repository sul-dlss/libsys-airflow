"""alembic heads merged

Revision ID: 0690013481e5
Revises: 3327b26c27ad, 5d7df5e8f5d7
Create Date: 2023-05-08 17:10:03.263661

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0690013481e5'
down_revision = ('3327b26c27ad', '5d7df5e8f5d7')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
