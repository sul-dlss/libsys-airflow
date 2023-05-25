"""merge expected-load-time changes

Revision ID: 1d954292be5e
Revises: 2c3487e0cac6, a634337e2c85
Create Date: 2023-06-02 09:34:31.166291

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1d954292be5e'
down_revision = ('2c3487e0cac6', 'a634337e2c85')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
