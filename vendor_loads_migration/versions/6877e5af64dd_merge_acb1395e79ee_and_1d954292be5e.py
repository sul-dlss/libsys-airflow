"""merge acb1395e79ee and 1d954292be5e

Revision ID: 6877e5af64dd
Revises: 1d954292be5e, acb1395e79ee
Create Date: 2023-06-05 08:21:06.927579

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6877e5af64dd'
down_revision = ('1d954292be5e', 'acb1395e79ee')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
