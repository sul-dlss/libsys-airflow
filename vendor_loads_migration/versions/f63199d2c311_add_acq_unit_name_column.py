"""Add acq unit name column

Revision ID: f63199d2c311
Revises: a602475b6cd7
Create Date: 2023-05-24 08:00:30.348237

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f63199d2c311'
down_revision = 'a602475b6cd7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('vendors', sa.Column('acquisitions_unit_name_from_folio', sa.String(length=36), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('vendors', 'acquisitions_unit_name_from_folio')
    # ### end Alembic commands ###