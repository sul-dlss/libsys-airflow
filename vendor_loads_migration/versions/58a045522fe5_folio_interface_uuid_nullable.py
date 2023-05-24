"""folio_interface_uuid nullable

Revision ID: 58a045522fe5
Revises: a602475b6cd7
Create Date: 2023-05-23 15:52:45.813722

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '58a045522fe5'
down_revision = 'a602475b6cd7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendor_interfaces', 'folio_interface_uuid',
               existing_type=sa.VARCHAR(length=36),
               nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendor_interfaces', 'folio_interface_uuid',
               existing_type=sa.VARCHAR(length=36),
               nullable=False)
    # ### end Alembic commands ###
