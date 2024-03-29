"""New status and expected execution column

Revision ID: 0ba6b75cc77a
Revises: 35ba6262f0a0
Create Date: 2023-05-11 15:05:45.148798

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0ba6b75cc77a'
down_revision = '35ba6262f0a0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('vendor_files', sa.Column('expected_execution', sa.Date(), nullable=True))
    op.execute("""UPDATE vendor_files SET expected_execution=subquery.created 
    FROM (SELECT created FROM vendor_files) AS subquery;""")
    op.alter_column("vendor_files", "expected_execution", nullable=False)
    op.execute(f"ALTER TYPE filestatus ADD VALUE 'purged'")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('vendor_files', 'expected_execution')
    op.execute("""DELETE FROM pg_enum
    WHERE enumlabel='purged'
    AND enumtypid= (
      SELECT oid FROM pg_type WHERE typname='filestatus'
    )""")
    # ### end Alembic commands ###
