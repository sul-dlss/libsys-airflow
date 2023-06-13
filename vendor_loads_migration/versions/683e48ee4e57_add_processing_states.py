"""Add processing states

Revision ID: 683e48ee4e57
Revises: b09e821d201c
Create Date: 2023-06-12 12:30:04.294730

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '683e48ee4e57'
down_revision = 'b09e821d201c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(f"ALTER TYPE filestatus ADD VALUE 'processing'")
    op.execute(f"ALTER TYPE filestatus ADD VALUE 'processed'")
    op.execute(f"ALTER TYPE filestatus ADD VALUE 'processing_error'")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""DELETE FROM pg_enum
    WHERE enumlabel='processing'
    AND enumtypid= (
      SELECT oid FROM pg_type WHERE typname='filestatus'
    )""")
    op.execute("""DELETE FROM pg_enum
    WHERE enumlabel='processed'
    AND enumtypid= (
      SELECT oid FROM pg_type WHERE typname='filestatus'
    )""")
    op.execute("""DELETE FROM pg_enum
    WHERE enumlabel='processing_error'
    AND enumtypid= (
      SELECT oid FROM pg_type WHERE typname='filestatus'
    )""")
    # ### end Alembic commands ###