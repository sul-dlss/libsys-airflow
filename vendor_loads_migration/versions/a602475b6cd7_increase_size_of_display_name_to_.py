"""Increase size of display_name to reflect existing organizations

Revision ID: a602475b6cd7
Revises: a54c17f1dd1b
Create Date: 2023-05-19 15:23:52.282107

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a602475b6cd7'
down_revision = 'a54c17f1dd1b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendors', 'display_name',
               existing_type=sa.VARCHAR(length=50),
               type_=sa.VARCHAR(length=120),
               existing_nullable=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendors', 'display_name',
            existing_type=sa.VARCHAR(length=120),
            type_=sa.VARCHAR(length=50),
            existing_nullable=False)
    # ### end Alembic commands ###
