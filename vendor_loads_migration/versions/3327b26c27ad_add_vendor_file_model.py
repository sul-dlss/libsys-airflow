"""add vendor_file model

Revision ID: 3327b26c27ad
Revises: 55557ab268c9
Create Date: 2023-05-05 12:11:19.976710

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3327b26c27ad'
down_revision = '55557ab268c9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('vendor_files',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=False),
    sa.Column('vendor_interface_id', sa.Integer(), nullable=True),
    sa.Column('vendor_filename', sa.String(length=250), nullable=False),
    sa.Column('filesize', sa.Integer(), nullable=False),
    sa.Column('vendor_timestamp', sa.DateTime(), nullable=True),
    sa.Column('loaded_timestamp', sa.DateTime(), nullable=True),
    sa.Column('status', sa.Enum('not_fetched', 'fetching_error', 'fetched', 'loading', 'loading_error', 'loaded', name='filestatus'), server_default='not_fetched', nullable=False),
    sa.ForeignKeyConstraint(['vendor_interface_id'], ['vendor_interfaces.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('vendor_files')
    # ### end Alembic commands ###