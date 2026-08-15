"""add m4b columns to books

Revision ID: add_m4b_columns_to_books
Revises: add_sync_mode_column
Create Date: 2026-08-15 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_m4b_columns_to_books'
down_revision: Union[str, Sequence[str], None] = 'add_sync_mode_column'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('books')]

    if 'm4b_status' not in columns:
        op.add_column('books', sa.Column('m4b_status', sa.String(length=50), nullable=True, server_default='pending'))
    if 'm4b_progress' not in columns:
        op.add_column('books', sa.Column('m4b_progress', sa.Float(), nullable=True, server_default='0.0'))
    if 'm4b_output_file' not in columns:
        op.add_column('books', sa.Column('m4b_output_file', sa.String(length=500), nullable=True))
    if 'm4b_error' not in columns:
        op.add_column('books', sa.Column('m4b_error', sa.Text(), nullable=True))
    if 'm4b_updated_at' not in columns:
        op.add_column('books', sa.Column('m4b_updated_at', sa.Float(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('books', 'm4b_updated_at')
    op.drop_column('books', 'm4b_error')
    op.drop_column('books', 'm4b_output_file')
    op.drop_column('books', 'm4b_progress')
    op.drop_column('books', 'm4b_status')

