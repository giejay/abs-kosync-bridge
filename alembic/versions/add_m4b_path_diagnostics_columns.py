"""add m4b path diagnostics columns

Revision ID: add_m4b_path_diagnostics_columns
Revises: add_m4b_columns_to_books
Create Date: 2026-08-15 12:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_m4b_path_diagnostics_columns'
down_revision: Union[str, Sequence[str], None] = 'add_m4b_columns_to_books'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('books')]

    if 'm4b_source_paths' not in columns:
        op.add_column('books', sa.Column('m4b_source_paths', sa.Text(), nullable=True))
    if 'm4b_path_strategy' not in columns:
        op.add_column('books', sa.Column('m4b_path_strategy', sa.String(length=50), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('books', 'm4b_path_strategy')
    op.drop_column('books', 'm4b_source_paths')

