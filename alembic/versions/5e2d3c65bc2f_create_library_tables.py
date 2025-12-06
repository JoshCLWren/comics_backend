"""create library tables

Revision ID: 5e2d3c65bc2f
Revises: 
Create Date: 2025-12-06 11:08:10.905577

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5e2d3c65bc2f"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "series",
        sa.Column("series_id", sa.Integer(), primary_key=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("publisher", sa.Text(), nullable=True),
        sa.Column("series_group", sa.Text(), nullable=True),
        sa.Column("age", sa.Text(), nullable=True),
    )

    op.create_table(
        "issues",
        sa.Column("issue_id", sa.Integer(), primary_key=True),
        sa.Column("series_id", sa.Integer(), nullable=False),
        sa.Column("issue_nr", sa.Text(), nullable=True),
        sa.Column("variant", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("subtitle", sa.Text(), nullable=True),
        sa.Column("full_title", sa.Text(), nullable=True),
        sa.Column("cover_date", sa.Text(), nullable=True),
        sa.Column("cover_year", sa.Integer(), nullable=True),
        sa.Column("story_arc", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "series_id",
            "issue_nr",
            "variant",
            name="uq_issues_series_issue_variant",
        ),
        sa.ForeignKeyConstraint(
            ("series_id",),
            ["series.series_id"],
            name="fk_issues_series_id_series",
        ),
        sqlite_autoincrement=True,
    )

    op.create_table(
        "copies",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("clz_comic_id", sa.Integer(), nullable=True),
        sa.Column("issue_id", sa.Integer(), nullable=False),
        sa.Column("custom_label", sa.Text(), nullable=True),
        sa.Column("format", sa.Text(), nullable=True),
        sa.Column("grade", sa.Text(), nullable=True),
        sa.Column("grader_notes", sa.Text(), nullable=True),
        sa.Column("grading_company", sa.Text(), nullable=True),
        sa.Column("raw_slabbed", sa.Text(), nullable=True),
        sa.Column("signed_by", sa.Text(), nullable=True),
        sa.Column("slab_cert_number", sa.Text(), nullable=True),
        sa.Column("purchase_date", sa.Text(), nullable=True),
        sa.Column("purchase_price", sa.Float(), nullable=True),
        sa.Column("purchase_store", sa.Text(), nullable=True),
        sa.Column("purchase_year", sa.Integer(), nullable=True),
        sa.Column("date_sold", sa.Text(), nullable=True),
        sa.Column("price_sold", sa.Float(), nullable=True),
        sa.Column("sold_year", sa.Integer(), nullable=True),
        sa.Column("my_value", sa.Float(), nullable=True),
        sa.Column("covrprice_value", sa.Float(), nullable=True),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("language", sa.Text(), nullable=True),
        sa.Column("age", sa.Text(), nullable=True),
        sa.Column("barcode", sa.Text(), nullable=True),
        sa.Column("cover_price", sa.Float(), nullable=True),
        sa.Column("page_quality", sa.Text(), nullable=True),
        sa.Column("key_flag", sa.Text(), nullable=True),
        sa.Column("key_category", sa.Text(), nullable=True),
        sa.Column("key_reason", sa.Text(), nullable=True),
        sa.Column("label_type", sa.Text(), nullable=True),
        sa.Column("no_of_pages", sa.Integer(), nullable=True),
        sa.Column("variant_description", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ("issue_id",),
            ["issues.issue_id"],
            name="fk_copies_issue_id_issues",
        ),
        sqlite_autoincrement=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("copies")
    op.drop_table("issues")
    op.drop_table("series")
