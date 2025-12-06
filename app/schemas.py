"""Pydantic schema definitions for the comics API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class APIModel(BaseModel):
    """Base model with conservative defaults."""

    model_config = ConfigDict(extra="forbid")


class PagingResponse(APIModel):
    """Mixin for paginated responses."""

    next_page_token: str | None = Field(
        default=None,
        description="Opaque token clients pass to retrieve the next page.",
    )


class SeriesBase(APIModel):
    title: str | None = Field(default=None, description="Series display title")
    publisher: str | None = Field(default=None, description="Publisher name")
    series_group: str | None = Field(default=None, description="Grouping label")
    age: str | None = Field(default=None, description="Age category from CLZ")


class Series(SeriesBase):
    series_id: int = Field(description="Primary key for a series")


class CreateSeriesRequest(SeriesBase):
    series_id: int = Field(description="User supplied unique identifier")


class UpdateSeriesRequest(SeriesBase):
    @model_validator(mode="after")
    def ensure_payload(self) -> "UpdateSeriesRequest":
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListSeriesResponse(PagingResponse):
    series: list[Series]


class IssueBase(APIModel):
    issue_nr: str = Field(description="CLZ normalized issue number")
    variant: str | None = Field(default="", description="Variant designation")
    title: str | None = None
    subtitle: str | None = None
    full_title: str | None = None
    cover_date: str | None = None
    cover_year: int | None = None
    story_arc: str | None = None


class Issue(IssueBase):
    issue_id: int = Field(description="Primary key for an issue")
    series_id: int


class CreateIssueRequest(IssueBase):
    pass


class UpdateIssueRequest(APIModel):
    issue_nr: str | None = None
    variant: str | None = None
    title: str | None = None
    subtitle: str | None = None
    full_title: str | None = None
    cover_date: str | None = None
    cover_year: int | None = None
    story_arc: str | None = None

    @model_validator(mode="after")
    def ensure_payload(self) -> "UpdateIssueRequest":
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListIssuesResponse(PagingResponse):
    issues: list[Issue]


class CopyBase(APIModel):
    clz_comic_id: int | None = None
    custom_label: str | None = None
    format: str | None = None
    grade: str | None = None
    grader_notes: str | None = None
    grading_company: str | None = None
    raw_slabbed: str | None = None
    signed_by: str | None = None
    slab_cert_number: str | None = None
    purchase_date: str | None = None
    purchase_price: float | None = None
    purchase_store: str | None = None
    purchase_year: int | None = None
    date_sold: str | None = None
    price_sold: float | None = None
    sold_year: int | None = None
    my_value: float | None = None
    covrprice_value: float | None = None
    value: float | None = None
    country: str | None = None
    language: str | None = None
    age: str | None = None
    barcode: str | None = None
    cover_price: float | None = None
    page_quality: str | None = None
    key_flag: str | None = None
    key_category: str | None = None
    key_reason: str | None = None
    label_type: str | None = None
    no_of_pages: int | None = None
    variant_description: str | None = None


class Copy(CopyBase):
    copy_id: int = Field(description="Primary key for a copy")
    issue_id: int


class CreateCopyRequest(CopyBase):
    pass


class UpdateCopyRequest(CopyBase):
    @model_validator(mode="after")
    def ensure_payload(self) -> "UpdateCopyRequest":
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListCopiesResponse(PagingResponse):
    copies: list[Copy]


SerializedModel = Series | Issue | Copy


def dict_from_row(row: Any) -> dict[str, Any]:
    """Convert a sqlite Row into a standard dict."""
    return {key: row[key] for key in row.keys()}
