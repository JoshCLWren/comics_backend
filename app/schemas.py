"""Pydantic schema definitions for the comics API."""

from __future__ import annotations

from enum import Enum
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
    """Shared optional fields for working with a series."""

    title: str | None = Field(default=None, description="Series display title")
    publisher: str | None = Field(default=None, description="Publisher name")
    series_group: str | None = Field(default=None, description="Grouping label")
    age: str | None = Field(default=None, description="Age category from CLZ")


class Series(SeriesBase):
    """Representation of a stored series."""

    series_id: int = Field(description="Primary key for a series")


class CreateSeriesRequest(SeriesBase):
    """Payload accepted when creating a series."""

    series_id: int = Field(description="User supplied unique identifier")


class UpdateSeriesRequest(SeriesBase):
    """Partial update model for series attributes."""

    @model_validator(mode="after")
    def ensure_payload(self) -> "UpdateSeriesRequest":
        """Prevent empty payloads since PATCH must toggle something."""
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListSeriesResponse(PagingResponse):
    """Paginated response for the list series endpoint."""

    series: list[Series]


class IssueBase(APIModel):
    """Base model shared by issue create and response schemas."""

    issue_nr: str = Field(description="CLZ normalized issue number")
    variant: str | None = Field(default="", description="Variant designation")
    title: str | None = None
    subtitle: str | None = None
    full_title: str | None = None
    cover_date: str | None = None
    cover_year: int | None = None
    story_arc: str | None = None


class Issue(IssueBase):
    """Representation of an issue tied to a series."""

    issue_id: int = Field(description="Primary key for an issue")
    series_id: int


class CreateIssueRequest(IssueBase):
    """Payload accepted when creating an issue."""


class UpdateIssueRequest(APIModel):
    """Partial update schema for issues."""

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
        """Reject empty updates to keep validation consistent."""
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListIssuesResponse(PagingResponse):
    """Paginated response for issue listings."""

    issues: list[Issue]


class CopyBase(APIModel):
    """Base attributes shared across copy schemas."""

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
    """Representation of a stored copy for an issue."""

    copy_id: int = Field(description="Primary key for a copy")
    issue_id: int


class CreateCopyRequest(CopyBase):
    """Payload accepted when creating a copy."""


class UpdateCopyRequest(CopyBase):
    """Partial update schema for copies."""

    @model_validator(mode="after")
    def ensure_payload(self) -> "UpdateCopyRequest":
        """Reject empty updates to keep validation consistent."""
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("At least one field must be provided")
        return self


class ListCopiesResponse(PagingResponse):
    """Paginated response used when listing copies."""

    copies: list[Copy]


class ImageType(str, Enum):
    """Enumeration of supported comic image types."""

    FRONT = "front"
    BACK = "back"
    SPINE = "spine"
    STAPLES = "staples"
    INTERIOR_FRONT_COVER = "interior_front_cover"
    INTERIOR_BACK_COVER = "interior_back_cover"
    MISC = "misc"


class ComicImage(APIModel):
    """Metadata describing a saved image."""

    series_id: int
    issue_id: int
    copy_id: int
    image_type: ImageType
    file_name: str
    relative_path: str


class ListCopyImagesResponse(APIModel):
    """Response containing all image metadata for a copy."""

    images: list[ComicImage]


class JobStatus(str, Enum):
    """Possible states for an image upload job."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class ImageUploadJob(APIModel):
    """Representation of an asynchronous image upload job."""

    job_id: str
    series_id: int
    issue_id: int
    copy_id: int
    image_type: ImageType
    status: JobStatus
    detail: str | None = None
    result: ComicImage | None = None


SerializedModel = Series | Issue | Copy


def dict_from_row(row: Any) -> dict[str, Any]:
    """Convert a sqlite Row into a standard dict."""
    return {key: row[key] for key in row.keys()}
