"""Common helpers shared by the library routers."""

from __future__ import annotations

import sqlite3
from typing import TypeVar

import aiosqlite
from fastapi import HTTPException, status

from app import schemas

SerializedModelT = TypeVar("SerializedModelT", bound=schemas.SerializedModel)

MAX_PAGE_SIZE = 100


def parse_page_token(page_token: str | None) -> int:
    if not page_token:
        return 0
    try:
        offset = int(page_token)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=400, detail="invalid page_token") from exc
    if offset < 0:
        raise HTTPException(status_code=400, detail="invalid page_token")
    return offset


def next_page_token(offset: int, page_size: int, rows_returned: int) -> str | None:
    if rows_returned > page_size:
        return str(offset + page_size)
    return None


def row_to_model(
    model_cls: type[SerializedModelT], row: sqlite3.Row
) -> SerializedModelT:
    data = schemas.dict_from_row(row)
    return model_cls(**data)


async def ensure_series(conn: aiosqlite.Connection, series_id: int) -> None:
    async with conn.execute(
        "SELECT 1 FROM series WHERE series_id = ?", (series_id,)
    ) as cursor:
        exists = await cursor.fetchone()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"series {series_id} not found",
        )


async def fetch_series(conn: aiosqlite.Connection, series_id: int) -> sqlite3.Row:
    async with conn.execute(
        """
        SELECT series_id, title, publisher, series_group, age
        FROM series
        WHERE series_id = ?
        """,
        (series_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"series {series_id} not found",
        )
    return row


async def fetch_issue(
    conn: aiosqlite.Connection, series_id: int, issue_id: int
) -> sqlite3.Row:
    async with conn.execute(
        """
        SELECT issue_id, series_id, issue_nr, variant, title, subtitle,
               full_title, cover_date, cover_year, story_arc
        FROM issues
        WHERE series_id = ? AND issue_id = ?
        """,
        (series_id, issue_id),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"issue {issue_id} not found in series {series_id}",
        )
    return row


async def fetch_copy(
    conn: aiosqlite.Connection, issue_id: int, copy_id: int
) -> sqlite3.Row:
    async with conn.execute(
        """
        SELECT id AS copy_id, issue_id, clz_comic_id, custom_label, format,
               grade, grader_notes, grading_company, raw_slabbed, signed_by,
               slab_cert_number, purchase_date, purchase_price, purchase_store,
               purchase_year, date_sold, price_sold, sold_year, my_value,
               covrprice_value, value, country, language, age, barcode,
               cover_price, page_quality, key_flag, key_category, key_reason,
               label_type, no_of_pages, variant_description
        FROM copies
        WHERE issue_id = ? AND id = ?
        """,
        (issue_id, copy_id),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"copy {copy_id} not found for issue {issue_id}",
        )
    return row


async def ensure_issue_exists(conn: aiosqlite.Connection, issue_id: int) -> None:
    async with conn.execute(
        "SELECT 1 FROM issues WHERE issue_id = ?",
        (issue_id,),
    ) as cursor:
        exists = await cursor.fetchone()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"issue {issue_id} not found",
        )
