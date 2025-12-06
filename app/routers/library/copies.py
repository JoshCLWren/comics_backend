"""Copy endpoints."""

from __future__ import annotations

import aiosqlite
import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app import schemas
from app.db import get_connection

from . import helpers

router = APIRouter()

COPY_COLUMNS = [
    "clz_comic_id",
    "custom_label",
    "format",
    "grade",
    "grader_notes",
    "grading_company",
    "raw_slabbed",
    "signed_by",
    "slab_cert_number",
    "purchase_date",
    "purchase_price",
    "purchase_store",
    "purchase_year",
    "date_sold",
    "price_sold",
    "sold_year",
    "my_value",
    "covrprice_value",
    "value",
    "country",
    "language",
    "age",
    "barcode",
    "cover_price",
    "page_quality",
    "key_flag",
    "key_category",
    "key_reason",
    "label_type",
    "no_of_pages",
    "variant_description",
]


@router.get(
    "/issues/{issue_id}/copies",
    response_model=schemas.ListCopiesResponse,
)
async def list_copies(
    issue_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=helpers.MAX_PAGE_SIZE),
    page_token: str | None = None,
) -> schemas.ListCopiesResponse:
    await helpers.ensure_issue_exists(conn, issue_id)
    offset = helpers.parse_page_token(page_token)
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
        WHERE issue_id = ?
        ORDER BY id
        LIMIT ? OFFSET ?
        """,
        (issue_id, page_size + 1, offset),
    ) as cursor:
        rows = await cursor.fetchall()
    payload = [helpers.row_to_model(schemas.Copy, row) for row in rows[:page_size]]
    return schemas.ListCopiesResponse(
        copies=payload,
        next_page_token=helpers.next_page_token(offset, page_size, len(rows)),
    )


@router.post(
    "/issues/{issue_id}/copies",
    response_model=schemas.Copy,
    status_code=status.HTTP_201_CREATED,
)
async def create_copy(
    issue_id: int,
    request: schemas.CreateCopyRequest,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Copy:
    await helpers.ensure_issue_exists(conn, issue_id)
    data = request.model_dump()
    columns = ", ".join(COPY_COLUMNS)
    placeholders = ", ".join(f":{col}" for col in COPY_COLUMNS)
    data_with_issue = data | {"issue_id": issue_id}
    try:
        cursor = await conn.execute(
            f"""
            INSERT INTO copies (issue_id, {columns})
            VALUES (:issue_id, {placeholders})
            """,
            data_with_issue,
        )
        copy_id = cursor.lastrowid
        await cursor.close()
        await conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=400, detail="failed to create copy") from exc

    row = await helpers.fetch_copy(conn, issue_id, copy_id)
    return helpers.row_to_model(schemas.Copy, row)


@router.get(
    "/issues/{issue_id}/copies/{copy_id}",
    response_model=schemas.Copy,
)
async def get_copy(
    issue_id: int,
    copy_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Copy:
    row = await helpers.fetch_copy(conn, issue_id, copy_id)
    return helpers.row_to_model(schemas.Copy, row)


@router.patch(
    "/issues/{issue_id}/copies/{copy_id}",
    response_model=schemas.Copy,
)
async def update_copy(
    issue_id: int,
    copy_id: int,
    request: schemas.UpdateCopyRequest,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Copy:
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")
    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"copy_id": copy_id, "issue_id": issue_id}
    cursor = await conn.execute(
        f"UPDATE copies SET {assignments} WHERE id = :copy_id AND issue_id = :issue_id",
        params,
    )
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="copy not found")
    finally:
        await cursor.close()
    await conn.commit()
    row = await helpers.fetch_copy(conn, issue_id, copy_id)
    return helpers.row_to_model(schemas.Copy, row)


@router.delete(
    "/issues/{issue_id}/copies/{copy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_copy(
    issue_id: int,
    copy_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> None:
    cursor = await conn.execute(
        "DELETE FROM copies WHERE id = ? AND issue_id = ?",
        (copy_id, issue_id),
    )
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="copy not found")
    finally:
        await cursor.close()
    await conn.commit()
