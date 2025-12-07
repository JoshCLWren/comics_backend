"""Series endpoints."""

from __future__ import annotations

import sqlite3
from typing import Any

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app import schemas
from app.db import get_connection

from . import helpers, search_utils

router = APIRouter()


@router.get("/series", response_model=schemas.ListSeriesResponse)
async def list_series(
    *,
    conn: aiosqlite.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=helpers.MAX_PAGE_SIZE),
    page_token: str | None = None,
    publisher: str | None = Query(default=None, description="Filter by publisher"),
    title_search: str | None = Query(
        default=None, description="Substring filter for series title"
    ),
) -> schemas.ListSeriesResponse:
    """Return paginated series optionally filtered by publisher or title."""
    print("handler /series: start", flush=True)
    print(
        f"handler /series: page_size={page_size}, page_token={page_token}, publisher={publisher}, title_search={title_search}",
        flush=True,
    )

    offset = helpers.parse_page_token(page_token)
    print(f"handler /series: offset={offset}", flush=True)

    params: list[Any] = []
    clauses: list[str] = []

    if publisher:
        print("handler /series: adding publisher clause", flush=True)
        clauses.append("publisher = ?")
        params.append(publisher)

    if title_search:
        print(
            "handler /series: title_search detected; filtering will occur post-query",
            flush=True,
        )

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    print(f"handler /series: where='{where}'", flush=True)

    base_query = f"""
        SELECT series_id, title, publisher, series_group, age
        FROM series
        {where}
    """

    if title_search:
        print("handler /series: applying fuzzy ordering", flush=True)
        async with conn.execute(base_query, params) as cursor:
            rows = await cursor.fetchall()

        filtered_rows = [
            row
            for row in rows
            if search_utils.matches_search(row["title"] or "", title_search or "")
        ]

        ranked_rows = sorted(
            filtered_rows,
            key=lambda row: (
                -search_utils.fuzzy_score(row["title"] or "", title_search),
                (row["title"] or "").lower(),
                row["series_id"],
            ),
        )
        print("handler /series: ranked rows", flush=True)
        window = ranked_rows[offset : offset + page_size + 1]
        payload_rows = window[:page_size]
        next_token = helpers.next_page_token(offset, page_size, len(window))
    else:
        query = f"""
            {base_query}
            ORDER BY series_id
            LIMIT ? OFFSET ?
        """
        params_with_paging = params + [page_size + 1, offset]
        print("handler /series: executing alphabetical query", flush=True)
        async with conn.execute(query, params_with_paging) as cursor:
            rows = await cursor.fetchall()
        payload_rows = list(rows)[:page_size]
        next_token = helpers.next_page_token(offset, page_size, len(rows))

    payload = [helpers.row_to_model(schemas.Series, row) for row in payload_rows]
    print(f"handler /series: built payload size={len(payload)}", flush=True)

    print(f"handler /series: next_page_token={next_token}", flush=True)

    print("handler /series: returning response", flush=True)
    return schemas.ListSeriesResponse(
        series=payload,
        next_page_token=next_token,
    )


@router.post(
    "/series",
    response_model=schemas.Series,
    status_code=status.HTTP_201_CREATED,
)
async def create_series(
    *,
    conn: aiosqlite.Connection = Depends(get_connection),
    request: schemas.CreateSeriesRequest,
) -> schemas.Series:
    """Create a new series row."""
    data = request.model_dump()
    try:
        await conn.execute(
            """
            INSERT INTO series (series_id, title, publisher, series_group, age)
            VALUES (:series_id, :title, :publisher, :series_group, :age)
            """,
            data,
        )
        await conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"series {request.series_id} already exists",
        ) from exc

    return schemas.Series(**data)


@router.get("/series/{series_id}", response_model=schemas.Series)
async def get_series(
    series_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Series:
    """Fetch a single series by identifier."""
    async with conn.execute(
        """
        SELECT series_id, title, publisher, series_group, age
        FROM series WHERE series_id = ?
        """,
        (series_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="series not found")
    return helpers.row_to_model(schemas.Series, row)


@router.patch("/series/{series_id}", response_model=schemas.Series)
async def update_series(
    series_id: int,
    request: schemas.UpdateSeriesRequest,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Series:
    """Apply partial updates to a series."""
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")

    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"series_id": series_id}
    cursor = await conn.execute(
        f"UPDATE series SET {assignments} WHERE series_id = :series_id",
        params,
    )
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="series not found")
    finally:
        await cursor.close()
    await conn.commit()
    return await get_series(series_id, conn)


@router.delete("/series/{series_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_series(
    series_id: int, conn: aiosqlite.Connection = Depends(get_connection)
) -> None:
    """Remove a series from the catalog."""
    cursor = await conn.execute("DELETE FROM series WHERE series_id = ?", (series_id,))
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="series not found")
    finally:
        await cursor.close()
    await conn.commit()
