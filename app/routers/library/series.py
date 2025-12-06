"""Series endpoints."""

from __future__ import annotations

import sqlite3
from typing import Any

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app import schemas
from app.db import get_connection

from . import helpers

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
        print("handler /series: adding title_search clause", flush=True)
        clauses.append("title LIKE ?")
        params.append(f"%{title_search}%")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    print(f"handler /series: where='{where}'", flush=True)

    query = f"""
        SELECT series_id, title, publisher, series_group, age
        FROM series
        {where}
        ORDER BY series_id
        LIMIT ? OFFSET ?
    """
    params.extend([page_size + 1, offset])
    print(f"handler /series: about to execute query with params={params}", flush=True)

    async with conn.execute(query, params) as cursor:
        print("handler /series: query executed, before fetchall", flush=True)
        rows = await cursor.fetchall()
        print(f"handler /series: after fetchall, rows={len(rows)}", flush=True)

    payload = [helpers.row_to_model(schemas.Series, row) for row in rows[:page_size]]
    print(f"handler /series: built payload size={len(payload)}", flush=True)

    next_token = helpers.next_page_token(offset, page_size, len(rows))
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
    cursor = await conn.execute("DELETE FROM series WHERE series_id = ?", (series_id,))
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="series not found")
    finally:
        await cursor.close()
    await conn.commit()
