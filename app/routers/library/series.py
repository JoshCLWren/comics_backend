"""Series endpoints."""

from __future__ import annotations

import re
import sqlite3
from difflib import SequenceMatcher
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
            if _matches_search(row["title"] or "", title_search or "")
        ]

        ranked_rows = sorted(
            filtered_rows,
            key=lambda row: (
                -_fuzzy_score(row["title"] or "", title_search),
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


_TOKEN_RE = re.compile(r"[0-9a-zA-Z]+")


def _tokenize(text: str) -> list[str]:
    """Break a string into lowercase alphanumeric tokens."""
    return [token for token in _TOKEN_RE.findall(text.lower()) if token]


def _normalized_text(tokens: list[str]) -> str:
    """Join normalized tokens with single spaces."""
    return " ".join(tokens)


def _collapsed_text(tokens: list[str]) -> str:
    """Return all tokens concatenated together."""
    return "".join(tokens)


def _fuzzy_score(title: str, query: str) -> float:
    """Return a fuzzy matching score between 0 and 1."""
    title_tokens = _tokenize(title)
    query_tokens = _tokenize(query)
    if not title_tokens or not query_tokens:
        return 0.0

    normalized_title = _normalized_text(title_tokens)
    normalized_query = _normalized_text(query_tokens)
    collapsed_title = _collapsed_text(title_tokens)
    collapsed_query = _collapsed_text(query_tokens)

    ratio = SequenceMatcher(None, normalized_query, normalized_title).ratio()

    if normalized_title == normalized_query or collapsed_title == collapsed_query:
        ratio += 0.3
    if normalized_title.startswith(normalized_query):
        ratio += 0.1
    if normalized_title.endswith(normalized_query):
        ratio += 0.08
    if normalized_query in normalized_title:
        ratio += 0.05
    if collapsed_query in collapsed_title:
        ratio += 0.07
    if set(query_tokens).issubset(title_tokens):
        ratio += 0.05
    return min(ratio, 1.0)


def _matches_search(title: str, query: str) -> bool:
    """Return True when the title should be considered a match for the query."""
    query_tokens = _tokenize(query)
    if not query_tokens:
        return True

    title_tokens = _tokenize(title)
    if not title_tokens:
        return False

    normalized_title = _normalized_text(title_tokens)
    normalized_query = _normalized_text(query_tokens)
    collapsed_title = _collapsed_text(title_tokens)
    collapsed_query = _collapsed_text(query_tokens)

    if normalized_query in normalized_title:
        return True
    if collapsed_query and collapsed_query in collapsed_title:
        return True

    title_token_set = set(title_tokens)
    query_token_set = set(query_tokens)
    if query_token_set <= title_token_set:
        return True
    if title_token_set & query_token_set:
        return True

    for q in query_token_set:
        for token in title_tokens:
            if len(q) >= 3 and q in token:
                return True
            if len(token) >= 3 and token in q:
                return True
    return False
