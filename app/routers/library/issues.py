"""Issue endpoints."""

from __future__ import annotations

from typing import Any

import aiosqlite
import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app import schemas
from app.db import get_connection

from . import helpers, search_utils

router = APIRouter()


@router.get(
    "/issues",
    response_model=schemas.ListIssuesResponse,
)
async def search_issues(
    *,
    conn: aiosqlite.Connection = Depends(get_connection),
    title_search: str = Query(
        ...,
        min_length=1,
        description="Filter issues by matching their parent series title",
    ),
    page_size: int = Query(default=25, ge=1, le=helpers.MAX_PAGE_SIZE),
    page_token: str | None = None,
) -> schemas.ListIssuesResponse:
    """Return issues whose series titles match the provided search string."""
    query = title_search.strip()
    if not query:
        raise HTTPException(status_code=400, detail="title_search must not be empty")

    offset = helpers.parse_page_token(page_token)
    rows = await _collect_matching_issue_rows(
        conn,
        query=query,
        offset=offset,
        limit=page_size + 1,
    )
    payload = [helpers.row_to_model(schemas.Issue, row) for row in rows[:page_size]]
    return schemas.ListIssuesResponse(
        issues=payload,
        next_page_token=helpers.next_page_token(offset, page_size, len(rows)),
    )


@router.get(
    "/series/{series_id}/issues",
    response_model=schemas.ListIssuesResponse,
)
async def list_issues(
    series_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=helpers.MAX_PAGE_SIZE),
    page_token: str | None = None,
    story_arc: str | None = Query(
        default=None, description="Filter by story arc exact match"
    ),
) -> schemas.ListIssuesResponse:
    """List issues for a series with optional story arc filtering."""
    await helpers.ensure_series(conn, series_id)
    offset = helpers.parse_page_token(page_token)
    clauses = ["series_id = ?"]
    params: list[Any] = [series_id]
    if story_arc:
        clauses.append("story_arc = ?")
        params.append(story_arc)
    where = " AND ".join(clauses)
    query = f"""
        SELECT issue_id, series_id, issue_nr, variant, title, subtitle,
               full_title, cover_date, cover_year, story_arc
        FROM issues
        WHERE {where}
        ORDER BY issue_nr, variant, issue_id
        LIMIT ? OFFSET ?
    """
    params.extend([page_size + 1, offset])
    async with conn.execute(query, params) as cursor:
        rows = list(await cursor.fetchall())
    payload = [helpers.row_to_model(schemas.Issue, row) for row in rows[:page_size]]
    return schemas.ListIssuesResponse(
        issues=payload,
        next_page_token=helpers.next_page_token(offset, page_size, len(rows)),
    )


@router.post(
    "/series/{series_id}/issues",
    response_model=schemas.Issue,
    status_code=status.HTTP_201_CREATED,
)
async def create_issue(
    series_id: int,
    request: schemas.CreateIssueRequest,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Issue:
    """Persist a new issue under the target series."""
    await helpers.ensure_series(conn, series_id)
    data = request.model_dump()
    data["variant"] = data.get("variant") or ""
    data["series_id"] = series_id
    issue_id: int | None = None
    try:
        cursor = await conn.execute(
            """
            INSERT INTO issues (
                series_id, issue_nr, variant, title, subtitle,
                full_title, cover_date, cover_year, story_arc
            ) VALUES (:series_id, :issue_nr, :variant, :title, :subtitle,
                      :full_title, :cover_date, :cover_year, :story_arc)
            """,
            data,
        )
        issue_id = cursor.lastrowid
        await cursor.close()
        await conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="issue already exists for this series",
        ) from exc

    if issue_id is None:
        raise HTTPException(status_code=500, detail="failed to create issue")
    row = await helpers.fetch_issue(conn, series_id, issue_id)
    return helpers.row_to_model(schemas.Issue, row)


@router.get(
    "/series/{series_id}/issues/{issue_id}",
    response_model=schemas.Issue,
)
async def get_issue(
    series_id: int,
    issue_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Issue:
    """Retrieve a single issue by id."""
    row = await helpers.fetch_issue(conn, series_id, issue_id)
    return helpers.row_to_model(schemas.Issue, row)


@router.patch(
    "/series/{series_id}/issues/{issue_id}",
    response_model=schemas.Issue,
)
async def update_issue(
    series_id: int,
    issue_id: int,
    request: schemas.UpdateIssueRequest,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.Issue:
    """Apply partial updates to an issue."""
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")
    if "variant" in updates and updates["variant"] is None:
        updates["variant"] = ""
    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"issue_id": issue_id, "series_id": series_id}
    cursor = await conn.execute(
        f"UPDATE issues SET {assignments} WHERE issue_id = :issue_id AND series_id = :series_id",
        params,
    )
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="issue not found")
    finally:
        await cursor.close()
    await conn.commit()
    row = await helpers.fetch_issue(conn, series_id, issue_id)
    return helpers.row_to_model(schemas.Issue, row)


@router.delete(
    "/series/{series_id}/issues/{issue_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_issue(
    series_id: int,
    issue_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> None:
    """Delete an issue from a series."""
    cursor = await conn.execute(
        "DELETE FROM issues WHERE issue_id = ? AND series_id = ?",
        (issue_id, series_id),
    )
    try:
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="issue not found")
    finally:
        await cursor.close()
    await conn.commit()


async def _collect_matching_issue_rows(
    conn: aiosqlite.Connection, *, query: str, offset: int, limit: int
) -> list[sqlite3.Row]:
    """Gather matching issue rows across series ordered by series relevance."""
    ranked_series = await _rank_matching_series(conn, query)
    rows: list[sqlite3.Row] = []
    skipped = 0
    for series_row in ranked_series:
        async with conn.execute(
            """
            SELECT issue_id, series_id, issue_nr, variant, title, subtitle,
                   full_title, cover_date, cover_year, story_arc
            FROM issues
            WHERE series_id = ?
            ORDER BY issue_nr, variant, issue_id
            """,
            (series_row["series_id"],),
        ) as cursor:
            series_issues = await cursor.fetchall()
        for issue_row in series_issues:
            if skipped < offset:
                skipped += 1
                continue
            rows.append(issue_row)
            if len(rows) >= limit:
                return rows
    return rows


async def _rank_matching_series(
    conn: aiosqlite.Connection, query: str
) -> list[sqlite3.Row]:
    """Return series rows ordered by fuzzy relevance to the provided query."""
    async with conn.execute(
        """
        SELECT series_id, title
        FROM series
        """
    ) as cursor:
        rows = await cursor.fetchall()
    filtered = [
        row for row in rows if search_utils.matches_search(row["title"] or "", query)
    ]
    ranked = sorted(
        filtered,
        key=lambda row: (
            -search_utils.fuzzy_score(row["title"] or "", query),
            (row["title"] or "").lower(),
            row["series_id"],
        ),
    )
    return ranked
