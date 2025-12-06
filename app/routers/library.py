"""FastAPI router exposing CRUD endpoints for the comics library."""
from __future__ import annotations

from typing import Any

import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app import schemas
from app.db import get_connection

router = APIRouter(prefix="/v1", tags=["library"])

MAX_PAGE_SIZE = 100


def _parse_page_token(page_token: str | None) -> int:
    if not page_token:
        return 0
    try:
        offset = int(page_token)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=400, detail="invalid page_token") from exc
    if offset < 0:
        raise HTTPException(status_code=400, detail="invalid page_token")
    return offset


def _next_page_token(offset: int, page_size: int, rows_returned: int) -> str | None:
    if rows_returned > page_size:
        return str(offset + page_size)
    return None


def _row_to_model(model_cls: type[schemas.SerializedModel], row: sqlite3.Row) -> Any:
    data = schemas.dict_from_row(row)
    return model_cls(**data)


def _ensure_series(conn: sqlite3.Connection, series_id: int) -> None:
    exists = conn.execute(
        "SELECT 1 FROM series WHERE series_id = ?", (series_id,)
    ).fetchone()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"series {series_id} not found",
        )


def _fetch_issue(conn: sqlite3.Connection, series_id: int, issue_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT issue_id, series_id, issue_nr, variant, title, subtitle,
               full_title, cover_date, cover_year, story_arc
        FROM issues
        WHERE series_id = ? AND issue_id = ?
        """,
        (series_id, issue_id),
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"issue {issue_id} not found in series {series_id}",
        )
    return row


def _fetch_copy(
    conn: sqlite3.Connection, issue_id: int, copy_id: int
) -> sqlite3.Row:
    row = conn.execute(
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
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"copy {copy_id} not found for issue {issue_id}",
        )
    return row


@router.get("/series", response_model=schemas.ListSeriesResponse)
def list_series(
    *,
    conn: sqlite3.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=MAX_PAGE_SIZE),
    page_token: str | None = None,
    publisher: str | None = Query(default=None, description="Filter by publisher"),
    title_search: str | None = Query(
        default=None, description="Substring filter for series title"
    ),
) -> schemas.ListSeriesResponse:
    offset = _parse_page_token(page_token)
    params: list[Any] = []
    clauses: list[str] = []
    if publisher:
        clauses.append("publisher = ?")
        params.append(publisher)
    if title_search:
        clauses.append("title LIKE ?")
        params.append(f"%{title_search}%")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT series_id, title, publisher, series_group, age
        FROM series
        {where}
        ORDER BY series_id
        LIMIT ? OFFSET ?
    """
    params.extend([page_size + 1, offset])
    rows = conn.execute(query, params).fetchall()
    payload = [
        _row_to_model(schemas.Series, row) for row in rows[:page_size]
    ]
    return schemas.ListSeriesResponse(
        series=payload,
        next_page_token=_next_page_token(offset, page_size, len(rows)),
    )


@router.post(
    "/series",
    response_model=schemas.Series,
    status_code=status.HTTP_201_CREATED,
)
def create_series(
    *,
    conn: sqlite3.Connection = Depends(get_connection),
    request: schemas.CreateSeriesRequest,
) -> schemas.Series:
    data = request.model_dump()
    try:
        conn.execute(
            """
            INSERT INTO series (series_id, title, publisher, series_group, age)
            VALUES (:series_id, :title, :publisher, :series_group, :age)
            """,
            data,
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"series {request.series_id} already exists",
        ) from exc

    return schemas.Series(**data)


@router.get("/series/{series_id}", response_model=schemas.Series)
def get_series(
    series_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Series:
    row = conn.execute(
        """
        SELECT series_id, title, publisher, series_group, age
        FROM series WHERE series_id = ?
        """,
        (series_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="series not found")
    return _row_to_model(schemas.Series, row)


@router.patch("/series/{series_id}", response_model=schemas.Series)
def update_series(
    series_id: int,
    request: schemas.UpdateSeriesRequest,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Series:
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")

    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"series_id": series_id}
    cur = conn.execute(
        f"UPDATE series SET {assignments} WHERE series_id = :series_id",
        params,
    )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="series not found")
    conn.commit()
    return get_series(series_id, conn)


@router.delete(
    "/series/{series_id}", status_code=status.HTTP_204_NO_CONTENT
)
def delete_series(series_id: int, conn: sqlite3.Connection = Depends(get_connection)) -> None:
    cur = conn.execute("DELETE FROM series WHERE series_id = ?", (series_id,))
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="series not found")
    conn.commit()


@router.get(
    "/series/{series_id}/issues",
    response_model=schemas.ListIssuesResponse,
)
def list_issues(
    series_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=MAX_PAGE_SIZE),
    page_token: str | None = None,
    story_arc: str | None = Query(
        default=None, description="Filter by story arc exact match"
    ),
) -> schemas.ListIssuesResponse:
    _ensure_series(conn, series_id)
    offset = _parse_page_token(page_token)
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
    rows = conn.execute(query, params).fetchall()
    payload = [
        _row_to_model(schemas.Issue, row) for row in rows[:page_size]
    ]
    return schemas.ListIssuesResponse(
        issues=payload,
        next_page_token=_next_page_token(offset, page_size, len(rows)),
    )


@router.post(
    "/series/{series_id}/issues",
    response_model=schemas.Issue,
    status_code=status.HTTP_201_CREATED,
)
def create_issue(
    series_id: int,
    request: schemas.CreateIssueRequest,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Issue:
    _ensure_series(conn, series_id)
    data = request.model_dump()
    data["variant"] = data.get("variant") or ""
    data["series_id"] = series_id
    try:
        cur = conn.execute(
            """
            INSERT INTO issues (
                series_id, issue_nr, variant, title, subtitle,
                full_title, cover_date, cover_year, story_arc
            ) VALUES (:series_id, :issue_nr, :variant, :title, :subtitle,
                      :full_title, :cover_date, :cover_year, :story_arc)
            """,
            data,
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="issue already exists for this series",
        ) from exc

    issue_id = cur.lastrowid
    row = _fetch_issue(conn, series_id, issue_id)
    return _row_to_model(schemas.Issue, row)


@router.get(
    "/series/{series_id}/issues/{issue_id}",
    response_model=schemas.Issue,
)
def get_issue(
    series_id: int,
    issue_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Issue:
    row = _fetch_issue(conn, series_id, issue_id)
    return _row_to_model(schemas.Issue, row)


@router.patch(
    "/series/{series_id}/issues/{issue_id}",
    response_model=schemas.Issue,
)
def update_issue(
    series_id: int,
    issue_id: int,
    request: schemas.UpdateIssueRequest,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Issue:
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")
    if "variant" in updates and updates["variant"] is None:
        updates["variant"] = ""
    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"issue_id": issue_id, "series_id": series_id}
    cur = conn.execute(
        f"UPDATE issues SET {assignments} WHERE issue_id = :issue_id AND series_id = :series_id",
        params,
    )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="issue not found")
    conn.commit()
    row = _fetch_issue(conn, series_id, issue_id)
    return _row_to_model(schemas.Issue, row)


@router.delete(
    "/series/{series_id}/issues/{issue_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_issue(
    series_id: int,
    issue_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
) -> None:
    cur = conn.execute(
        "DELETE FROM issues WHERE issue_id = ? AND series_id = ?",
        (issue_id, series_id),
    )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="issue not found")
    conn.commit()


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


def _fetch_issue_for_copy(conn: sqlite3.Connection, issue_id: int) -> None:
    exists = conn.execute(
        "SELECT 1 FROM issues WHERE issue_id = ?",
        (issue_id,)
    ).fetchone()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"issue {issue_id} not found",
        )


@router.get(
    "/issues/{issue_id}/copies",
    response_model=schemas.ListCopiesResponse,
)
def list_copies(
    issue_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
    page_size: int = Query(default=25, ge=1, le=MAX_PAGE_SIZE),
    page_token: str | None = None,
) -> schemas.ListCopiesResponse:
    _fetch_issue_for_copy(conn, issue_id)
    offset = _parse_page_token(page_token)
    rows = conn.execute(
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
    ).fetchall()
    payload = [
        _row_to_model(schemas.Copy, row) for row in rows[:page_size]
    ]
    return schemas.ListCopiesResponse(
        copies=payload,
        next_page_token=_next_page_token(offset, page_size, len(rows)),
    )


@router.post(
    "/issues/{issue_id}/copies",
    response_model=schemas.Copy,
    status_code=status.HTTP_201_CREATED,
)
def create_copy(
    issue_id: int,
    request: schemas.CreateCopyRequest,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Copy:
    _fetch_issue_for_copy(conn, issue_id)
    data = request.model_dump()
    columns = ", ".join(COPY_COLUMNS)
    placeholders = ", ".join(f":{col}" for col in COPY_COLUMNS)
    data_with_issue = data | {"issue_id": issue_id}
    try:
        cur = conn.execute(
            f"""
            INSERT INTO copies (issue_id, {columns})
            VALUES (:issue_id, {placeholders})
            """,
            data_with_issue,
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=400, detail="failed to create copy") from exc

    row = _fetch_copy(conn, issue_id, cur.lastrowid)
    return _row_to_model(schemas.Copy, row)


@router.get(
    "/issues/{issue_id}/copies/{copy_id}",
    response_model=schemas.Copy,
)
def get_copy(
    issue_id: int,
    copy_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Copy:
    row = _fetch_copy(conn, issue_id, copy_id)
    return _row_to_model(schemas.Copy, row)


@router.patch(
    "/issues/{issue_id}/copies/{copy_id}",
    response_model=schemas.Copy,
)
def update_copy(
    issue_id: int,
    copy_id: int,
    request: schemas.UpdateCopyRequest,
    conn: sqlite3.Connection = Depends(get_connection),
) -> schemas.Copy:
    updates = request.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")
    assignments = ", ".join(f"{field} = :{field}" for field in updates.keys())
    params = updates | {"copy_id": copy_id, "issue_id": issue_id}
    cur = conn.execute(
        f"UPDATE copies SET {assignments} WHERE id = :copy_id AND issue_id = :issue_id",
        params,
    )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="copy not found")
    conn.commit()
    row = _fetch_copy(conn, issue_id, copy_id)
    return _row_to_model(schemas.Copy, row)


@router.delete(
    "/issues/{issue_id}/copies/{copy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_copy(
    issue_id: int,
    copy_id: int,
    conn: sqlite3.Connection = Depends(get_connection),
) -> None:
    cur = conn.execute(
        "DELETE FROM copies WHERE id = ? AND issue_id = ?",
        (copy_id, issue_id),
    )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="copy not found")
    conn.commit()
