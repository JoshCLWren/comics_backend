"""Database helpers and dependencies for FastAPI routes."""
from __future__ import annotations

import os
from pathlib import Path
from typing import AsyncIterator

import aiosqlite
from fastapi import HTTPException, status

DEFAULT_DB_PATH = Path("my_database.db")
DB_PATH_ENV_VAR = "COMICS_DB_PATH"


def resolve_db_path() -> Path:
    """Return the SQLite path, honoring the COMICS_DB_PATH override."""
    override = os.environ.get(DB_PATH_ENV_VAR)
    path = Path(override) if override else DEFAULT_DB_PATH
    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"database not found at {path}",
        )
    return path


async def get_connection() -> AsyncIterator[aiosqlite.Connection]:
    """FastAPI dependency that yields an async SQLite connection."""
    conn = await aiosqlite.connect(resolve_db_path())
    conn.row_factory = aiosqlite.Row
    try:
        yield conn
    finally:
        await conn.close()
