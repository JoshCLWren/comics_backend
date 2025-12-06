"""Database helpers and dependencies for FastAPI routes."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterator

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


def get_connection() -> Iterator[sqlite3.Connection]:
    """FastAPI dependency that yields a SQLite connection."""
    conn = sqlite3.connect(resolve_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

