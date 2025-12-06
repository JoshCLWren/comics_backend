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
    """Return the SQLite path, honoring the COMICS_DB_PATH override.

    If the resolved file does not exist, raise a 500 so the API fails loudly.
    """
    print("RESOLVE_DB_PATH: called", flush=True)
    env_value = os.environ.get(DB_PATH_ENV_VAR)
    print(f"RESOLVE_DB_PATH: env[{DB_PATH_ENV_VAR}] = {env_value}", flush=True)

    path = Path(env_value) if env_value else DEFAULT_DB_PATH

    if not path.exists():
        # Fail loudly so misconfigurations are obvious
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"database file not found at {path}",
        )

    print(f"RESOLVE_DB_PATH: returning existing path = {path}", flush=True)
    return path


async def get_connection() -> AsyncIterator[aiosqlite.Connection]:
    """FastAPI dependency that yields an async SQLite connection."""

    conn = await aiosqlite.connect(resolve_db_path())
    print("get_connection: connected", flush=True)

    conn.row_factory = aiosqlite.Row
    try:
        print("get_connection: yielding connection", flush=True)
        yield conn
    finally:
        print("get_connection: closing connection", flush=True)
        await conn.close()
        print("get_connection: closed connection", flush=True)
