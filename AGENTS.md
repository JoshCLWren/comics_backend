# Repository Guidelines

## Project Structure & Module Organization
Source lives in `main.py`, exposing the FastAPI app used by uvicorn. Data-loading utilities are under `db/` (notably `db/build_library.py` which builds `my_database.db` from `data/clz_export.csv`). Treat `data/` as read-only inputs and regenerate SQLite artifacts instead of editing them by hand. Keep future routers in `app/` or `src/` subpackages to avoid crowding the root, and colocate fixtures beside their modules.

## Build, Test, and Development Commands
- `make setup` installs uv, pins Python 3.12, and creates `.venv`; run once per machine.
- `make run` (or `uv run uvicorn main:app --reload`) starts the API with hot reload.
- `uv run python db/build_library.py` rebuilds the SQLite library after CSV updates.
Prefer `uv add <pkg>` for dependencies so `pyproject.toml` and `uv.lock` stay in sync.

## Coding Style & Naming Conventions
Use Python typing and FastAPI conventions: snake_case function names, PascalCase for Pydantic models, and descriptive module names (e.g., `routers/issues.py`). Keep imports sorted (stdlib, third-party, local) and format with `ruff format` or `uv run ruff format .` before committing. Avoid hard-coding paths; use `pathlib.Path` as shown in the DB script.

## Testing Guidelines
Adopt `pytest` with async support via `pytest-asyncio`. Mirror source layout in a `tests/` folder such as `tests/test_main.py` for API smoke tests and `tests/db/test_build_library.py` for ETL helpers. Name tests `test_<behavior>` and aim for high-value coverage on normalization helpers and DB creation logic. Run `uv run pytest` locally; add regression fixtures when touching schema or request handlers.

## Commit & Pull Request Guidelines
Follow the repo’s existing style—concise, present-tense summaries (e.g., “add series endpoint”). Reference issue IDs when available and group related changes into single commits. PRs should describe intent, screenshots of API responses when UI clients are affected, reproduction or testing notes (`make run`, `uv run pytest`), and call out schema migrations or CSV expectations so reviewers can rebuild databases confidently.
