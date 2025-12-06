## Database Library

The `db/build_library.py` script rebuilds `my_database.db` from `data/clz_export.csv`.
It now relies on Alembic migrations located under `alembic/`. Whenever you change the
schema, create a new revision with `uv run alembic revision -m "describe change"` and
update the migration script instead of editing the ETL job directly. Rebuild the
database with:

```bash
uv run python db/build_library.py
```

## CI parity via Docker Compose

You can mimic the GitHub Actions Debian environment locally with Docker Compose.

```bash
# build the Debian 13 image with uv preinstalled
docker compose build ci

# install dependencies into the container-mounted .venv
docker compose run --rm ci uv sync --group dev --all-extras

# run the same lint and test commands that CI executes
docker compose run --rm ci uv run ruff check .
docker compose run --rm ci uv run pytest
```

If you want an interactive shell in the CI image, run
`docker compose run --rm ci bash`. The repo is mounted at `/app`, so any
changes you make are reflected on the host filesystem.
