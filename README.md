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

## API design

The FastAPI surface follows the Google API Design Guide by using resource-oriented
paths, granular verbs, and cursor-based pagination. All endpoints are served under
`/v1` so future, breaking schema revisions can sit beside the current contract.

| Operation | Path | Notes |
| --- | --- | --- |
| `ListSeries` | `GET /v1/series?page_size=&page_token=&publisher=&title_search=` | Returns `ListSeriesResponse` with an array of `Series` resources and a `next_page_token` clients can echo back. |
| `GetSeries` | `GET /v1/series/{series_id}` | 404s when the resource is absent. |
| `CreateSeries` | `POST /v1/series` | Caller provides the `series_id` so it matches the CLZ identifiers. |
| `UpdateSeries` | `PATCH /v1/series/{series_id}` | Partial updates, rejecting empty bodies per the guide’s recommendation. |
| `DeleteSeries` | `DELETE /v1/series/{series_id}` | Idempotent deletes. |
| `ListIssues` | `GET /v1/series/{series_id}/issues?page_size=&page_token=&story_arc=` | Parents issues under their owning series. |
| `GetIssue`/`UpdateIssue`/`DeleteIssue` | `/v1/series/{series_id}/issues/{issue_id}` | Keeps canonical resource names stable (`series/{series}/issues/{issue}`). |
| `CreateIssue` | `POST /v1/series/{series_id}/issues` | Enforces uniqueness on the `(series_id, issue_nr, variant)` tuple so duplicates surface as `409 Conflict`. |
| `ListCopies` | `GET /v1/issues/{issue_id}/copies?page_size=&page_token=` | Copies are subordinate to issues and page using the same token semantics. |
| `GetCopy`/`UpdateCopy`/`DeleteCopy` | `/v1/issues/{issue_id}/copies/{copy_id}` | Updates rely on sparse PATCH payloads. |
| `CreateCopy` | `POST /v1/issues/{issue_id}/copies` | Inserts a copy row with any optional metadata that’s available. |

Requests and responses are described in `app/schemas.py`. They map 1:1 to SQLite
columns, so future schema changes only require updating that module plus the SQL
statements in `app/routers/library.py`. All list responses share the same pagination
shape, which mirrors the Google spec and lets clients loop until `next_page_token`
is empty. The router also enforces typical guide recommendations such as 404-versus-409
distinctions, explicit field masks (implemented via sparse payloads), and nested
resource names so relationships remain obvious in the URI.
