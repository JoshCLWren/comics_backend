## Database Library

The `db/build_library.py` script rebuilds `my_database.db` from `data/clz_export.csv`.
It now relies on Alembic migrations located under `alembic/`. Whenever you change the
schema, create a new revision with `uv run alembic revision -m "describe change"` and
update the migration script instead of editing the ETL job directly. Rebuild the
database with:

```bash
uv run python db/build_library.py
```
