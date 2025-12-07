"""Utilities for building the SQLite library from the CLZ export."""

import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from alembic.config import Config

from alembic import command

logger = logging.getLogger(__name__)

CSV_PATH = Path("./data/clz_export.csv")
DB_PATH = Path("my_database.db")
ALEMBIC_INI_PATH = Path("alembic.ini")


def parse_optional_number(value: Any) -> int | float | None:
    """Try to coerce a value to a numeric type, return None if that fails."""
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return value

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if pd.isna(number):
        return None
    return number


def normalize_issue_nr(value) -> str:
    """
    Normalize Issue Nr into a string so that 1.0 becomes "1",
    0.5 stays "0.5", and NaN becomes "".
    """
    if pd.isna(value):
        return ""
    try:
        f = float(value)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except (TypeError, ValueError):
        return str(value)


def normalize_text(value) -> str:
    """
    Convert NaN or None to empty string, otherwise cast to string.
    """
    if pd.isna(value):
        return ""
    return str(value)


def describe_row(row: pd.Series) -> str:
    """
    Provide a terse identifier for a CSV row so we can log problems clearly.
    """
    series_name = normalize_text(row.get("Series"))

    issue_nr_norm = row.get("IssueNrNorm")
    issue_nr_value = (
        issue_nr_norm if issue_nr_norm not in (None, "") else row.get("Issue Nr")
    )
    issue_nr = normalize_text(issue_nr_value)

    variant_norm = row.get("VariantNorm")
    variant_value = (
        variant_norm if variant_norm not in (None, "") else row.get("Variant")
    )
    variant = normalize_text(variant_value)

    descriptor_parts = []
    if series_name:
        descriptor_parts.append(series_name)
    if issue_nr:
        descriptor_parts.append(f"issue {issue_nr}")
    descriptor = " ".join(descriptor_parts).strip()
    if descriptor and variant:
        descriptor = f"{descriptor} (variant {variant})"
    elif descriptor and not variant:
        descriptor = descriptor
    elif not descriptor and variant:
        descriptor = f"variant {variant}"

    fields = {
        "index": row.name,
        "CoreSeriesID": row.get("Core SeriesID"),
        "CoreComicID": row.get("Core ComicID"),
        "IssueNr": row.get("Issue Nr"),
        "Variant": row.get("Variant"),
        "Title": row.get("Title"),
    }
    details = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        details.append(f"{key}={value}")

    if descriptor and details:
        return f"{descriptor} [{', '.join(details)}]"
    if descriptor:
        return descriptor
    return ", ".join(details) if details else f"index={row.name}"


def log_row_skip(
    stage: str, row: pd.Series, reason: str, error: Optional[Exception] = None
) -> None:
    """
    Emit a warning when a row is not inserted into the database.
    """
    context = describe_row(row)
    if error:
        logger.warning("%s: skipped %s (%s) - %s", stage, context, reason, error)
    else:
        logger.warning("%s: skipped %s - %s", stage, context, reason)


def apply_migrations(db_path: Path) -> None:
    """
    Build or update the SQLite schema using Alembic migrations.
    """
    if not ALEMBIC_INI_PATH.exists():
        raise FileNotFoundError(
            f"Alembic configuration not found at {ALEMBIC_INI_PATH}"
        )

    alembic_cfg = Config(str(ALEMBIC_INI_PATH))
    alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    alembic_cfg.attributes["configure_logger"] = False

    logger.info("Applying Alembic migrations to %s", db_path)
    command.upgrade(alembic_cfg, "head")


def load_csv() -> pd.DataFrame:
    """Load and normalize the CLZ CSV export."""
    logger.info("Loading CSV from %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    logger.info("Loaded %d records from CSV", len(df))

    # Normalize Issue Nr to a string field that we will use consistently
    df["IssueNrNorm"] = df["Issue Nr"].apply(normalize_issue_nr)

    # Normalize Variant too
    df["VariantNorm"] = df["Variant"].apply(normalize_text)

    return df


def populate_series(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert unique series rows extracted from the CSV."""
    cur = conn.cursor()
    cur.execute("SELECT series_id FROM series;")
    existing_series_ids = {row[0] for row in cur.fetchall()}

    total_rows = len(df)
    unique_series = df["Core SeriesID"].nunique(dropna=True)
    logger.info(
        "Processing %d rows to populate up to %d unique series",
        total_rows,
        unique_series,
    )

    inserted = 0
    updated = 0
    skipped = 0
    seen_series_ids: dict[int, str] = {}

    for _, row in df.iterrows():
        series_id_raw = row.get("Core SeriesID")
        if series_id_raw is None or pd.isna(series_id_raw):
            skipped += 1
            log_row_skip("series", row, "missing Core SeriesID")
            continue

        try:
            series_id = int(series_id_raw)
        except (TypeError, ValueError) as exc:
            skipped += 1
            log_row_skip("series", row, "invalid Core SeriesID", exc)
            continue

        if series_id in seen_series_ids:
            skipped += 1
            existing = seen_series_ids[series_id]
            log_row_skip(
                "series",
                row,
                f"duplicate Core SeriesID {series_id} (already inserted from {existing})",
            )
            continue

        title = normalize_text(row.get("Series"))
        publisher = normalize_text(row.get("Publisher"))
        series_group = normalize_text(row.get("Series Group"))
        age = normalize_text(row.get("Age"))

        try:
            cur.execute(
                """
                INSERT INTO series (series_id, title, publisher, series_group, age)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    title=excluded.title,
                    publisher=excluded.publisher,
                    series_group=excluded.series_group,
                    age=excluded.age;
                """,
                (series_id, title, publisher, series_group, age),
            )
        except sqlite3.IntegrityError as exc:
            skipped += 1
            log_row_skip(
                "series",
                row,
                f"constraint violation while inserting series_id={series_id}",
                exc,
            )
            continue

        seen_series_ids[series_id] = describe_row(row)
        if series_id in existing_series_ids:
            updated += 1
        else:
            inserted += 1
            existing_series_ids.add(series_id)

    conn.commit()
    logger.info(
        "Finished upserting series rows (inserted=%d, updated=%d, skipped=%d)",
        inserted,
        updated,
        skipped,
    )


def populate_issues(conn: sqlite3.Connection, df: pd.DataFrame) -> dict:
    """Insert issues and return a lookup keyed by (series_id, issue_nr, variant)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT issue_id, series_id, COALESCE(issue_nr, ''), COALESCE(variant, '')
        FROM issues;
        """
    )
    existing_issues = {
        (row[1], row[2], row[3]): row[0]
        for row in cur.fetchall()
    }

    issue_key_cols = ["Core SeriesID", "IssueNrNorm", "VariantNorm"]
    unique_issue_count = df.drop_duplicates(subset=issue_key_cols).shape[0]
    logger.info(
        "Processing %d rows to populate up to %d unique issues",
        len(df),
        unique_issue_count,
    )

    issue_map = {}
    issue_sources: dict[tuple[int, str, str], str] = {}
    inserted = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        series_id_raw = row.get("Core SeriesID")
        if series_id_raw is None or pd.isna(series_id_raw):
            skipped += 1
            log_row_skip("issues", row, "missing Core SeriesID")
            continue

        try:
            series_id = int(series_id_raw)
        except (TypeError, ValueError) as exc:
            skipped += 1
            log_row_skip("issues", row, "invalid Core SeriesID", exc)
            continue

        issue_nr = normalize_text(row.get("IssueNrNorm", ""))
        variant = normalize_text(row.get("VariantNorm", ""))
        key = (series_id, issue_nr, variant)
        if key in issue_map:
            skipped += 1
            existing = issue_sources.get(key, "previous row")
            log_row_skip(
                "issues",
                row,
                f"duplicate issue key (series_id={series_id}, issue_nr='{issue_nr}', variant='{variant}') already inserted from {existing}",
            )
            continue

        title = normalize_text(row.get("Title"))
        subtitle = normalize_text(row.get("Subtitle"))
        full_title = normalize_text(row.get("Full Title"))
        cover_date = normalize_text(row.get("Cover Date"))

        cover_year_raw = row.get("Cover Year")

        if isinstance(cover_year_raw, (int, float, str)):
            try:
                cover_year = int(cover_year_raw)
            except ValueError:
                cover_year = None
        else:
            cover_year = None

        story_arc = normalize_text(row.get("Story Arc"))

        try:
            cur.execute(
                """
                INSERT INTO issues (
                    series_id, issue_nr, variant,
                    title, subtitle, full_title,
                    cover_date, cover_year, story_arc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_id, issue_nr, variant) DO UPDATE SET
                    title=excluded.title,
                    subtitle=excluded.subtitle,
                    full_title=excluded.full_title,
                    cover_date=excluded.cover_date,
                    cover_year=excluded.cover_year,
                    story_arc=excluded.story_arc;
                """,
                (
                    series_id,
                    issue_nr,
                    variant,
                    title,
                    subtitle,
                    full_title,
                    cover_date,
                    cover_year,
                    story_arc,
                ),
            )
        except sqlite3.IntegrityError as exc:
            skipped += 1
            log_row_skip(
                "issues",
                row,
                f"constraint violation for key (series_id={series_id}, issue_nr='{issue_nr}', variant='{variant}')",
                exc,
            )
            continue

        cur.execute(
            """
            SELECT issue_id FROM issues
            WHERE series_id=? AND issue_nr=? AND variant=?;
            """,
            (series_id, issue_nr, variant),
        )
        result = cur.fetchone()
        if result is None:
            skipped += 1
            log_row_skip(
                "issues",
                row,
                f"unable to locate issue after upsert for key (series_id={series_id}, issue_nr='{issue_nr}', variant='{variant}')",
            )
            continue

        issue_id = result[0]
        issue_map[key] = issue_id
        issue_sources[key] = describe_row(row)
        if key in existing_issues:
            updated += 1
        else:
            inserted += 1
        existing_issues[key] = issue_id

    conn.commit()
    logger.info(
        "Finished upserting issues rows (inserted=%d, updated=%d, skipped=%d)",
        inserted,
        updated,
        skipped,
    )
    return issue_map


def populate_copies(
    conn: sqlite3.Connection, df: pd.DataFrame, issue_map: dict
) -> None:
    """Insert copy rows while referencing the issue map."""
    cur = conn.cursor()
    cur.execute(
        "SELECT id, clz_comic_id FROM copies WHERE clz_comic_id IS NOT NULL;"
    )
    existing_copy_ids = {row[1]: row[0] for row in cur.fetchall()}

    inserted = 0
    updated = 0
    skipped = 0
    processed_clz_ids: set[Any] = set()

    for _, row in df.iterrows():
        series_id_raw = row.get("Core SeriesID")
        if series_id_raw is None or pd.isna(series_id_raw):
            skipped += 1
            log_row_skip("copies", row, "missing Core SeriesID")
            continue

        try:
            series_id = int(series_id_raw)
        except (TypeError, ValueError) as exc:
            skipped += 1
            log_row_skip("copies", row, "invalid Core SeriesID", exc)
            continue

        issue_nr = normalize_text(row.get("IssueNrNorm", ""))
        variant = normalize_text(row.get("VariantNorm", ""))

        key = (series_id, issue_nr, variant)
        issue_id = issue_map.get(key)

        if issue_id is None:
            # Should not happen since we just built issue_map from the same df,
            # but guard just in case
            skipped += 1
            log_row_skip(
                "copies",
                row,
                f"issue_id missing for key (series_id={series_id}, issue_nr='{issue_nr}', variant='{variant}')",
            )
            continue

        clz_comic_id_raw = row.get("Core ComicID")
        clz_comic_id = parse_optional_number(clz_comic_id_raw)
        clz_lookup_key = clz_comic_id
        if isinstance(clz_lookup_key, float) and clz_lookup_key.is_integer():
            clz_lookup_key = int(clz_lookup_key)
        if isinstance(clz_comic_id, float) and clz_comic_id.is_integer():
            clz_comic_id = int(clz_comic_id)
        if clz_lookup_key is not None:
            if clz_lookup_key in processed_clz_ids:
                skipped += 1
                log_row_skip(
                    "copies",
                    row,
                    f"duplicate Core ComicID {clz_lookup_key} encountered in CSV",
                )
                continue
            processed_clz_ids.add(clz_lookup_key)

        custom_label = normalize_text(row.get("Custom Label"))
        fmt = normalize_text(row.get("Format"))
        grade = normalize_text(row.get("Grade"))
        grader_notes = normalize_text(row.get("Grader Notes"))
        grading_company = normalize_text(row.get("Grading Company"))
        raw_slabbed = normalize_text(row.get("Raw / Slabbed"))
        signed_by = normalize_text(row.get("Signed by"))
        slab_cert_number = normalize_text(row.get("Slab Certification Number"))

        purchase_date = normalize_text(row.get("Purchase Date"))
        purchase_store = normalize_text(row.get("Purchase Store"))
        purchase_year = parse_optional_number(row.get("Purchase Year"))

        date_sold = normalize_text(row.get("Date Sold"))
        price_sold = parse_optional_number(row.get("Price Sold"))

        sold_year_raw = row.get("Sold Year")
        sold_year = parse_optional_number(sold_year_raw)

        my_value_raw = row.get("My Value")
        my_value = parse_optional_number(my_value_raw)

        covrprice_value_raw = row.get("CovrPrice Value")
        covrprice_value = parse_optional_number(covrprice_value_raw)

        value_raw = row.get("Value")
        value = parse_optional_number(value_raw)

        country = normalize_text(row.get("Country"))
        language = normalize_text(row.get("Language"))
        age = normalize_text(row.get("Age"))
        barcode = normalize_text(row.get("Barcode"))

        cover_price_raw = row.get("Cover Price")
        cover_price = parse_optional_number(cover_price_raw)

        page_quality = normalize_text(row.get("Page Quality"))

        key_flag = normalize_text(row.get("Key"))
        key_category = normalize_text(row.get("Key Category"))
        key_reason = normalize_text(row.get("Key Reason"))
        label_type = normalize_text(row.get("Label Type"))

        no_of_pages_raw = row.get("No. of Pages")
        no_of_pages = parse_optional_number(no_of_pages_raw)

        variant_description = normalize_text(row.get("Variant Description"))

        purchase_price_raw = row.get("Purchase Price")
        purchase_price = parse_optional_number(purchase_price_raw)

        copy_values = (
            clz_comic_id,
            issue_id,
            custom_label,
            fmt,
            grade,
            grader_notes,
            grading_company,
            raw_slabbed,
            signed_by,
            slab_cert_number,
            purchase_date,
            purchase_price,
            purchase_store,
            purchase_year,
            date_sold,
            price_sold,
            sold_year,
            my_value,
            covrprice_value,
            value,
            country,
            language,
            age,
            barcode,
            cover_price,
            page_quality,
            key_flag,
            key_category,
            key_reason,
            label_type,
            no_of_pages,
            variant_description,
        )

        copy_id = (
            existing_copy_ids.get(clz_lookup_key)
            if clz_lookup_key is not None
            else None
        )

        if copy_id is not None:
            try:
                cur.execute(
                    """
                    UPDATE copies SET
                        clz_comic_id=?, issue_id=?,
                        custom_label=?, format=?, grade=?,
                        grader_notes=?, grading_company=?,
                        raw_slabbed=?, signed_by=?, slab_cert_number=?,
                        purchase_date=?, purchase_price=?, purchase_store=?, purchase_year=?,
                        date_sold=?, price_sold=?, sold_year=?,
                        my_value=?, covrprice_value=?, value=?,
                        country=?, language=?, age=?, barcode=?,
                        cover_price=?, page_quality=?,
                        key_flag=?, key_category=?, key_reason=?, label_type=?,
                        no_of_pages=?, variant_description=?
                    WHERE id=?;
                    """,
                    copy_values + (copy_id,),
                )
            except sqlite3.IntegrityError as exc:
                skipped += 1
                log_row_skip(
                    "copies",
                    row,
                    f"constraint violation while updating copy clz_comic_id={clz_lookup_key}",
                    exc,
                )
                continue
            updated += 1
        else:
            try:
                cur.execute(
                    """
                    INSERT INTO copies (
                        clz_comic_id, issue_id,
                        custom_label, format, grade,
                        grader_notes, grading_company,
                        raw_slabbed, signed_by, slab_cert_number,
                        purchase_date, purchase_price, purchase_store, purchase_year,
                        date_sold, price_sold, sold_year,
                        my_value, covrprice_value, value,
                        country, language, age, barcode,
                        cover_price, page_quality,
                        key_flag, key_category, key_reason, label_type,
                        no_of_pages, variant_description
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    copy_values,
                )
            except sqlite3.IntegrityError as exc:
                skipped += 1
                log_row_skip(
                    "copies", row, "constraint violation while inserting copy", exc
                )
                continue

            inserted += 1
            if clz_lookup_key is not None:
                existing_copy_ids[clz_lookup_key] = cur.lastrowid

    conn.commit()
    logger.info(
        "Upserted copies rows (inserted=%d, updated=%d, skipped=%d)",
        inserted,
        updated,
        skipped,
    )


def main() -> None:
    """Command-line entry point to rebuild the SQLite database."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    logger.info("Starting database build using CSV %s", CSV_PATH)
    df = load_csv()
    if DB_PATH.exists():
        logger.info("Updating existing database at %s", DB_PATH)
    else:
        logger.info("Creating new database at %s", DB_PATH)

    apply_migrations(DB_PATH)
    conn = sqlite3.connect(DB_PATH)

    try:
        populate_series(conn, df)
        issue_map = populate_issues(conn, df)
        populate_copies(conn, df, issue_map)
    finally:
        conn.close()

    logger.info("Database updated at %s", DB_PATH.resolve())


if __name__ == "__main__":
    main()
