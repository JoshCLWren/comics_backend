import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

CSV_PATH = Path("./data/clz_export.csv")
DB_PATH = Path("my_database.db")


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


def create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # Drop existing tables so the script is idempotent and reproducible
    cur.execute("DROP TABLE IF EXISTS copies;")
    cur.execute("DROP TABLE IF EXISTS issues;")
    cur.execute("DROP TABLE IF EXISTS series;")

    logger.info("Creating tables: series, issues, copies")
    cur.execute(
        """
        CREATE TABLE series (
            series_id INTEGER PRIMARY KEY,
            title TEXT,
            publisher TEXT,
            series_group TEXT,
            age TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE issues (
            issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            issue_nr TEXT,
            variant TEXT,
            title TEXT,
            subtitle TEXT,
            full_title TEXT,
            cover_date TEXT,
            cover_year INTEGER,
            story_arc TEXT,
            UNIQUE(series_id, issue_nr, variant),
            FOREIGN KEY(series_id) REFERENCES series(series_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE copies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            clz_comic_id INTEGER,
            issue_id INTEGER NOT NULL,
            custom_label TEXT,
            format TEXT,
            grade TEXT,
            grader_notes TEXT,
            grading_company TEXT,
            raw_slabbed TEXT,
            signed_by TEXT,
            slab_cert_number TEXT,
            purchase_date TEXT,
            purchase_price REAL,
            purchase_store TEXT,
            purchase_year INTEGER,
            date_sold TEXT,
            price_sold REAL,
            sold_year INTEGER,
            my_value REAL,
            covrprice_value REAL,
            value REAL,
            country TEXT,
            language TEXT,
            age TEXT,
            barcode TEXT,
            cover_price REAL,
            page_quality TEXT,
            key_flag TEXT,
            key_category TEXT,
            key_reason TEXT,
            label_type TEXT,
            no_of_pages INTEGER,
            variant_description TEXT,
            FOREIGN KEY(issue_id) REFERENCES issues(issue_id)
        );
        """
    )

    conn.commit()


def load_csv() -> pd.DataFrame:
    logger.info("Loading CSV from %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    logger.info("Loaded %d records from CSV", len(df))

    # Normalize Issue Nr to a string field that we will use consistently
    df["IssueNrNorm"] = df["Issue Nr"].apply(normalize_issue_nr)

    # Normalize Variant too
    df["VariantNorm"] = df["Variant"].apply(normalize_text)

    return df


def populate_series(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()

    series_cols = [
        "Core SeriesID",
        "Series",
        "Publisher",
        "Series Group",
        "Age",
    ]

    series_df = df[series_cols].drop_duplicates(subset=["Core SeriesID"])
    logger.info("Populating %d series rows", len(series_df))

    for _, row in series_df.iterrows():
        series_id = int(row["Core SeriesID"])
        title = normalize_text(row["Series"])
        publisher = normalize_text(row["Publisher"])
        series_group = normalize_text(row["Series Group"])
        age = normalize_text(row["Age"])

        cur.execute(
            """
            INSERT INTO series (series_id, title, publisher, series_group, age)
            VALUES (?, ?, ?, ?, ?);
            """,
            (series_id, title, publisher, series_group, age),
        )

    conn.commit()
    logger.info("Finished inserting series rows")


def populate_issues(conn: sqlite3.Connection, df: pd.DataFrame) -> dict:
    """
    Create issues and return a mapping:
        (series_id, issue_nr, variant) -> issue_id
    """
    cur = conn.cursor()

    issue_key_cols = ["Core SeriesID", "IssueNrNorm", "VariantNorm"]

    # Deduplicate issues by series + issue number + variant
    issues_df = df.drop_duplicates(subset=issue_key_cols)
    logger.info("Populating %d unique issues", len(issues_df))

    issue_map = {}

    for _, row in issues_df.iterrows():
        series_id = int(row["Core SeriesID"])
        issue_nr = normalize_text(row["IssueNrNorm"])
        variant = normalize_text(row["VariantNorm"])

        title = normalize_text(row.get("Title"))
        subtitle = normalize_text(row.get("Subtitle"))
        full_title = normalize_text(row.get("Full Title"))
        cover_date = normalize_text(row.get("Cover Date"))

        cover_year_raw = row.get("Cover Year")
        cover_year = None
        if not pd.isna(cover_year_raw):
            try:
                cover_year = int(cover_year_raw)
            except ValueError:
                cover_year = None

        story_arc = normalize_text(row.get("Story Arc"))

        cur.execute(
            """
            INSERT INTO issues (
                series_id, issue_nr, variant,
                title, subtitle, full_title,
                cover_date, cover_year, story_arc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
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

        issue_id = cur.lastrowid
        issue_map[(series_id, issue_nr, variant)] = issue_id

    conn.commit()
    logger.info("Finished inserting issues rows")
    return issue_map


def populate_copies(
    conn: sqlite3.Connection, df: pd.DataFrame, issue_map: dict
) -> None:
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        series_id = int(row["Core SeriesID"])
        issue_nr = normalize_text(row["IssueNrNorm"])
        variant = normalize_text(row["VariantNorm"])

        key = (series_id, issue_nr, variant)
        issue_id = issue_map.get(key)

        if issue_id is None:
            # Should not happen since we just built issue_map from the same df,
            # but guard just in case
            skipped += 1
            logger.warning(
                "Skipping copy for series_id=%s issue_nr=%s variant=%s because issue_id missing",
                series_id,
                issue_nr,
                variant,
            )
            continue

        copy_id = int(row["Core ComicID"])
        clz_comic_id_raw = row.get("Core ComicID")
        clz_comic_id = None
        if not pd.isna(clz_comic_id_raw):
            try:
                clz_comic_id = int(clz_comic_id_raw)
            except ValueError:
                clz_comic_id = None
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

        purchase_year_raw = row.get("Purchase Year")
        purchase_year = None
        if not pd.isna(purchase_year_raw):
            try:
                purchase_year = int(purchase_year_raw)
            except ValueError:
                purchase_year = None

        date_sold = normalize_text(row.get("Date Sold"))

        price_sold_raw = row.get("Price Sold")
        price_sold = None
        if not pd.isna(price_sold_raw):
            try:
                price_sold = float(price_sold_raw)
            except ValueError:
                price_sold = None

        sold_year_raw = row.get("Sold Year")
        sold_year = None
        if not pd.isna(sold_year_raw):
            try:
                sold_year = int(sold_year_raw)
            except ValueError:
                sold_year = None

        my_value_raw = row.get("My Value")
        my_value = None
        if not pd.isna(my_value_raw):
            try:
                my_value = float(my_value_raw)
            except ValueError:
                my_value = None

        covrprice_value_raw = row.get("CovrPrice Value")
        covrprice_value = None
        if not pd.isna(covrprice_value_raw):
            try:
                covrprice_value = float(covrprice_value_raw)
            except ValueError:
                covrprice_value = None

        value_raw = row.get("Value")
        value = None
        if not pd.isna(value_raw):
            try:
                value = float(value_raw)
            except ValueError:
                value = None

        country = normalize_text(row.get("Country"))
        language = normalize_text(row.get("Language"))
        age = normalize_text(row.get("Age"))
        barcode = normalize_text(row.get("Barcode"))

        cover_price_raw = row.get("Cover Price")
        cover_price = None
        if not pd.isna(cover_price_raw):
            try:
                cover_price = float(cover_price_raw)
            except ValueError:
                cover_price = None

        page_quality = normalize_text(row.get("Page Quality"))

        key_flag = normalize_text(row.get("Key"))
        key_category = normalize_text(row.get("Key Category"))
        key_reason = normalize_text(row.get("Key Reason"))
        label_type = normalize_text(row.get("Label Type"))

        no_of_pages_raw = row.get("No. of Pages")
        no_of_pages = None
        if not pd.isna(no_of_pages_raw):
            try:
                no_of_pages = int(no_of_pages_raw)
            except ValueError:
                no_of_pages = None

        variant_description = normalize_text(row.get("Variant Description"))

        purchase_price_raw = row.get("Purchase Price")
        purchase_price = None
        if not pd.isna(purchase_price_raw):
            try:
                purchase_price = float(purchase_price_raw)
            except ValueError:
                purchase_price = None
                clz_comic_id_raw = row.get("Core ComicID")
                clz_comic_id = None
                if not pd.isna(clz_comic_id_raw):
                    try:
                        clz_comic_id = int(clz_comic_id_raw)
                    except ValueError:
                        clz_comic_id = None

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
                    (
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
                    ),
                )
                inserted += 1

    conn.commit()
    logger.info("Inserted %d copies rows (skipped %d)", inserted, skipped)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    logger.info("Starting database build using CSV %s", CSV_PATH)
    df = load_csv()
    conn = sqlite3.connect(DB_PATH)

    try:
        create_schema(conn)
        populate_series(conn, df)
        issue_map = populate_issues(conn, df)
        populate_copies(conn, df, issue_map)
    finally:
        conn.close()

    logger.info("Database created at %s", DB_PATH.resolve())


if __name__ == "__main__":
    main()
