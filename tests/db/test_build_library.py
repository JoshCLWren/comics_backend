"""Tests for the CSV-to-SQLite ETL helpers."""

import sqlite3
import sys
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import build_library as bl


def test_normalize_issue_nr_various_inputs():
    """normalize_issue_nr handles floats, NaN, and arbitrary objects."""
    assert bl.normalize_issue_nr(1.0) == "1"
    assert bl.normalize_issue_nr(0.5) == "0.5"
    assert bl.normalize_issue_nr(float("nan")) == ""
    assert bl.normalize_issue_nr("ABC") == "ABC"

    class Weird:
        def __float__(self):
            raise TypeError("boom")

        def __str__(self):
            return "Weird object"

    assert bl.normalize_issue_nr(Weird()) == "Weird object"


def test_normalize_text_handles_nan():
    """normalize_text coerces NaN to an empty string."""
    assert bl.normalize_text(float("nan")) == ""
    assert bl.normalize_text("value") == "value"


def test_describe_row_handles_multiple_branches():
    """describe_row combines the available identifier fields."""
    primary_row = pd.Series(
        {
            "Series": "Amazing Tales",
            "IssueNrNorm": "1",
            "VariantNorm": "A",
            "Core SeriesID": 123,
            "Core ComicID": 456,
            "Issue Nr": 1,
            "Variant": "B",
            "Title": "Pilot",
        }
    )
    primary_row.name = 7
    text = bl.describe_row(primary_row)
    assert "Amazing Tales issue 1 (variant A)" in text
    assert "CoreSeriesID=123" in text

    variant_row = pd.Series({"Variant": "Foil"})
    variant_row.name = 9
    variant_text = bl.describe_row(variant_row)
    assert variant_text.startswith("variant Foil")
    assert "Variant=Foil" in variant_text

    details_row = pd.Series({"Core SeriesID": 1, "Title": "One-Off"})
    details_row.name = 11
    assert bl.describe_row(details_row) == "index=11, CoreSeriesID=1, Title=One-Off"


def test_log_row_skip_logs_reason(caplog):
    """log_row_skip emits readable messages."""
    row = pd.Series({"Series": "Test"})
    with caplog.at_level("WARNING"):
        bl.log_row_skip("stage", row, "missing data")
    assert "stage: skipped Test - missing data" in caplog.text

    caplog.clear()
    err = ValueError("bad")
    with caplog.at_level("WARNING"):
        bl.log_row_skip("stage", row, "boom", err)
    assert "stage: skipped Test (boom) - bad" in caplog.text


def test_apply_migrations_requires_config(monkeypatch, tmp_path):
    """apply_migrations raises when the Alembic ini file is missing."""
    monkeypatch.setattr(bl, "ALEMBIC_INI_PATH", tmp_path / "missing.ini")
    with pytest.raises(FileNotFoundError):
        bl.apply_migrations(tmp_path / "db.sqlite")


def test_apply_migrations_invokes_alembic(monkeypatch, tmp_path):
    """apply_migrations wires up the alembic upgrade call."""
    cfg_path = tmp_path / "alembic.ini"
    cfg_path.write_text("[alembic]\n")
    monkeypatch.setattr(bl, "ALEMBIC_INI_PATH", cfg_path)

    called = {}

    def fake_upgrade(cfg, target):
        called["cfg"] = cfg
        called["target"] = target

    monkeypatch.setattr(bl.command, "upgrade", fake_upgrade)
    db_path = tmp_path / "library.db"
    bl.apply_migrations(db_path)
    assert called["target"] == "head"
    assert called["cfg"].get_main_option("sqlalchemy.url") == f"sqlite:///{db_path}"


def test_load_csv_normalizes_issue_and_variant(monkeypatch, tmp_path):
    """load_csv adds normalized columns for downstream processing."""
    csv_path = tmp_path / "export.csv"
    csv_path.write_text("Issue Nr,Variant\n1,\n0.5,Special\n")
    monkeypatch.setattr(bl, "CSV_PATH", csv_path)

    df = bl.load_csv()
    assert list(df["IssueNrNorm"]) == ["1", "0.5"]
    assert list(df["VariantNorm"]) == ["", "Special"]


def test_populate_series_inserts_and_skips(caplog):
    """populate_series inserts valid rows and logs the rest."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
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
    df = pd.DataFrame(
        [
            {
                "Core SeriesID": 1,
                "Series": "Alpha",
                "Publisher": "Pub",
                "Series Group": "",
                "Age": "Modern",
            },
            {"Core SeriesID": None},
            {"Core SeriesID": "bad"},
            {
                "Core SeriesID": 1,
                "Series": "Duplicate",
            },
        ]
    )

    with caplog.at_level("WARNING"):
        bl.populate_series(conn, df)

    rows = conn.execute("SELECT series_id, title FROM series").fetchall()
    assert rows == [(1, "Alpha")]
    assert "missing Core SeriesID" in caplog.text
    assert "invalid Core SeriesID" in caplog.text
    assert "duplicate Core SeriesID 1" in caplog.text


def test_populate_series_handles_integrity_error(monkeypatch):
    """populate_series swallows sqlite constraint errors."""
    class DummyCursor:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.IntegrityError("boom")

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

    df = pd.DataFrame([{"Core SeriesID": 5, "Series": "Faulty"}])
    conn = cast(sqlite3.Connection, DummyConn())
    bl.populate_series(conn, df)


def _create_issues_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER,
            issue_nr TEXT,
            variant TEXT,
            title TEXT,
            subtitle TEXT,
            full_title TEXT,
            cover_date TEXT,
            cover_year INTEGER,
            story_arc TEXT
        );
        """
    )


def _create_copies_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE copies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            clz_comic_id INTEGER,
            issue_id INTEGER,
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
            variant_description TEXT
        );
        """
    )


def test_populate_issues_builds_issue_map(caplog):
    """populate_issues inserts normalized issues and logs skips."""
    conn = sqlite3.connect(":memory:")
    _create_issues_table(conn)

    df = pd.DataFrame(
        [
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "1",
                "VariantNorm": "",
                "Title": "One",
                "Subtitle": "Intro",
                "Full Title": "One Intro",
                "Cover Date": "2020-01",
                "Cover Year": "2020",
                "Story Arc": "Arc",
            },
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "1",
                "VariantNorm": "",
            },
            {"Core SeriesID": None},
            {"Core SeriesID": "bad"},
            {
                "Core SeriesID": 2,
                "IssueNrNorm": "2",
                "VariantNorm": "",
                "Cover Year": "n/a",
            },
        ]
    )

    with caplog.at_level("WARNING"):
        issue_map = bl.populate_issues(conn, df)

    rows = conn.execute(
        "SELECT series_id, issue_nr, variant, cover_year FROM issues"
    ).fetchall()
    assert rows[0] == (1, "1", "", 2020)
    assert len(issue_map) == 2
    assert (1, "1", "") in issue_map
    assert "duplicate issue key" in caplog.text
    assert "missing Core SeriesID" in caplog.text
    assert "invalid Core SeriesID" in caplog.text
    assert "cover_year" not in caplog.text


def test_populate_issues_handles_integrity_error():
    """populate_issues swallows sqlite constraint errors."""
    class DummyCursor:
        lastrowid = 0

        def execute(self, *_args, **_kwargs):
            raise sqlite3.IntegrityError("nope")

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

    df = pd.DataFrame(
        [
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "1",
                "VariantNorm": "",
            }
        ]
    )

    conn = cast(sqlite3.Connection, DummyConn())
    bl.populate_issues(conn, df)


def _copies_rows():
    return pd.DataFrame(
        [
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "1",
                "VariantNorm": "",
                "Core ComicID": 101,
                "Custom Label": "Label",
                "Format": "TPB",
                "Grade": "9.8",
                "Grader Notes": "Notes",
                "Grading Company": "CGC",
                "Raw / Slabbed": "Slabbed",
                "Signed by": "Author",
                "Slab Certification Number": "1234",
                "Purchase Date": "2020-01-01",
                "Purchase Price": "19.99",
                "Purchase Store": "LCS",
                "Purchase Year": "2020",
                "Date Sold": "",
                "Price Sold": "29.99",
                "Sold Year": "2021",
                "My Value": "30",
                "CovrPrice Value": "31",
                "Value": "32",
                "Country": "USA",
                "Language": "EN",
                "Age": "Modern",
                "Barcode": "123",
                "Cover Price": "3.99",
                "Page Quality": "White",
                "Key": "Yes",
                "Key Category": "Debut",
                "Key Reason": "First",
                "Label Type": "Blue",
                "No. of Pages": "32",
                "Variant Description": "Regular",
            },
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "2",
                "VariantNorm": "B",
                "Core ComicID": "oops",
                "Purchase Year": "bad",
                "Price Sold": "bad",
                "Sold Year": "bad",
                "My Value": "bad",
                "CovrPrice Value": "bad",
                "Value": "bad",
                "Cover Price": "bad",
                "No. of Pages": "bad",
                "Purchase Price": "bad",
                "Purchase Date": "",
                "Purchase Store": "",
                "Variant Description": "",
            },
            {"Core SeriesID": None},
            {"Core SeriesID": "bad"},
            {
                "Core SeriesID": 3,
                "IssueNrNorm": "1",
                "VariantNorm": "",
            },
        ]
    )


def test_populate_copies_inserts_rows(caplog):
    """populate_copies inserts rows and logs problematic data."""
    conn = sqlite3.connect(":memory:")
    _create_copies_table(conn)

    df = _copies_rows()
    issue_map = {
        (1, "1", ""): 1,
        (1, "2", "B"): 2,
    }

    with caplog.at_level("WARNING"):
        bl.populate_copies(conn, df, issue_map)

    rows = conn.execute(
        """
        SELECT issue_id, purchase_price, price_sold, my_value,
               covrprice_value, value, cover_price, no_of_pages
        FROM copies
        ORDER BY id
        """
    ).fetchall()
    assert rows[0] == (1, 19.99, 29.99, 30.0, 31.0, 32.0, 3.99, 32)
    assert rows[1] == (2, None, None, None, None, None, None, None)
    assert "missing Core SeriesID" in caplog.text
    assert "invalid Core SeriesID" in caplog.text
    assert "issue_id missing" in caplog.text


def test_populate_copies_handles_integrity_error():
    """populate_copies continues after sqlite constraint errors."""
    class DummyCursor:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.IntegrityError("copies")

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

    df = pd.DataFrame(
        [
            {
                "Core SeriesID": 1,
                "IssueNrNorm": "1",
                "VariantNorm": "",
            }
        ]
    )
    issue_map = {(1, "1", ""): 9}
    conn = cast(sqlite3.Connection, DummyConn())
    bl.populate_copies(conn, df, issue_map)


def test_main_happy_path(monkeypatch, tmp_path):
    """main rebuilds the DB and cleans up old artifacts."""
    csv_path = tmp_path / "export.csv"
    csv_path.write_text("Issue Nr,Variant\n1,\n")
    db_path = tmp_path / "library.db"
    db_path.write_text("placeholder")

    monkeypatch.setattr(bl, "CSV_PATH", csv_path)
    monkeypatch.setattr(bl, "DB_PATH", db_path)

    df = pd.DataFrame([{"Issue Nr": 1, "Variant": ""}])
    monkeypatch.setattr(bl, "load_csv", lambda: df)

    apply_called = []
    monkeypatch.setattr(bl, "apply_migrations", lambda path: apply_called.append(path))

    class DummyConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    dummy_conn = DummyConn()
    monkeypatch.setattr(sqlite3, "connect", lambda _: dummy_conn)

    series_called = []
    monkeypatch.setattr(
        bl, "populate_series", lambda conn, frame: series_called.append((conn, frame))
    )

    monkeypatch.setattr(
        bl,
        "populate_issues",
        lambda conn, frame: {("k",): 1},
    )

    copies_called = []

    def fake_populate_copies(conn, frame, issue_map):
        copies_called.append((conn, frame, issue_map))

    monkeypatch.setattr(bl, "populate_copies", fake_populate_copies)

    bl.main()

    assert apply_called == [db_path]
    assert series_called == [(dummy_conn, df)]
    assert copies_called == [(dummy_conn, df, {("k",): 1})]
    assert dummy_conn.closed
    assert not db_path.exists()


def test_main_requires_existing_csv(monkeypatch, tmp_path):
    """main fails fast when the CSV input is missing."""
    monkeypatch.setattr(bl, "CSV_PATH", tmp_path / "missing.csv")
    with pytest.raises(FileNotFoundError):
        bl.main()


def test_module_guard_executes_main(monkeypatch):
    """The CLI guard executes the builder when invoked directly."""
    read_calls = []

    def fake_read_csv(_path):
        read_calls.append(True)
        return pd.DataFrame(columns=pd.Index(["Issue Nr", "Variant", "Core SeriesID"]))

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    real_exists = Path.exists

    def fake_exists(self):
        if str(self).endswith("my_database.db"):
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    monkeypatch.setattr(Path, "unlink", lambda self: None)

    connect_calls = []

    class DummyCursor:
        lastrowid = 1

        def execute(self, *_args, **_kwargs):  # pragma: no cover - stub helper
            return None

    class DummyConn:
        def __init__(self):
            self._cursor = DummyCursor()
            self.closed = False

        def cursor(self):
            return self._cursor

        def commit(self):
            pass

        def close(self):
            self.closed = True

    def fake_connect(*_args, **_kwargs):
        conn = DummyConn()
        connect_calls.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    upgrade_calls = []

    def fake_upgrade(cfg, target):
        upgrade_calls.append((cfg, target))

    monkeypatch.setattr("alembic.command.upgrade", fake_upgrade)

    class DummyConfig:
        def __init__(self, *args, **_kwargs):
            self.attrs = {}

        def set_main_option(self, *_args, **_kwargs):
            pass

        @property
        def attributes(self):
            return self.attrs

    monkeypatch.setattr("alembic.config.Config", DummyConfig)

    import database.build_library as build_library

    build_library.main()

    assert read_calls
    assert connect_calls
    assert upgrade_calls
