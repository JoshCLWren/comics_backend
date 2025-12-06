import os
import sqlite3
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from main import app


def _create_schema(conn: sqlite3.Connection) -> None:
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
    conn.execute(
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
    conn.execute(
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


def _seed_data(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO series (series_id, title, publisher, age) VALUES (1, 'Alpha', 'ACME', 'Modern')"
    )
    conn.execute(
        "INSERT INTO series (series_id, title, publisher, age) VALUES (2, 'Beta', 'ACME', 'Silver')"
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, cover_year)
        VALUES (1, 1, '1', '', 'Arrival', 2020)
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title)
        VALUES (2, 1, '2', 'A', 'Second')
        """
    )
    conn.execute(
        """
        INSERT INTO copies (
            id, issue_id, custom_label, format, grade, purchase_price,
            purchase_year, my_value, value
        ) VALUES (1, 1, 'Signed', 'Floppy', '9.8', 19.99, 2020, 30.0, 32.0)
        """
    )


@pytest.fixture()
def api_client(tmp_path, monkeypatch) -> Iterator[TestClient]:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    _create_schema(conn)
    _seed_data(conn)
    conn.commit()
    conn.close()

    monkeypatch.setenv("COMICS_DB_PATH", str(db_path))
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()


def test_list_series_paginates(api_client: TestClient):
    resp = api_client.get("/v1/series", params={"page_size": 1})
    assert resp.status_code == 200
    body = resp.json()
    assert body["series"][0]["series_id"] == 1
    assert body["next_page_token"] == "1"

    resp = api_client.get("/v1/series", params={"page_token": body["next_page_token"]})
    assert resp.status_code == 200
    assert resp.json()["series"][0]["series_id"] == 2


def test_create_issue_and_fetch(api_client: TestClient):
    payload = {
        "issue_nr": "3",
        "variant": "",
        "title": "Finale",
        "cover_year": 2025,
    }
    resp = api_client.post("/v1/series/1/issues", json=payload)
    assert resp.status_code == 201
    created = resp.json()
    assert created["issue_nr"] == "3"
    assert created["series_id"] == 1

    resp = api_client.get(f"/v1/series/1/issues/{created['issue_id']}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Finale"


def test_update_copy_flow(api_client: TestClient):
    resp = api_client.patch(
        "/v1/issues/1/copies/1",
        json={"grade": "9.6", "key_flag": "Yes"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["grade"] == "9.6"
    assert body["key_flag"] == "Yes"

    resp = api_client.get("/v1/issues/1/copies", params={"page_size": 1})
    assert resp.status_code == 200
    assert resp.json()["copies"][0]["grade"] == "9.6"


def test_missing_series_returns_404(api_client: TestClient):
    resp = api_client.get("/v1/series/999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "series not found"
