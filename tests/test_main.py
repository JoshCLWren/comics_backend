"""API-level tests for the FastAPI service."""

import sqlite3
import sys
import time
from typing import Iterator

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import db
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
        INSERT INTO series (series_id, title, publisher, series_group, age)
        VALUES (17561, 'The Authority: Prime', 'DC Comics', 'The Authority', 'Modern')
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, cover_year, story_arc)
        VALUES (1, 1, '1', '', 'Arrival', 2020, 'Launch')
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, story_arc)
        VALUES (2, 1, '2', 'A', 'Second', 'Flashback')
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, cover_year, story_arc)
        VALUES (100, 17561, '3', '', 'Part 3', 2008, 'Breach of Trust')
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, cover_year, story_arc)
        VALUES (101, 17561, '5', '', 'Part 5', 2008, 'Breach of Trust')
        """
    )
    conn.execute(
        """
        INSERT INTO issues (issue_id, series_id, issue_nr, variant, title, cover_year, story_arc)
        VALUES (102, 17561, '6', '', 'Part 6', 2008, 'Breach of Trust')
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
def db_path(tmp_path, monkeypatch):
    """Create a fresh temp DB and point COMICS_DB_PATH at it."""
    path = tmp_path / "test.db"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    _create_schema(conn)
    _seed_data(conn)
    conn.commit()
    conn.close()

    monkeypatch.setenv("COMICS_DB_PATH", str(path))
    return path


@pytest.fixture()
def image_root(tmp_path, monkeypatch):
    """Temporary directory for storing uploaded images during tests."""
    root = tmp_path / "images"
    root.mkdir()
    monkeypatch.setenv("COMICS_IMAGE_ROOT", str(root))
    return root


@pytest.fixture()
def api_client(db_path, image_root) -> Iterator[TestClient]:
    """FastAPI TestClient wired to the temp DB."""
    # COMICS_DB_PATH already set by db_path fixture
    client = TestClient(app, raise_server_exceptions=True)
    try:
        yield client
    finally:
        client.close()


def _wait_for_job_completion(api_client: TestClient, job_id: str, timeout: float = 1.0):
    """Poll the job endpoint until it completes or fails."""
    deadline = time.time() + timeout
    last_payload: dict | None = None
    while time.time() < deadline:
        resp = api_client.get(f"/v1/jobs/{job_id}")
        assert resp.status_code == 200
        payload = resp.json()
        if payload["status"] == "completed":
            return payload
        if payload["status"] == "failed":
            pytest.fail(f"job {job_id} failed: {payload['detail']}")
        last_payload = payload
        time.sleep(0.01)
    pytest.fail(
        f"job {job_id} did not finish, last status {last_payload['status'] if last_payload else 'unknown'}"
    )


def test_list_series_paginates(api_client: TestClient):
    """The series endpoint returns deterministic pagination tokens."""
    print("\nTEST: start test_list_series_paginates", file=sys.stderr, flush=True)

    print("TEST: before first GET /v1/series", file=sys.stderr, flush=True)
    resp = api_client.get("/v1/series", params={"page_size": 1})
    print("TEST: after first GET /v1/series", file=sys.stderr, flush=True)

    print(
        f"TEST: first response status = {resp.status_code}", file=sys.stderr, flush=True
    )
    print(f"TEST: first response text = {resp.text}", file=sys.stderr, flush=True)

    assert resp.status_code == 200

    print("TEST: parsing JSON for first page", file=sys.stderr, flush=True)
    body = resp.json()
    print(f"TEST: parsed body = {body}", file=sys.stderr, flush=True)

    print("TEST: checking first page assertions", file=sys.stderr, flush=True)
    assert body["series"][0]["series_id"] == 1
    assert body["next_page_token"] == "1"

    print("TEST: before second GET /v1/series", file=sys.stderr, flush=True)
    resp = api_client.get("/v1/series", params={"page_token": body["next_page_token"]})
    print("TEST: after second GET /v1/series", file=sys.stderr, flush=True)

    print(
        f"TEST: second response status = {resp.status_code}",
        file=sys.stderr,
        flush=True,
    )
    print(f"TEST: second response text = {resp.text}", file=sys.stderr, flush=True)

    assert resp.status_code == 200

    print("TEST: parsing JSON for second page", file=sys.stderr, flush=True)
    body2 = resp.json()
    print(f"TEST: parsed second page body = {body2}", file=sys.stderr, flush=True)

    print("TEST: checking second page assertions", file=sys.stderr, flush=True)
    assert body2["series"][0]["series_id"] == 2

    print("TEST: end test_list_series_paginates", file=sys.stderr, flush=True)


def test_create_issue_and_fetch(api_client: TestClient):
    """Creating an issue makes it available via GET."""
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


def test_upload_and_list_copy_images(api_client: TestClient, image_root):
    """Image uploads enqueue jobs and later list results."""
    resp = api_client.post(
        "/v1/series/1/issues/1/copies/1/images",
        data={"image_type": "front"},
        files={"file": ("front.jpg", b"binarydata", "image/jpeg")},
    )
    assert resp.status_code == 202
    job_body = resp.json()
    assert job_body["status"] == "pending"
    finished = _wait_for_job_completion(api_client, job_body["job_id"])
    assert finished["status"] == "completed"
    assert finished["result"] is not None
    saved_path = image_root / finished["result"]["relative_path"]
    assert saved_path.exists()

    resp = api_client.get("/v1/series/1/issues/1/copies/1/images")
    assert resp.status_code == 200
    listing = resp.json()
    assert len(listing["images"]) == 1
    assert listing["images"][0]["file_name"] == finished["result"]["file_name"]


def test_upload_interior_cover_images_use_snake_case(api_client: TestClient, image_root):
    """Interior cover uploads return snake_case image types in listings."""
    for kind in ("interior_front_cover", "interior_back_cover"):
        resp = api_client.post(
            "/v1/series/1/issues/1/copies/1/images",
            data={"image_type": kind},
            files={"file": (f"{kind}.jpg", b"bytes", "image/jpeg")},
        )
        assert resp.status_code == 202
        _wait_for_job_completion(api_client, resp.json()["job_id"])

    resp = api_client.get("/v1/series/1/issues/1/copies/1/images")
    assert resp.status_code == 200
    returned_types = {image["image_type"] for image in resp.json()["images"]}
    assert {"interior_front_cover", "interior_back_cover"} <= returned_types


def test_replace_copy_image(api_client: TestClient, image_root):
    """Setting replace_existing removes older images of the same type."""
    resp = api_client.post(
        "/v1/series/1/issues/1/copies/1/images",
        data={"image_type": "front"},
        files={"file": ("front.jpg", b"first", "image/jpeg")},
    )
    first_job = _wait_for_job_completion(api_client, resp.json()["job_id"])
    first_file = first_job["result"]["file_name"]
    first_path = image_root / first_job["result"]["relative_path"]
    assert first_path.exists()

    resp = api_client.post(
        "/v1/series/1/issues/1/copies/1/images",
        data={"image_type": "front", "replace_existing": "true"},
        files={"file": ("front_new.jpg", b"second", "image/jpeg")},
    )
    second_job = _wait_for_job_completion(api_client, resp.json()["job_id"])
    second_file = second_job["result"]["file_name"]
    assert second_file != first_file

    resp = api_client.get("/v1/series/1/issues/1/copies/1/images")
    assert resp.status_code == 200
    listing = resp.json()["images"]
    assert len(listing) == 1
    assert listing[0]["file_name"] == second_file
    assert not first_path.exists()


def test_delete_copy_image(api_client: TestClient, image_root):
    """Image delete endpoint removes files and updates listings."""
    resp = api_client.post(
        "/v1/series/1/issues/1/copies/1/images",
        data={"image_type": "front"},
        files={"file": ("front.jpg", b"binarydata", "image/jpeg")},
    )
    job = _wait_for_job_completion(api_client, resp.json()["job_id"])
    file_name = job["result"]["file_name"]
    saved_path = image_root / job["result"]["relative_path"]
    assert saved_path.exists()

    resp = api_client.delete(f"/v1/series/1/issues/1/copies/1/images/{file_name}")
    assert resp.status_code == 204
    assert not saved_path.exists()

    resp = api_client.get("/v1/series/1/issues/1/copies/1/images")
    assert resp.status_code == 200
    assert resp.json()["images"] == []

    resp = api_client.delete(f"/v1/series/1/issues/1/copies/1/images/{file_name}")
    assert resp.status_code == 404


def test_job_lookup_not_found(api_client: TestClient):
    """GET /jobs returns 404 for unknown identifiers."""
    resp = api_client.get("/v1/jobs/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "job not found"


def test_update_copy_flow(api_client: TestClient):
    """Copy PATCH applies updates and validates errors."""
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
    """Fetching a missing series returns a 404."""
    resp = api_client.get("/v1/series/999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "series not found"


def test_root_returns_message(api_client: TestClient):
    """Root endpoint responds with a friendly payload."""
    resp = api_client.get("/")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["message"] == "hello comics world"
    assert "documentation" in payload


def test_series_filters_and_conflict(api_client: TestClient):
    """Series endpoint supports filters and conflict handling."""
    resp = api_client.get(
        "/v1/series", params={"publisher": "ACME", "title_search": "Al"}
    )
    assert resp.status_code == 200
    assert len(resp.json()["series"]) == 1

    new_series = {
        "series_id": 3,
        "title": "Gamma",
        "publisher": "Indie",
        "age": "Golden",
    }
    resp = api_client.post("/v1/series", json=new_series)
    assert resp.status_code == 201
    assert resp.json()["publisher"] == "Indie"

    resp = api_client.get("/v1/series", params={"publisher": "Indie"})
    assert resp.status_code == 200
    assert resp.json()["series"][0]["series_id"] == 3

    resp = api_client.post("/v1/series", json=new_series)
    assert resp.status_code == 409
    assert resp.json()["detail"] == "series 3 already exists"


def test_series_title_search_returns_relevance_order(api_client: TestClient):
    """title_search sorts results by fuzzy similarity instead of alphabetically."""
    variants = [
        {"series_id": 10, "title": "X-Men", "publisher": "Marvel"},
        {"series_id": 11, "title": "Uncanny X-Men", "publisher": "Marvel"},
        {"series_id": 12, "title": "X-Men Red", "publisher": "Marvel"},
    ]
    for payload in variants:
        resp = api_client.post("/v1/series", json=payload)
        assert resp.status_code == 201

    resp = api_client.get(
        "/v1/series",
        params={"title_search": "x-men", "page_size": 5},
    )
    assert resp.status_code == 200
    payload = resp.json()["series"]
    titles = [series["title"] for series in payload]
    assert titles[0] == "X-Men"
    assert {"Uncanny X-Men", "X-Men Red"}.issubset(set(titles[:3]))


def test_series_title_search_ignores_hyphens(api_client: TestClient):
    """title_search treats hyphenated queries the same as condensed terms."""
    resp = api_client.post(
        "/v1/series",
        json={"series_id": 20, "title": "X-Men", "publisher": "Marvel"},
    )
    assert resp.status_code == 201

    hyphenated = api_client.get("/v1/series", params={"title_search": "x-men"})
    condensed = api_client.get("/v1/series", params={"title_search": "xmen"})
    assert hyphenated.status_code == condensed.status_code == 200
    assert hyphenated.json()["series"] == condensed.json()["series"]


def test_series_title_search_handles_partial_tokens(api_client: TestClient):
    """title_search prefers the best partial token matches."""
    xtreme = {
        "series_id": 30,
        "title": "X-Treme X-Men, Vol. 1",
        "publisher": "Marvel",
    }
    xfarce = {
        "series_id": 31,
        "title": "X-Farce",
        "publisher": "Marvel",
    }
    for payload in (xtreme, xfarce):
        resp = api_client.post("/v1/series", json=payload)
        assert resp.status_code == 201

    resp = api_client.get("/v1/series", params={"title_search": "Xtrem"})
    assert resp.status_code == 200

    series_titles = [item["title"] for item in resp.json()["series"]]
    assert series_titles[0] == "X-Treme X-Men, Vol. 1"
    assert all("Farce" not in title for title in series_titles)


def test_issue_search_returns_authority_prime(api_client: TestClient):
    """Global issue search returns issues for matching series titles."""
    resp = api_client.get("/v1/issues", params={"title_search": "authority"})
    assert resp.status_code == 200
    body = resp.json()
    issue_nrs = [item["issue_nr"] for item in body["issues"]]
    assert issue_nrs == ["3", "5", "6"]
    assert all(item["series_id"] == 17561 for item in body["issues"])
    assert body["next_page_token"] is None


def test_issue_search_paginates_results(api_client: TestClient):
    """Issue search honors pagination semantics across the flattened set."""
    first = api_client.get(
        "/v1/issues", params={"title_search": "authority", "page_size": 2}
    )
    assert first.status_code == 200
    first_body = first.json()
    assert [item["issue_nr"] for item in first_body["issues"]] == ["3", "5"]
    assert first_body["next_page_token"] == "2"

    second = api_client.get(
        "/v1/issues",
        params={
            "title_search": "authority",
            "page_size": 2,
            "page_token": first_body["next_page_token"],
        },
    )
    assert second.status_code == 200
    second_body = second.json()
    assert [item["issue_nr"] for item in second_body["issues"]] == ["6"]
    assert second_body["next_page_token"] is None


def test_series_update_and_delete_flow(api_client: TestClient):
    """Series records support PATCH and DELETE operations."""
    resp = api_client.patch(
        "/v1/series/2", json={"title": "Beta Prime", "age": "Bronze"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "Beta Prime"
    assert body["age"] == "Bronze"

    resp = api_client.get("/v1/series/2")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Beta Prime"

    resp = api_client.delete("/v1/series/2")
    assert resp.status_code == 204

    resp = api_client.delete("/v1/series/2")
    assert resp.status_code == 404


def test_series_invalid_page_token(api_client: TestClient):
    """Invalid pagination tokens yield 400 responses."""
    resp = api_client.get("/v1/series", params={"page_token": "-5"})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "invalid page_token"


def test_list_issues_filters_and_missing_series(api_client: TestClient):
    """Issues endpoint filters by story arc and handles 404s."""
    resp = api_client.get("/v1/series/1/issues", params={"story_arc": "Launch"})
    assert resp.status_code == 200
    issues = resp.json()["issues"]
    assert len(issues) == 1
    assert issues[0]["story_arc"] == "Launch"

    resp = api_client.get("/v1/series/999/issues")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "series 999 not found"


def test_issue_conflict_and_variant_normalization(api_client: TestClient):
    """Create issue enforces uniqueness and normalizes variants."""
    payload = {
        "issue_nr": "10",
        "variant": None,
        "title": "Special",
        "story_arc": "Launch",
    }
    resp = api_client.post("/v1/series/1/issues", json=payload)
    assert resp.status_code == 201
    created = resp.json()
    assert created["variant"] == ""

    resp = api_client.post("/v1/series/1/issues", json=payload)
    assert resp.status_code == 409
    assert resp.json()["detail"] == "issue already exists for this series"


def test_update_issue_and_delete(api_client: TestClient):
    """Issue updates persist and delete removes the record."""
    resp = api_client.patch(
        "/v1/series/1/issues/1", json={"title": "Arrival+", "variant": None}
    )
    assert resp.status_code == 200
    assert resp.json()["variant"] == ""

    resp = api_client.get("/v1/series/1/issues/1")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Arrival+"

    resp = api_client.delete("/v1/series/1/issues/1")
    assert resp.status_code == 204

    resp = api_client.delete("/v1/series/1/issues/1")
    assert resp.status_code == 404

    resp = api_client.get("/v1/series/1/issues/1")
    assert resp.status_code == 404

    resp = api_client.patch("/v1/series/1/issues/999", json={"title": "Ghost"})
    assert resp.status_code == 404


def test_copy_crud_and_missing_resources(api_client: TestClient):
    """Copy endpoints support CRUD semantics and 404 handling."""
    resp = api_client.get("/v1/issues/1/copies/999")
    assert resp.status_code == 404

    resp = api_client.get("/v1/issues/999/copies")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "issue 999 not found"

    resp = api_client.post("/v1/issues/999/copies", json={"format": "Digital"})
    assert resp.status_code == 404

    create_payload = {"format": "Digital", "value": 5.5}
    resp = api_client.post("/v1/issues/1/copies", json=create_payload)
    assert resp.status_code == 201
    copy_id = resp.json()["copy_id"]

    resp = api_client.patch(f"/v1/issues/1/copies/{copy_id}", json={"value": 42.0})
    assert resp.status_code == 200
    assert resp.json()["value"] == 42.0

    resp = api_client.delete(f"/v1/issues/1/copies/{copy_id}")
    assert resp.status_code == 204

    resp = api_client.patch(f"/v1/issues/1/copies/{copy_id}", json={"value": 55.0})
    assert resp.status_code == 404

    resp = api_client.delete(f"/v1/issues/1/copies/{copy_id}")
    assert resp.status_code == 404


def test_resolve_db_path_errors_when_missing(tmp_path, monkeypatch):
    """resolve_db_path raises a helpful error when the DB is missing."""
    missing = tmp_path / "nope.db"
    monkeypatch.setenv(db.DB_PATH_ENV_VAR, str(missing))
    with pytest.raises(HTTPException) as exc:
        db.resolve_db_path()
    assert exc.value.status_code == 500
