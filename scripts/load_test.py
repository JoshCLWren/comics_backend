#!/usr/bin/env python
"""Locust-powered load test that replays CSV data through the API."""

from __future__ import annotations

import csv
import logging
import os
from collections import Counter
from pathlib import Path
from typing import Any

from gevent.lock import Semaphore
from gevent.queue import Empty, Queue
from locust import HttpUser, between, events, task
from locust.exception import StopUser

logger = logging.getLogger("load_test.locust")

DEFAULT_CSV_PATH = Path("data") / "clz_export.csv"
CSV_PATH = Path(os.environ.get("LOAD_TEST_CSV_PATH", str(DEFAULT_CSV_PATH))).resolve()


def _parse_env_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid value for {name}: {value!r}") from exc


def _parse_env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid value for {name}: {value!r}") from exc


ROW_LIMIT = _parse_env_int("LOAD_TEST_ROW_LIMIT")
REQUEST_TIMEOUT = _parse_env_float("LOAD_TEST_TIMEOUT", 30.0)

ROW_QUEUE: Queue[tuple[int, dict[str, str]]] | None = None
TOTAL_ROWS = 0
STATS: Counter[str] = Counter()
STATS_LOCK = Semaphore()


def clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_int(value: str | None) -> int | None:
    normalized = clean(value)
    if not normalized:
        return None
    normalized = normalized.replace(",", "")
    try:
        return int(normalized)
    except ValueError:
        return None


def parse_float(value: str | None) -> float | None:
    normalized = clean(value)
    if not normalized:
        return None
    normalized = (
        normalized.replace("$", "")
        .replace(",", "")
        .replace("USD", "")
        .replace("US$", "")
        .strip()
    )
    try:
        return float(normalized)
    except ValueError:
        return None


def build_series_payload(row: dict[str, str]) -> dict[str, Any] | None:
    series_id = parse_int(row.get("Core SeriesID"))
    title = clean(row.get("Series"))
    publisher = clean(row.get("Publisher"))
    if series_id is None or not title or not publisher:
        return None
    return {
        "series_id": series_id,
        "title": title,
        "publisher": publisher,
        "series_group": clean(row.get("Series Group")),
        "age": clean(row.get("Age")),
    }


def build_issue_payload(row: dict[str, str]) -> dict[str, Any] | None:
    issue_nr = clean(row.get("Issue Nr")) or clean(row.get("Issue"))
    if not issue_nr:
        return None
    return {
        "issue_nr": issue_nr,
        "variant": clean(row.get("Variant")) or "",
        "title": clean(row.get("Title")),
        "subtitle": clean(row.get("Subtitle")),
        "full_title": clean(row.get("Full Title")),
        "cover_date": clean(row.get("Cover Date")),
        "cover_year": parse_int(row.get("Cover Year")),
        "story_arc": clean(row.get("Story Arc")),
    }


def build_copy_payload(row: dict[str, str]) -> dict[str, Any]:
    return {
        "clz_comic_id": parse_int(row.get("Core ComicID")),
        "custom_label": clean(row.get("Custom Label")),
        "format": clean(row.get("Format")),
        "grade": clean(row.get("Grade")),
        "grader_notes": clean(row.get("Grader Notes")),
        "grading_company": clean(row.get("Grading Company")),
        "raw_slabbed": clean(row.get("Raw / Slabbed")),
        "signed_by": clean(row.get("Signed by")),
        "slab_cert_number": clean(row.get("Slab Certification Number")),
        "purchase_date": clean(row.get("Purchase Date")),
        "purchase_price": parse_float(row.get("Purchase Price")),
        "purchase_store": clean(row.get("Purchase Store")),
        "purchase_year": parse_int(row.get("Purchase Year")),
        "date_sold": clean(row.get("Date Sold")),
        "price_sold": parse_float(row.get("Price Sold")),
        "sold_year": parse_int(row.get("Sold Year")),
        "my_value": parse_float(row.get("My Value")),
        "covrprice_value": parse_float(row.get("CovrPrice Value")),
        "value": parse_float(row.get("Value")),
        "country": clean(row.get("Country")),
        "language": clean(row.get("Language")),
        "age": clean(row.get("Age")),
        "barcode": clean(row.get("Barcode")),
        "cover_price": parse_float(row.get("Cover Price")),
        "page_quality": clean(row.get("Page Quality")),
        "key_flag": clean(row.get("Key")),
        "key_category": clean(row.get("Key Category")),
        "key_reason": clean(row.get("Key Reason")),
        "label_type": clean(row.get("Label Type")),
        "no_of_pages": parse_int(row.get("No. of Pages")),
        "variant_description": clean(row.get("Variant Description")),
    }


def rows_from_csv(csv_path: Path, limit: int | None) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for idx, row in enumerate(reader):
            if limit is not None and idx >= limit:
                break
            rows.append(row)
    return rows


def record_stat(name: str, value: int = 1) -> None:
    with STATS_LOCK:
        STATS[name] += value


def next_row() -> tuple[int, dict[str, str]] | None:
    if ROW_QUEUE is None:
        return None
    try:
        return ROW_QUEUE.get_nowait()
    except Empty:
        return None


@events.init.add_listener
def _configure_dataset(environment, **_kwargs) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    rows = rows_from_csv(CSV_PATH, ROW_LIMIT)
    if not rows:
        raise RuntimeError(f"No rows available in {CSV_PATH}")
    global ROW_QUEUE, TOTAL_ROWS
    ROW_QUEUE = Queue(len(rows))
    for item in enumerate(rows, start=1):
        ROW_QUEUE.put_nowait(item)
    TOTAL_ROWS = len(rows)
    logger.info(
        "prepared %s rows from %s (limit=%s)",
        TOTAL_ROWS,
        CSV_PATH,
        ROW_LIMIT or "all",
    )


@events.test_stop.add_listener
def _log_summary(environment, **_kwargs) -> None:
    if not STATS:
        return
    logger.info("load test summary:")
    for key, value in sorted(STATS.items()):
        logger.info("  %s: %s", key, value)


class LibraryLoadUser(HttpUser):
    wait_time = between(0.01, 0.1)
    request_timeout = REQUEST_TIMEOUT

    def on_start(self) -> None:
        self._warmup()

    def _warmup(self) -> None:
        with self.client.get(
            "/",
            name="root:get",
            timeout=self.request_timeout,
            catch_response=True,
        ) as response:
            if response.status_code >= 400:
                response.failure("API unavailable")
                raise StopUser()
            response.success()

    @task
    def replay_catalog_row(self) -> None:
        item = next_row()
        if item is None:
            raise StopUser()
        idx, row = item
        try:
            self.process_row(idx, row)
        except Exception:  # pragma: no cover - load generator best effort
            logger.exception("row %s failed", idx)
            record_stat("failures")

    def process_row(self, idx: int, row: dict[str, str]) -> None:
        series_payload = build_series_payload(row)
        if not series_payload:
            record_stat("series_skipped")
            return
        issue_payload = build_issue_payload(row)
        if not issue_payload:
            record_stat("issue_skipped")
            return
        copy_payload = build_copy_payload(row)

        series = self.ensure_series(series_payload)
        self.exercise_series(series_payload["series_id"], series)

        issue = self.ensure_issue(series["series_id"], issue_payload)
        self.exercise_issue(series["series_id"], issue, issue_payload)

        copy = self.create_copy(issue["issue_id"], copy_payload)
        self.exercise_copies(issue["issue_id"], copy, copy_payload)

        record_stat("rows_processed")
        if TOTAL_ROWS and idx % 25 == 0:
            logger.info("processed %s/%s rows", idx, TOTAL_ROWS)

    def ensure_series(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.client.post(
            "/v1/series",
            json=payload,
            name="series:create",
            timeout=self.request_timeout,
            catch_response=True,
        ) as response:
            if response.status_code == 201:
                record_stat("series_created")
                return response.json()
            if response.status_code == 409:
                response.success()
                existing = self.client.get(
                    f"/v1/series/{payload['series_id']}",
                    name="series:get",
                    timeout=self.request_timeout,
                )
                existing.raise_for_status()
                record_stat("series_reused")
                return existing.json()
            response.failure(f"unexpected status {response.status_code}")
            raise RuntimeError(
                f"failed to create series {payload['series_id']}: {response.text}"
            )

    def ensure_issue(self, series_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        with self.client.post(
            f"/v1/series/{series_id}/issues",
            json=payload,
            name="issues:create",
            timeout=self.request_timeout,
            catch_response=True,
        ) as response:
            if response.status_code == 201:
                record_stat("issues_created")
                return response.json()
            if response.status_code == 409:
                response.success()
                issue = self.find_issue(
                    series_id, payload["issue_nr"], payload["variant"]
                )
                if issue:
                    record_stat("issues_reused")
                    return issue
                response.failure("issue conflict but issue missing")
                raise RuntimeError(
                    f"conflict without match for issue {payload['issue_nr']}"
                )
            response.failure(f"unexpected status {response.status_code}")
            raise RuntimeError(
                f"failed to create issue {payload['issue_nr']}: {response.text}"
            )

    def find_issue(
        self, series_id: int, issue_nr: str, variant: str
    ) -> dict[str, Any] | None:
        page_token: str | None = None
        while True:
            response = self.client.get(
                f"/v1/series/{series_id}/issues",
                params={"page_size": 100, "page_token": page_token},
                name="issues:list",
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            payload = response.json()
            for issue in payload.get("issues", []):
                if issue["issue_nr"] == issue_nr and issue["variant"] == variant:
                    return issue
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return None

    def exercise_series(self, series_id: int, payload: dict[str, Any]) -> None:
        self.client.get(
            "/v1/series",
            params={"page_size": 5},
            name="series:list",
            timeout=self.request_timeout,
        )
        self.client.get(
            f"/v1/series/{series_id}",
            name="series:get",
            timeout=self.request_timeout,
        )
        update = self._series_update_payload(payload)
        if update:
            self.client.patch(
                f"/v1/series/{series_id}",
                json=update,
                name="series:update",
                timeout=self.request_timeout,
            )

    def _series_update_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        for field in ("title", "publisher", "series_group", "age"):
            value = payload.get(field)
            if value:
                return {field: value}
        return None

    def exercise_issue(
        self,
        series_id: int,
        issue: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        self.client.get(
            f"/v1/series/{series_id}/issues",
            params={"page_size": 5},
            name="issues:list",
            timeout=self.request_timeout,
        )
        self.client.get(
            f"/v1/series/{series_id}/issues/{issue['issue_id']}",
            name="issues:get",
            timeout=self.request_timeout,
        )
        update = self._issue_update_payload(payload, issue)
        if update:
            self.client.patch(
                f"/v1/series/{series_id}/issues/{issue['issue_id']}",
                json=update,
                name="issues:update",
                timeout=self.request_timeout,
            )

    def _issue_update_payload(
        self, payload: dict[str, Any], issue: dict[str, Any]
    ) -> dict[str, Any] | None:
        for field in ("title", "full_title", "story_arc", "subtitle"):
            value = payload.get(field) or issue.get(field)
            if value:
                return {field: value}
        return None

    def create_copy(self, issue_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.client.post(
            f"/v1/issues/{issue_id}/copies",
            json=payload,
            name="copies:create",
            timeout=self.request_timeout,
        )
        response.raise_for_status()
        record_stat("copies_created")
        return response.json()

    def exercise_copies(
        self,
        issue_id: int,
        copy: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        self.client.get(
            f"/v1/issues/{issue_id}/copies",
            params={"page_size": 5},
            name="copies:list",
            timeout=self.request_timeout,
        )
        self.client.get(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}",
            name="copies:get",
            timeout=self.request_timeout,
        )
        update = self._copy_update_payload(payload, copy)
        self.client.patch(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}",
            json=update,
            name="copies:update",
            timeout=self.request_timeout,
        )
        self.client.delete(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}",
            name="copies:delete",
            timeout=self.request_timeout,
        )
        record_stat("copies_deleted")

    def _copy_update_payload(
        self, payload: dict[str, Any], copy: dict[str, Any]
    ) -> dict[str, Any]:
        label = payload.get("custom_label") or f"LoadTest Copy {copy['copy_id']}"
        return {"custom_label": label}
