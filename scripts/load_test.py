#!/usr/bin/env python
"""Simple load test that replays CSV data through the API."""
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger("load_test")


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


class LoadTester:
    def __init__(
        self,
        base_url: str,
        csv_rows: list[dict[str, str]],
        concurrency: int,
        timeout: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rows = csv_rows
        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.stats: Counter[str] = Counter()

    async def run(self) -> None:
        logger.info(
            "starting load test against %s for %s rows", self.base_url, len(self.rows)
        )
        async with httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout
        ) as client:
            self.client = client
            await self._warmup()
            tasks = [
                asyncio.create_task(self._bounded_process(idx, row))
                for idx, row in enumerate(self.rows, start=1)
            ]
            await asyncio.gather(*tasks)
        self._print_summary()

    async def _warmup(self) -> None:
        try:
            response = await self.client.get("/")
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - best effort check
            logger.error(
                "unable to reach API at %s (%s)", self.base_url, exc
            )
            raise SystemExit(1) from exc

    async def _bounded_process(self, idx: int, row: dict[str, str]) -> None:
        async with self.sem:
            try:
                await self.process_row(idx, row)
            except Exception:  # pragma: no cover - load script best effort
                logger.exception("row %s failed", idx)
                self.stats["failures"] += 1

    async def process_row(self, idx: int, row: dict[str, str]) -> None:
        series_payload = build_series_payload(row)
        if not series_payload:
            self.stats["series_skipped"] += 1
            return
        issue_payload = build_issue_payload(row)
        if not issue_payload:
            self.stats["issue_skipped"] += 1
            return
        copy_payload = build_copy_payload(row)

        series = await self.ensure_series(series_payload)
        await self.exercise_series(series_payload["series_id"], series)

        issue = await self.ensure_issue(series["series_id"], issue_payload)
        await self.exercise_issue(series["series_id"], issue, issue_payload)

        copy = await self.create_copy(issue["issue_id"], copy_payload)
        await self.exercise_copies(issue["issue_id"], copy, copy_payload)

        self.stats["rows_processed"] += 1
        if idx % 25 == 0:
            logger.info("processed %s rows so far", idx)

    async def ensure_series(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self.client.post("/v1/series", json=payload)
        if response.status_code == 201:
            self.stats["series_created"] += 1
            return response.json()
        if response.status_code == 409:
            existing = await self.client.get(f"/v1/series/{payload['series_id']}")
            existing.raise_for_status()
            self.stats["series_reused"] += 1
            return existing.json()
        response.raise_for_status()
        return response.json()

    async def ensure_issue(
        self, series_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        response = await self.client.post(
            f"/v1/series/{series_id}/issues", json=payload
        )
        if response.status_code == 201:
            self.stats["issues_created"] += 1
            return response.json()
        if response.status_code == 409:
            issue = await self.find_issue(series_id, payload["issue_nr"], payload["variant"])
            if issue:
                self.stats["issues_reused"] += 1
                return issue
        response.raise_for_status()
        return response.json()

    async def find_issue(
        self, series_id: int, issue_nr: str, variant: str
    ) -> dict[str, Any] | None:
        page_token: str | None = None
        while True:
            response = await self.client.get(
                f"/v1/series/{series_id}/issues",
                params={"page_size": 100, "page_token": page_token},
            )
            response.raise_for_status()
            payload = response.json()
            for issue in payload["issues"]:
                if issue["issue_nr"] == issue_nr and issue["variant"] == variant:
                    return issue
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return None

    async def exercise_series(self, series_id: int, payload: dict[str, Any]) -> None:
        await self.client.get("/v1/series", params={"page_size": 5})
        await self.client.get(f"/v1/series/{series_id}")
        update = self._series_update_payload(payload)
        if update:
            await self.client.patch(f"/v1/series/{series_id}", json=update)

    def _series_update_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        for field in ("title", "publisher", "series_group", "age"):
            value = payload.get(field)
            if value:
                return {field: value}
        return None

    async def exercise_issue(
        self,
        series_id: int,
        issue: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        await self.client.get(
            f"/v1/series/{series_id}/issues", params={"page_size": 5}
        )
        await self.client.get(
            f"/v1/series/{series_id}/issues/{issue['issue_id']}"
        )
        update = self._issue_update_payload(payload, issue)
        if update:
            await self.client.patch(
                f"/v1/series/{series_id}/issues/{issue['issue_id']}",
                json=update,
            )

    def _issue_update_payload(
        self, payload: dict[str, Any], issue: dict[str, Any]
    ) -> dict[str, Any] | None:
        for field in ("title", "full_title", "story_arc", "subtitle"):
            value = payload.get(field) or issue.get(field)
            if value:
                return {field: value}
        return None

    async def create_copy(
        self, issue_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        response = await self.client.post(
            f"/v1/issues/{issue_id}/copies", json=payload
        )
        response.raise_for_status()
        self.stats["copies_created"] += 1
        return response.json()

    async def exercise_copies(
        self,
        issue_id: int,
        copy: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        await self.client.get(
            f"/v1/issues/{issue_id}/copies", params={"page_size": 5}
        )
        await self.client.get(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}"
        )
        update = self._copy_update_payload(payload, copy)
        await self.client.patch(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}",
            json=update,
        )
        await self.client.delete(
            f"/v1/issues/{issue_id}/copies/{copy['copy_id']}"
        )
        self.stats["copies_deleted"] += 1

    def _copy_update_payload(
        self, payload: dict[str, Any], copy: dict[str, Any]
    ) -> dict[str, Any]:
        label = payload.get("custom_label") or f"LoadTest Copy {copy['copy_id']}"
        return {"custom_label": label}

    def _print_summary(self) -> None:
        logger.info("load test summary:")
        for key, value in sorted(self.stats.items()):
            logger.info("  %s: %s", key, value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay CSV rows against the API.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("data/clz_export.csv"),
        help="Path to the CLZ export file.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="FastAPI server base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of rows to replay (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Concurrent requests to issue (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: %(default)s)",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    rows = rows_from_csv(args.csv, args.limit)
    tester = LoadTester(
        base_url=args.base_url,
        csv_rows=rows,
        concurrency=max(1, args.concurrency),
        timeout=args.timeout,
    )
    await tester.run()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
