"""In-memory tracking for background image upload jobs."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Dict
from uuid import uuid4

from app import schemas


@dataclass
class _ImageJobRecord:
    job_id: str
    series_id: int
    issue_id: int
    copy_id: int
    image_type: schemas.ImageType
    status: schemas.JobStatus = schemas.JobStatus.PENDING
    detail: str | None = None
    result: schemas.ComicImage | None = None


class ImageJobManager:
    """Simple in-memory manager for upload jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, _ImageJobRecord] = {}
        self._lock = Lock()

    def create_job(
        self,
        *,
        series_id: int,
        issue_id: int,
        copy_id: int,
        image_type: schemas.ImageType,
    ) -> schemas.ImageUploadJob:
        record = _ImageJobRecord(
            job_id=uuid4().hex,
            series_id=series_id,
            issue_id=issue_id,
            copy_id=copy_id,
            image_type=image_type,
        )
        with self._lock:
            self._jobs[record.job_id] = record
        return self._serialize(record)

    def mark_in_progress(self, job_id: str) -> None:
        with self._lock:
            record = self._require(job_id)
            record.status = schemas.JobStatus.IN_PROGRESS
            record.detail = None

    def mark_completed(self, job_id: str, result: schemas.ComicImage) -> None:
        with self._lock:
            record = self._require(job_id)
            record.status = schemas.JobStatus.COMPLETED
            record.detail = None
            record.result = result

    def mark_failed(self, job_id: str, detail: str) -> None:
        with self._lock:
            record = self._require(job_id)
            record.status = schemas.JobStatus.FAILED
            record.detail = detail

    def get_job(self, job_id: str) -> schemas.ImageUploadJob | None:
        with self._lock:
            record = self._jobs.get(job_id)
            if not record:
                return None
            return self._serialize(record)

    def _require(self, job_id: str) -> _ImageJobRecord:
        record = self._jobs.get(job_id)
        if not record:
            raise KeyError(job_id)
        return record

    def _serialize(self, record: _ImageJobRecord) -> schemas.ImageUploadJob:
        return schemas.ImageUploadJob(
            job_id=record.job_id,
            series_id=record.series_id,
            issue_id=record.issue_id,
            copy_id=record.copy_id,
            image_type=record.image_type,
            status=record.status,
            detail=record.detail,
            result=record.result,
        )


image_jobs = ImageJobManager()

__all__ = ["image_jobs", "ImageJobManager"]
