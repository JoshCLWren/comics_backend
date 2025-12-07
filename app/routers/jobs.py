"""Routes for querying image upload job status."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app import schemas
from app.jobs import image_jobs

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=schemas.ImageUploadJob)
async def get_job(job_id: str) -> schemas.ImageUploadJob:
    job = image_jobs.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="job not found"
        )
    return job


__all__ = ["router"]
