"""Routes for storing and retrieving comic copy images."""

from __future__ import annotations

import asyncio

import aiosqlite
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)

from app import cache, schemas, storage
from app.db import get_connection
from app.jobs import image_jobs

from . import helpers

router = APIRouter()


async def _build_context(
    conn: aiosqlite.Connection,
    *,
    series_id: int,
    issue_id: int,
    copy_id: int,
    image_type: schemas.ImageType,
) -> storage.ImageContext:
    series = await helpers.fetch_series(conn, series_id)
    issue = await helpers.fetch_issue(conn, series_id, issue_id)
    await helpers.fetch_copy(conn, issue_id, copy_id)
    return storage.ImageContext(
        series_id=series_id,
        series_title=series["title"],
        issue_id=issue_id,
        issue_number=issue["issue_nr"],
        issue_variant=issue["variant"],
        copy_id=copy_id,
        image_type=image_type,
    )


@router.post(
    "/series/{series_id}/issues/{issue_id}/copies/{copy_id}/images",
    response_model=schemas.ImageUploadJob,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_copy_image(
    series_id: int,
    issue_id: int,
    copy_id: int,
    background_tasks: BackgroundTasks,
    image_type: schemas.ImageType = Form(...),
    replace_existing: bool = Form(False),
    file: UploadFile = File(...),
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.ImageUploadJob:
    """Accept an upload, enqueue the async processor, and return the job."""
    original_filename = file.filename
    payload = await file.read()
    await file.close()

    if not payload:
        raise HTTPException(status_code=400, detail="empty image upload")

    context = await _build_context(
        conn,
        series_id=series_id,
        issue_id=issue_id,
        copy_id=copy_id,
        image_type=image_type,
    )
    job = image_jobs.create_job(
        series_id=series_id,
        issue_id=issue_id,
        copy_id=copy_id,
        image_type=image_type,
    )
    background_tasks.add_task(
        _enqueue_image_job,
        job.job_id,
        context,
        payload,
        original_filename,
        replace_existing,
    )
    return job


@router.get(
    "/series/{series_id}/issues/{issue_id}/copies/{copy_id}/images",
    response_model=schemas.ListCopyImagesResponse,
)
async def list_copy_images(
    series_id: int,
    issue_id: int,
    copy_id: int,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> schemas.ListCopyImagesResponse:
    """List any stored images for a copy."""
    context = await _build_context(
        conn,
        series_id=series_id,
        issue_id=issue_id,
        copy_id=copy_id,
        image_type=schemas.ImageType.FRONT,
    )
    images = await storage.list_copy_images(context)
    return schemas.ListCopyImagesResponse(images=images)


@router.delete(
    "/series/{series_id}/issues/{issue_id}/copies/{copy_id}/images/{file_name}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_copy_image(
    series_id: int,
    issue_id: int,
    copy_id: int,
    file_name: str,
    conn: aiosqlite.Connection = Depends(get_connection),
) -> None:
    """Remove a stored image identified by its file name."""
    context = await _build_context(
        conn,
        series_id=series_id,
        issue_id=issue_id,
        copy_id=copy_id,
        image_type=schemas.ImageType.FRONT,
    )
    try:
        removed = await storage.delete_copy_image_by_name(
            context, file_name=file_name
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not removed:
        raise HTTPException(status_code=404, detail="image not found")
    await cache.invalidate_paths(
        [
            f"/series/{series_id}/issues/{issue_id}/copies/{copy_id}/images",
        ]
    )


def _enqueue_image_job(
    job_id: str,
    context: storage.ImageContext,
    payload: bytes,
    original_filename: str | None,
    replace_existing: bool,
) -> None:
    asyncio.run(
        _process_image_job(
            job_id, context, payload, original_filename, replace_existing
        )
    )


async def _process_image_job(
    job_id: str,
    context: storage.ImageContext,
    payload: bytes,
    original_filename: str | None,
    replace_existing: bool,
) -> None:
    image_jobs.mark_in_progress(job_id)
    await cache.invalidate_paths([f"/v1/jobs/{job_id}"])
    try:
        result = await storage.save_copy_image(
            context,
            payload=payload,
            original_filename=original_filename,
        )
        if replace_existing:
            await storage.delete_copy_images_by_type(
                context,
                image_type=context.image_type,
                exclude={result.file_name},
            )
    except Exception as exc:  # pragma: no cover - defensive failure handling
        image_jobs.mark_failed(job_id, str(exc))
        await cache.invalidate_paths([f"/v1/jobs/{job_id}"])
    else:
        image_jobs.mark_completed(job_id, result)
        await cache.invalidate_paths(
            [
                f"/v1/jobs/{job_id}",
                f"/series/{context.series_id}/issues/{context.issue_id}/copies/{context.copy_id}/images",
            ]
        )
