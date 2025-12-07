"""Helpers for organizing image storage on disk."""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import aiofiles

from app import schemas

DEFAULT_IMAGE_ROOT = Path("collection_images")
IMAGE_ROOT_ENV_VAR = "COMICS_IMAGE_ROOT"
_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass
class ImageContext:
    """Normalized metadata about where an image belongs."""

    series_id: int
    series_title: str | None
    issue_id: int
    issue_number: str | None
    issue_variant: str | None
    copy_id: int
    image_type: schemas.ImageType


def resolve_image_root() -> Path:
    """Return the writable root for storing uploaded images."""

    raw = os.environ.get(IMAGE_ROOT_ENV_VAR)
    root = Path(raw) if raw else DEFAULT_IMAGE_ROOT
    root.mkdir(parents=True, exist_ok=True)
    return root


async def save_copy_image(
    context: ImageContext,
    *,
    payload: bytes,
    original_filename: str | None,
) -> schemas.ComicImage:
    """Persist the given bytes and return the API representation."""

    root = resolve_image_root()
    series_dir = _series_directory(context.series_title, context.series_id)
    issue_dir = _issue_directory(
        context.issue_number, context.issue_variant, context.issue_id
    )
    full_dir = root / series_dir / issue_dir
    full_dir.mkdir(parents=True, exist_ok=True)

    filename = _build_filename(
        copy_id=context.copy_id,
        image_type=context.image_type,
        original_filename=original_filename,
    )
    destination = full_dir / filename
    async with aiofiles.open(destination, "wb") as stream:
        await stream.write(payload)

    return schemas.ComicImage(
        series_id=context.series_id,
        issue_id=context.issue_id,
        copy_id=context.copy_id,
        image_type=context.image_type,
        file_name=filename,
        relative_path=str(destination.relative_to(root)),
    )


async def list_copy_images(context: ImageContext) -> list[schemas.ComicImage]:
    """Return metadata for all stored images for the given copy."""

    root = resolve_image_root()
    series_dir = _series_directory(context.series_title, context.series_id)
    issue_dir = _issue_directory(
        context.issue_number, context.issue_variant, context.issue_id
    )
    full_dir = root / series_dir / issue_dir
    if not full_dir.exists():
        return []

    prefix = f"copy{context.copy_id}_"
    return await asyncio.to_thread(
        _list_images_sync, full_dir, prefix, root, context.copy_id, context
    )


def _list_images_sync(
    directory: Path,
    prefix: str,
    root: Path,
    copy_id: int,
    context: ImageContext,
) -> list[schemas.ComicImage]:
    responses: list[schemas.ComicImage] = []
    for path in sorted(_iter_image_files(directory, prefix)):
        image_type = _parse_image_type(copy_id, path.name)
        if image_type is None:
            continue
        responses.append(
            schemas.ComicImage(
                series_id=context.series_id,
                issue_id=context.issue_id,
                copy_id=context.copy_id,
                image_type=image_type,
                file_name=path.name,
                relative_path=str(path.relative_to(root)),
            )
        )
    return responses


async def delete_copy_images_by_type(
    context: ImageContext,
    *,
    image_type: schemas.ImageType,
    exclude: set[str] | None = None,
) -> int:
    """Delete stored images for the copy filtered by type."""

    root = resolve_image_root()
    series_dir = _series_directory(context.series_title, context.series_id)
    issue_dir = _issue_directory(
        context.issue_number, context.issue_variant, context.issue_id
    )
    full_dir = root / series_dir / issue_dir
    if not full_dir.exists():
        return 0

    prefix = f"copy{context.copy_id}_"
    return await asyncio.to_thread(
        _delete_images_by_type_sync,
        full_dir,
        prefix,
        context.copy_id,
        image_type,
        exclude or set(),
    )


async def delete_copy_image_by_name(
    context: ImageContext, *, file_name: str
) -> bool:
    """Delete a specific stored image if it exists."""

    if not _is_safe_filename(file_name):
        raise ValueError("invalid image file name")

    root = resolve_image_root()
    series_dir = _series_directory(context.series_title, context.series_id)
    issue_dir = _issue_directory(
        context.issue_number, context.issue_variant, context.issue_id
    )
    full_dir = root / series_dir / issue_dir
    if not full_dir.exists():
        return False

    destination = full_dir / file_name
    if not destination.exists():
        return False

    await asyncio.to_thread(destination.unlink)
    return True


def _series_directory(series_title: str | None, series_id: int) -> Path:
    title = series_title or f"series_{series_id}"
    name = _sanitize_component(title, f"series_{series_id}")
    return Path(name)


def _issue_directory(
    issue_number: str | None, issue_variant: str | None, issue_id: int
) -> Path:
    base = issue_number or f"issue_{issue_id}"
    label = _sanitize_component(f"issue_{base}", f"issue_{issue_id}")
    if issue_variant:
        variant = _sanitize_component(issue_variant, f"variant_{issue_id}")
        label = f"{label}_{variant}"
    return Path(label)


def _sanitize_component(raw: str, fallback: str) -> str:
    cleaned = _SAFE_CHARS_RE.sub("_", raw.strip())
    cleaned = cleaned.strip("_")
    return cleaned or fallback


def _build_filename(
    *,
    copy_id: int,
    image_type: schemas.ImageType,
    original_filename: str | None,
) -> str:
    suffix = ""
    if original_filename:
        suffix = Path(original_filename).suffix.lower()
    suffix = suffix or ".bin"
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    token = uuid4().hex[:8]
    return f"copy{copy_id}_{image_type.value}_{timestamp}_{token}{suffix}"


def _iter_image_files(directory: Path, prefix: str) -> Iterable[Path]:
    for entry in directory.iterdir():
        if entry.is_file() and entry.name.startswith(prefix):
            yield entry


def _parse_image_type(copy_id: int, filename: str) -> schemas.ImageType | None:
    prefix = f"copy{copy_id}_"
    if not filename.startswith(prefix):
        return None
    remainder = filename[len(prefix) :]
    parts = remainder.split("_", 1)
    if not parts:
        return None
    image_type_value = parts[0]
    try:
        return schemas.ImageType(image_type_value)
    except ValueError:
        return None


def _delete_images_by_type_sync(
    directory: Path,
    prefix: str,
    copy_id: int,
    target_type: schemas.ImageType,
    exclude: set[str],
) -> int:
    removed = 0
    for path in _iter_image_files(directory, prefix):
        if path.name in exclude:
            continue
        image_type = _parse_image_type(copy_id, path.name)
        if image_type != target_type:
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
    return removed


def _is_safe_filename(name: str) -> bool:
    if not name:
        return False
    path = Path(name)
    if path.is_absolute():
        return False
    if path.name != name:
        return False
    if ".." in path.parts:
        return False
    if "/" in name or "\\" in name:
        return False
    return True
