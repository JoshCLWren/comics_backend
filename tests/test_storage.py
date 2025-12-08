"""Tests covering image storage helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from app import schemas, storage


@pytest.fixture()
def image_root(tmp_path, monkeypatch):
    """Temporary directory backing COMICS_IMAGE_ROOT."""
    root = tmp_path / "images"
    root.mkdir()
    monkeypatch.setenv("COMICS_IMAGE_ROOT", str(root))
    return root


def _build_context(
    *,
    series_id: int = 1,
    series_title: str = "Sample Series",
    issue_id: int = 1,
    issue_number: str = "1",
    issue_variant: str | None = None,
    copy_id: int = 1,
    image_type: schemas.ImageType = schemas.ImageType.FRONT,
) -> storage.ImageContext:
    return storage.ImageContext(
        series_id=series_id,
        series_title=series_title,
        issue_id=issue_id,
        issue_number=issue_number,
        issue_variant=issue_variant,
        copy_id=copy_id,
        image_type=image_type,
    )


@pytest.mark.asyncio()
async def test_delete_copy_image_by_name_cleans_empty_issue_and_series(image_root):
    """Test to seee if deletes work"""
    context = _build_context(series_id=20, issue_id=30, copy_id=5)
    stored = await storage.save_copy_image(
        context, payload=b"payload", original_filename="front.jpg"
    )

    root = storage.resolve_image_root()
    issue_path = root / Path(stored.relative_path).parent
    series_path = issue_path.parent
    assert issue_path.exists()
    assert series_path.exists()

    removed = await storage.delete_copy_image_by_name(
        context, file_name=stored.file_name
    )
    assert removed is True
    assert not issue_path.exists()
    assert not series_path.exists()


@pytest.mark.asyncio()
async def test_delete_copy_images_by_type_removes_series_when_last_issue_deleted(
    image_root,
):
    """Another delete tests case"""
    context_one = _build_context(
        series_id=77, issue_id=101, issue_number="1", copy_id=1
    )
    context_two = _build_context(
        series_id=77, issue_id=102, issue_number="2", copy_id=2
    )

    stored_one = await storage.save_copy_image(
        context_one, payload=b"one", original_filename="front.jpg"
    )
    stored_two = await storage.save_copy_image(
        context_two, payload=b"two", original_filename="front.jpg"
    )

    root = storage.resolve_image_root()
    issue_one_path = root / Path(stored_one.relative_path).parent
    issue_two_path = root / Path(stored_two.relative_path).parent
    series_path = issue_one_path.parent

    removed_one = await storage.delete_copy_images_by_type(
        context_one, image_type=schemas.ImageType.FRONT
    )
    assert removed_one == 1
    assert not issue_one_path.exists()
    assert issue_two_path.exists()
    assert series_path.exists()

    removed_two = await storage.delete_copy_images_by_type(
        context_two, image_type=schemas.ImageType.FRONT
    )
    assert removed_two == 1
    assert not issue_two_path.exists()
    assert not series_path.exists()


@pytest.mark.asyncio()
async def test_list_copy_images_preserves_snake_case_types(image_root):
    """Another test case!"""
    copy_context = _build_context(copy_id=5)
    for image_type in (
        schemas.ImageType.FRONT,
        schemas.ImageType.INTERIOR_FRONT_COVER,
        schemas.ImageType.INTERIOR_BACK_COVER,
    ):
        await storage.save_copy_image(
            _build_context(copy_id=5, image_type=image_type),
            payload=image_type.value.encode(),
            original_filename=f"{image_type.value}.jpg",
        )

    images = await storage.list_copy_images(copy_context)
    assert {
        schemas.ImageType.FRONT,
        schemas.ImageType.INTERIOR_FRONT_COVER,
        schemas.ImageType.INTERIOR_BACK_COVER,
    } <= {image.image_type for image in images}
