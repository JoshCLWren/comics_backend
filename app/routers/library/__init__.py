"""Grouped routers for the library endpoints."""
from fastapi import APIRouter

from . import copies, issues, series

router = APIRouter(prefix="/v1", tags=["library"])
router.include_router(series.router)
router.include_router(issues.router)
router.include_router(copies.router)

__all__ = ["router"]
