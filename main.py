"""FastAPI application entry point."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.cache import RedisResponseCacheMiddleware, close_redis_client
from app.routers import jobs, library

app = FastAPI(title="Comics Library API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_origin_regex=r"http://\d{1,3}(?:\.\d{1,3}){3}:5173",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RedisResponseCacheMiddleware)

# Static images config
BASE_DIR = Path(__file__).resolve().parent
IMAGE_ROOT = BASE_DIR / "collection_images"

# This serves files like:
# /home/josh/code/comics_backend/collection_images/1963/issue_1_A/...
# at URLs like:
# http://127.0.0.1:8000/collection_images/1963/issue_1_A/...
app.mount(
    "/collection_images",
    StaticFiles(directory=str(IMAGE_ROOT)),
    name="collection_images",
)

app.include_router(library.router)
app.include_router(jobs.router)


@app.get("/")
def read_root():
    """Return a friendly root payload so uptime checks have a target."""
    return {
        "message": "hello comics world",
        "documentation": "See /docs for the OpenAPI schema.",
    }


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Close redis connection pools gracefully."""
    await close_redis_client()
