"""FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
app.include_router(library.router)
app.include_router(jobs.router)


@app.get("/")
def read_root():
    """Return a friendly root payload so uptime checks have a target."""
    return {
        "message": "hello comics world",
        "documentation": "See /docs for the OpenAPI schema.",
    }
