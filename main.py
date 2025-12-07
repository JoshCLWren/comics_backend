from fastapi import FastAPI

from app.routers import jobs, library

app = FastAPI(title="Comics Library API", version="1.0.0")
app.include_router(library.router)
app.include_router(jobs.router)


@app.get("/")
def read_root():
    return {
        "message": "hello comics world",
        "documentation": "See /docs for the OpenAPI schema.",
    }
