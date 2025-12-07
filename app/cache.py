"""Redis response caching utilities and middleware."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Awaitable, Callable, Iterable, Sequence

from fastapi import Request, Response
from fastapi.concurrency import iterate_in_threadpool
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
REDIS_URL_ENV_VAR = "COMICS_REDIS_URL"
CACHE_TTL_ENV_VAR = "COMICS_CACHE_TTL_SECONDS"
DEFAULT_CACHE_TTL = 60

TAG_KEY_PREFIX = "cache:tag:"


@dataclass(frozen=True)
class TagInfo:
    """Tags for caching and related resource invalidations."""

    cache_tags: frozenset[str]
    related_tags: frozenset[str]


class _RedisClientManager:
    """Manage a per-event-loop Redis client."""

    _instance: "_RedisClientManager | None" = None

    def __init__(self) -> None:
        self._client: Redis | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @classmethod
    def instance(cls) -> "_RedisClientManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def client(self) -> Redis:
        loop = asyncio.get_running_loop()
        if (
            self._client is None
            or self._loop is None
            or self._loop.is_closed()
            or self._loop != loop
        ):
            await self._close_locked()
            redis_url = os.environ.get(REDIS_URL_ENV_VAR, DEFAULT_REDIS_URL)
            self._client = Redis.from_url(redis_url, decode_responses=False)
            self._loop = loop
        return self._client

    async def close(self) -> None:
        await self._close_locked()

    async def _close_locked(self) -> None:
        client = self._client
        loop = self._loop
        if client is None:
            return
        self._client = None
        self._loop = None
        try:
            if (
                loop is not None
                and loop is not asyncio.get_running_loop()
                and loop.is_running()
                and not loop.is_closed()
            ):
                # Ensure the close coroutine runs on the loop that created the client.
                future = asyncio.run_coroutine_threadsafe(client.aclose(), loop)
                try:
                    await asyncio.wrap_future(future)
                except asyncio.CancelledError:  # pragma: no cover - defensive
                    future.cancel()
                    logger.warning(
                        "redis close cancelled on shutting-down loop; ignoring"
                    )
            else:
                await client.aclose()
        except asyncio.CancelledError:  # pragma: no cover - defensive
            logger.warning("redis close cancelled; assuming loop shutdown")
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("failed to close redis client cleanly: %s", exc)


async def get_redis_client() -> Redis:
    """Return a singleton Redis client."""
    return await _RedisClientManager.instance().client()


async def close_redis_client() -> None:
    """Close the cached Redis client if it exists."""
    await _RedisClientManager.instance().close()


async def invalidate_paths(paths: Sequence[str]) -> None:
    """Invalidate caches covering the provided HTTP paths."""
    tags: set[str] = set()
    for path in paths:
        info = derive_tags(path)
        tags.update(info.cache_tags)
        tags.update(info.related_tags)
    await invalidate_tags(tags)


async def invalidate_tags(tags: Iterable[str]) -> None:
    """Remove cache entries for the provided tag identifiers."""
    filtered = [tag for tag in set(tags) if tag]
    if not filtered:
        return
    try:
        redis = await get_redis_client()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("redis unavailable, cannot invalidate tags: %s", exc)
        return
    for tag in filtered:
        try:
            await _invalidate_tag(redis, tag)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("failed to invalidate tag %s: %s", tag, exc)


def derive_tags(path: str) -> TagInfo:
    """Compute cache tags associated with a request path."""
    segments = [segment for segment in path.strip("/").split("/") if segment]
    if not segments:
        return TagInfo(frozenset({"root"}), frozenset())
    head = segments[0]

    if head == "series":
        return _series_tags(segments)
    if head == "issues":
        return _issue_copy_tags(segments)
    if head == "v1" and len(segments) >= 2 and segments[1] == "jobs":
        if len(segments) >= 3:
            job_tag = f"jobs:{segments[2]}"
            return TagInfo(frozenset({job_tag}), frozenset())
        return TagInfo(frozenset({"jobs:list"}), frozenset())
    return TagInfo(frozenset({f"path:{path}"}), frozenset())


def _series_tags(segments: list[str]) -> TagInfo:
    if len(segments) == 1:
        return TagInfo(frozenset({"series:list"}), frozenset())
    series_id = segments[1]
    series_tag = f"series:{series_id}"
    if len(segments) == 2:
        return TagInfo(frozenset({series_tag}), frozenset({"series:list"}))
    if segments[2] != "issues":
        return TagInfo(frozenset({f"path:/{'/'.join(segments)}"}), frozenset())
    if len(segments) == 3:
        return TagInfo(frozenset({f"{series_tag}:issues:list"}), frozenset())
    issue_id = segments[3]
    issue_tag = f"{series_tag}:issues:{issue_id}"
    if len(segments) == 4:
        return TagInfo(frozenset({issue_tag}), frozenset({f"{series_tag}:issues:list"}))
    if segments[4] != "copies":
        return TagInfo(frozenset({f"path:/{'/'.join(segments)}"}), frozenset())
    if len(segments) == 5:
        return TagInfo(frozenset({f"{issue_tag}:copies:list"}), frozenset({issue_tag}))
    copy_id = segments[5]
    copy_tag = f"{issue_tag}:copies:{copy_id}"
    if len(segments) == 6:
        related = frozenset({f"{issue_tag}:copies:list"})
        return TagInfo(frozenset({copy_tag}), related)
    if len(segments) == 7 and segments[6] == "images":
        image_tag = f"{copy_tag}:images"
        related = frozenset({copy_tag})
        return TagInfo(frozenset({image_tag}), related)
    return TagInfo(frozenset({f"path:/{'/'.join(segments)}"}), frozenset())


def _issue_copy_tags(segments: list[str]) -> TagInfo:
    if len(segments) == 1:
        return TagInfo(frozenset({"issues:list"}), frozenset())
    issue_id = segments[1]
    issue_tag = f"issues:{issue_id}"
    if len(segments) == 2:
        return TagInfo(frozenset({issue_tag}), frozenset({"issues:list"}))
    if segments[2] != "copies":
        return TagInfo(frozenset({f"path:/{'/'.join(segments)}"}), frozenset())
    if len(segments) == 3:
        return TagInfo(frozenset({f"{issue_tag}:copies:list"}), frozenset({issue_tag}))
    copy_id = segments[3]
    copy_tag = f"{issue_tag}:copies:{copy_id}"
    related = frozenset({f"{issue_tag}:copies:list"})
    return TagInfo(frozenset({copy_tag}), related)


def _cache_ttl() -> int:
    raw = os.environ.get(CACHE_TTL_ENV_VAR)
    if not raw:
        return DEFAULT_CACHE_TTL
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_CACHE_TTL
    return max(value, 1)


class RedisResponseCacheMiddleware(BaseHTTPMiddleware):
    """Middleware that caches idempotent responses and busts cache on mutations."""

    SAFE_METHODS: set[str] = {"GET"}
    _SKIP_HEADERS = {"content-length", "date", "server"}

    def __init__(
        self,
        app,
        *,
        redis_factory: Callable[[], Awaitable[Redis]] = get_redis_client,
        cache_ttl_seconds: int | None = None,
    ) -> None:
        """Init Cache"""
        super().__init__(app)
        self._redis_factory = redis_factory
        self._cache_ttl = cache_ttl_seconds or _cache_ttl()

    async def dispatch(self, request: Request, call_next):
        """return dispatched cache wtf?"""
        try:
            redis = await self._redis_factory()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("redis unavailable, skipping cache: %s", exc)
            return await call_next(request)

        method = request.method.upper()
        if method in self.SAFE_METHODS:
            return await self._handle_read(request, call_next, redis)
        return await self._handle_mutation(request, call_next, redis)

    async def _handle_read(self, request: Request, call_next, redis: Redis) -> Response:
        tags = derive_tags(request.url.path).cache_tags or frozenset(
            {f"path:{request.url.path}"}
        )
        cache_key = self._cache_key(request)
        cached = None
        try:
            cached = await redis.get(cache_key)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("failed to read cache: %s", exc)
        if cached:
            try:
                payload = json.loads(cached)
                body = base64.b64decode(payload["body"])
                logger.info(
                    "cache hit for %s %s tags=%s",
                    request.method,
                    request.url.path,
                    sorted(tags),
                )
                response = Response(
                    content=body,
                    status_code=payload["status_code"],
                    media_type=payload.get("media_type"),
                )
                for key, value in payload.get("headers", {}).items():
                    response.headers[key] = value
                response.headers["x-cache"] = "hit"
                return response
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("failed to deserialize cache entry: %s", exc)
                await redis.delete(cache_key)

        response = await call_next(request)
        body = await self._consume_body(response)
        if response.status_code < 500:
            entry = {
                "status_code": response.status_code,
                "media_type": response.media_type,
                "headers": self._cache_headers(response.headers.items()),
                "body": base64.b64encode(body).decode("ascii"),
            }
            try:
                await redis.setex(cache_key, self._cache_ttl, json.dumps(entry))
                await _register_tags(redis, cache_key, tags, self._cache_ttl)
                logger.info(
                    "cache stored for %s %s tags=%s ttl=%s",
                    request.method,
                    request.url.path,
                    sorted(tags),
                    self._cache_ttl,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("failed to cache response: %s", exc)
        response.headers["x-cache"] = "miss"
        response.body_iterator = iterate_in_threadpool(iter([body]))
        return response

    async def _handle_mutation(
        self, request: Request, call_next, redis: Redis
    ) -> Response:
        response = await call_next(request)
        if response.status_code < 500:
            info = derive_tags(request.url.path)
            tags = set(info.cache_tags) | set(info.related_tags)
            if tags:
                await _invalidate_tag_set(redis, tags)
        return response

    def _cache_key(self, request: Request) -> str:
        descriptor = json.dumps(
            {
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query,
                "accept": request.headers.get("accept"),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        digest = hashlib.sha256(descriptor).hexdigest()
        return f"cache:responses:{digest}"

    async def _consume_body(self, response: Response) -> bytes:
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
        return body

    def _cache_headers(self, headers: Iterable[tuple[str, str]]) -> dict[str, str]:
        filtered: dict[str, str] = {}
        for key, value in headers:
            if key.lower() in self._SKIP_HEADERS:
                continue
            filtered[key] = value
        return filtered


async def _register_tags(
    redis: Redis, cache_key: str, tags: Iterable[str], ttl: int
) -> None:
    tag_list = [tag for tag in tags if tag]
    if not tag_list:
        return
    pipe = redis.pipeline()
    for tag in tag_list:
        key = TAG_KEY_PREFIX + tag
        pipe.sadd(key, cache_key)
        pipe.expire(key, ttl)
    await pipe.execute()


async def _invalidate_tag(redis: Redis, tag: str) -> None:
    key = TAG_KEY_PREFIX + tag
    members = await redis.smembers(key)
    if members:
        await redis.delete(*members)
    await redis.delete(key)
    logger.info("invalidated tag %s (%s keys)", tag, len(members))


async def _invalidate_tag_set(
    redis: Redis, tags: Iterable[str], *, retries: int = 2
) -> None:
    for tag in tags:
        attempt = 0
        while True:
            try:
                await _invalidate_tag(redis, tag)
                break
            except asyncio.CancelledError:  # pragma: no cover - defensive
                logger.warning("tag invalidation cancelled for %s", tag)
                return
            except Exception as exc:  # pragma: no cover - transient redis issues
                if attempt >= retries:
                    logger.warning("failed to invalidate tag %s: %s", tag, exc)
                    break
                backoff = min(0.05 * (attempt + 1), 0.25)
                logger.debug(
                    "retrying invalidation for %s after %ss: %s",
                    tag,
                    backoff,
                    exc,
                )
                await asyncio.sleep(backoff)
                attempt += 1


__all__ = [
    "RedisResponseCacheMiddleware",
    "close_redis_client",
    "derive_tags",
    "get_redis_client",
    "invalidate_paths",
    "invalidate_tags",
]
