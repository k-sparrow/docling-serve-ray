"""Dependency providers.

Shared clients/sessions (S3, Redis, the upstream docling-serve HTTP client)
are built once in main.py's lifespan and hung off `app.state`; these are thin
`Depends()`-compatible accessors so route handlers never touch `app.state`
directly. The S3 session is the one exception to "one shared long-lived
client": aioboto3.Session() itself is cheap/stateless (no I/O), but the
actual client it produces holds a real aiohttp connector, so `s3_client()`
below opens one fresh per call site instead of reusing a single client for
the app's whole lifetime.
"""

from functools import lru_cache

import aioboto3
import httpx
from fastapi import Request
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FACADE_")

    docling_serve_url: str = "http://docling-serve:5001"
    redis_url: str = "redis://redis:6379/1"
    record_ttl_seconds: int = (
        3600 * 24 * 7
    )  # matches docling-serve's max_document_timeout default

    s3_endpoint: str = "minio:9000"
    s3_verify_ssl: bool = False
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    s3_input_bucket: str = "docling-input"
    s3_output_bucket: str = "docling-output"
    s3_input_prefix: str = "facade-uploads/"
    s3_output_prefix: str = "facade-results/"

    tenant_id_header: str = "X-Tenant-Id"

    # Sync endpoint (/v1/convert/file) internal poll loop -- mirrors
    # docling-serve's own DOCLING_SERVE_SYNC_POLL_INTERVAL/MAX_SYNC_WAIT.
    sync_poll_interval_seconds: float = 2.0
    max_sync_wait_seconds: float = 120.0


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_s3_session(request: Request) -> aioboto3.Session:
    return request.app.state.s3_session


def s3_client(session: aioboto3.Session, settings: Settings):
    """Open a short-lived S3 client scoped to one unit of work.

    aioboto3 clients are async context managers backed by their own aiohttp
    connector -- not meant to be held open for the app's whole lifetime the
    way the old sync boto3 client was. Mirrors Artemis's own
    S3ByteStore._client() pattern: one shared Session (cheap, no I/O of its
    own), a fresh client per operation. Returns the context manager itself
    (not yet entered) -- callers do `async with s3_client(session, settings)
    as client:`.
    """
    scheme = "https" if settings.s3_verify_ssl else "http"
    return session.client(
        "s3",
        endpoint_url=f"{scheme}://{settings.s3_endpoint}",
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


def get_docling_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.docling_client
