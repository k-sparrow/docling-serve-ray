"""Dependency providers.

Shared clients (S3, Redis, the upstream docling-serve HTTP client) are built
once in main.py's lifespan and hung off `app.state`; these are thin
`Depends()`-compatible accessors so route handlers never touch `app.state`
directly.
"""

from functools import lru_cache

import httpx
from fastapi import Request
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FACADE_")

    docling_serve_url: str = "http://docling-serve:5001"
    redis_url: str = "redis://redis:6379/1"
    record_ttl_seconds: int = 3600 * 24 * 7  # matches docling-serve's max_document_timeout default

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


def get_s3_client(request: Request):
    return request.app.state.s3_client


def get_docling_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.docling_client
