"""Hand-rolled test doubles, not a mocking library.

Follows Artemis's convention (see docling-serve-ray-facade-design in project
memory for the research this mirrors): small in-repo fakes for storage/cache
backends instead of moto or similar, kept to only the subset of the real
client's interface the facade actually calls.
"""

import io
from typing import Any

import httpx


def fake_response(status_code: int, **kwargs: Any) -> httpx.Response:
    """httpx.Response built without a real request has no `._request`, so
    `.raise_for_status()` blows up with an unrelated RuntimeError instead of
    the intended HTTPStatusError -- attach one so mocked responses behave
    like real ones for anything that calls raise_for_status().
    """
    response = httpx.Response(status_code, request=httpx.Request("GET", "http://test"), **kwargs)
    return response


class FakeS3Client:
    """Enough of boto3's S3 client surface for facade.service/facade.utils."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(body)}

    def get_paginator(self, operation_name: str) -> "_FakePaginator":
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self)


class _FakePaginator:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str):
        keys = [key for (bucket, key) in self._client.objects if bucket == Bucket and key.startswith(Prefix)]
        yield {"Contents": [{"Key": key} for key in keys]}


class FakeRedis:
    """Enough of redis.asyncio.Redis's surface for facade.service."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)
