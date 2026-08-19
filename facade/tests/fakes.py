"""Hand-rolled test doubles, not a mocking library.

Follows Artemis's convention (see docling-serve-ray-facade-design in project
memory for the research this mirrors): small in-repo fakes for storage/cache
backends instead of moto or similar, kept to only the subset of the real
client's interface the facade actually calls.
"""

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
    """Enough of aioboto3's async S3 client (plus its own Session) surface
    for facade.service/facade.utils.

    Doubles as its own fake "session": `.client("s3", **kwargs)` returns an
    async context manager yielding itself, since there's no meaningful
    session/client distinction worth faking separately -- real aioboto3
    sessions are cheap/stateless, only the client they produce actually
    holds a connector. This lets tests pass one `FakeS3Client` instance as
    `s3_session` directly, and still seed/inspect it directly too (e.g.
    `fake_s3.objects`, `fake_s3.put_object(...)` for arrange-phase setup).
    """

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def client(self, service_name: str, **kwargs: Any) -> "FakeS3Client":
        assert service_name == "s3"
        return self

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        # Test-seeding convenience, kept synchronous -- production code
        # uploads via upload_fileobj (streaming) now, never put_object
        # directly, so nothing awaits this in practice.
        self.objects[(Bucket, Key)] = Body

    async def upload_fileobj(self, Fileobj: Any, Bucket: str, Key: str) -> None:
        self.objects[(Bucket, Key)] = Fileobj.read()

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.objects[(Bucket, Key)]
        return {"Body": _FakeStreamingBody(body)}

    def get_paginator(self, operation_name: str) -> "_FakePaginator":
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self)

    async def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> None:
        for entry in Delete["Objects"]:
            self.objects.pop((Bucket, entry["Key"]), None)


class _FakeStreamingBody:
    """Mimics aiobotocore's streaming response Body: async context manager
    plus an async `.read()`, matching how facade.utils consumes it."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aenter__(self) -> "_FakeStreamingBody":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def read(self) -> bytes:
        return self._data


class _FakePaginator:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str) -> "_FakePageIterator":
        keys = [key for (bucket, key) in self._client.objects if bucket == Bucket and key.startswith(Prefix)]
        return _FakePageIterator(keys)


class _FakePageIterator:
    """A single-page async iterator -- the fake never needs real pagination,
    just the `async for page in paginator.paginate(...)` shape."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys
        self._done = False

    def __aiter__(self) -> "_FakePageIterator":
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._done:
            raise StopAsyncIteration
        self._done = True
        return {"Contents": [{"Key": key} for key in self._keys]}


class StaticS3Session:
    """Wraps an arbitrary pre-built client (e.g. a broken/erroring mock) so
    it can be passed as `s3_session` -- `.client(...)` returns an async
    context manager yielding that same client, matching the shape
    `dependencies.s3_client()` expects from a real `aioboto3.Session`.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def client(self, service_name: str, **kwargs: Any) -> "StaticS3Session":
        assert service_name == "s3"
        return self

    async def __aenter__(self) -> Any:
        return self._client

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class FakeRedis:
    """Enough of redis.asyncio.Redis's surface for facade.service."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)
