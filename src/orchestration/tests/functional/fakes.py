"""Test doubles for the activities' external dependencies."""

import asyncio
from http import HTTPStatus

import httpx


class FakeS3:
    """The one boto3 surface collect_outputs uses: a list_objects_v2
    paginator, returning the given keys in pages of `page_size`."""

    def __init__(self, keys: list[str], page_size: int = 2):
        self.keys = keys
        self.page_size = page_size
        self.calls: list[dict] = []

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        matching = [k for k in self.keys if k.startswith(kwargs["Prefix"])]
        for i in range(0, max(len(matching), 1), self.page_size):
            chunk = matching[i : i + self.page_size]
            yield {"Contents": [{"Key": k} for k in chunk]} if chunk else {}


class FakeDocling:
    """docling-serve as the real activities call it, as an httpx.MockTransport
    handler: every submit is accepted (task-1, task-2, ...), taking
    `submit_seconds`, and every task reports converted at its first status
    check.

    Records how many submits were in flight at once and how each one ended,
    so tests can watch the worker's submit concurrency and its shutdown.
    """

    def __init__(self, submit_seconds: float = 0.0):
        self.submit_seconds = submit_seconds
        self.submits = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.submit_started = asyncio.Event()
        self.submit_endings: list[str] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/convert/source/batch":
            return await self._submit()
        task_id = path.rsplit("/", 1)[-1]
        if path.startswith("/v1/status/poll/"):
            return httpx.Response(
                HTTPStatus.OK, json={"task_id": task_id, "task_status": "success"}
            )
        if path.startswith("/v1/result/"):
            return httpx.Response(
                HTTPStatus.OK,
                json={
                    "num_converted": 1,
                    "num_succeeded": 1,
                    "num_partially_succeeded": 0,
                    "num_failed": 0,
                    "processing_time": 0.1,
                },
            )
        return httpx.Response(HTTPStatus.NOT_FOUND)

    async def _submit(self) -> httpx.Response:
        self.submits += 1
        task_id = f"task-{self.submits}"
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.submit_started.set()
        try:
            await asyncio.sleep(self.submit_seconds)
        except asyncio.CancelledError:
            self.submit_endings.append("cancelled")
            raise
        finally:
            self.in_flight -= 1
        self.submit_endings.append("completed")
        return httpx.Response(
            HTTPStatus.OK, json={"task_id": task_id, "task_status": "pending"}
        )
