"""docling-serve's own behaviour that the orchestration is built on, driven
directly over HTTP on the same real stack.

The patched image exists for one change: a full queue answers 429 instead of
an unhandled 500 (docling-serve#581). The worker's submit retries rest on
it -- QueueFull is the expected answer to a full queue -- yet nothing else
exercises it: the image's structure test only finds the handler in the
source.
"""

import time
import uuid
from http import HTTPStatus
from pathlib import Path

import httpx
import pytest

from orchestration.config import Settings
from orchestration.docling_client import build_batch_convert_body

pytestmark = pytest.mark.integration

_FIXTURE = (
    Path(__file__).resolve().parents[1] / "integration" / "fixtures" / "sample.pdf"
)
# MAX_QUEUED_TASKS + MAX_CONCURRENT_TASKS in docker-compose.yml: the most
# tasks docling holds at once.
_CAPACITY = 8 + 2


def _drain(http: httpx.Client, task_ids: list[str], *, timeout: float) -> None:
    """Waits out the accepted tasks, leaving docling's queue empty for the
    tests after this one."""
    deadline = time.monotonic() + timeout
    pending = set(task_ids)
    while pending and time.monotonic() < deadline:
        for task_id in list(pending):
            status = http.get(f"/v1/status/poll/{task_id}").json()["task_status"]
            if status in {"success", "failure"}:
                pending.discard(task_id)
        time.sleep(1)
    assert not pending, f"{len(pending)} tasks still running after {timeout}s"


def test_a_full_queue_answers_429_never_500(docling_url, s3):
    key = f"docling-it/{uuid.uuid4()}/sample.pdf"
    s3.upload_file(str(_FIXTURE), "docling-input", key)
    # Settings' defaults are this stack's S3 endpoint and credentials.
    body = build_batch_convert_body(
        Settings(),
        bucket="docling-input",
        key=key,
        options={"to_formats": ["json"]},
        callbacks=[],
    )

    with httpx.Client(base_url=docling_url(), timeout=30) as http:
        # One after the other, as the worker submits: concurrent submits can
        # overshoot the limit (its check isn't atomic), serial ones can't.
        responses = [
            http.post("/v1/convert/source/batch", json=body)
            for _ in range(2 * _CAPACITY)
        ]
        accepted = [
            r.json()["task_id"] for r in responses if r.status_code == HTTPStatus.OK
        ]
        try:
            codes = [r.status_code for r in responses]
            assert set(codes) <= {HTTPStatus.OK, HTTPStatus.TOO_MANY_REQUESTS}, codes
            assert HTTPStatus.TOO_MANY_REQUESTS in codes
            assert 0 < len(accepted) <= _CAPACITY
            rejected = responses[codes.index(HTTPStatus.TOO_MANY_REQUESTS)]
            assert "queue limit" in rejected.json()["detail"].lower()
        finally:
            _drain(http, accepted, timeout=600)
