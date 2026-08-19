"""Integration: the facade container against a real docling-serve backend.

nginx is out of scope here on purpose (see tests/e2e/ for the full-stack,
nginx-included layer) -- this proves the facade's own claim-check logic
(upload to real S3, submit to a real docling-serve, reassemble from real S3
output) against real infrastructure, not that routing to it is correct.
"""

import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_SAMPLE_PDF = Path(__file__).resolve().parent / "fixtures" / "sample.pdf"


def _sample_pdf_bytes() -> bytes:
    # A minimal, self-contained one-page PDF -- what this layer tests is the
    # facade's claim-check round-trip (real S3 upload, real docling-serve
    # submission, real S3 output reassembly), not docling's conversion
    # quality, so the fixture just needs to be a valid, tiny PDF.
    return _SAMPLE_PDF.read_bytes()


def test_async_submission_returns_a_real_docling_serve_task_id(facade_client, docling_client):
    response = facade_client.post(
        "/v1/convert/file/async",
        files=[("files", ("sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Poll docling-serve directly (facade doesn't implement this route --
    # nginx passthrough covers it in the real deployment) to confirm the
    # task_id the facade returned is one docling-serve actually knows about.
    status = docling_client.get(f"/v1/status/poll/{task_id}", params={"wait": 30}).json()
    assert status["task_id"] == task_id


def test_result_reconstruction_against_real_s3_output(facade_client, docling_client):
    submit = facade_client.post(
        "/v1/convert/file/async",
        files=[("files", ("integration-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    )
    task_id = submit.json()["task_id"]

    deadline = time.monotonic() + 120
    status = "pending"
    while time.monotonic() < deadline and status not in {"success", "failure"}:
        status = docling_client.get(f"/v1/status/poll/{task_id}", params={"wait": 10}).json()["task_status"]
    assert status == "success"

    result = facade_client.get(f"/v1/result/{task_id}")
    assert result.status_code == 200
    body = result.json()
    assert body["document"]["filename"] == "integration-sample.pdf"
    assert body["document"]["md_content"]


def test_sync_endpoint_completes_in_one_round_trip(facade_client):
    response = facade_client.post(
        "/v1/convert/file",
        files=[("files", ("sync-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    )
    assert response.status_code == 200
    assert response.json()["document"]["md_content"]
