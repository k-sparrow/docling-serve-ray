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


def _poll_until_terminal(docling_client, task_id: str) -> str:
    deadline = time.monotonic() + 120
    status = "pending"
    while time.monotonic() < deadline and status not in {"success", "failure"}:
        status = docling_client.get(f"/v1/status/poll/{task_id}", params={"wait": 10}).json()["task_status"]
    return status


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
    assert _poll_until_terminal(docling_client, task_id) == "success"

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


def test_multi_file_submission_returns_a_real_zip(facade_client, docling_client):
    # Only ever unit-tested against fakes before -- exactly the layer where a
    # real bug was previously caught (zip entries named after the original
    # filename instead of the stem). This proves the fix against the real
    # Ray/docling-serve S3 output layout, not a simulated one.
    import zipfile
    from io import BytesIO

    submit = facade_client.post(
        "/v1/convert/file/async",
        files=[
            ("files", ("multi-a.pdf", _sample_pdf_bytes(), "application/pdf")),
            ("files", ("multi-b.pdf", _sample_pdf_bytes(), "application/pdf")),
        ],
    )
    assert submit.status_code == 200
    task_id = submit.json()["task_id"]
    assert _poll_until_terminal(docling_client, task_id) == "success"

    result = facade_client.get(f"/v1/result/{task_id}")
    assert result.status_code == 200
    assert result.headers["content-type"] == "application/zip"
    assert "x-facade-partial-failure" not in result.headers

    with zipfile.ZipFile(BytesIO(result.content)) as zf:
        names = zf.namelist()
        assert "multi-a.md" in names
        assert "multi-b.md" in names
        assert "_status.json" in names


def test_facade_response_shape_matches_native_docling_serve(facade_client, docling_client):
    # docling_client talks to docling-serve's own container directly (no
    # facade, no S3 redirection) -- its /v1/convert/file is the exact same
    # route the facade intercepts, so this is the real native shape to
    # reconstruct against, not an assumption about it.
    facade_result = facade_client.post(
        "/v1/convert/file",
        files=[("files", ("parity-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    ).json()
    native_result = docling_client.post(
        "/v1/convert/file",
        files=[("files", ("parity-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    ).json()

    assert facade_result["status"] == native_result["status"] == "success"
    assert facade_result.keys() == native_result.keys()
    assert facade_result["document"].keys() == native_result["document"].keys()
    assert facade_result["document"]["filename"] == native_result["document"]["filename"]
    assert facade_result["document"]["md_content"] == native_result["document"]["md_content"]

    # Known, structural deviation: the facade reconstructs its response from
    # S3-persisted output artifacts (the md/json/... files Ray wrote), which
    # don't carry docling's in-process per-page confidence scores or
    # per-stage timing breakdown -- those only exist for the process that
    # actually ran the conversion. Documented here rather than silently
    # asserting equality on fields that structurally can't match.
    assert facade_result["timings"] == {}
    assert facade_result["confidence"] is None


def _list_all_keys(s3_client, bucket: str) -> set[str]:
    response = s3_client.list_objects_v2(Bucket=bucket)
    return {obj["Key"] for obj in response.get("Contents", [])}


def test_result_delivery_cleans_up_s3_and_redis_after_fetch(
    facade_client, docling_client, s3_client, redis_client
):
    # Proves the cleanup FastAPI background task (facade.service.
    # _cleanup_task_storage, scheduled in resolve_result) actually runs
    # against real MinIO/Redis, not just the in-memory fakes the unit
    # suite exercises -- connects to both independently of the facade so
    # it's observing the effect from the outside, the same way any other
    # client of this infrastructure would.
    before_input = _list_all_keys(s3_client, "docling-input")
    before_output = _list_all_keys(s3_client, "docling-output")

    submit = facade_client.post(
        "/v1/convert/file/async",
        files=[("files", ("cleanup-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
    )
    task_id = submit.json()["task_id"]
    assert _poll_until_terminal(docling_client, task_id) == "success"
    assert redis_client.get(f"facade:task:{task_id}") is not None

    result = facade_client.get(f"/v1/result/{task_id}")
    assert result.status_code == 200

    # Cleanup is a background task, scheduled to run strictly after this
    # response was already sent (Starlette's Response.__call__ only awaits
    # background after the ASGI body send) -- give it a moment to actually
    # finish rather than asserting in the same instant.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if redis_client.get(f"facade:task:{task_id}") is None:
            break
        time.sleep(0.5)

    assert redis_client.get(f"facade:task:{task_id}") is None
    assert _list_all_keys(s3_client, "docling-input") == before_input
    assert _list_all_keys(s3_client, "docling-output") == before_output


def test_multi_format_reconstruction_against_real_output(facade_client, docling_client):
    submit = facade_client.post(
        "/v1/convert/file/async",
        files=[("files", ("multi-format-sample.pdf", _sample_pdf_bytes(), "application/pdf"))],
        data={"to_formats": ["md", "json"]},
    )
    assert submit.status_code == 200
    task_id = submit.json()["task_id"]
    assert _poll_until_terminal(docling_client, task_id) == "success"

    result = facade_client.get(f"/v1/result/{task_id}")
    assert result.status_code == 200
    document = result.json()["document"]
    assert document["md_content"]
    assert document["json_content"] is not None
