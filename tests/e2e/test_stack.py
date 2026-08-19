"""Full-stack e2e: everything through nginx, the real public entrypoint.

Happy path only (per Artemis's documented e2e principle: "failure modes
belong at lower layers") -- this layer answers "is the routing correct end
to end," not "does every edge case work," which the unit and integration
layers already cover.
"""

import time
import uuid
from pathlib import Path

import boto3
import pytest

pytestmark = pytest.mark.e2e

_SAMPLE_PDF = Path(__file__).resolve().parents[1] / "integration" / "fixtures" / "sample.pdf"

_MINIO_ENDPOINT = "http://localhost:9000"
_MINIO_ACCESS_KEY = "minioadmin"
_MINIO_SECRET_KEY = "minioadmin123"


def test_health_passes_through_nginx_to_docling_serve(client):
    response = client.get("/health")
    assert response.status_code == 200


def test_multipart_upload_round_trips_through_facade(client):
    submit = client.post(
        "/v1/convert/file/async",
        files=[("files", ("e2e-sample.pdf", _SAMPLE_PDF.read_bytes(), "application/pdf"))],
    )
    assert submit.status_code == 200
    task_id = submit.json()["task_id"]

    # /v1/status/poll is nginx passthrough straight to docling-serve --
    # confirms the facade's task_id is real, not something it invented.
    deadline = time.monotonic() + 120
    status = "pending"
    while time.monotonic() < deadline and status not in {"success", "failure"}:
        status = client.get(f"/v1/status/poll/{task_id}", params={"wait": 10}).json()["task_status"]
    assert status == "success"

    result = client.get(f"/v1/result/{task_id}")
    assert result.status_code == 200
    body = result.json()
    assert body["document"]["filename"] == "e2e-sample.pdf"
    assert body["document"]["md_content"]


def test_native_source_batch_submission_is_unaffected_by_the_facade(client):
    # Regression test for the facade's routing scope: /v1/convert/source/batch
    # is deliberately pure nginx passthrough (see docling-serve-ray-facade-design
    # in project memory) -- this proves the facade being present doesn't
    # change its behavior at all, including on /v1/result for a task_id the
    # facade never recorded.
    s3 = boto3.client(
        "s3",
        endpoint_url=_MINIO_ENDPOINT,
        aws_access_key_id=_MINIO_ACCESS_KEY,
        aws_secret_access_key=_MINIO_SECRET_KEY,
    )
    request_id = uuid.uuid4().hex
    input_prefix = f"e2e-native/{request_id}/"
    s3.put_object(
        Bucket="docling-input", Key=f"{input_prefix}sample.pdf", Body=_SAMPLE_PDF.read_bytes()
    )

    submit = client.post(
        "/v1/convert/source/batch",
        json={
            "sources": [
                {
                    "kind": "s3",
                    "endpoint": "minio:9000",
                    "verify_ssl": False,
                    "access_key": _MINIO_ACCESS_KEY,
                    "secret_key": _MINIO_SECRET_KEY,
                    "bucket": "docling-input",
                    "key_prefix": input_prefix,
                }
            ],
            "target": {
                "kind": "s3",
                "endpoint": "minio:9000",
                "verify_ssl": False,
                "access_key": _MINIO_ACCESS_KEY,
                "secret_key": _MINIO_SECRET_KEY,
                "bucket": "docling-output",
                "key_prefix": f"e2e-native/{request_id}/",
            },
        },
    )
    assert submit.status_code == 200
    task_id = submit.json()["task_id"]

    deadline = time.monotonic() + 120
    status = "pending"
    while time.monotonic() < deadline and status not in {"success", "failure"}:
        status = client.get(f"/v1/status/poll/{task_id}", params={"wait": 10}).json()["task_status"]
    assert status == "success"

    # This task_id was never submitted through the facade, so /v1/result
    # (also routed to the facade by nginx) must fall through to docling-serve's
    # own native response unmodified -- bare counts, not a reconstructed
    # inline document.
    result = client.get(f"/v1/result/{task_id}").json()
    assert result == {
        "num_converted": 1,
        "num_succeeded": 1,
        "num_partially_succeeded": 0,
        "num_failed": 0,
        "processing_time": result["processing_time"],
    }
