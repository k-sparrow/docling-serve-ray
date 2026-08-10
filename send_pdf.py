#!/usr/bin/env python3
"""Upload every PDF in a directory to MinIO, then submit a single S3-source
batch conversion job to docling-serve, polling until it terminates.

This exercises the S3Coordinates / S3Target path (`/v1/convert/source/batch`)
instead of direct multipart upload — the coordinator lists objects under
INPUT_PREFIX and fans out conversion to converter replicas, writing results
back to OUTPUT_PREFIX in the same store.

Usage:
    python send_pdf.py
"""

import asyncio
import sys
import time
from pathlib import Path

import boto3
import httpx
from tenacity import retry, retry_if_result, stop_after_delay, wait_fixed

PDF_DIR = Path("/tmp/docling-pdfs")
BASE_URL = "http://localhost:5001"
TENANT_ID = "test-tenant"

# This script (host) -> MinIO's published port.
MINIO_ENDPOINT_HOST = "http://localhost:9000"
# docling-serve's Ray cluster (ray-worker container) -> MinIO, via compose DNS.
MINIO_ENDPOINT_INTERNAL = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

INPUT_BUCKET = "docling-input"
OUTPUT_BUCKET = "docling-output"
INPUT_PREFIX = "incoming/"
OUTPUT_PREFIX = "processed/"

TERMINAL_STATUSES = {"success", "failure"}


def _not_terminal(status_payload: dict) -> bool:
    return status_payload["task_status"] not in TERMINAL_STATUSES


@retry(
    retry=retry_if_result(_not_terminal),
    wait=wait_fixed(1),
    stop=stop_after_delay(1800),
    reraise=True,
)
async def _poll_once(client: httpx.AsyncClient, task_id: str, headers: dict) -> dict:
    # wait=5 asks the server to long-poll for up to 5s for a status change
    # before responding, so most of the waiting happens server-side.
    response = await client.get(
        f"{BASE_URL}/v1/status/poll/{task_id}",
        headers=headers,
        params={"wait": 5},
    )
    response.raise_for_status()
    payload = response.json()
    print(f"  status={payload['task_status']!r} position={payload.get('task_position')}")
    return payload


def upload_pdfs(s3_client) -> list[str]:
    pdf_paths = sorted(p for p in PDF_DIR.iterdir() if p.suffix.lower() == ".pdf")
    if not pdf_paths:
        print(f"error: no *.pdf files found in {PDF_DIR}", file=sys.stderr)
        sys.exit(1)

    keys = []
    for pdf_path in pdf_paths:
        key = f"{INPUT_PREFIX}{pdf_path.name}"
        s3_client.upload_file(str(pdf_path), INPUT_BUCKET, key)
        keys.append(key)
        print(f"  uploaded {pdf_path.name} -> s3://{INPUT_BUCKET}/{key}")
    return keys


async def run() -> None:
    if not PDF_DIR.is_dir():
        print(f"error: {PDF_DIR} is not a directory", file=sys.stderr)
        sys.exit(1)

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_HOST,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    print(f"Uploading PDFs from {PDF_DIR} to s3://{INPUT_BUCKET}/{INPUT_PREFIX} ...")
    upload_pdfs(s3_client)

    headers = {"X-Tenant-Id": TENANT_ID}
    body = {
        "sources": [
            {
                "kind": "s3",
                "endpoint": MINIO_ENDPOINT_INTERNAL,
                "verify_ssl": False,
                "access_key": MINIO_ACCESS_KEY,
                "secret_key": MINIO_SECRET_KEY,
                "bucket": INPUT_BUCKET,
                "key_prefix": INPUT_PREFIX,
            }
        ],
        "target": {
            "kind": "s3",
            "endpoint": MINIO_ENDPOINT_INTERNAL,
            "verify_ssl": False,
            "access_key": MINIO_ACCESS_KEY,
            "secret_key": MINIO_SECRET_KEY,
            "bucket": OUTPUT_BUCKET,
            "key_prefix": OUTPUT_PREFIX,
        },
    }

    start = time.monotonic()
    async with httpx.AsyncClient(timeout=60.0) as client:
        print(f"\nPOST {BASE_URL}/v1/convert/source/batch")
        response = await client.post(
            f"{BASE_URL}/v1/convert/source/batch", headers=headers, json=body
        )
        response.raise_for_status()
        task = response.json()
        task_id = task["task_id"]
        print(f"  task_id={task_id} initial_status={task['task_status']!r}")

        print("\nPolling until terminal status...")
        final_status = await _poll_once(client, task_id, headers)
        elapsed = time.monotonic() - start
        print(f"\nterminal status: {final_status['task_status']!r} ({elapsed:.1f}s)")

        if final_status["task_status"] != "success":
            print(f"error_message: {final_status.get('error_message')}")
            sys.exit(1)

        result_response = await client.get(f"{BASE_URL}/v1/result/{task_id}", headers=headers)
        result_response.raise_for_status()
        print("\nraw /v1/result payload:")
        print(result_response.json())

    print(f"\nListing s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX} for output objects...")
    response = s3_client.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=OUTPUT_PREFIX)
    for obj in response.get("Contents", []):
        print(f"  {obj['Key']}  ({obj['Size']} bytes)")
    if "Contents" not in response:
        print("  (no objects found)")


if __name__ == "__main__":
    asyncio.run(run())
