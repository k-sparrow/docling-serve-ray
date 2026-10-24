#!/usr/bin/env python3
"""Spike: can a chunk task consume a conversion's DoclingDocument JSON
straight from S3, without re-running the conversion pipeline?

docling-serve's chunk endpoints accept only inline (base64) or HTTP sources
-- no S3 source kind -- so the chain is:

    converted {stem}.json in S3
      -> presigned GET (signed for the *internal* host the Ray workers use)
      -> POST /v1/chunk/hybrid/source/async
           sources=[{kind: http, url: <presigned>}]
           convert_options.from_formats=["json_docling"]   # DoclingJSONBackend,
                                                           # no layout/OCR models
           target=S3Target
      -> chunks

docling-core's SSRF guard rejects any URL resolving to a private address
unless DOCLINGCORE_ALLOWED_PRIVATE_IPS allowlists it -- and the MinIO URL
always does (compose network). The fetch happens on ray-worker, so that's
where the allowlist must be set. Run once without the allowlist (expect the
document to fail with "URL is not allowed") and once with it.

Findings this script established against v1.36.0 / jobkit 3.8.1:
  - task_status is "success" even when the source fetch failed; the
    per-document status inside the result is the real verdict.
  - S3Target is silently ignored for chunk tasks: jobkit's chunk export
    honors only InBodyTarget and PutTarget, everything else becomes a zip
    (chunked_result.json) in docling's Redis result store, served by
    /v1/result. Chunks therefore never land in S3 on their own -- a caller
    has to fetch the result and write them out. The S3 listing below stays
    as a canary for when that changes upstream.

Usage:
    python scripts/chunk_chain_spike.py <json-key-in-docling-output> [--expect-blocked]
"""

import io
import json
import sys
import time
import zipfile

import boto3
import httpx

DOCLING = "http://localhost:5001"  # through nginx; chunk routes pass through
S3_HOST = "http://localhost:9000"  # this script -> MinIO
S3_INTERNAL = "http://minio:9000"  # Ray workers -> MinIO (what gets signed)
CREDS = {"aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin123"}
BUCKET = "docling-output"


def main() -> int:
    json_key = sys.argv[1]
    expect_blocked = "--expect-blocked" in sys.argv
    out_prefix = json_key.rsplit("/convert/", 1)[0] + "/chunk/spike/"

    # Presigning is pure computation (no request), so the internal hostname
    # needn't resolve from here -- it only has to match what docling fetches.
    signer = boto3.client(
        "s3", endpoint_url=S3_INTERNAL, region_name="us-east-1", **CREDS
    )
    url = signer.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": json_key},
        ExpiresIn=7 * 24 * 3600,
    )
    body = {
        "sources": [{"kind": "http", "url": url}],
        "convert_options": {"from_formats": ["json_docling"]},
        "target": {
            "kind": "s3",
            "endpoint": "minio:9000",
            "verify_ssl": False,
            "access_key": CREDS["aws_access_key_id"],
            "secret_key": CREDS["aws_secret_access_key"],
            "bucket": BUCKET,
            "key_prefix": out_prefix,
        },
    }

    start = time.monotonic()
    with httpx.Client(base_url=DOCLING, timeout=60) as http:
        r = http.post("/v1/chunk/hybrid/source/async", json=body)
        print(f"submit -> HTTP {r.status_code}")
        r.raise_for_status()
        task_id = r.json()["task_id"]
        while True:
            status = http.get(f"/v1/status/poll/{task_id}", params={"wait": 5}).json()
            if status["task_status"] in {"success", "failure"}:
                break
        elapsed = time.monotonic() - start
        print(f"task {task_id}: {status['task_status']} in {elapsed:.1f}s")
        response = http.get(f"/v1/result/{task_id}")

    # A chunk task's result can come back as a zip holding
    # chunked_result.json rather than plain JSON. task_status says "success"
    # either way -- the per-document status inside is the real verdict.
    if response.headers.get("content-type", "").startswith("application/zip"):
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            result = json.loads(zf.read("chunked_result.json"))
    else:
        result = response.json()
    docs = result.get("documents", [])
    doc_statuses = [d.get("status") for d in docs]
    errors = [e.get("error_message") for d in docs for e in d.get("errors", [])]
    print(
        f"result kind={result.get('kind')} document statuses={doc_statuses} inline chunks={len(result.get('chunks', []))}"
    )
    if errors:
        print(f"errors: {[e[:120] for e in errors]}")
    s3 = boto3.client("s3", endpoint_url=S3_HOST, **CREDS)
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=out_prefix).get(
            "Contents", []
        )
    ]
    print(
        f"S3 target honored: {'yes' if keys else 'no'} "
        f"({len(keys)} object(s) under {out_prefix})"
    )

    if expect_blocked:
        blocked = any("URL is not allowed" in e for e in errors)
        print(
            "BLOCKED as expected"
            if blocked
            else "NOT blocked -- allowlist already active?"
        )
        return 0 if blocked else 1

    chunks = result.get("chunks", [])
    if doc_statuses != ["success"] or not chunks:
        print("FAIL: document not chunked")
        return 1
    print(f"OK: {len(chunks)} chunk(s); first: {json.dumps(chunks[0])[:200]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
