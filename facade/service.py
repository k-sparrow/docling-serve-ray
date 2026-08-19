"""Claim-check submission and result reconciliation.

Owns the two things that make this facade necessary:
  1. Redirecting multipart uploads through S3 instead of embedding the file
     bytes into the Task object docling-serve RPUSHes into Redis.
  2. Reconstructing docling-serve's native inline response shape from the S3
     artifacts the Ray converter wrote, for tasks the facade itself
     originated (see schemas.TaskRecord).
"""

import asyncio
import time
import uuid
from typing import Any

import httpx
from fastapi import HTTPException, UploadFile
from redis.asyncio import Redis

from facade.dependencies import Settings
from facade.schemas import (
    ConvertDocumentResponse,
    RecordKind,
    TaskRecord,
    TaskStatusResponse,
)
from facade.utils import (
    build_export_document_response,
    build_zip_archive,
    fetch_artifacts_by_stem,
)

_RECORD_KEY_PREFIX = "facade:task:"


def _record_key(task_id: str) -> str:
    return f"{_RECORD_KEY_PREFIX}{task_id}"


async def submit_file_upload(
    *,
    files: list[UploadFile],
    to_formats: list[str],
    as_zip: bool,
    tenant_id: str | None,
    settings: Settings,
    s3_client,
    redis: Redis,
    docling_client: httpx.AsyncClient,
) -> TaskStatusResponse:
    request_id = str(uuid.uuid4())
    filenames: list[str] = []

    for file in files:
        name = file.filename or "file.pdf"
        filenames.append(name)
        body = await file.read()
        s3_client.put_object(
            Bucket=settings.s3_input_bucket,
            Key=f"{settings.s3_input_prefix}{request_id}/{name}",
            Body=body,
        )

    body: dict[str, Any] = {
        "sources": [
            {
                "kind": "s3",
                "endpoint": settings.s3_endpoint,
                "verify_ssl": settings.s3_verify_ssl,
                "access_key": settings.s3_access_key,
                "secret_key": settings.s3_secret_key,
                "bucket": settings.s3_input_bucket,
                "key_prefix": f"{settings.s3_input_prefix}{request_id}/",
            }
        ],
        "target": {
            "kind": "s3",
            "endpoint": settings.s3_endpoint,
            "verify_ssl": settings.s3_verify_ssl,
            "access_key": settings.s3_access_key,
            "secret_key": settings.s3_secret_key,
            "bucket": settings.s3_output_bucket,
            "key_prefix": f"{settings.s3_output_prefix}{request_id}/",
        },
        "convert_options": {"to_formats": to_formats},
    }

    headers = {settings.tenant_id_header: tenant_id} if tenant_id else {}
    response = await docling_client.post(
        "/v1/convert/source/batch", json=body, headers=headers
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    task = response.json()

    record = TaskRecord(
        kind=RecordKind.FILE_UPLOAD,
        request_id=request_id,
        tenant_id=tenant_id,
        filenames=filenames,
        to_formats=to_formats,
        as_zip=as_zip,
    )
    await redis.set(
        _record_key(task["task_id"]),
        record.model_dump_json(),
        ex=settings.record_ttl_seconds,
    )

    return TaskStatusResponse(
        task_id=task["task_id"],
        task_type=task["task_type"],
        task_status=task["task_status"],
        task_position=task.get("task_position"),
        task_meta=task.get("task_meta"),
        error_message=task.get("error_message"),
        failure=task.get("failure"),
    )


async def wait_for_completion(
    *, task_id: str, tenant_id: str | None, settings: Settings, docling_client: httpx.AsyncClient
) -> bool:
    """Mirrors docling-serve's own _wait_task_complete for the sync endpoint."""
    headers = {settings.tenant_id_header: tenant_id} if tenant_id else {}
    deadline = time.monotonic() + settings.max_sync_wait_seconds
    while time.monotonic() < deadline:
        response = await docling_client.get(
            f"/v1/status/poll/{task_id}",
            headers=headers,
            params={"wait": settings.sync_poll_interval_seconds},
        )
        response.raise_for_status()
        if response.json()["task_status"] in {"success", "failure"}:
            return True
    return False


async def resolve_result(
    *,
    task_id: str,
    tenant_id: str | None,
    settings: Settings,
    s3_client,
    redis: Redis,
    docling_client: httpx.AsyncClient,
) -> httpx.Response | ConvertDocumentResponse | bytes:
    """Returns either a passthrough httpx.Response (foreign/native tasks), a
    reconstructed ConvertDocumentResponse (single file_upload task), or raw
    zip bytes (multi-file/zip-requested file_upload task).
    """
    raw = await redis.get(_record_key(task_id))
    headers = {settings.tenant_id_header: tenant_id} if tenant_id else {}

    if raw is None:
        # Not a task the facade originated -- nothing to reconstruct, proxy
        # docling-serve's own response unmodified.
        return await docling_client.get(f"/v1/result/{task_id}", headers=headers)

    record = TaskRecord.model_validate_json(raw)

    native_response = await docling_client.get(f"/v1/result/{task_id}", headers=headers)
    if native_response.status_code >= 400:
        return native_response
    native_result = native_response.json()
    if native_result.get("num_succeeded", 0) == 0:
        return native_response

    prefix = f"{settings.s3_output_prefix}{record.request_id}/"
    grouped = await asyncio.to_thread(
        fetch_artifacts_by_stem, s3_client, bucket=settings.s3_output_bucket, prefix=prefix
    )

    stems = [filename.rsplit(".", 1)[0] for filename in record.filenames]

    if record.as_zip or len(record.filenames) > 1:
        zip_entries = [(stem, grouped.get(stem, {})) for stem in stems]
        return build_zip_archive(zip_entries)

    filename, stem = record.filenames[0], stems[0]
    artifacts = grouped.get(stem, {})
    document = build_export_document_response(filename, artifacts)
    return ConvertDocumentResponse(
        document=document,
        status="success",
        processing_time=native_result.get("processing_time", 0.0),
    )
