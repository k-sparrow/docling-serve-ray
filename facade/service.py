"""Claim-check submission and result reconciliation.

Owns the two things that make this facade necessary:
  1. Redirecting multipart uploads through S3 instead of embedding the file
     bytes into the Task object docling-serve RPUSHes into Redis.
  2. Reconstructing docling-serve's native inline response shape from the S3
     artifacts the Ray converter wrote, for tasks the facade itself
     originated (see schemas.TaskRecord).
"""

import functools
import logging
import time
import uuid
from typing import Any, Callable, Coroutine, TypeVar

import botocore.exceptions
import httpx
from fastapi import BackgroundTasks, HTTPException, UploadFile
from redis.asyncio import Redis

from facade.dependencies import Settings, s3_client as open_s3_client
from facade.schemas import (
    ConvertDocumentResponse,
    RecordKind,
    TaskRecord,
    TaskStatusResponse,
    ZipResult,
)
from facade.utils import (
    DEFAULT_TO_FORMATS,
    build_export_document_response,
    build_zip_archive,
    fetch_artifacts_by_stem,
    list_artifacts,
)

_log = logging.getLogger(__name__)

_RECORD_KEY_PREFIX = "facade:task:"

# Both S3 client calls and httpx calls can fail at the transport level (S3
# unreachable, docling-serve unreachable, connection reset, DNS failure) --
# distinct from an HTTP error *response*, which each call site already
# handles explicitly by inspecting status_code. Left uncaught, these raised
# as bare, unhandled exceptions straight through to FastAPI's default 500
# handler with an internal stack trace leaking to the client. Caught
# centrally here and re-raised as a clean 502 instead.
_TRANSPORT_ERRORS = (httpx.HTTPError, botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError)

_F = TypeVar("_F", bound=Callable[..., Coroutine[Any, Any, Any]])


def _translate_transport_errors(func: _F) -> _F:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            raise
        except _TRANSPORT_ERRORS as exc:
            raise HTTPException(status_code=502, detail=f"Upstream unavailable: {exc}") from exc

    return wrapper  # type: ignore[return-value]


def _record_key(task_id: str) -> str:
    return f"{_RECORD_KEY_PREFIX}{task_id}"


async def _cleanup_task_storage(
    *, s3_session, redis: Redis, settings: Settings, task_id: str, record: TaskRecord
) -> None:
    """Delete a task's claim-check artifacts (both the uploaded input and the
    produced output, plus the Redis record itself) once its result has been
    fetched and returned inline -- this storage is deliberately single-use,
    not meant to persist past the one delivery.

    Scheduled as a FastAPI background task so it runs strictly after the
    response has already been sent to the client -- cleanup never delays or
    risks delivery. Best-effort: logs and swallows failures rather than
    raising, since by the time this runs there's no request left to surface
    an error to.
    """
    try:
        async with open_s3_client(s3_session, settings) as s3_client:
            for bucket, prefix in (
                (settings.s3_input_bucket, f"{settings.s3_input_prefix}{record.request_id}/"),
                (settings.s3_output_bucket, f"{settings.s3_output_prefix}{record.request_id}/"),
            ):
                keys = await list_artifacts(s3_client, bucket=bucket, prefix=prefix)
                if keys:
                    await s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": key} for key in keys]},
                    )
        await redis.delete(_record_key(task_id))
    except Exception:
        _log.exception("Cleanup failed for task %s (request %s)", task_id, record.request_id)


@_translate_transport_errors
async def submit_file_upload(
    *,
    files: list[UploadFile],
    convert_options: dict[str, Any],
    as_zip: bool,
    tenant_id: str | None,
    settings: Settings,
    s3_session,
    redis: Redis,
    docling_client: httpx.AsyncClient,
) -> TaskStatusResponse:
    request_id = str(uuid.uuid4())
    filenames: list[str] = []

    # upload_fileobj streams `file.file` (Starlette's already-spooled
    # SpooledTemporaryFile, positioned at 0 -- the multipart parser seeks it
    # back after writing) straight to S3 in chunks via aioboto3's patched
    # multipart-upload support, instead of `await file.read()` materializing
    # the whole upload as one Python `bytes` object first. Peak memory per
    # upload becomes bounded by aioboto3's part size, not file size -- a
    # 50MB PDF and a 5GB one cost about the same to hold in RAM here.
    async with open_s3_client(s3_session, settings) as s3_client:
        for file in files:
            name = file.filename or "file.pdf"
            filenames.append(name)
            await s3_client.upload_fileobj(
                file.file,
                settings.s3_input_bucket,
                f"{settings.s3_input_prefix}{request_id}/{name}",
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
        # NOTE: this key is genuinely "options", not "convert_options" -- confirmed
        # by reading docling (core)'s BatchConvertSourcesRequest directly:
        # `options: ConvertDocumentsOptions = ConvertDocumentsOptions()`. There is no
        # `convert_options` field on this model at all. Sending "convert_options"
        # (as this code did until this fix) doesn't error -- the model has no
        # `extra="forbid"`, so Pydantic's default `extra="ignore"` silently drops the
        # unrecognized key and `options` falls back to its default
        # (`to_formats=["md"]`), which is exactly the "only markdown ever comes back"
        # bug this repo spent a long debugging session mistakenly attributing to an
        # upstream Ray-orchestrator limitation before finding this. See
        # docling-serve-ray-facade-test-suite in project memory for the full,
        # embarrassing arc.
        "options": convert_options,
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
        to_formats=list(convert_options.get("to_formats", DEFAULT_TO_FORMATS)),
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


@_translate_transport_errors
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


@_translate_transport_errors
async def resolve_result(
    *,
    task_id: str,
    tenant_id: str | None,
    settings: Settings,
    s3_session,
    redis: Redis,
    docling_client: httpx.AsyncClient,
    background_tasks: BackgroundTasks,
) -> httpx.Response | ConvertDocumentResponse | ZipResult:
    """Returns either a passthrough httpx.Response (foreign/native tasks), a
    reconstructed ConvertDocumentResponse (single file_upload task), or a
    ZipResult (multi-file/zip-requested file_upload task).
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
    async with open_s3_client(s3_session, settings) as s3_client:
        grouped = await fetch_artifacts_by_stem(s3_client, bucket=settings.s3_output_bucket, prefix=prefix)

    stems = [filename.rsplit(".", 1)[0] for filename in record.filenames]

    background_tasks.add_task(
        _cleanup_task_storage, s3_session=s3_session, redis=redis, settings=settings, task_id=task_id, record=record
    )

    if record.as_zip or len(record.filenames) > 1:
        zip_entries = [
            (filename, stem, grouped.get(stem, {})) for filename, stem in zip(record.filenames, stems)
        ]
        return build_zip_archive(zip_entries)

    filename, stem = record.filenames[0], stems[0]
    artifacts = grouped.get(stem, {})
    document = build_export_document_response(filename, artifacts)
    return ConvertDocumentResponse(
        document=document,
        status="success",
        processing_time=native_result.get("processing_time", 0.0),
    )
