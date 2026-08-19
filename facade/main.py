"""Claim-check facade for docling-serve's multipart endpoints.

Sits behind nginx, which routes only three paths here:
  POST /v1/convert/file
  POST /v1/convert/file/async
  GET  /v1/result/{task_id}
Everything else nginx sends straight to docling-serve; see
docling-serve-ray-facade-design in project memory for the full routing
rationale and the endpoints that were deliberately left out of scope.
"""

from contextlib import asynccontextmanager
from typing import Annotated

import boto3
import httpx
from fastapi import Depends, FastAPI, Form, Header, HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from facade import service
from facade.dependencies import Settings, get_docling_client, get_redis, get_s3_client, get_settings
from facade.schemas import ConvertDocumentResponse, TaskStatusResponse
from facade.utils import DEFAULT_TO_FORMATS


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    scheme = "https" if settings.s3_verify_ssl else "http"
    app.state.s3_client = boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{settings.s3_endpoint}",
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.docling_client = httpx.AsyncClient(
        base_url=settings.docling_serve_url, timeout=120.0
    )
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await app.state.docling_client.aclose()


app = FastAPI(title="docling-serve-ray facade", lifespan=lifespan)


def _tenant_id(x_tenant_id: Annotated[str | None, Header()] = None) -> str | None:
    return x_tenant_id


@app.post("/v1/convert/file/async", response_model=TaskStatusResponse)
async def convert_file_async(
    files: list[UploadFile],
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_client: Annotated[object, Depends(get_s3_client)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
    to_formats: Annotated[list[str] | None, Form()] = None,
    target_type: Annotated[str, Form()] = "inbody",
):
    if not files:
        raise HTTPException(status_code=422, detail="No files provided.")
    return await service.submit_file_upload(
        files=files,
        to_formats=to_formats or DEFAULT_TO_FORMATS,
        as_zip=target_type == "zip",
        tenant_id=tenant_id,
        settings=settings,
        s3_client=s3_client,
        redis=redis,
        docling_client=docling_client,
    )


@app.post("/v1/convert/file")
async def convert_file_sync(
    files: list[UploadFile],
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_client: Annotated[object, Depends(get_s3_client)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
    to_formats: Annotated[list[str] | None, Form()] = None,
    target_type: Annotated[str, Form()] = "inbody",
):
    if not files:
        raise HTTPException(status_code=422, detail="No files provided.")
    task = await service.submit_file_upload(
        files=files,
        to_formats=to_formats or DEFAULT_TO_FORMATS,
        as_zip=target_type == "zip",
        tenant_id=tenant_id,
        settings=settings,
        s3_client=s3_client,
        redis=redis,
        docling_client=docling_client,
    )

    completed = await service.wait_for_completion(
        task_id=task.task_id, tenant_id=tenant_id, settings=settings, docling_client=docling_client
    )
    if not completed:
        raise HTTPException(
            status_code=504,
            detail=(
                "Conversion is taking too long. The maximum wait time is "
                f"configured as FACADE_MAX_SYNC_WAIT_SECONDS={settings.max_sync_wait_seconds}."
            ),
        )
    return await _finish_result(
        task.task_id, tenant_id, settings, s3_client, redis, docling_client
    )


@app.get("/v1/result/{task_id}")
async def get_result(
    task_id: str,
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_client: Annotated[object, Depends(get_s3_client)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
):
    return await _finish_result(task_id, tenant_id, settings, s3_client, redis, docling_client)


async def _finish_result(
    task_id: str,
    tenant_id: str | None,
    settings: Settings,
    s3_client,
    redis: Redis,
    docling_client: httpx.AsyncClient,
):
    result = await service.resolve_result(
        task_id=task_id,
        tenant_id=tenant_id,
        settings=settings,
        s3_client=s3_client,
        redis=redis,
        docling_client=docling_client,
    )
    if isinstance(result, httpx.Response):
        return Response(
            content=result.content,
            status_code=result.status_code,
            media_type=result.headers.get("content-type"),
        )
    if isinstance(result, bytes):
        return Response(
            content=result,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="converted_docs.zip"'},
        )
    if isinstance(result, ConvertDocumentResponse):
        return JSONResponse(content=result.model_dump())
    raise HTTPException(status_code=500, detail="Unexpected result type from resolve_result.")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
