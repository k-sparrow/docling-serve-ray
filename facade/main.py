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

import aioboto3
import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request, Response, UploadFile
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from starlette.datastructures import UploadFile as StarletteUploadFile

from facade import service
from facade.dependencies import Settings, get_docling_client, get_redis, get_s3_session, get_settings
from facade.schemas import ConvertDocumentResponse, TaskStatusResponse, ZipResult
from facade.utils import build_convert_options


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    # aioboto3.Session() does no I/O itself -- the real per-connector cost
    # only happens when something calls session.client(...), which the
    # facade does per-operation (see dependencies.s3_client), not here.
    app.state.s3_session = aioboto3.Session()
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


async def _parse_submission(request: Request) -> tuple[list[UploadFile], dict[str, object], bool]:
    """Split an incoming multipart form into (files, convert_options, as_zip).

    Reads the raw form instead of declaring individual FastAPI Form()
    parameters so every docling-serve conversion option a client sends
    (do_ocr, page_range, ocr_lang, pipeline, ... -- 47 fields total as of the
    version this repo targets) reaches docling-serve untouched, not just the
    couple the facade happens to have declared explicitly.
    """
    form = await request.form()
    # request.form() (unlike a declared `files: list[UploadFile]` parameter,
    # which goes through FastAPI's own wrapping) hands back plain
    # starlette.datastructures.UploadFile instances -- fastapi.UploadFile is
    # a subclass of it, so checking against the Starlette base class is what
    # actually matches here.
    files = [f for f in form.getlist("files") if isinstance(f, StarletteUploadFile)]
    target_type = form.get("target_type", "inbody")
    convert_options = build_convert_options(form)
    return files, convert_options, target_type == "zip"


@app.post("/v1/convert/file/async", response_model=TaskStatusResponse)
async def convert_file_async(
    request: Request,
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_session: Annotated[object, Depends(get_s3_session)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
):
    files, convert_options, as_zip = await _parse_submission(request)
    if not files:
        raise HTTPException(status_code=422, detail="No files provided.")
    return await service.submit_file_upload(
        files=files,
        convert_options=convert_options,
        as_zip=as_zip,
        tenant_id=tenant_id,
        settings=settings,
        s3_session=s3_session,
        redis=redis,
        docling_client=docling_client,
    )


@app.post("/v1/convert/file")
async def convert_file_sync(
    request: Request,
    background_tasks: BackgroundTasks,
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_session: Annotated[object, Depends(get_s3_session)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
):
    files, convert_options, as_zip = await _parse_submission(request)
    if not files:
        raise HTTPException(status_code=422, detail="No files provided.")
    task = await service.submit_file_upload(
        files=files,
        convert_options=convert_options,
        as_zip=as_zip,
        tenant_id=tenant_id,
        settings=settings,
        s3_session=s3_session,
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
        task.task_id, tenant_id, settings, s3_session, redis, docling_client, background_tasks
    )


@app.get("/v1/result/{task_id}")
async def get_result(
    task_id: str,
    background_tasks: BackgroundTasks,
    tenant_id: Annotated[str | None, Depends(_tenant_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    s3_session: Annotated[object, Depends(get_s3_session)],
    redis: Annotated[Redis, Depends(get_redis)],
    docling_client: Annotated[httpx.AsyncClient, Depends(get_docling_client)],
):
    return await _finish_result(
        task_id, tenant_id, settings, s3_session, redis, docling_client, background_tasks
    )


async def _finish_result(
    task_id: str,
    tenant_id: str | None,
    settings: Settings,
    s3_session,
    redis: Redis,
    docling_client: httpx.AsyncClient,
    background_tasks: BackgroundTasks,
):
    result = await service.resolve_result(
        task_id=task_id,
        tenant_id=tenant_id,
        settings=settings,
        s3_session=s3_session,
        redis=redis,
        docling_client=docling_client,
        background_tasks=background_tasks,
    )
    if isinstance(result, httpx.Response):
        return Response(
            content=result.content,
            status_code=result.status_code,
            media_type=result.headers.get("content-type"),
        )
    if isinstance(result, ZipResult):
        headers = {"Content-Disposition": 'attachment; filename="converted_docs.zip"'}
        if result.has_failures:
            headers["X-Facade-Partial-Failure"] = "true"
        return Response(content=result.content, media_type="application/zip", headers=headers)
    if isinstance(result, ConvertDocumentResponse):
        return JSONResponse(content=result.model_dump())
    raise HTTPException(status_code=500, detail="Unexpected result type from resolve_result.")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
