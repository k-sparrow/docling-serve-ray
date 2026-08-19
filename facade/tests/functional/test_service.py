import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import HTTPException, UploadFile

from facade import service
from facade.schemas import ConvertDocumentResponse, RecordKind, TaskRecord, ZipResult
from facade.tests.functional.fakes import StaticS3Session, fake_response


def _upload(name: str, content: bytes = b"%PDF-1.4 fake") -> UploadFile:
    return UploadFile(file=BytesIO(content), filename=name)


class TestSubmitFileUpload:
    async def test_uploads_to_s3_and_submits_via_source_batch_not_multipart(
        self, settings, fake_s3, fake_redis
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(
            200,
            json={"task_id": "t1", "task_type": "convert", "task_status": "pending"},
        )

        result = await service.submit_file_upload(
            files=[_upload("35013.pdf")],
            convert_options={"to_formats": ["md"]},
            as_zip=False,
            tenant_id="tenant-a",
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert result.task_id == "t1"
        # The bytes went to S3, not into the body posted to docling-serve --
        # this is the entire point of the facade.
        posted_body = docling_client.post.call_args.kwargs["json"]
        assert posted_body["sources"][0]["kind"] == "s3"
        assert "35013.pdf" not in json.dumps(posted_body)
        assert len(fake_s3.objects) == 1

    async def test_to_formats_reaches_docling_serve_unmodified(
        self, settings, fake_s3, fake_redis
    ):
        # Whatever list the caller passes lands byte-for-byte in the posted
        # body's `options.to_formats`, no filtering or truncation anywhere in
        # submit_file_upload. Regression test for a real bug: the body key
        # was "convert_options" until this fix, but BatchConvertSourcesRequest
        # (docling core) names the field "options" -- the wrong key was
        # silently ignored (no extra="forbid" on that model) rather than
        # erroring, so `options` fell back to its schema default
        # (to_formats=["md"]) regardless of what was requested. Mistaken for
        # an upstream Ray-orchestrator bug for a while; see
        # docling-serve-ray-facade-test-suite in project memory for the full
        # arc, including how the "confirmation" via a raw-curl bypass test
        # was itself invalidated by making the identical key-name mistake.
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(
            200,
            json={"task_id": "t10", "task_type": "convert", "task_status": "pending"},
        )

        await service.submit_file_upload(
            files=[_upload("a.pdf")],
            convert_options={"to_formats": ["md", "json"]},
            as_zip=False,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        posted_body = docling_client.post.call_args.kwargs["json"]
        assert posted_body["options"] == {"to_formats": ["md", "json"]}

    async def test_records_a_task_record_keyed_by_the_real_task_id(
        self, settings, fake_s3, fake_redis
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(
            200,
            json={"task_id": "t2", "task_type": "convert", "task_status": "pending"},
        )

        await service.submit_file_upload(
            files=[_upload("a.pdf"), _upload("b.pdf")],
            convert_options={"to_formats": ["md"]},
            as_zip=False,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        raw = await fake_redis.get("facade:task:t2")
        record = TaskRecord.model_validate_json(raw)
        assert record.kind == RecordKind.FILE_UPLOAD
        assert record.filenames == ["a.pdf", "b.pdf"]

    async def test_propagates_a_docling_serve_submission_error(
        self, settings, fake_s3, fake_redis
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(422, text="bad request")

        with pytest.raises(Exception) as exc_info:
            await service.submit_file_upload(
                files=[_upload("a.pdf")],
                convert_options={"to_formats": ["md"]},
                as_zip=False,
                tenant_id=None,
                settings=settings,
                s3_session=fake_s3,
                redis=fake_redis,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 422


class TestResolveResult:
    async def test_unrecorded_task_id_passes_through_unmodified(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        native = fake_response(200, json={"num_converted": 1, "num_succeeded": 1})
        docling_client.get.return_value = native

        result = await service.resolve_result(
            task_id="foreign-task",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert result is native

    async def test_recorded_task_reassembles_a_single_document_response(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t3",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-1",
                filenames=["35013.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-1/hash123/35013.md",
            Body=b"# converted content",
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 1, "num_succeeded": 1, "processing_time": 1.5}
        )

        result = await service.resolve_result(
            task_id="t3",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, ConvertDocumentResponse)
        assert result.document.filename == "35013.pdf"
        assert result.document.md_content == "# converted content"

    async def test_recorded_multi_file_task_returns_a_zip(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t4",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-2",
                filenames=["a.pdf", "b.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-2/h/a.md",
            Body=b"doc a",
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-2/h/b.md",
            Body=b"doc b",
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 2, "num_succeeded": 2, "processing_time": 2.0}
        )

        result = await service.resolve_result(
            task_id="t4",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, ZipResult)
        assert result.content[:2] == b"PK"  # zip magic bytes
        assert result.has_failures is False

    async def test_recorded_task_with_a_partially_failed_batch_marks_it_in_the_manifest(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        # docling-serve reports num_succeeded=1 (not 0), so resolve_result
        # proceeds into reconstruction rather than the early passthrough --
        # but only "a.pdf" actually has S3 output. "b.pdf" must come back
        # marked failed, not silently empty with no indication anything went
        # wrong.
        await fake_redis.set(
            "facade:task:t8",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-4",
                filenames=["a.pdf", "b.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-4/h/a.md",
            Body=b"doc a",
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200,
            json={
                "num_converted": 2,
                "num_succeeded": 1,
                "num_failed": 1,
                "processing_time": 1.0,
            },
        )

        result = await service.resolve_result(
            task_id="t8",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, ZipResult)
        assert result.has_failures is True
        statuses = {s.filename: s.status for s in result.document_statuses}
        assert statuses == {"a.pdf": "success", "b.pdf": "failed"}

    async def test_recorded_task_reassembles_multiple_formats_for_one_document(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t9",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-5",
                filenames=["35013.pdf"],
                to_formats=["md", "json"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-5/h/35013.md",
            Body=b"# converted",
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-5/h/35013.json",
            Body=b'{"doc": true}',
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 1, "num_succeeded": 1, "processing_time": 1.0}
        )

        result = await service.resolve_result(
            task_id="t9",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, ConvertDocumentResponse)
        assert result.document.md_content == "# converted"
        assert result.document.json_content == {"doc": True}

    async def test_recorded_task_still_pending_propagates_the_404(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t5",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-3",
                filenames=["a.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        not_ready = fake_response(404, json={"detail": "Task result not found."})
        docling_client.get.return_value = not_ready

        result = await service.resolve_result(
            task_id="t5",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert result is not_ready


class TestCleanupAfterDelivery:
    """resolve_result schedules _cleanup_task_storage as a background task
    (see main._finish_result / FastAPI's Response.background wiring) once a
    recorded task's result has actually been reconstructed and is about to
    be returned. background_tasks.tasks is just a queued list at this point
    -- nothing has run yet, since Starlette only awaits it after the
    response is sent -- so these tests run the queue explicitly to assert on
    its effects.
    """

    async def test_single_document_delivery_schedules_cleanup_of_input_output_and_record(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t10",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-6",
                filenames=["35013.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_input_bucket,
            Key=f"{settings.s3_input_prefix}req-6/35013.pdf",
            Body=b"%PDF-1.4 fake",
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-6/hash123/35013.md",
            Body=b"# converted content",
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 1, "num_succeeded": 1, "processing_time": 1.5}
        )

        result = await service.resolve_result(
            task_id="t10",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )
        assert isinstance(result, ConvertDocumentResponse)

        assert len(background_tasks.tasks) == 1
        await background_tasks()

        assert (
            settings.s3_input_bucket,
            f"{settings.s3_input_prefix}req-6/35013.pdf",
        ) not in fake_s3.objects
        assert (
            settings.s3_output_bucket,
            f"{settings.s3_output_prefix}req-6/hash123/35013.md",
        ) not in fake_s3.objects
        assert await fake_redis.get("facade:task:t10") is None

    async def test_zip_delivery_schedules_cleanup_too(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t11",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-7",
                filenames=["a.pdf", "b.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-7/h/a.md",
            Body=b"doc a",
        )
        fake_s3.put_object(
            Bucket=settings.s3_output_bucket,
            Key=f"{settings.s3_output_prefix}req-7/h/b.md",
            Body=b"doc b",
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 2, "num_succeeded": 2, "processing_time": 2.0}
        )

        result = await service.resolve_result(
            task_id="t11",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )
        assert isinstance(result, ZipResult)

        await background_tasks()

        remaining = [
            key
            for (bucket, key) in fake_s3.objects
            if bucket == settings.s3_output_bucket
        ]
        assert remaining == []
        assert await fake_redis.get("facade:task:t11") is None

    async def test_unrecorded_task_does_not_schedule_cleanup(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"num_converted": 1, "num_succeeded": 1}
        )

        await service.resolve_result(
            task_id="foreign-task",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert background_tasks.tasks == []

    async def test_still_pending_task_does_not_schedule_cleanup(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        await fake_redis.set(
            "facade:task:t12",
            TaskRecord(
                kind=RecordKind.FILE_UPLOAD,
                request_id="req-8",
                filenames=["a.pdf"],
                to_formats=["md"],
                as_zip=False,
            ).model_dump_json(),
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            404, json={"detail": "Task result not found."}
        )

        await service.resolve_result(
            task_id="t12",
            background_tasks=background_tasks,
            tenant_id=None,
            settings=settings,
            s3_session=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert background_tasks.tasks == []
        # The Redis record must survive an unready result -- it's still needed
        # the next time the client polls for this task.
        assert await fake_redis.get("facade:task:t12") is not None

    async def test_cleanup_failure_is_swallowed_not_raised(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        # Cleanup runs after the response has already been sent -- there's no
        # request left to surface a failure to, so _cleanup_task_storage must
        # log and swallow rather than let an exception propagate out of the
        # background task runner.
        record = TaskRecord(
            kind=RecordKind.FILE_UPLOAD,
            request_id="req-9",
            filenames=["a.pdf"],
            to_formats=["md"],
            as_zip=False,
        )
        broken_client = AsyncMock()
        # get_paginator is sync in the real client (only .paginate() is
        # async) -- override the AsyncMock default so this raises the same
        # way a real synchronous failure would.
        broken_client.get_paginator = MagicMock(
            side_effect=RuntimeError("S3 unreachable")
        )

        await service._cleanup_task_storage(
            s3_session=StaticS3Session(broken_client),
            redis=fake_redis,
            settings=settings,
            task_id="t13",
            record=record,
        )


class TestWaitForCompletion:
    async def test_returns_true_once_status_reaches_a_terminal_state(self, settings):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"task_status": "success"}
        )

        completed = await service.wait_for_completion(
            task_id="t6",
            tenant_id=None,
            settings=settings,
            docling_client=docling_client,
        )

        assert completed is True

    async def test_times_out_if_status_never_reaches_a_terminal_state(self, settings):
        settings.max_sync_wait_seconds = 0.05
        settings.sync_poll_interval_seconds = 0.01
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(
            200, json={"task_status": "started"}
        )

        completed = await service.wait_for_completion(
            task_id="t7",
            tenant_id=None,
            settings=settings,
            docling_client=docling_client,
        )

        assert completed is False


class TestTransportErrorHandling:
    """S3/docling-serve failures at the transport level (unreachable,
    connection reset, DNS failure) are distinct from an HTTP error
    *response*, which each call site already handles by inspecting
    status_code. Left uncaught, these would surface as a raw unhandled
    exception -- an internal 500 with a leaked stack trace -- instead of a
    clean, client-facing error.
    """

    async def test_submit_file_upload_translates_an_s3_failure(
        self, settings, fake_redis
    ):
        import botocore.exceptions

        broken_client = AsyncMock()
        broken_client.upload_fileobj.side_effect = (
            botocore.exceptions.EndpointConnectionError(
                endpoint_url="http://minio:9000"
            )
        )
        docling_client = AsyncMock(spec=httpx.AsyncClient)

        with pytest.raises(HTTPException) as exc_info:
            await service.submit_file_upload(
                files=[_upload("a.pdf")],
                convert_options={"to_formats": ["md"]},
                as_zip=False,
                tenant_id=None,
                settings=settings,
                s3_session=StaticS3Session(broken_client),
                redis=fake_redis,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 502

    async def test_submit_file_upload_translates_a_docling_serve_connection_error(
        self, settings, fake_s3, fake_redis
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.side_effect = httpx.ConnectError("connection refused")

        with pytest.raises(HTTPException) as exc_info:
            await service.submit_file_upload(
                files=[_upload("a.pdf")],
                convert_options={"to_formats": ["md"]},
                as_zip=False,
                tenant_id=None,
                settings=settings,
                s3_session=fake_s3,
                redis=fake_redis,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 502

    async def test_resolve_result_translates_a_docling_serve_connection_error(
        self, settings, fake_s3, fake_redis, background_tasks
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.side_effect = httpx.ConnectError("connection refused")

        with pytest.raises(HTTPException) as exc_info:
            await service.resolve_result(
                task_id="some-task",
                background_tasks=background_tasks,
                tenant_id=None,
                settings=settings,
                s3_session=fake_s3,
                redis=fake_redis,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 502

    async def test_wait_for_completion_translates_a_docling_serve_connection_error(
        self, settings
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.side_effect = httpx.ConnectError("connection refused")

        with pytest.raises(HTTPException) as exc_info:
            await service.wait_for_completion(
                task_id="some-task",
                tenant_id=None,
                settings=settings,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 502
