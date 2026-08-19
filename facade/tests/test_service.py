import json
from io import BytesIO
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import UploadFile

from facade import service
from facade.schemas import ConvertDocumentResponse, RecordKind, TaskRecord
from facade.tests.fakes import fake_response


def _upload(name: str, content: bytes = b"%PDF-1.4 fake") -> UploadFile:
    return UploadFile(file=BytesIO(content), filename=name)


class TestSubmitFileUpload:
    async def test_uploads_to_s3_and_submits_via_source_batch_not_multipart(
        self, settings, fake_s3, fake_redis
    ):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(
            200, json={"task_id": "t1", "task_type": "convert", "task_status": "pending"}
        )

        result = await service.submit_file_upload(
            files=[_upload("35013.pdf")],
            to_formats=["md"],
            as_zip=False,
            tenant_id="tenant-a",
            settings=settings,
            s3_client=fake_s3,
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

    async def test_records_a_task_record_keyed_by_the_real_task_id(self, settings, fake_s3, fake_redis):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(
            200, json={"task_id": "t2", "task_type": "convert", "task_status": "pending"}
        )

        await service.submit_file_upload(
            files=[_upload("a.pdf"), _upload("b.pdf")],
            to_formats=["md"],
            as_zip=False,
            tenant_id=None,
            settings=settings,
            s3_client=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        raw = await fake_redis.get("facade:task:t2")
        record = TaskRecord.model_validate_json(raw)
        assert record.kind == RecordKind.FILE_UPLOAD
        assert record.filenames == ["a.pdf", "b.pdf"]

    async def test_propagates_a_docling_serve_submission_error(self, settings, fake_s3, fake_redis):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.post.return_value = fake_response(422, text="bad request")

        with pytest.raises(Exception) as exc_info:
            await service.submit_file_upload(
                files=[_upload("a.pdf")],
                to_formats=["md"],
                as_zip=False,
                tenant_id=None,
                settings=settings,
                s3_client=fake_s3,
                redis=fake_redis,
                docling_client=docling_client,
            )
        assert exc_info.value.status_code == 422


class TestResolveResult:
    async def test_unrecorded_task_id_passes_through_unmodified(self, settings, fake_s3, fake_redis):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        native = fake_response(200, json={"num_converted": 1, "num_succeeded": 1})
        docling_client.get.return_value = native

        result = await service.resolve_result(
            task_id="foreign-task",
            tenant_id=None,
            settings=settings,
            s3_client=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert result is native

    async def test_recorded_task_reassembles_a_single_document_response(
        self, settings, fake_s3, fake_redis
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
            tenant_id=None,
            settings=settings,
            s3_client=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, ConvertDocumentResponse)
        assert result.document.filename == "35013.pdf"
        assert result.document.md_content == "# converted content"

    async def test_recorded_multi_file_task_returns_a_zip(self, settings, fake_s3, fake_redis):
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
            tenant_id=None,
            settings=settings,
            s3_client=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert isinstance(result, bytes)
        assert result[:2] == b"PK"  # zip magic bytes

    async def test_recorded_task_still_pending_propagates_the_404(self, settings, fake_s3, fake_redis):
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
            tenant_id=None,
            settings=settings,
            s3_client=fake_s3,
            redis=fake_redis,
            docling_client=docling_client,
        )

        assert result is not_ready


class TestWaitForCompletion:
    async def test_returns_true_once_status_reaches_a_terminal_state(self, settings):
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(200, json={"task_status": "success"})

        completed = await service.wait_for_completion(
            task_id="t6", tenant_id=None, settings=settings, docling_client=docling_client
        )

        assert completed is True

    async def test_times_out_if_status_never_reaches_a_terminal_state(self, settings):
        settings.max_sync_wait_seconds = 0.05
        settings.sync_poll_interval_seconds = 0.01
        docling_client = AsyncMock(spec=httpx.AsyncClient)
        docling_client.get.return_value = fake_response(200, json={"task_status": "started"})

        completed = await service.wait_for_completion(
            task_id="t7", tenant_id=None, settings=settings, docling_client=docling_client
        )

        assert completed is False
