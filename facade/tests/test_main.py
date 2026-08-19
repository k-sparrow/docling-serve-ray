from facade.schemas import RecordKind, TaskRecord
from facade.tests.fakes import fake_response


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_convert_file_async_rejects_an_empty_upload(client):
    response = client.post("/v1/convert/file/async", files=[])
    assert response.status_code == 422


def test_convert_file_async_returns_the_real_docling_serve_task_id(client, mock_docling_client):
    mock_docling_client.post.return_value = fake_response(
        200, json={"task_id": "abc-123", "task_type": "convert", "task_status": "pending"}
    )

    response = client.post(
        "/v1/convert/file/async",
        files=[("files", ("35013.pdf", b"%PDF-1.4 fake", "application/pdf"))],
    )

    assert response.status_code == 200
    assert response.json()["task_id"] == "abc-123"


def test_repeated_to_formats_form_fields_all_reach_docling_serve(client, mock_docling_client):
    # Real end-to-end proof through the actual HTTP/Form-parsing layer (not
    # just service.py called directly) that a repeated `to_formats` field --
    # what a real multipart client sends for a multi-value list -- survives
    # FastAPI's Form() parsing intact, with none of the values dropped.
    mock_docling_client.post.return_value = fake_response(
        200, json={"task_id": "multi-format-task", "task_type": "convert", "task_status": "pending"}
    )

    response = client.post(
        "/v1/convert/file/async",
        files=[("files", ("a.pdf", b"%PDF-1.4 fake", "application/pdf"))],
        data={"to_formats": ["md", "json"]},
    )

    assert response.status_code == 200
    posted_body = mock_docling_client.post.call_args.kwargs["json"]
    assert posted_body["options"] == {"to_formats": ["md", "json"]}


def test_result_for_an_unrecorded_task_id_passes_through_docling_serve(client, mock_docling_client):
    mock_docling_client.get.return_value = fake_response(
        200, json={"num_converted": 1, "num_succeeded": 1}
    )

    response = client.get("/v1/result/some-foreign-task-id")

    assert response.status_code == 200
    assert response.json() == {"num_converted": 1, "num_succeeded": 1}


async def test_result_for_a_recorded_task_returns_reconstructed_content(
    client, mock_docling_client, fake_redis, fake_s3, settings
):
    await fake_redis.set(
        "facade:task:my-task",
        TaskRecord(
            kind=RecordKind.FILE_UPLOAD,
            request_id="req-9",
            filenames=["35013.pdf"],
            to_formats=["md"],
            as_zip=False,
        ).model_dump_json(),
    )
    fake_s3.put_object(
        Bucket=settings.s3_output_bucket,
        Key=f"{settings.s3_output_prefix}req-9/hash/35013.md",
        Body=b"# reconstructed",
    )
    mock_docling_client.get.return_value = fake_response(
        200, json={"num_converted": 1, "num_succeeded": 1, "processing_time": 3.0}
    )

    response = client.get("/v1/result/my-task")

    assert response.status_code == 200
    body = response.json()
    assert body["document"]["filename"] == "35013.pdf"
    assert body["document"]["md_content"] == "# reconstructed"


def test_convert_file_sync_returns_reconstructed_content_in_one_round_trip(
    client, mock_docling_client, fake_s3, settings
):
    mock_docling_client.post.return_value = fake_response(
        200, json={"task_id": "sync-task", "task_type": "convert", "task_status": "pending"}
    )
    mock_docling_client.get.side_effect = [
        fake_response(200, json={"task_status": "success"}),  # wait_for_completion poll
        fake_response(200, json={"num_converted": 1, "num_succeeded": 1, "processing_time": 1.0}),
    ]

    response = client.post(
        "/v1/convert/file",
        files=[("files", ("35013.pdf", b"%PDF-1.4 fake", "application/pdf"))],
    )

    # Without a seeded S3 object the document fields are correctly all-None
    # (nothing to reconstruct) -- this test asserts the round trip completes
    # synchronously and returns the inline shape, not the S3 content itself.
    assert response.status_code == 200
    assert response.json()["document"]["filename"] == "35013.pdf"


def test_convert_file_sync_returns_504_when_conversion_never_completes(
    client, mock_docling_client, settings
):
    settings.max_sync_wait_seconds = 0.05
    settings.sync_poll_interval_seconds = 0.01
    mock_docling_client.post.return_value = fake_response(
        200, json={"task_id": "slow-task", "task_type": "convert", "task_status": "pending"}
    )
    mock_docling_client.get.return_value = fake_response(200, json={"task_status": "started"})

    response = client.post(
        "/v1/convert/file",
        files=[("files", ("35013.pdf", b"%PDF-1.4 fake", "application/pdf"))],
    )

    assert response.status_code == 504
    assert "FACADE_MAX_SYNC_WAIT_SECONDS" in response.json()["detail"]


def test_convert_file_async_multi_file_result_returns_a_zip(
    client, mock_docling_client, fake_s3, settings
):
    mock_docling_client.post.return_value = fake_response(
        200, json={"task_id": "multi-task", "task_type": "convert", "task_status": "pending"}
    )

    submit = client.post(
        "/v1/convert/file/async",
        files=[
            ("files", ("a.pdf", b"%PDF-1.4 fake a", "application/pdf")),
            ("files", ("b.pdf", b"%PDF-1.4 fake b", "application/pdf")),
        ],
    )
    assert submit.status_code == 200
    assert submit.json()["task_id"] == "multi-task"

    # The facade mints request_id itself (a UUID) at submission time -- only
    # the fixed input prefix is known ahead of time, so recover the real one
    # from what actually got uploaded rather than trying to predict it.
    input_prefix = settings.s3_input_prefix
    uploaded_key = next(key for _bucket, key in fake_s3.objects if key.startswith(input_prefix))
    request_id = uploaded_key[len(input_prefix) :].split("/", 1)[0]

    fake_s3.put_object(
        Bucket=settings.s3_output_bucket,
        Key=f"{settings.s3_output_prefix}{request_id}/h/a.md",
        Body=b"doc a",
    )
    fake_s3.put_object(
        Bucket=settings.s3_output_bucket,
        Key=f"{settings.s3_output_prefix}{request_id}/h/b.md",
        Body=b"doc b",
    )
    mock_docling_client.get.return_value = fake_response(
        200, json={"num_converted": 2, "num_succeeded": 2, "processing_time": 1.0}
    )

    result = client.get("/v1/result/multi-task")

    assert result.status_code == 200
    assert result.headers["content-type"] == "application/zip"
    assert "x-facade-partial-failure" not in result.headers
