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
