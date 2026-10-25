"""Activity behavior against a mocked docling-serve.

The point of these is failure classification: which upstream responses
Temporal retries on its own (QueueFull, UpstreamError) and which it must not
(SubmitRejected, TaskLost) -- that split is what the workflow's retry
policies and resubmit loop are built on.
"""

import dataclasses
import json
from http import HTTPStatus

import httpx
import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from orchestration.activities import DoclingActivities, format_of
from orchestration.models import (
    CollectOutputsInput,
    ErrorType,
    SubmitConversionInput,
    TaskCompleted,
    TaskStatus,
)
from orchestration.tests.functional.fakes import FakeS3

WORKFLOW_ID = "convert:docling-input/incoming/a.pdf"


def _activities(settings, handler, s3=None) -> DoclingActivities:
    http = httpx.AsyncClient(
        base_url=settings.docling_serve_url, transport=httpx.MockTransport(handler)
    )
    return DoclingActivities(settings, http, s3 or FakeS3([]))


def _env(workflow_id: str | None = WORKFLOW_ID) -> ActivityEnvironment:
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, workflow_id=workflow_id)
    return env


SUBMIT = SubmitConversionInput(
    bucket="docling-input", key="incoming/a.pdf", options={"to_formats": ["json", "md"]}
)


def _capture_submit(seen: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            HTTPStatus.OK, json={"task_id": "t-1", "task_status": "pending"}
        )

    return handler


async def test_submit_sends_single_object_batch_and_returns_task_id(settings):
    seen = {}

    task_id = await _env().run(
        _activities(settings, _capture_submit(seen)).submit_conversion, SUBMIT
    )

    assert task_id == "t-1"
    assert seen["path"] == "/v1/convert/source/batch"
    body = seen["body"]
    assert body["sources"][0]["bucket"] == "docling-input"
    assert body["sources"][0]["key_prefix"] == "incoming/a.pdf"
    assert body["target"]["bucket"] == "out"
    assert (
        body["target"]["key_prefix"] == "temporal/docling-input/incoming/a.pdf/convert/"
    )
    # "options", never "convert_options" -- the latter is silently ignored.
    assert body["options"] == {"to_formats": ["json", "md"]}
    assert "convert_options" not in body


async def test_submit_registers_callback_addressed_to_its_workflow(settings):
    seen = {}

    await _env().run(
        _activities(settings, _capture_submit(seen)).submit_conversion, SUBMIT
    )

    assert seen["body"]["callbacks"] == [
        {
            # The relay unescapes this back into the workflow id it signals.
            "url": "http://relay.test/v1/callbacks/convert%3Adocling-input%2Fincoming%2Fa.pdf",
            "headers": {"X-Callback-Token": "tok"},
        }
    ]


@pytest.mark.parametrize(
    ("callback_base_url", "workflow_id"),
    [(None, WORKFLOW_ID), ("http://relay.test", None)],
    ids=["no-relay-configured", "not-run-by-a-workflow"],
)
async def test_submit_without_relay_or_workflow_registers_no_callback(
    settings, callback_base_url, workflow_id
):
    settings = settings.model_copy(update={"callback_base_url": callback_base_url})
    seen = {}

    await _env(workflow_id).run(
        _activities(settings, _capture_submit(seen)).submit_conversion, SUBMIT
    )

    assert seen["body"]["callbacks"] == []


@pytest.mark.parametrize(
    ("status", "error_type", "retryable"),
    [
        (HTTPStatus.TOO_MANY_REQUESTS, ErrorType.QUEUE_FULL, True),
        (HTTPStatus.SERVICE_UNAVAILABLE, ErrorType.UPSTREAM_ERROR, True),
        (HTTPStatus.UNPROCESSABLE_ENTITY, ErrorType.SUBMIT_REJECTED, False),
        (HTTPStatus.BAD_REQUEST, ErrorType.SUBMIT_REJECTED, False),
    ],
)
async def test_submit_classifies_upstream_errors(
    settings, status, error_type, retryable
):
    acts = _activities(settings, lambda _: httpx.Response(status, text="nope"))

    with pytest.raises(ApplicationError) as exc:
        await _env().run(acts.submit_conversion, SUBMIT)

    assert exc.value.type == error_type
    assert exc.value.non_retryable is (not retryable)


def _status(body: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/status/poll/t-1"
        # A single poll: no server-side long-wait, the workflow sleeps instead.
        assert "wait" not in request.url.params
        return httpx.Response(HTTPStatus.OK, json=body)

    return handler


@pytest.mark.parametrize("in_progress", ["pending", "started"])
async def test_check_task_in_progress_is_none(settings, in_progress):
    acts = _activities(settings, _status({"task_status": in_progress}))

    assert await _env().run(acts.check_task, "t-1") is None


async def test_check_task_success_is_completed(settings):
    acts = _activities(settings, _status({"task_status": "success"}))

    completed = await _env().run(acts.check_task, "t-1")

    assert completed == TaskCompleted("t-1", TaskStatus.SUCCESS)


async def test_check_task_failure_is_completed_with_docling_message(settings):
    acts = _activities(
        settings, _status({"task_status": "failure", "error_message": "bad pdf"})
    )

    completed = await _env().run(acts.check_task, "t-1")

    # A failed task isn't an activity error: the workflow decides, the same
    # way it does for a failure reported by callback.
    assert completed == TaskCompleted("t-1", TaskStatus.FAILURE, "bad pdf")


async def test_check_task_404_is_task_lost_and_not_retried_by_temporal(settings):
    acts = _activities(settings, lambda _: httpx.Response(HTTPStatus.NOT_FOUND))

    with pytest.raises(ApplicationError) as exc:
        await _env().run(acts.check_task, "t-1")

    assert exc.value.type == ErrorType.TASK_LOST
    assert exc.value.non_retryable


async def test_check_task_5xx_is_retryable(settings):
    acts = _activities(settings, lambda _: httpx.Response(HTTPStatus.BAD_GATEWAY))

    with pytest.raises(ApplicationError) as exc:
        await _env().run(acts.check_task, "t-1")

    assert exc.value.type == ErrorType.UPSTREAM_ERROR
    assert not exc.value.non_retryable


async def test_fetch_outcome_returns_per_document_counts(settings):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/result/t-1"
        return httpx.Response(
            HTTPStatus.OK,
            json={"num_succeeded": 1, "num_partially_succeeded": 0, "num_failed": 0},
        )

    outcome = await _env().run(_activities(settings, handler).fetch_outcome, "t-1")

    assert (outcome.num_succeeded, outcome.num_failed) == (1, 0)


async def test_fetch_outcome_with_expired_result_still_succeeds(settings):
    acts = _activities(settings, lambda _: httpx.Response(HTTPStatus.NOT_FOUND))

    outcome = await _env().run(acts.fetch_outcome, "t-1")

    assert outcome.num_succeeded is None


async def test_collect_lists_recursively_through_jobkit_hash_dir(settings):
    prefix = "temporal/docling-input/incoming/a.pdf/convert/"
    s3 = FakeS3(
        [
            f"{prefix}0123456789ab/a.json",
            f"{prefix}0123456789ab/a.md",
            f"{prefix}0123456789ab/a.doctags.txt",
            # A sibling document's outputs must not leak in.
            "temporal/docling-input/incoming/b.pdf/convert/ffffffffffff/b.json",
        ],
        page_size=2,
    )
    acts = _activities(
        settings, lambda _: httpx.Response(HTTPStatus.INTERNAL_SERVER_ERROR), s3=s3
    )

    collected = await _env().run(
        acts.collect_outputs,
        CollectOutputsInput(bucket="docling-input", key="incoming/a.pdf"),
    )

    assert collected.bucket == "out"
    assert collected.outputs == {
        "json": [f"{prefix}0123456789ab/a.json"],
        "md": [f"{prefix}0123456789ab/a.md"],
        "doctags": [f"{prefix}0123456789ab/a.doctags.txt"],
    }
    assert s3.calls == [{"Bucket": "out", "Prefix": prefix}]


@pytest.mark.parametrize(
    ("key", "fmt"),
    [
        ("p/h/doc.doctags.txt", "doctags"),
        ("p/h/doc.chunks.jsonl", "chunks"),
        ("p/h/doc.txt", "text"),
        ("p/h/doc.json", "json"),
        ("p/h/doc.md", "md"),
        ("p/h/doc.yaml", "yaml"),
        ("p/h/README", "unknown"),
    ],
)
def test_format_of_prefers_longest_suffix(key, fmt):
    assert format_of(key) == fmt
