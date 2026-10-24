"""Activity behavior against a mocked docling-serve.

The point of these is failure classification: which upstream responses
Temporal retries on its own (QueueFull, UpstreamError) and which it must not
(SubmitRejected, TaskLost, ConversionFailed) -- that split is what the
workflow's retry policies and resubmit loop are built on.
"""

import json

import httpx
import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from orchestration.activities import DoclingActivities, format_of
from orchestration.models import CollectOutputsInput, SubmitConversionInput
from orchestration.tests.functional.fakes import FakeS3


def _activities(settings, handler, s3=None) -> DoclingActivities:
    http = httpx.AsyncClient(
        base_url=settings.docling_serve_url, transport=httpx.MockTransport(handler)
    )
    return DoclingActivities(settings, http, s3 or FakeS3([]))


SUBMIT = SubmitConversionInput(
    bucket="docling-input", key="incoming/a.pdf", options={"to_formats": ["json", "md"]}
)


async def test_submit_sends_single_object_batch_and_returns_task_id(settings):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"task_id": "t-1", "task_status": "pending"})

    task_id = await ActivityEnvironment().run(
        _activities(settings, handler).submit_conversion, SUBMIT
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


@pytest.mark.parametrize(
    ("status", "error_type", "retryable"),
    [
        (429, "QueueFull", True),
        (503, "UpstreamError", True),
        (422, "SubmitRejected", False),
        (400, "SubmitRejected", False),
    ],
)
async def test_submit_classifies_upstream_errors(
    settings, status, error_type, retryable
):
    acts = _activities(settings, lambda _: httpx.Response(status, text="nope"))

    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(acts.submit_conversion, SUBMIT)

    assert exc.value.type == error_type
    assert exc.value.non_retryable is (not retryable)


def _poll_then(statuses: list[dict], result: httpx.Response | None = None):
    remaining = list(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/v1/status/poll/"):
            return httpx.Response(200, json=remaining.pop(0))
        assert request.url.path == "/v1/result/t-1"
        return result

    return handler


async def test_wait_heartbeats_every_poll_and_returns_counts(settings):
    handler = _poll_then(
        [
            {"task_status": "pending", "task_position": 3},
            {"task_status": "started"},
            {"task_status": "success"},
        ],
        httpx.Response(
            200,
            json={"num_succeeded": 1, "num_partially_succeeded": 0, "num_failed": 0},
        ),
    )
    env = ActivityEnvironment()
    beats: list = []
    env.on_heartbeat = lambda *details: beats.append(details[0])

    outcome = await env.run(_activities(settings, handler).wait_for_task, "t-1")

    assert [b["task_status"] for b in beats] == ["pending", "started", "success"]
    assert beats[0]["position"] == 3
    assert (outcome.num_succeeded, outcome.num_failed) == (1, 0)


async def test_wait_404_is_task_lost_and_not_retried_by_temporal(settings):
    acts = _activities(settings, lambda _: httpx.Response(404))

    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(acts.wait_for_task, "t-1")

    assert exc.value.type == "TaskLost"
    assert exc.value.non_retryable


async def test_wait_failure_status_is_conversion_failed(settings):
    handler = _poll_then([{"task_status": "failure", "error_message": "bad pdf"}])

    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(
            _activities(settings, handler).wait_for_task, "t-1"
        )

    assert exc.value.type == "ConversionFailed"
    assert exc.value.non_retryable
    assert "bad pdf" in str(exc.value)


async def test_wait_5xx_while_polling_is_retryable(settings):
    acts = _activities(settings, lambda _: httpx.Response(502))

    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(acts.wait_for_task, "t-1")

    assert exc.value.type == "UpstreamError"
    assert not exc.value.non_retryable


async def test_wait_success_with_expired_result_still_succeeds(settings):
    handler = _poll_then([{"task_status": "success"}], httpx.Response(404))

    outcome = await ActivityEnvironment().run(
        _activities(settings, handler).wait_for_task, "t-1"
    )

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
    acts = _activities(settings, lambda _: httpx.Response(500), s3=s3)

    collected = await ActivityEnvironment().run(
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
