"""ConvertDocumentWorkflow against the real stacks: a real Temporal server
(Postgres-backed), the real worker and callback relay images, real
docling-serve on Ray.

One test per way a sleeping workflow gets woken:
  - the callback: docling calls the relay back, the relay signals, and the
    workflow finishes without a single safety poll;
  - the safety poll: with the relay down the callback is lost, and the
    workflow's own polls carry it to completion;
  - durability: a callback that arrives while no worker is running at all
    waits in the workflow's history and is acted on after restart -- the
    same docling task, nothing resubmitted.

How it ends when docling can't deliver, as real docling reports it:
  - a document docling can't read: the task still succeeds, with
    num_failed=1, so it's the workflow's per-document check that fails it;
  - a task docling fails (its source is gone): docling's failure callback;
  - a task docling loses: resubmitted;
  - a result that expired before it was fetched: the outputs check decides.

And what it rides out: a Temporal server restart, and a relay holding the
wrong callback token.
"""

import asyncio
import io
import json
import subprocess
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont
from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
)

from orchestration.models import TASK_QUEUE, ConvertInput, ConvertResult, ErrorType
from orchestration.workflows import ConvertDocumentWorkflow

pytestmark = pytest.mark.integration

_FIXTURE = (
    Path(__file__).resolve().parents[1] / "integration" / "fixtures" / "sample.pdf"
)
# Far beyond any conversion in these tests (min 0.8h after jitter): a
# workflow that still finishes promptly was woken by its callback.
_NO_SAFETY_POLL = 3600


def _upload(s3, data: bytes | None = None, name: str = "sample.pdf") -> str:
    key = f"temporal-it/{uuid.uuid4()}/{name}"
    if data is None:
        s3.upload_file(str(_FIXTURE), "docling-input", key)
    else:
        s3.put_object(Bucket="docling-input", Key=key, Body=data)
    return key


def _scanned_pdf(pages: int) -> bytes:
    """Text rendered into images -- no text layer, so docling must OCR every
    page. Slow on purpose: the conversion has to still be running when the
    durability test kills the worker right after submitting it."""
    font = ImageFont.load_default(size=28)
    images = []
    for page in range(pages):
        image = Image.new("RGB", (1240, 1754), "white")  # A4 at 150 dpi
        draw = ImageDraw.Draw(image)
        for line in range(36):
            draw.text(
                (80, 80 + line * 44),
                f"Page {page + 1}, line {line + 1}: the quick brown fox jumps over the lazy dog",
                fill="black",
                font=font,
            )
        images.append(image)
    buf = io.BytesIO()
    images[0].save(
        buf, format="PDF", save_all=True, append_images=images[1:], resolution=150
    )
    return buf.getvalue()


async def _start(client, key: str, **inp) -> WorkflowHandle:
    return await client.start_workflow(
        ConvertDocumentWorkflow.run,
        ConvertInput(bucket="docling-input", key=key, to_formats=["md"], **inp),
        id=f"convert:docling-input/{key}",
        task_queue=TASK_QUEUE,
    )


async def _activity_history(handle: WorkflowHandle) -> dict[str, list[dict]]:
    """activity type -> [{"attempt": n, "result": decoded}] for each run."""
    scheduled: dict[int, str] = {}
    runs: dict[int, dict] = {}
    async for event in handle.fetch_history_events():
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            scheduled[event.event_id] = (
                event.activity_task_scheduled_event_attributes.activity_type.name
            )
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            a = event.activity_task_started_event_attributes
            runs[a.scheduled_event_id] = {
                "type": scheduled[a.scheduled_event_id],
                "attempt": a.attempt,
            }
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            a = event.activity_task_completed_event_attributes
            # None (check_task's "still running") is a binary/null payload
            # with no data at all.
            data = a.result.payloads[0].data if a.result.payloads else b""
            runs[a.scheduled_event_id]["result"] = json.loads(data) if data else None
    by_type: dict[str, list[dict]] = {}
    for run in runs.values():
        by_type.setdefault(run["type"], []).append(run)
    return by_type


async def _signal_payloads(handle: WorkflowHandle) -> list[dict]:
    """Each task_completed signal's argument, as the relay sent it."""
    return [
        json.loads(
            event.workflow_execution_signaled_event_attributes.input.payloads[0].data
        )
        async for event in handle.fetch_history_events()
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    ]


async def _signals(handle: WorkflowHandle) -> int:
    return len(await _signal_payloads(handle))


async def _until(condition, *, timeout: float, interval: float = 0.2) -> None:
    async def poll():
        while not await condition():
            await asyncio.sleep(interval)

    await asyncio.wait_for(poll(), timeout)


async def _wait_submitted(handle: WorkflowHandle) -> str:
    """The docling task id, as soon as the workflow has recorded it."""
    task_ids: list[str] = []

    async def submitted() -> bool:
        runs = (await _activity_history(handle)).get("submit_conversion", [])
        task_ids[:] = [run["result"] for run in runs if "result" in run]
        return bool(task_ids)

    await _until(submitted, timeout=300)
    return task_ids[0]


@contextmanager
def _worker_down(worker: str):
    # SIGKILL, not stop: no graceful drain.
    subprocess.run(["docker", "kill", worker], check=True)
    try:
        yield
    finally:
        subprocess.run(["docker", "start", worker], check=True)


async def _until_signaled(handle: WorkflowHandle) -> None:
    async def signaled() -> bool:
        return await _signals(handle) > 0

    await _until(signaled, timeout=600, interval=1)


async def test_callback_wakes_the_workflow_without_safety_polls(temporal_client, s3):
    key = _upload(s3)

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )
    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

    assert result.submissions == 1
    assert result.safety_polls == 0
    assert await _signals(handle) >= 1
    # json added even though only md was asked for.
    assert set(result.outputs) >= {"json", "md"}
    for fmt in ("json", "md"):
        for out_key in result.outputs[fmt]:
            assert out_key.startswith(f"temporal/docling-input/{key}/convert/")
            head = s3.head_object(Bucket=result.output_bucket, Key=out_key)
            assert head["ContentLength"] > 0


async def test_lost_callback_is_covered_by_safety_polls(
    relay_down, temporal_client, s3
):
    key = _upload(s3)

    with relay_down():
        handle = await _start(temporal_client, key, safety_poll_interval_seconds=10)
        result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

    assert result.submissions == 1
    assert result.safety_polls >= 1
    assert await _signals(handle) == 0
    assert result.outputs["json"]


async def test_callback_while_no_worker_runs_is_acted_on_after_restart(
    stacks, temporal_client, s3
):
    key = _upload(s3, _scanned_pdf(pages=8), "scanned.pdf")

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )
    submitted = await _wait_submitted(handle)

    with _worker_down(stacks["worker_container"]):
        # docling finishes and calls back with no worker anywhere: the
        # relay's signal can only land in the workflow's history.
        await _until_signaled(handle)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        assert "fetch_outcome" not in await _activity_history(handle), (
            "the conversion finished before the worker was killed -- this "
            "test needs a slower fixture to exercise anything"
        )

    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=300)

    assert result.task_id == submitted
    assert result.submissions == 1
    assert result.safety_polls == 0


async def test_a_document_docling_cannot_read_fails_the_workflow(temporal_client, s3):
    key = _upload(s3, b"%PDF-1.7\nnot really a pdf\n%%EOF\n", "corrupt.pdf")

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )
    with pytest.raises(WorkflowFailureError) as exc:
        await asyncio.wait_for(handle.result(), timeout=600)

    # docling reports it as a successful task with one failed document --
    # the callback says success, the per-document counts say otherwise.
    assert (await _signal_payloads(handle))[0]["task_status"] == "success"
    assert exc.value.cause.type == ErrorType.CONVERSION_FAILED
    assert "converted nothing" in str(exc.value.cause)
    history = await _activity_history(handle)
    assert history["fetch_outcome"][0]["result"]["num_failed"] == 1
    assert "collect_outputs" not in history


async def test_a_task_docling_fails_ends_on_its_failure_callback(temporal_client):
    # Its source is gone before docling reads it (deleted after dispatch,
    # say), which fails the whole task. docling's dispatcher reports that,
    # not the Ray deployment that reports successes: the other callback path.
    key = f"temporal-it/{uuid.uuid4()}/deleted-before-conversion.pdf"

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )
    with pytest.raises(WorkflowFailureError) as exc:
        await asyncio.wait_for(handle.result(), timeout=600)

    callback = (await _signal_payloads(handle))[0]
    assert callback["task_status"] == "failure"
    assert exc.value.cause.type == ErrorType.CONVERSION_FAILED
    # docling's own reason, carried by the callback into the failure.
    assert callback["error_message"]
    assert callback["error_message"] in str(exc.value.cause)
    # Woken by the callback alone: no safety poll, no outcome fetch.
    assert set(await _activity_history(handle)) == {"submit_conversion"}


async def test_a_task_docling_loses_is_resubmitted(
    relay_down, docling_loses, temporal_client, s3
):
    key = _upload(s3)

    # Relay down: the lost task's conversion may well still finish, and its
    # callback would complete the workflow on a task docling no longer knows.
    with relay_down():
        handle = await _start(temporal_client, key, safety_poll_interval_seconds=10)
        lost = await _wait_submitted(handle)
        docling_loses(lost)
        result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

    assert result.submissions == 2
    assert result.task_id != lost
    submits = (await _activity_history(handle))["submit_conversion"]
    assert [run["result"] for run in submits if "result" in run] == [
        lost,
        result.task_id,
    ]
    assert result.outputs["json"]


async def test_an_expired_result_leaves_the_verdict_to_the_outputs(
    stacks, redis_cli, temporal_client, s3
):
    key = _upload(s3, _scanned_pdf(pages=8), "scanned.pdf")

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )
    task_id = await _wait_submitted(handle)

    # The result has to expire between docling finishing and the workflow
    # fetching it: no worker until the callback is in and the result gone.
    with _worker_down(stacks["worker_container"]):
        await _until_signaled(handle)
        assert "fetch_outcome" not in await _activity_history(handle)
        # What docling's RESULTS_TTL does to a result nobody fetched in time.
        # The key is jobkit's own: a "0" here means its layout changed.
        assert redis_cli("DEL", f"docling:ray:results:task:{task_id}:result") == "1"

    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=300)

    outcome = (await _activity_history(handle))["fetch_outcome"][0]["result"]
    assert outcome["num_succeeded"] is None
    assert result.task_id == task_id
    assert result.submissions == 1
    assert result.outputs["json"]


async def test_a_workflow_rides_out_a_temporal_server_restart(
    restart_temporal, temporal_client, s3
):
    key = _upload(s3, _scanned_pdf(pages=8), "scanned.pdf")

    # Polls short enough to finish regardless: docling's callback may land
    # while the server is down, and its three tries all get the relay's 503.
    handle = await _start(temporal_client, key, safety_poll_interval_seconds=20)
    submitted = await _wait_submitted(handle)
    before = await handle.describe()
    assert before.status == WorkflowExecutionStatus.RUNNING

    restart_temporal()

    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

    assert result.task_id == submitted
    assert result.submissions == 1
    assert (await handle.describe()).run_id == before.run_id
    assert result.outputs["json"]


async def test_a_relay_with_the_wrong_token_leaves_workflows_to_safety_polls(
    wrong_relay_token, temporal_client, s3
):
    key = _upload(s3)

    with wrong_relay_token() as relay_logs:
        handle = await _start(temporal_client, key, safety_poll_interval_seconds=10)
        result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

        # docling did call back, with the worker's token -- which this relay
        # turned away.
        async def rejected() -> bool:
            for line in relay_logs().splitlines():
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if (
                    entry.get("msg") == "rejected callback with an invalid token"
                    and entry.get("workflow_id") == handle.id
                ):
                    return True
            return False

        await _until(rejected, timeout=60, interval=1)

    assert await _signals(handle) == 0
    assert result.safety_polls >= 1
    assert result.outputs["json"]
