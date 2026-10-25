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
"""

import asyncio
import io
import json
import subprocess
import uuid
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowExecutionStatus, WorkflowHandle

from orchestration.models import TASK_QUEUE, ConvertInput, ConvertResult
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
            payload = a.result.payloads[0].data if a.result.payloads else b"null"
            runs[a.scheduled_event_id]["result"] = json.loads(payload)
    by_type: dict[str, list[dict]] = {}
    for run in runs.values():
        by_type.setdefault(run["type"], []).append(run)
    return by_type


async def _signals(handle: WorkflowHandle) -> int:
    return sum(
        [
            1
            async for event in handle.fetch_history_events()
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        ]
    )


async def _until(condition, *, timeout: float, interval: float = 0.2) -> None:
    async def poll():
        while not await condition():
            await asyncio.sleep(interval)

    await asyncio.wait_for(poll(), timeout)


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
    worker = stacks["worker_container"]

    handle = await _start(
        temporal_client, key, safety_poll_interval_seconds=_NO_SAFETY_POLL
    )

    async def submitted() -> bool:
        runs = (await _activity_history(handle)).get("submit_conversion", [])
        return any("result" in run for run in runs)

    await _until(submitted, timeout=300)
    # SIGKILL, not stop: no graceful drain.
    subprocess.run(["docker", "kill", worker], check=True)
    try:

        async def signaled() -> bool:
            return await _signals(handle) > 0

        # docling finishes and calls back with no worker anywhere: the
        # relay's signal can only land in the workflow's history.
        await _until(signaled, timeout=600, interval=1)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        assert "fetch_outcome" not in await _activity_history(handle), (
            "the conversion finished before the worker was killed -- this "
            "test needs a slower fixture to exercise anything"
        )
    finally:
        subprocess.run(["docker", "start", worker], check=True)

    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=300)

    history = await _activity_history(handle)
    assert result.task_id == history["submit_conversion"][0]["result"]
    assert result.submissions == 1
    assert result.safety_polls == 0
