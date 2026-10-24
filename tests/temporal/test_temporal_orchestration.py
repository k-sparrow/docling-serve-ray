"""ConvertDocumentWorkflow against the real stacks: a real Temporal server
(Postgres-backed), the real worker image, real docling-serve on Ray.

The crash test is the property the whole orchestration layer exists for: a
worker SIGKILLed while it polls must be replaced by a retry that resumes the
SAME docling task -- not a second conversion.
"""

import asyncio
import json
import subprocess
import uuid
from pathlib import Path

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHandle

from orchestration.models import TASK_QUEUE, ConvertInput, ConvertResult
from orchestration.workflows import ConvertDocumentWorkflow

pytestmark = pytest.mark.integration

_FIXTURE = (
    Path(__file__).resolve().parents[1] / "integration" / "fixtures" / "sample.pdf"
)


def _upload(s3) -> str:
    key = f"temporal-it/{uuid.uuid4()}/sample.pdf"
    s3.upload_file(str(_FIXTURE), "docling-input", key)
    return key


async def _start(client, key: str) -> WorkflowHandle:
    return await client.start_workflow(
        ConvertDocumentWorkflow.run,
        ConvertInput(bucket="docling-input", key=key, to_formats=["md"]),
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


async def _wait_until_polling(handle: WorkflowHandle, *, timeout: float = 300) -> None:
    async def poll():
        while True:
            desc = await handle.describe()
            pending = [
                p.activity_type.name for p in desc.raw_description.pending_activities
            ]
            if "wait_for_task" in pending:
                return
            await asyncio.sleep(0.5)

    await asyncio.wait_for(poll(), timeout)


async def test_convert_round_trip(temporal_client, s3):
    key = _upload(s3)

    handle = await _start(temporal_client, key)
    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=600)

    assert result.submissions == 1
    assert set(result.outputs) >= {
        "json",
        "md",
    }  # json added even though only md was asked for
    for fmt in ("json", "md"):
        for out_key in result.outputs[fmt]:
            assert out_key.startswith(f"temporal/docling-input/{key}/convert/")
            assert (
                s3.head_object(Bucket=result.output_bucket, Key=out_key)[
                    "ContentLength"
                ]
                > 0
            )


async def test_worker_killed_mid_poll_resumes_same_docling_task(
    stacks, temporal_client, s3
):
    key = _upload(s3)
    handle = await _start(temporal_client, key)
    await _wait_until_polling(handle)
    submitted_task_id = (await _activity_history(handle))["submit_conversion"][0][
        "result"
    ]

    # SIGKILL, not stop: no graceful drain, the activity just goes silent.
    subprocess.run(["docker", "kill", stacks["worker_container"]], check=True)
    await asyncio.sleep(5)
    subprocess.run(["docker", "start", stacks["worker_container"]], check=True)

    # The dead attempt is only abandoned after its heartbeat timeout (2 min),
    # then retried on the restarted worker.
    result: ConvertResult = await asyncio.wait_for(handle.result(), timeout=900)

    history = await _activity_history(handle)
    assert result.task_id == submitted_task_id
    assert result.submissions == 1
    assert len(history["submit_conversion"]) == 1
    assert history["wait_for_task"][-1]["attempt"] >= 2
