"""The worker process's own setup (worker.build_workers) -- not workers built
for the test -- running the real activities against a fake docling-serve, in
Temporal's time-skipping test server.

Serial submission is the deployment's central guarantee: docling-serve's
queue-limit check isn't atomic, so concurrent submits can overshoot it. The
workflow tests prove submits are routed to the submit queue; these prove the
worker the process actually runs takes them from it one at a time, and that
stopping it lets a submit already in flight finish.
"""

import asyncio
import uuid

from temporalio.api.enums.v1 import EventType

from orchestration.models import TASK_QUEUE, ConvertInput
from orchestration.tests.functional.fakes import FakeDocling
from orchestration.workflows import ConvertDocumentWorkflow

BUCKET = "docling-input"


async def _start(env, key: str):
    return await env.client.start_workflow(
        ConvertDocumentWorkflow.run,
        ConvertInput(bucket=BUCKET, key=key),
        id=f"test-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )


async def _all_submitted(docling: FakeDocling, count: int) -> None:
    while len(docling.submit_endings) < count:
        await asyncio.sleep(0.05)


async def test_submits_run_one_at_a_time_across_a_burst_of_workflows(
    workflow_env, real_workers
):
    # Each submit takes long enough that any two allowed to run together
    # would overlap.
    docling = FakeDocling(submit_seconds=0.2)
    keys = [f"burst-{i}.pdf" for i in range(6)]
    main, submit = real_workers(docling, BUCKET, keys)

    async with main, submit:
        handles = [await _start(workflow_env, key) for key in keys]
        # In real time: the test server skips time only while a result is
        # awaited, and would jump straight past the submits' start-to-close
        # timeouts, failing them mid-flight.
        await asyncio.wait_for(_all_submitted(docling, len(keys)), timeout=60)
        # One at a time: concurrent result() calls trip the test server's
        # time-skipping bookkeeping, and the first one advances time for all.
        results = [await h.result() for h in handles]

    assert docling.submits == len(keys)
    assert docling.max_in_flight == 1
    assert sorted(r.task_id for r in results) == sorted(
        f"task-{i}" for i in range(1, len(keys) + 1)
    )


async def test_stopping_the_worker_lets_an_in_flight_submit_finish(
    workflow_env, real_workers
):
    docling = FakeDocling(submit_seconds=1.0)
    main, submit = real_workers(docling, BUCKET, ["slow.pdf"])

    async with main, submit:
        handle = await _start(workflow_env, "slow.pdf")
        await asyncio.wait_for(docling.submit_started.wait(), timeout=30)
    # Leaving the block shut both workers down, mid-submit.

    try:
        assert docling.submit_endings == ["completed"]
        # ...and its result reached Temporal: the workflow knows its task
        # id, so a restarted worker won't submit the document again.
        completed = [
            event
            async for event in handle.fetch_history_events()
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
        ]
        assert len(completed) == 1
    finally:
        # No worker will ever finish it, and the next test's workers share
        # the task queue.
        await handle.terminate()
