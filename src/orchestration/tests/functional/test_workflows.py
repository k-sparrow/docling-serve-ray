"""ConvertDocumentWorkflow logic in Temporal's time-skipping test server,
with mocked activities.

The submit mock is registered ONLY on SUBMIT_TASK_QUEUE's worker, so every
passing test also proves submits are routed to the serial queue: misrouted,
the activity would never be picked up and the workflow would time out.

Callbacks are sent the way the relay sends them: a task_completed signal
whose argument is plain JSON (a dict here), not a Python TaskCompleted -- so
these also pin the signal's wire contract. Tests without a signal complete
through safety polls; time skipping makes the 5-minute sleeps instant.
"""

import asyncio
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from orchestration.models import (
    DEFAULT_SAFETY_POLL_INTERVAL_SECONDS,
    SUBMIT_TASK_QUEUE,
    TASK_COMPLETED_SIGNAL,
    TASK_QUEUE,
    CollectedOutputs,
    CollectOutputsInput,
    ConvertInput,
    ConvertResult,
    ErrorType,
    SubmitConversionInput,
    TaskCompleted,
    TaskOutcome,
    TaskStatus,
)
from orchestration.workflows import (
    MAX_SUBMISSIONS,
    SAFETY_POLL_JITTER,
    ConvertDocumentWorkflow,
)

JSON_AND_MD = {
    "json": ["temporal/b/k/convert/h/k.json"],
    "md": ["temporal/b/k/convert/h/k.md"],
}


class Mocks:
    """Scriptable stand-ins for the four activities, recording calls.

    check_script: what each check_task call reports, in order (the last
    step repeats) -- "running" (still in progress), "success", "failed",
    "lost" (404), or "down" (docling-serve unreachable; retryable).
    """

    def __init__(
        self,
        *,
        check_script=None,
        submit_failures=0,
        submit_gate: asyncio.Event | None = None,
        outcome=None,
        outputs=None,
    ):
        self.calls: Counter = Counter()
        self.submitted: list[SubmitConversionInput] = []
        self._check_script = list(check_script or ["success"])
        self._submit_failures = submit_failures
        self._outcome = outcome or TaskOutcome("unset", 1, 0, 0)
        self._outputs = JSON_AND_MD if outputs is None else outputs

        @activity.defn(name="submit_conversion")
        async def submit_conversion(inp: SubmitConversionInput) -> str:
            self.calls["submit"] += 1
            if activity.info().attempt <= self._submit_failures:
                raise ApplicationError("queue full", type=ErrorType.QUEUE_FULL)
            if submit_gate is not None:
                await submit_gate.wait()
            self.submitted.append(inp)
            return f"task-{len(self.submitted)}"

        @activity.defn(name="check_task")
        async def check_task(task_id: str) -> TaskCompleted | None:
            self.calls["check"] += 1
            step = (
                self._check_script.pop(0)
                if len(self._check_script) > 1
                else self._check_script[0]
            )
            if step == "lost":
                raise ApplicationError(
                    "gone", type=ErrorType.TASK_LOST, non_retryable=True
                )
            if step == "down":
                raise ApplicationError("503", type=ErrorType.UPSTREAM_ERROR)
            if step == "running":
                return None
            if step == "failed":
                return TaskCompleted(task_id, TaskStatus.FAILURE, "bad pdf")
            return TaskCompleted(task_id, TaskStatus.SUCCESS)

        @activity.defn(name="fetch_outcome")
        async def fetch_outcome(task_id: str) -> TaskOutcome:
            self.calls["outcome"] += 1
            return TaskOutcome(
                task_id,
                self._outcome.num_succeeded,
                self._outcome.num_partially_succeeded,
                self._outcome.num_failed,
            )

        @activity.defn(name="collect_outputs")
        async def collect_outputs(inp: CollectOutputsInput) -> CollectedOutputs:
            self.calls["collect"] += 1
            return CollectedOutputs(bucket="out", outputs=self._outputs)

        self.submit = submit_conversion
        self.main = [check_task, fetch_outcome, collect_outputs]


@asynccontextmanager
async def workers(env, mocks: Mocks):
    async with (
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConvertDocumentWorkflow],
            activities=mocks.main,
        ),
        Worker(env.client, task_queue=SUBMIT_TASK_QUEUE, activities=[mocks.submit]),
    ):
        yield


def callback(task_id: str, status: str, error_message: str | None = None) -> dict:
    """The relay's signal argument, as the JSON it puts on the wire."""
    event = {"task_id": task_id, "task_status": status}
    if error_message is not None:
        event["error_message"] = error_message
    return event


async def run_convert(
    env,
    mocks: Mocks,
    inp: ConvertInput | None = None,
    *,
    signals: tuple[dict, ...] = (),
    after_signals=None,
) -> ConvertResult:
    async with workers(env, mocks):
        handle = await env.client.start_workflow(
            ConvertDocumentWorkflow.run,
            inp or ConvertInput(bucket="b", key="k", to_formats=["md"]),
            id=f"test-{uuid.uuid4()}",
            task_queue=TASK_QUEUE,
        )
        # Sent before awaiting the result: time only skips while a result is
        # awaited, so no 5-minute timer can fire ahead of these.
        for event in signals:
            await handle.signal(TASK_COMPLETED_SIGNAL, event)
        if after_signals is not None:
            after_signals()
        return await handle.result()


async def test_callback_completes_without_any_safety_poll(workflow_env):
    mocks = Mocks()

    result = await run_convert(
        workflow_env, mocks, signals=(callback("task-1", "success"),)
    )

    assert result.task_id == "task-1"
    assert result.outputs == JSON_AND_MD
    assert result.output_bucket == "out"
    assert result.submissions == 1
    assert result.safety_polls == 0
    assert mocks.calls == Counter(submit=1, outcome=1, collect=1)


async def test_callback_arriving_before_the_submit_result_is_kept(workflow_env):
    gate = asyncio.Event()
    mocks = Mocks(submit_gate=gate)

    # docling can finish (and call back) before submit_conversion's result
    # is recorded -- the signal lands while the workflow doesn't yet know
    # which task id it's waiting for.
    result = await run_convert(
        workflow_env,
        mocks,
        signals=(callback("task-1", "success"),),
        after_signals=gate.set,
    )

    assert result.safety_polls == 0
    assert mocks.calls["check"] == 0


async def test_lost_callback_falls_back_to_safety_polls(workflow_env):
    mocks = Mocks(check_script=["running", "running", "success"])

    result = await run_convert(workflow_env, mocks)

    # Each in-progress answer counted, then slept on again.
    assert result.safety_polls == 3
    assert mocks.calls == Counter(submit=1, check=3, outcome=1, collect=1)


async def test_safety_poll_sleeps_are_jittered_around_the_interval(workflow_env):
    mocks = Mocks(check_script=["running"] * 5 + ["success"])

    async with workers(workflow_env, mocks):
        handle = await workflow_env.client.start_workflow(
            ConvertDocumentWorkflow.run,
            ConvertInput(bucket="b", key="k"),
            id=f"test-{uuid.uuid4()}",
            task_queue=TASK_QUEUE,
        )
        await handle.result()
        sleeps = [
            event.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
            async for event in handle.fetch_history_events()
            if event.event_type == EventType.EVENT_TYPE_TIMER_STARTED
        ]

    interval = timedelta(seconds=DEFAULT_SAFETY_POLL_INTERVAL_SECONDS)
    assert len(sleeps) == 6
    assert all(
        interval * (1 - SAFETY_POLL_JITTER) <= s <= interval * (1 + SAFETY_POLL_JITTER)
        for s in sleeps
    )
    assert len(set(sleeps)) > 1


async def test_poll_budget_exhausted_fails_the_workflow(workflow_env):
    mocks = Mocks(check_script=["running"])

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(
            workflow_env,
            mocks,
            ConvertInput(bucket="b", key="k", max_safety_polls=3),
        )

    assert exc.value.cause.type == ErrorType.POLL_BUDGET_EXHAUSTED
    assert "3 safety polls" in str(exc.value.cause)
    assert mocks.calls == Counter(submit=1, check=3)


async def test_unreachable_docling_counts_as_a_poll_and_sleeps_again(workflow_env):
    # Poll 1: three failed attempts (CHECK_RETRY's cap), given up on.
    # Poll 2: answered.
    mocks = Mocks(check_script=["down", "down", "down", "success"])

    result = await run_convert(workflow_env, mocks)

    assert result.safety_polls == 2
    assert mocks.calls["check"] == 4
    assert result.submissions == 1


async def test_callback_for_another_task_is_ignored(workflow_env):
    mocks = Mocks(check_script=["running", "success"])

    # A failure for some other task id -- e.g. a late callback from a task
    # this workflow already replaced -- must neither complete nor fail it.
    result = await run_convert(
        workflow_env, mocks, signals=(callback("task-99", "failure", "stale"),)
    )

    assert result.task_id == "task-1"
    assert result.safety_polls == 2


async def test_duplicate_callbacks_are_harmless(workflow_env):
    mocks = Mocks()

    result = await run_convert(
        workflow_env,
        mocks,
        signals=(callback("task-1", "success"), callback("task-1", "success")),
    )

    assert result.safety_polls == 0
    assert mocks.calls == Counter(submit=1, outcome=1, collect=1)


async def test_failure_callback_fails_workflow_without_resubmitting(workflow_env):
    mocks = Mocks()

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(
            workflow_env, mocks, signals=(callback("task-1", "failure", "bad pdf"),)
        )

    assert exc.value.cause.type == ErrorType.CONVERSION_FAILED
    assert "bad pdf" in str(exc.value.cause)
    assert mocks.calls == Counter(submit=1)


async def test_failure_found_by_safety_poll_fails_the_same_way(workflow_env):
    mocks = Mocks(check_script=["failed"])

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.type == ErrorType.CONVERSION_FAILED
    assert "bad pdf" in str(exc.value.cause)
    assert mocks.calls == Counter(submit=1, check=1)


async def test_task_lost_resubmits_then_completes(workflow_env):
    mocks = Mocks(check_script=["lost", "success"])

    result = await run_convert(workflow_env, mocks)

    assert result.submissions == 2
    assert result.task_id == "task-2"
    assert mocks.calls == Counter(submit=2, check=2, outcome=1, collect=1)


async def test_task_lost_resubmits_are_capped(workflow_env):
    mocks = Mocks(check_script=["lost"])

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.cause.type == ErrorType.TASK_LOST
    assert mocks.calls["submit"] == MAX_SUBMISSIONS
    assert mocks.calls["collect"] == 0


async def test_queue_full_is_retried_not_failed(workflow_env):
    mocks = Mocks(submit_failures=3)

    result = await run_convert(workflow_env, mocks)

    assert result.submissions == 1
    assert mocks.calls["submit"] == 4  # 3 x 429, then accepted


async def test_json_is_always_requested_and_options_forwarded(workflow_env):
    mocks = Mocks()

    await run_convert(
        workflow_env,
        mocks,
        ConvertInput(
            bucket="b", key="k", to_formats=["md", "html"], options={"do_ocr": False}
        ),
    )

    assert mocks.submitted[0].options == {
        "do_ocr": False,
        "to_formats": ["json", "md", "html"],
    }


async def test_success_with_zero_converted_documents_fails(workflow_env):
    mocks = Mocks(outcome=TaskOutcome("x", 0, 0, 1))

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.type == ErrorType.CONVERSION_FAILED
    assert "converted nothing" in str(exc.value.cause)
    assert mocks.calls["collect"] == 0


async def test_partial_success_counts_as_produced(workflow_env):
    mocks = Mocks(outcome=TaskOutcome("x", 0, 1, 0))

    result = await run_convert(workflow_env, mocks)

    assert result.outputs == JSON_AND_MD


async def test_expired_result_counts_fall_back_to_outputs_check(workflow_env):
    mocks = Mocks(outcome=TaskOutcome("x", None, None, None))

    result = await run_convert(workflow_env, mocks)

    assert result.outputs == JSON_AND_MD


async def test_missing_json_output_fails(workflow_env):
    mocks = Mocks(outputs={"md": ["temporal/b/k/convert/h/k.md"]})

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.type == ErrorType.MISSING_OUTPUTS
