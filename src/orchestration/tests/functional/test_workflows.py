"""ConvertDocumentWorkflow logic in Temporal's time-skipping test server,
with mocked activities.

The submit mock is registered ONLY on SUBMIT_TASK_QUEUE's worker, so every
passing test also proves submits are routed to the serial queue: misrouted,
the activity would never be picked up and the workflow would time out.
"""

import uuid
from collections import Counter
from contextlib import asynccontextmanager

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from orchestration.models import (
    SUBMIT_TASK_QUEUE,
    TASK_QUEUE,
    CollectedOutputs,
    CollectOutputsInput,
    ConvertInput,
    ConvertResult,
    SubmitConversionInput,
    TaskOutcome,
)
from orchestration.workflows import MAX_SUBMISSIONS, ConvertDocumentWorkflow

JSON_AND_MD = {
    "json": ["temporal/b/k/convert/h/k.json"],
    "md": ["temporal/b/k/convert/h/k.md"],
}


class Mocks:
    """Scriptable stand-ins for the three activities, recording calls."""

    def __init__(
        self, *, wait_script=None, submit_failures=0, outcome=None, outputs=None
    ):
        self.calls: Counter = Counter()
        self.submitted: list[SubmitConversionInput] = []
        self._wait_script = list(wait_script or ["ok"])
        self._submit_failures = submit_failures
        self._outcome = outcome or TaskOutcome("unset", 1, 0, 0)
        self._outputs = JSON_AND_MD if outputs is None else outputs

        @activity.defn(name="submit_conversion")
        async def submit_conversion(inp: SubmitConversionInput) -> str:
            self.calls["submit"] += 1
            if activity.info().attempt <= self._submit_failures:
                raise ApplicationError("queue full", type="QueueFull")
            self.submitted.append(inp)
            return f"task-{len(self.submitted)}"

        @activity.defn(name="wait_for_task")
        async def wait_for_task(task_id: str) -> TaskOutcome:
            self.calls["wait"] += 1
            step = (
                self._wait_script.pop(0)
                if len(self._wait_script) > 1
                else self._wait_script[0]
            )
            if step == "lost":
                raise ApplicationError("gone", type="TaskLost", non_retryable=True)
            if step == "failed":
                raise ApplicationError(
                    "bad pdf", type="ConversionFailed", non_retryable=True
                )
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
        self.main = [wait_for_task, collect_outputs]


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


async def run_convert(
    env, mocks: Mocks, inp: ConvertInput | None = None
) -> ConvertResult:
    async with workers(env, mocks):
        return await env.client.execute_workflow(
            ConvertDocumentWorkflow.run,
            inp or ConvertInput(bucket="b", key="k", to_formats=["md"]),
            id=f"test-{uuid.uuid4()}",
            task_queue=TASK_QUEUE,
        )


async def test_happy_path_routes_submit_to_serial_queue(workflow_env):
    mocks = Mocks()

    result = await run_convert(workflow_env, mocks)

    assert result.task_id == "task-1"
    assert result.outputs == JSON_AND_MD
    assert result.output_bucket == "out"
    assert result.submissions == 1
    assert mocks.calls == Counter(submit=1, wait=1, collect=1)


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


async def test_queue_full_is_retried_not_failed(workflow_env):
    mocks = Mocks(submit_failures=3)

    result = await run_convert(workflow_env, mocks)

    assert result.submissions == 1
    assert mocks.calls["submit"] == 4  # 3 x 429, then accepted


async def test_task_lost_resubmits_then_completes(workflow_env):
    mocks = Mocks(wait_script=["lost", "ok"])

    result = await run_convert(workflow_env, mocks)

    assert result.submissions == 2
    assert result.task_id == "task-2"
    assert mocks.calls == Counter(submit=2, wait=2, collect=1)


async def test_task_lost_resubmits_are_capped(workflow_env):
    mocks = Mocks(wait_script=["lost"])

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.cause.type == "TaskLost"
    assert mocks.calls["submit"] == MAX_SUBMISSIONS
    assert mocks.calls["collect"] == 0


async def test_conversion_failure_fails_workflow_without_resubmitting(workflow_env):
    mocks = Mocks(wait_script=["failed"])

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.cause.type == "ConversionFailed"
    assert mocks.calls == Counter(submit=1, wait=1)


async def test_success_with_zero_converted_documents_fails(workflow_env):
    mocks = Mocks(outcome=TaskOutcome("x", 0, 0, 1))

    with pytest.raises(WorkflowFailureError) as exc:
        await run_convert(workflow_env, mocks)

    assert exc.value.cause.type == "ConversionFailed"
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

    assert exc.value.cause.type == "MissingOutputs"
