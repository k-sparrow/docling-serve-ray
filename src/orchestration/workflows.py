"""ConvertDocumentWorkflow: one S3 object through docling-serve, durably.

    submit (serial queue) -> sleep until docling calls back -> collect outputs

docling-serve reports completion by HTTP callback, which the callback relay
(src/callback_relay) turns into a task_completed signal. Waiting holds no
worker slot: the workflow sleeps on a durable timer in the Temporal server,
and either the signal or the timer wakes it. Callback delivery is best-effort
(3 attempts over ~3s, then dropped), so whenever the timer expires without a
signal the workflow checks the task's status once itself -- a safety poll --
and goes back to sleep if it's still in progress, up to a poll budget.

Durability comes from Temporal persisting each step: a worker or server
restart resumes the sleep where it was, and a signal that arrives while no
worker is running waits in the workflow's history. The one path that
resubmits is TaskLost -- docling-serve itself no longer knows the task --
bounded by MAX_SUBMISSIONS.
"""

from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from orchestration.activities import DoclingActivities
    from orchestration.config import get_settings
    from orchestration.models import (
        REQUIRED_FORMAT,
        SUBMIT_TASK_QUEUE,
        TASK_COMPLETED_SIGNAL,
        CollectOutputsInput,
        ConvertInput,
        ConvertResult,
        ErrorType,
        SubmitConversionInput,
        TaskCompleted,
        TaskStatus,
    )

# 1 initial submission + 3 resubmits after TaskLost.
MAX_SUBMISSIONS = 4

# Retried until docling's queue has room again: QueueFull (429) is the normal
# signal of a full queue, not an error.
SUBMIT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
)

# Each sleep is the input's interval +/-20%, so documents submitted in the
# same burst don't keep polling docling in lockstep for hours.
SAFETY_POLL_JITTER = 0.2

# A safety poll is one status request. If docling-serve can't answer it
# after a few quick attempts, that poll is given up on and the workflow
# sleeps again -- still wakeable by a callback -- rather than retrying
# against a down API for as long as it stays down.
CHECK_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)


def _is_task_lost(err: ActivityError) -> bool:
    return (
        isinstance(err.cause, ApplicationError)
        and err.cause.type == ErrorType.TASK_LOST
    )


@workflow.defn
class ConvertDocumentWorkflow:
    def __init__(self) -> None:
        # Keyed by task id rather than "the current task": a fast failure can
        # call back before submit_conversion's result is even recorded here,
        # and a lost task's late callback must not complete its replacement.
        self._completed: dict[str, TaskCompleted] = {}
        self._safety_polls = 0
        # The worker's ORCH_*_START_TO_CLOSE_SECONDS. Worker config, not
        # workflow input: the timeouts belong to the deployment, and changing
        # them is replay-safe (tests/functional/test_workflows.py proves it).
        self._settings = get_settings()

    @workflow.signal(name=TASK_COMPLETED_SIGNAL)
    def task_completed(self, event: TaskCompleted) -> None:
        # docling retries delivery, so duplicates are normal; first one wins.
        self._completed.setdefault(event.task_id, event)

    @workflow.run
    async def run(self, inp: ConvertInput) -> ConvertResult:
        to_formats = list(dict.fromkeys([REQUIRED_FORMAT, *inp.to_formats]))
        submit_input = SubmitConversionInput(
            bucket=inp.bucket,
            key=inp.key,
            options={**inp.options, "to_formats": to_formats},
        )

        submissions = 0
        while True:
            submissions += 1
            task_id = await workflow.execute_activity_method(
                DoclingActivities.submit_conversion,
                submit_input,
                task_queue=SUBMIT_TASK_QUEUE,
                start_to_close_timeout=timedelta(
                    seconds=self._settings.submit_conversion_start_to_close_seconds
                ),
                retry_policy=SUBMIT_RETRY,
            )
            try:
                completed = await self._wait_until_terminal(task_id, inp)
                break
            except ActivityError as err:
                if not _is_task_lost(err) or submissions >= MAX_SUBMISSIONS:
                    raise
                workflow.logger.warning(
                    "docling task %s lost; resubmitting (%d/%d)",
                    task_id,
                    submissions + 1,
                    MAX_SUBMISSIONS,
                )

        if completed.task_status == TaskStatus.FAILURE:
            raise ApplicationError(
                f"task {task_id} failed: {completed.error_message}",
                type=ErrorType.CONVERSION_FAILED,
                non_retryable=True,
            )

        # docling reports task_status=success even when every document in the
        # task failed -- the per-document counts are the real verdict, and
        # neither the callback nor a status poll carries them.
        outcome = await workflow.execute_activity_method(
            DoclingActivities.fetch_outcome,
            task_id,
            start_to_close_timeout=timedelta(
                seconds=self._settings.fetch_outcome_start_to_close_seconds
            ),
        )
        if outcome.num_succeeded is not None:
            produced = outcome.num_succeeded + (outcome.num_partially_succeeded or 0)
            if produced == 0:
                raise ApplicationError(
                    f"task {task_id} finished but converted nothing "
                    f"(succeeded={outcome.num_succeeded}, failed={outcome.num_failed}) "
                    f"-- is s3://{inp.bucket}/{inp.key} present and convertible?",
                    type=ErrorType.CONVERSION_FAILED,
                    non_retryable=True,
                )

        collected = await workflow.execute_activity_method(
            DoclingActivities.collect_outputs,
            CollectOutputsInput(bucket=inp.bucket, key=inp.key),
            start_to_close_timeout=timedelta(
                seconds=self._settings.collect_outputs_start_to_close_seconds
            ),
        )
        if REQUIRED_FORMAT not in collected.outputs:
            raise ApplicationError(
                f"task {task_id} produced no {REQUIRED_FORMAT} output "
                f"(found: {sorted(collected.outputs)})",
                type=ErrorType.MISSING_OUTPUTS,
                non_retryable=True,
            )

        return ConvertResult(
            task_id=task_id,
            outputs=collected.outputs,
            output_bucket=collected.bucket,
            submissions=submissions,
            safety_polls=self._safety_polls,
        )

    async def _wait_until_terminal(
        self, task_id: str, inp: ConvertInput
    ) -> TaskCompleted:
        """Sleeps until the task_completed signal for task_id arrives. Each
        time a (jittered) sleep runs out without it, polls docling once: a
        finished task ends the wait, one still in progress counts against
        max_safety_polls and the workflow sleeps again.

        Raises ActivityError(TaskLost) from a poll if docling-serve no longer
        knows the task -- run() resubmits on that -- and
        ApplicationError(PollBudgetExhausted) once the budget is spent. The
        docling task itself is left running either way.
        """
        waiting_since = workflow.now()
        polls = 0
        while True:
            try:
                # Returns at once, without starting a timer, if the callback
                # already arrived (even before the submit result did).
                await workflow.wait_condition(
                    lambda: task_id in self._completed,
                    timeout=self._next_sleep(inp.safety_poll_interval_seconds),
                )
                return self._completed[task_id]
            except TimeoutError:
                pass

            polls += 1
            self._safety_polls += 1
            polled = await self._check(task_id, polls, inp.max_safety_polls)
            if polled is not None:
                workflow.logger.info(
                    "safety poll %d found task %s %s before its callback",
                    polls,
                    task_id,
                    polled.task_status,
                )
                return polled
            if polls >= inp.max_safety_polls:
                raise ApplicationError(
                    f"task {task_id} still in progress after {polls} safety "
                    f"polls ({_elapsed_since(waiting_since)}) with no callback",
                    type=ErrorType.POLL_BUDGET_EXHAUSTED,
                    non_retryable=True,
                )

    async def _check(
        self, task_id: str, poll: int, budget: int
    ) -> TaskCompleted | None:
        """One safety poll. None while the task is still queued or running,
        and also when docling-serve couldn't be asked -- that poll still
        counts, and the next sleep (or a callback) follows as usual."""
        try:
            return await workflow.execute_activity_method(
                DoclingActivities.check_task,
                task_id,
                start_to_close_timeout=timedelta(
                    seconds=self._settings.check_task_start_to_close_seconds
                ),
                retry_policy=CHECK_RETRY,
            )
        except ActivityError as err:
            if _is_task_lost(err):
                raise
            workflow.logger.warning(
                "safety poll %d/%d for task %s failed (%s); sleeping again",
                poll,
                budget,
                task_id,
                err.cause or err,
            )
            return None

    def _next_sleep(self, interval_seconds: int) -> timedelta:
        # workflow.random() is seeded per workflow run, so replays draw the
        # same sleeps.
        factor = workflow.random().uniform(
            1 - SAFETY_POLL_JITTER, 1 + SAFETY_POLL_JITTER
        )
        return timedelta(seconds=interval_seconds * factor)


def _elapsed_since(start: datetime) -> str:
    return str(workflow.now() - start).split(".")[0]
