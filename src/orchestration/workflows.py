"""ConvertDocumentWorkflow: one S3 object through docling-serve, durably.

    submit (serial queue) -> wait (heartbeated long-poll) -> collect outputs

Durability comes from Temporal persisting each step's result: a worker or
server restart resumes at the step in flight. The wait step only needs the
docling task id, so resuming it polls the same task instead of converting the
document again. The one path that does resubmit is TaskLost -- docling-serve
itself no longer knows the task -- bounded by MAX_SUBMISSIONS.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from orchestration.activities import DoclingActivities
    from orchestration.models import (
        REQUIRED_FORMAT,
        SUBMIT_TASK_QUEUE,
        CollectOutputsInput,
        ConvertInput,
        ConvertResult,
        SubmitConversionInput,
    )

# 1 initial submission + 3 resubmits after TaskLost.
MAX_SUBMISSIONS = 4

# Retried until docling's queue has room again: QueueFull (429) is the normal
# signal of a full queue, not an error. Capped at 7 days overall.
SUBMIT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
)
SUBMIT_SCHEDULE_TO_CLOSE = timedelta(days=7)

# A heavy PDF can wait ~24h in docling's queue (depth 8 x 2 slots x ~6h)
# and then run up to DOCLING_SERVE_ENG_RAY_TASK_TIMEOUT (24h). Heartbeats
# arrive once per status poll (status_poll_wait_seconds, 30s by default), so
# a dead worker is detected within the heartbeat timeout and the poll moves
# to a live worker.
WAIT_START_TO_CLOSE = timedelta(hours=50)
WAIT_HEARTBEAT = timedelta(minutes=2)
WAIT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
)


@workflow.defn
class ConvertDocumentWorkflow:
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
                start_to_close_timeout=timedelta(minutes=2),
                schedule_to_close_timeout=SUBMIT_SCHEDULE_TO_CLOSE,
                retry_policy=SUBMIT_RETRY,
            )
            try:
                outcome = await workflow.execute_activity_method(
                    DoclingActivities.wait_for_task,
                    task_id,
                    start_to_close_timeout=WAIT_START_TO_CLOSE,
                    heartbeat_timeout=WAIT_HEARTBEAT,
                    retry_policy=WAIT_RETRY,
                )
                break
            except ActivityError as err:
                lost = (
                    isinstance(err.cause, ApplicationError)
                    and err.cause.type == "TaskLost"
                )
                if not lost or submissions >= MAX_SUBMISSIONS:
                    raise
                workflow.logger.warning(
                    "docling task %s lost; resubmitting (%d/%d)",
                    task_id,
                    submissions + 1,
                    MAX_SUBMISSIONS,
                )

        # docling reports task_status=success even when every document in the
        # task failed -- the per-document counts are the real verdict.
        if outcome.num_succeeded is not None:
            produced = outcome.num_succeeded + (outcome.num_partially_succeeded or 0)
            if produced == 0:
                raise ApplicationError(
                    f"task {task_id} finished but converted nothing "
                    f"(succeeded={outcome.num_succeeded}, failed={outcome.num_failed}) "
                    f"-- is s3://{inp.bucket}/{inp.key} present and convertible?",
                    type="ConversionFailed",
                    non_retryable=True,
                )

        collected = await workflow.execute_activity_method(
            DoclingActivities.collect_outputs,
            CollectOutputsInput(bucket=inp.bucket, key=inp.key),
            start_to_close_timeout=timedelta(minutes=5),
        )
        if REQUIRED_FORMAT not in collected.outputs:
            raise ApplicationError(
                f"task {task_id} produced no {REQUIRED_FORMAT} output "
                f"(found: {sorted(collected.outputs)})",
                type="MissingOutputs",
                non_retryable=True,
            )

        return ConvertResult(
            task_id=task_id,
            outputs=collected.outputs,
            output_bucket=collected.bucket,
            submissions=submissions,
        )
