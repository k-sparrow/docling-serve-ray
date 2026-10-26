"""Activities: every side effect a conversion workflow performs.

Failure classification is the important part -- it decides what Temporal
retries on its own and what the workflow has to handle:

  - QueueFull (429) / UpstreamError (5xx) / transport errors: retryable. The
    retry policy (workflows.py) backs off and resubmits; a 429 is docling's
    queue-limit backpressure, not an error.
  - SubmitRejected (any other 4xx): non-retryable -- a malformed request
    won't succeed on attempt 2.
  - TaskLost (404 on a status check): non-retryable here, handled by the
    workflow, which resubmits. docling-serve forgot the task (Redis flushed,
    results TTL elapsed, state lost) -- asking about the same id again can't
    help.

A task that ran and failed isn't an activity error at all: check_task reports
it as a TaskCompleted, the same shape the callback relay's signal carries, and
the workflow fails with ConversionFailed whichever path told it.
"""

import asyncio
from http import HTTPStatus
from typing import Any

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from orchestration.config import Settings
from orchestration.docling_client import (
    build_batch_convert_body,
    callback_specs,
    convert_output_prefix,
    tenant_headers,
)
from orchestration.models import (
    CollectedOutputs,
    CollectOutputsInput,
    ErrorType,
    SubmitConversionInput,
    TaskCompleted,
    TaskOutcome,
    TaskStatus,
)

# Longest suffix first: ".doctags.txt" and ".chunks.jsonl" would otherwise
# match ".txt" / ".json"-family entries meant for other formats.
_SUFFIX_TO_FORMAT: list[tuple[str, str]] = [
    (".doctags.txt", "doctags"),
    (".chunks.jsonl", "chunks"),
    (".json", "json"),
    (".md", "md"),
    (".html", "html"),
    (".dclg", "doclang"),
    (".txt", "text"),
]


def format_of(key: str) -> str:
    for suffix, fmt in _SUFFIX_TO_FORMAT:
        if key.endswith(suffix):
            return fmt
    return key.rsplit(".", 1)[-1] if "." in key.rsplit("/", 1)[-1] else "unknown"


def _raise_for_upstream_status(response: httpx.Response, *, action: str) -> None:
    if not response.is_error:
        return
    detail = f"{action}: HTTP {response.status_code}: {response.text[:500]}"
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
        raise ApplicationError(detail, type=ErrorType.QUEUE_FULL)
    if response.is_server_error:
        raise ApplicationError(detail, type=ErrorType.UPSTREAM_ERROR)
    raise ApplicationError(detail, type=ErrorType.SUBMIT_REJECTED, non_retryable=True)


class DoclingActivities:
    """Activities share one HTTP client (docling-serve) and one S3 client
    (sync boto3, called via asyncio.to_thread) for the worker's lifetime."""

    def __init__(self, settings: Settings, http: httpx.AsyncClient, s3: Any):
        self._settings = settings
        self._http = http
        self._s3 = s3

    @activity.defn
    async def submit_conversion(self, inp: SubmitConversionInput) -> str:
        body = build_batch_convert_body(
            self._settings,
            bucket=inp.bucket,
            key=inp.key,
            options=inp.options,
            callbacks=callback_specs(self._settings, activity.info().workflow_id),
        )
        response = await self._http.post(
            "/v1/convert/source/batch",
            json=body,
            headers=tenant_headers(self._settings),
        )
        _raise_for_upstream_status(response, action="submit")
        task_id = response.json()["task_id"]
        activity.logger.info(
            "submitted s3://%s/%s as task %s", inp.bucket, inp.key, task_id
        )
        return task_id

    @activity.defn
    async def check_task(self, task_id: str) -> TaskCompleted | None:
        """One status poll, no server-side wait: the workflow's safety net
        for a callback that's late or never comes. None while the task is
        still queued or running."""
        response = await self._http.get(
            f"/v1/status/poll/{task_id}", headers=tenant_headers(self._settings)
        )
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise ApplicationError(
                f"docling-serve no longer knows task {task_id}",
                type=ErrorType.TASK_LOST,
                non_retryable=True,
            )
        _raise_for_upstream_status(response, action="status poll")
        status = response.json()
        if status["task_status"] not in set(TaskStatus):
            return None
        return TaskCompleted(
            task_id=task_id,
            task_status=TaskStatus(status["task_status"]),
            error_message=status.get("error_message"),
        )

    @activity.defn
    async def fetch_outcome(self, task_id: str) -> TaskOutcome:
        response = await self._http.get(
            f"/v1/result/{task_id}", headers=tenant_headers(self._settings)
        )
        if response.status_code == HTTPStatus.NOT_FOUND:
            activity.logger.warning("result for %s already expired", task_id)
            return TaskOutcome(task_id, None, None, None)
        _raise_for_upstream_status(response, action="result fetch")
        result = response.json()
        return TaskOutcome(
            task_id=task_id,
            num_succeeded=result.get("num_succeeded"),
            num_partially_succeeded=result.get("num_partially_succeeded"),
            num_failed=result.get("num_failed"),
        )

    @activity.defn
    async def collect_outputs(self, inp: CollectOutputsInput) -> CollectedOutputs:
        bucket = self._settings.output_bucket
        prefix = convert_output_prefix(self._settings, inp.bucket, inp.key)
        keys = await asyncio.to_thread(self._list_keys, bucket, prefix)
        outputs: dict[str, list[str]] = {}
        for key in sorted(keys):
            outputs.setdefault(format_of(key), []).append(key)
        return CollectedOutputs(bucket=bucket, outputs=outputs)

    def _list_keys(self, bucket: str, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys
