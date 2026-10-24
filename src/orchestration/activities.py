"""Activities: every side effect a conversion workflow performs.

Failure classification is the important part -- it decides what Temporal
retries on its own and what the workflow has to handle:

  - QueueFull (429) / UpstreamError (5xx) / transport errors: retryable. The
    retry policy (workflows.py) backs off and resubmits; a 429 is docling's
    queue-limit backpressure, not an error.
  - SubmitRejected (any other 4xx): non-retryable -- a malformed request
    won't succeed on attempt 2.
  - TaskLost (404 while polling): non-retryable here, handled by the
    workflow, which resubmits. docling-serve forgot the task (Redis flushed,
    results TTL elapsed, state lost) -- polling the same id again can't help.
  - ConversionFailed: non-retryable -- docling ran the document and it
    failed.
"""

import asyncio
from typing import Any

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from orchestration.config import Settings
from orchestration.docling_client import (
    build_batch_convert_body,
    convert_output_prefix,
    tenant_headers,
)
from orchestration.models import (
    CollectedOutputs,
    CollectOutputsInput,
    SubmitConversionInput,
    TaskOutcome,
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

_TERMINAL_SUCCESS = "success"
_TERMINAL_FAILURE = "failure"


def format_of(key: str) -> str:
    for suffix, fmt in _SUFFIX_TO_FORMAT:
        if key.endswith(suffix):
            return fmt
    return key.rsplit(".", 1)[-1] if "." in key.rsplit("/", 1)[-1] else "unknown"


def _raise_for_upstream_status(response: httpx.Response, *, action: str) -> None:
    if response.status_code < 400:
        return
    detail = f"{action}: HTTP {response.status_code}: {response.text[:500]}"
    if response.status_code == 429:
        raise ApplicationError(detail, type="QueueFull")
    if response.status_code >= 500:
        raise ApplicationError(detail, type="UpstreamError")
    raise ApplicationError(detail, type="SubmitRejected", non_retryable=True)


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
            self._settings, bucket=inp.bucket, key=inp.key, options=inp.options
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
    async def wait_for_task(self, task_id: str) -> TaskOutcome:
        """Long-polls until the task is terminal, heartbeating once per poll.

        Safe to retry from scratch: its only input is the task id, so a retry
        after a worker crash (heartbeat timeout) resumes polling the same
        docling task rather than resubmitting anything.
        """
        wait = self._settings.status_poll_wait_seconds
        headers = tenant_headers(self._settings)
        while True:
            response = await self._http.get(
                f"/v1/status/poll/{task_id}",
                params={"wait": wait},
                headers=headers,
                timeout=wait + 30.0,
            )
            if response.status_code == 404:
                raise ApplicationError(
                    f"docling-serve no longer knows task {task_id}",
                    type="TaskLost",
                    non_retryable=True,
                )
            _raise_for_upstream_status(response, action="status poll")
            status = response.json()
            task_status = status["task_status"]
            activity.heartbeat(
                {"task_status": task_status, "position": status.get("task_position")}
            )
            if task_status == _TERMINAL_FAILURE:
                raise ApplicationError(
                    f"task {task_id} failed: {status.get('error_message')}",
                    status.get("failure"),
                    type="ConversionFailed",
                    non_retryable=True,
                )
            if task_status == _TERMINAL_SUCCESS:
                return await self._outcome(task_id, headers)

    async def _outcome(self, task_id: str, headers: dict[str, str]) -> TaskOutcome:
        response = await self._http.get(f"/v1/result/{task_id}", headers=headers)
        if response.status_code == 404:
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
