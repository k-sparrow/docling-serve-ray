"""Workflow and activity inputs/outputs.

Plain dataclasses (Temporal's default data converter round-trips them as
JSON) with no imports beyond the standard library, so the workflow sandbox can
load this module without passthrough.
"""

from dataclasses import dataclass, field
from enum import StrEnum

# Task queues are constants, not settings: the workflow routes activities to
# them, and workflow code can't read per-process configuration
# deterministically -- a worker started with a different queue name would
# silently strand every submit.
#
# Workflows plus the parallel-safe activities (polling, output listing).
TASK_QUEUE = "docling"
# Submits only. Its worker runs with max_concurrent_activities=1, which is
# what makes admission into docling-serve strictly serial across every
# workflow -- docling's per-tenant queue counter isn't updated atomically
# (check-then-push in jobkit's enqueue()), so concurrent submits overshoot
# MAX_QUEUED_TASKS. Serial submission is the accepted mitigation. This only
# holds with ONE worker process: N replicas would mean N concurrent submits.
SUBMIT_TASK_QUEUE = "docling-submit"

# DoclingDocument JSON is always produced, whatever the caller asks for: it's
# the lossless output, and what a downstream chunking step re-ingests
# (from_formats=["json_docling"]) without re-running the conversion pipeline.
REQUIRED_FORMAT = "json"
DEFAULT_TO_FORMATS = ["json", "md"]

# Signal the callback relay (src/callback_relay) sends when docling-serve
# reports a task finished. Part of the relay's contract, like TaskCompleted's
# field names below.
TASK_COMPLETED_SIGNAL = "task_completed"

# How long a workflow sleeps waiting for that signal before checking the task
# itself, and how many such checks it makes before giving up. 600 x ~5 min
# covers ~50h: up to 24h queued behind other documents (8 queued x 2 slots x
# ~6h) plus up to DOCLING_SERVE_ENG_RAY_TASK_TIMEOUT (24h) running.
DEFAULT_SAFETY_POLL_INTERVAL_SECONDS = 300
DEFAULT_MAX_SAFETY_POLLS = 600


class ErrorType(StrEnum):
    """ApplicationError.type of every failure the activities and workflow
    raise (see activities.py for which are retryable). The values are what
    Temporal records in history and shows in the UI, and what the workflow
    matches on -- renaming one breaks replay of runs already in flight."""

    QUEUE_FULL = "QueueFull"
    UPSTREAM_ERROR = "UpstreamError"
    SUBMIT_REJECTED = "SubmitRejected"
    TASK_LOST = "TaskLost"
    CONVERSION_FAILED = "ConversionFailed"
    MISSING_OUTPUTS = "MissingOutputs"
    POLL_BUDGET_EXHAUSTED = "PollBudgetExhausted"


class TaskStatus(StrEnum):
    """docling-serve's terminal task states."""

    SUCCESS = "success"
    FAILURE = "failure"


@dataclass
class ConvertInput:
    bucket: str
    key: str
    to_formats: list[str] = field(default_factory=lambda: list(DEFAULT_TO_FORMATS))
    # Any other ConvertDocumentsOptions fields (do_ocr, page_range, ...),
    # forwarded to docling-serve untouched.
    options: dict = field(default_factory=dict)
    # Average sleep between safety polls (jittered +/-20%) -- also the
    # worst-case extra latency when a callback is lost.
    safety_poll_interval_seconds: int = DEFAULT_SAFETY_POLL_INTERVAL_SECONDS
    # Safety polls that find the task still in progress before the workflow
    # gives up on it (PollBudgetExhausted). Per submission.
    max_safety_polls: int = DEFAULT_MAX_SAFETY_POLLS


@dataclass
class SubmitConversionInput:
    bucket: str
    key: str
    options: dict


@dataclass
class CollectOutputsInput:
    bucket: str
    key: str


@dataclass
class TaskCompleted:
    """Terminal state of a docling task, from either path: the callback
    relay's signal or a safety poll. Field names are the relay's JSON
    contract -- its Go struct is tagged the same way."""

    task_id: str
    task_status: TaskStatus
    error_message: str | None = None


@dataclass
class TaskOutcome:
    task_id: str
    # From /v1/result. None when the result record is already gone (it
    # expires DOCLING_SERVE_ENG_RAY_RESULTS_TTL after completion) -- the
    # outputs themselves are still in S3, so that alone isn't a failure.
    num_succeeded: int | None
    num_partially_succeeded: int | None
    num_failed: int | None


@dataclass
class CollectedOutputs:
    bucket: str
    # Format (json, md, ...) -> S3 keys, sorted.
    outputs: dict[str, list[str]]


@dataclass
class ConvertResult:
    task_id: str
    # Format (json, md, ...) -> S3 keys in the output bucket.
    outputs: dict[str, list[str]]
    output_bucket: str
    submissions: int
    # Safety polls made across all submissions -- 0 when every callback
    # arrived before its first timer expired.
    safety_polls: int


def workflow_id_for_convert(bucket: str, key: str) -> str:
    """One workflow per document: re-starting a still-running conversion of
    the same object is rejected by Temporal instead of submitted twice."""
    return f"convert:{bucket}/{key}"
