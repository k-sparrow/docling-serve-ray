"""Workflow and activity inputs/outputs.

Plain dataclasses (Temporal's default data converter round-trips them as
JSON) with no imports beyond the standard library, so the workflow sandbox can
load this module without passthrough.
"""

from dataclasses import dataclass, field

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


@dataclass
class ConvertInput:
    bucket: str
    key: str
    to_formats: list[str] = field(default_factory=lambda: list(DEFAULT_TO_FORMATS))
    # Any other ConvertDocumentsOptions fields (do_ocr, page_range, ...),
    # forwarded to docling-serve untouched.
    options: dict = field(default_factory=dict)


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


def workflow_id_for_convert(bucket: str, key: str) -> str:
    """One workflow per document: re-starting a still-running conversion of
    the same object is rejected by Temporal instead of submitted twice."""
    return f"convert:{bucket}/{key}"
