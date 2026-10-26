"""Starting a conversion -- the one place a workflow's identity is decided."""

from temporalio.client import Client, WorkflowHandle

from orchestration.models import (
    TASK_QUEUE,
    ConvertInput,
    ConvertResult,
    workflow_id_for_convert,
)
from orchestration.workflows import ConvertDocumentWorkflow


async def start_conversion(
    client: Client, inp: ConvertInput
) -> WorkflowHandle[ConvertDocumentWorkflow, ConvertResult]:
    """Starts ConvertDocumentWorkflow for one document. Its id is the
    document's coordinates, so while a conversion of it is in flight
    Temporal rejects another start (WorkflowAlreadyStartedError) instead of
    converting it twice; once that run has closed, starting again
    re-converts."""
    return await client.start_workflow(
        ConvertDocumentWorkflow.run,
        inp,
        id=workflow_id_for_convert(inp.bucket, inp.key),
        task_queue=TASK_QUEUE,
    )
