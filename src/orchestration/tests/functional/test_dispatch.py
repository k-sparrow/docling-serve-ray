"""dispatch.start_conversion: one conversion per document at a time, in
Temporal's time-skipping test server."""

import uuid

import pytest
from temporalio.exceptions import WorkflowAlreadyStartedError

from orchestration.dispatch import start_conversion
from orchestration.models import ConvertInput
from orchestration.tests.functional.fakes import FakeDocling

BUCKET = "docling-input"


async def test_a_document_in_flight_is_not_dispatched_twice(workflow_env, real_workers):
    key = f"dup-{uuid.uuid4()}.pdf"
    inp = ConvertInput(bucket=BUCKET, key=key)
    docling = FakeDocling()
    main, submit = real_workers(docling, BUCKET, [key])

    async with main, submit:
        first = await start_conversion(workflow_env.client, inp)
        assert first.id == f"convert:{BUCKET}/{key}"

        # Still in flight: sleeping until a callback, or the safety poll the
        # test server only reaches once a result is awaited.
        with pytest.raises(WorkflowAlreadyStartedError):
            await start_conversion(workflow_env.client, inp)

        await first.result()
        # Closed: dispatching the document again re-converts it.
        again = await start_conversion(workflow_env.client, inp)
        await again.result()

    assert again.id == first.id
    assert again.result_run_id != first.result_run_id
    assert docling.submits == 2
