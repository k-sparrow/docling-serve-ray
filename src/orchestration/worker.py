"""Worker process: hosts the workflow plus both activity task queues.

Run exactly ONE replica. The submit queue's max_concurrent_activities=1 is a
per-process limit, so N replicas would allow N concurrent submits into
docling-serve and defeat the serial-admission guarantee (see
models.SUBMIT_TASK_QUEUE).
"""

import asyncio
import logging
import signal
from datetime import timedelta

import boto3
import httpx
from temporalio.client import Client
from temporalio.worker import Worker

from orchestration.activities import DoclingActivities
from orchestration.config import get_settings
from orchestration.models import SUBMIT_TASK_QUEUE, TASK_QUEUE
from orchestration.workflows import ConvertDocumentWorkflow

# How long in-flight activities get to finish once shutdown starts, before
# they're cancelled (the SDK's default is to cancel them at once). Every
# activity here is a request or two, done in seconds. The two workers shut
# down one after the other, so a stop can take twice this:
# docker-compose.yml's stop_grace_period covers that before Docker kills.
GRACEFUL_SHUTDOWN = timedelta(seconds=10)


async def run() -> None:
    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    async with httpx.AsyncClient(
        base_url=settings.docling_serve_url, timeout=60.0
    ) as http:
        acts = DoclingActivities(settings, http, s3)
        main = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[ConvertDocumentWorkflow],
            activities=[acts.check_task, acts.fetch_outcome, acts.collect_outputs],
            max_concurrent_activities=settings.max_concurrent_activities,
            graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
        )
        submit = Worker(
            client,
            task_queue=SUBMIT_TASK_QUEUE,
            activities=[acts.submit_conversion],
            max_concurrent_activities=1,
            graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
        )
        async with main, submit:
            logging.info(
                "worker up: %s on %s, docling-serve at %s",
                [TASK_QUEUE, SUBMIT_TASK_QUEUE],
                settings.temporal_address,
                settings.docling_serve_url,
            )
            await shutdown.wait()
        # Leaving the context managers stops polling and lets in-flight
        # activities finish, for up to GRACEFUL_SHUTDOWN each. One still
        # running after that is cancelled: a status check is simply retried
        # later; a submit whose POST already landed can't be undone -- the
        # documented at-least-once case.


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
