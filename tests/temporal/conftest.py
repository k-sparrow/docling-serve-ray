"""Two real compose stacks, wired the way they're deployed: the main stack's
conversion backend (MinIO, Redis, Ray, docling-serve -- no facade/nginx, the
orchestration path doesn't use them) plus src/orchestration's Temporal stack,
whose worker joins the main stack's network by name.

Driven with plain `docker compose -p <project>` rather than testcontainers'
DockerCompose: the two stacks need distinct project names, and a
COMPOSE_PROJECT_NAME env var (the only knob DockerCompose offers) would
override the Temporal file's own `name:` too and merge both into one project.

GitHub-hosted CI runners have no GPU: DOCLING_COMPOSE_CPU_OVERLAY layers
tests/ci/docker-compose.cpu.yml on the main stack, as for tests/integration.
"""

import asyncio
import os
import socket
import subprocess
import time
from pathlib import Path

import boto3
import pytest
import pytest_asyncio
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
from temporalio.client import Client

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MAIN_PROJECT = "docling-serve-ray-temporal-it"
_TEMPORAL_PROJECT = "docling-temporal-it"
_BACKEND_SERVICES = [
    "minio",
    "minio-init",
    "redis",
    "ray-head",
    "ray-worker",
    "docling-serve",
]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _compose(
    project: str, files: list[str], *args: str, env: dict | None = None
) -> None:
    cmd = ["docker", "compose", "-p", project]
    for f in files:
        cmd += ["-f", f]
    subprocess.run(
        cmd + list(args),
        cwd=_REPO_ROOT,
        env={**os.environ, **(env or {})},
        check=True,
    )


def _wait_healthy(container: str, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    status = "unknown"
    while time.monotonic() < deadline:
        status = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            capture_output=True,
            text=True,
        ).stdout.strip()
        if status == "healthy":
            return
        time.sleep(3)
    raise TimeoutError(f"{container} not healthy after {timeout}s (last: {status})")


@pytest.fixture(scope="session")
def stacks():
    main_files = ["docker-compose.yml"]
    if os.environ.get("DOCLING_COMPOSE_CPU_OVERLAY"):
        main_files.append("tests/ci/docker-compose.cpu.yml")
    temporal_files = ["src/orchestration/docker-compose.yml"]
    temporal_port = _free_port()
    temporal_env = {
        "DOCLING_NETWORK": f"{_MAIN_PROJECT}_default",
        "TEMPORAL_HOST_PORT": str(temporal_port),
        "TEMPORAL_UI_HOST_PORT": str(_free_port()),
    }
    try:
        _compose(_MAIN_PROJECT, main_files, "up", "-d", *_BACKEND_SERVICES)
        _wait_healthy(f"{_MAIN_PROJECT}-docling-serve-1", timeout=600)
        # The worker attaches to the main stack's network, so this must come
        # second -- same order as a real deployment.
        _compose(_TEMPORAL_PROJECT, temporal_files, "up", "-d", env=temporal_env)
        _wait_healthy(f"{_TEMPORAL_PROJECT}-temporal-1", timeout=180)
        yield {
            "temporal_address": f"localhost:{temporal_port}",
            "worker_container": f"{_TEMPORAL_PROJECT}-worker-1",
            "temporal_project": _TEMPORAL_PROJECT,
            "temporal_files": temporal_files,
            "temporal_env": temporal_env,
        }
    finally:
        # Temporal first: its worker holds an endpoint on the main network.
        _compose(
            _TEMPORAL_PROJECT, temporal_files, "down", "--volumes", env=temporal_env
        )
        _compose(_MAIN_PROJECT, main_files, "down", "--volumes")


@pytest_asyncio.fixture
async def temporal_client(stacks) -> Client:
    # The namespace one-shot runs after the server reports healthy -- retry
    # until it exists rather than racing it.
    deadline = time.monotonic() + 120
    while True:
        try:
            client = await Client.connect(stacks["temporal_address"])
            await client.workflow_service.describe_namespace(
                DescribeNamespaceRequest(namespace="default")
            )
            return client
        except Exception:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(3)


@pytest.fixture(scope="session")
def s3(stacks):
    # Same credentials as the worker's ORCH_S3_* env in the Temporal stack.
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )
