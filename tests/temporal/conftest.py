"""Two real compose stacks, wired the way they're deployed: the main stack's
conversion backend (MinIO, Redis, Ray, docling-serve -- no facade/nginx, the
orchestration path doesn't use them) plus src/orchestration's Temporal stack,
whose worker and callback relay join the main stack's network by name.

Driven with plain `docker compose -p <project>` rather than testcontainers'
DockerCompose: the two stacks need distinct project names, and a
COMPOSE_PROJECT_NAME env var (the only knob DockerCompose offers) would
override the Temporal file's own `name:` too and merge both into one project.

GitHub-hosted CI runners have no GPU: DOCLING_COMPOSE_CPU_OVERLAY layers
tests/ci/docker-compose.cpu.yml on the main stack, as for tests/integration.
tests/temporal/docker-compose.override.yml publishes docling-serve and Redis
on ephemeral host ports, for the tests that drive docling directly or make it
lose a task.
"""

import asyncio
import os
import socket
import subprocess
import time
from contextlib import contextmanager
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
    main_files.append("tests/temporal/docker-compose.override.yml")
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
        _wait_healthy(f"{_TEMPORAL_PROJECT}-callback-relay-1", timeout=180)
        yield {
            "temporal_address": f"localhost:{temporal_port}",
            "temporal_container": f"{_TEMPORAL_PROJECT}-temporal-1",
            "worker_container": f"{_TEMPORAL_PROJECT}-worker-1",
            "relay_container": f"{_TEMPORAL_PROJECT}-callback-relay-1",
            "docling_container": f"{_MAIN_PROJECT}-docling-serve-1",
            "redis_container": f"{_MAIN_PROJECT}-redis-1",
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


@pytest.fixture
def relay_down(stacks):
    """The callback relay stopped for the test's duration -- every docling
    callback in that window is lost -- then restarted and healthy again
    before the next test."""

    @contextmanager
    def down():
        relay = stacks["relay_container"]
        subprocess.run(["docker", "stop", relay], check=True)
        try:
            yield
        finally:
            subprocess.run(["docker", "start", relay], check=True)
            _wait_healthy(relay, timeout=60)

    return down


@pytest.fixture
def wrong_relay_token(stacks):
    """The callback relay recreated with a token other than the worker's for
    the test's duration -- so it turns docling's callbacks away -- then
    recreated with the right one. Yields a function returning the relay's
    logs."""

    @contextmanager
    def wrong():
        relay = stacks["relay_container"]

        def recreate(token_env: dict) -> None:
            _compose(
                stacks["temporal_project"],
                stacks["temporal_files"],
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "callback-relay",
                env={**stacks["temporal_env"], **token_env},
            )
            _wait_healthy(relay, timeout=60)

        recreate({"CALLBACK_TOKEN": "not-the-workers-token"})
        try:
            yield lambda: subprocess.run(
                ["docker", "logs", relay], capture_output=True, text=True
            ).stdout
        finally:
            recreate({})

    return wrong


@pytest.fixture
def redis_cli(stacks):
    """Runs redis-cli in docling's Redis, returning its output."""

    def run(*args: str) -> str:
        return subprocess.run(
            ["docker", "exec", stacks["redis_container"], "redis-cli", *args],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    return run


@pytest.fixture
def docling_url(stacks):
    """docling-serve's base URL on the host. A function: the published port
    is ephemeral, and changes when the container restarts."""

    def url() -> str:
        mapped = subprocess.run(
            ["docker", "port", stacks["docling_container"], "5001"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()[0]
        return f"http://localhost:{mapped.rsplit(':', 1)[1]}"

    return url


@pytest.fixture
def docling_loses(stacks, redis_cli):
    """Makes docling-serve forget a task, the way it would losing Redis's
    data across a crash. Both must go: docling-serve also keeps its tasks in
    memory, and answers status polls from there while it runs."""

    def lose(task_id: str) -> None:
        docling = stacks["docling_container"]
        subprocess.run(["docker", "kill", docling], check=True)
        keys = redis_cli("--scan", "--pattern", f"*{task_id}*").split()
        assert keys, f"no Redis records for task {task_id}"
        redis_cli("DEL", *keys)
        subprocess.run(["docker", "start", docling], check=True)
        _wait_healthy(docling, timeout=300)

    return lose


@pytest.fixture
def restart_temporal(stacks):
    def restart() -> None:
        temporal = stacks["temporal_container"]
        subprocess.run(["docker", "restart", temporal], check=True)
        _wait_healthy(temporal, timeout=120)

    return restart


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
