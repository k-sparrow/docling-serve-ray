"""Facade -> docling-serve integration layer.

Follows Artemis's container-based integration convention (see
docling-serve-ray-facade-design in project memory for the research this
mirrors): the app under test runs as a real container against its real
direct dependencies, hit over real HTTP -- not app code called in-process
against a fixture backend. nginx is deliberately excluded here (that's the
e2e layer, see tests/e2e/); this layer exists to prove the facade's own
claim-check logic against a genuinely running docling-serve, without the
routing question e2e also covers.
"""

import os
import time
from pathlib import Path

import boto3
import httpx
import pytest
import redis as redis_sync
from testcontainers.compose import DockerCompose

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVICES = [
    "minio",
    "minio-init",
    "redis",
    "ray-head",
    "ray-worker",
    "docling-serve",
    "facade",
]
_CPU_OVERLAY = "tests/ci/docker-compose.cpu.yml"

# testcontainers' DockerCompose never sets a Compose project name itself, so
# plain `docker compose` falls back to its own default: the basename of the
# `context` directory. Both this layer and tests/e2e point `context` at the
# same repo root, so without an override they'd collide on the same project
# name/network/container names -- confirmed empirically (a stale integration
# run's containers got half-Recreated mid-way through an e2e run, breaking a
# dependency chain with a "No such container" error). Isolate explicitly.
os.environ["COMPOSE_PROJECT_NAME"] = "docling-serve-ray-integration"


def _compose_files(*base_files: str) -> list[str]:
    # GitHub-hosted CI runners have no GPU, and ray-head/ray-worker/
    # docling-serve all hard-require one in the base compose file -- see
    # tests/ci/docker-compose.cpu.yml's own header for the full reasoning.
    # Opt-in via env var so local/production runs stay on the real,
    # GPU-backed image by default.
    files = list(base_files)
    if os.environ.get("DOCLING_COMPOSE_CPU_OVERLAY"):
        files.append(_CPU_OVERLAY)
    return files


def _wait_for_http(url: str, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=5.0)
            if response.status_code < 500:
                return
        except httpx.HTTPError as exc:
            last_error = exc
        time.sleep(3)
    raise TimeoutError(f"{url} did not become ready within {timeout}s") from last_error


@pytest.fixture(scope="session")
def facade_stack():
    compose = DockerCompose(
        context=str(_REPO_ROOT),
        compose_file_name=_compose_files(
            "docker-compose.yml", "tests/integration/docker-compose.override.yml"
        ),
        services=_SERVICES,
        pull=False,
        build=False,
    )
    try:
        compose.start()
    except Exception:
        # `docker compose up --wait` treats a normally-exited one-shot init
        # container (minio-init, exit 0) as a wait failure regardless of its
        # exit code -- ignore and rely on the explicit HTTP polling below for
        # actual readiness instead of trusting compose's own wait exit code
        # (same workaround Artemis's own e2e suite uses for the same reason).
        pass
    try:
        facade_host, facade_port = compose.get_service_host_and_port("facade", 8000)
        docling_host, docling_port = compose.get_service_host_and_port(
            "docling-serve", 5001
        )
        minio_host, minio_port = compose.get_service_host_and_port("minio", 9000)
        redis_host, redis_port = compose.get_service_host_and_port("redis", 6379)
        facade_url = f"http://{facade_host}:{facade_port}"
        docling_url = f"http://{docling_host}:{docling_port}"
        _wait_for_http(f"{facade_url}/healthz", timeout=120)
        _wait_for_http(f"{docling_url}/health", timeout=120)
        yield {
            "facade_url": facade_url,
            "docling_url": docling_url,
            "minio_endpoint": f"{minio_host}:{minio_port}",
            "redis_url": f"redis://{redis_host}:{redis_port}/1",
        }
    finally:
        compose.stop()


@pytest.fixture(scope="session")
def facade_client(facade_stack):
    with httpx.Client(base_url=facade_stack["facade_url"], timeout=180.0) as client:
        yield client


@pytest.fixture(scope="session")
def docling_client(facade_stack):
    with httpx.Client(base_url=facade_stack["docling_url"], timeout=180.0) as client:
        yield client


@pytest.fixture(scope="session")
def s3_client(facade_stack):
    # Same credentials as the facade container's own FACADE_S3_* env vars in
    # docker-compose.yml -- this connects as an independent client, not
    # through the facade, so it can observe cleanup from the outside.
    return boto3.client(
        "s3",
        endpoint_url=f"http://{facade_stack['minio_endpoint']}",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )


@pytest.fixture(scope="session")
def redis_client(facade_stack):
    client = redis_sync.Redis.from_url(facade_stack["redis_url"], decode_responses=True)
    try:
        yield client
    finally:
        client.close()
