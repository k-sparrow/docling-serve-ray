"""Full-stack e2e layer: the real docker-compose.yml, hit through nginx --
the only entrypoint a real client would ever use. Mirrors Artemis's
DockerCompose-driven e2e pattern (see docling-serve-ray-facade-design in
project memory for the research this follows).

Locally/in production this runs docker-compose.yml genuinely unmodified. The
one exception is CI: GitHub-hosted runners have no GPU, and ray-head/
ray-worker/docling-serve all hard-require one in the base compose file, so
DOCLING_COMPOSE_CPU_OVERLAY layers tests/ci/docker-compose.cpu.yml on top --
same patch, same version, CPU-only image and no GPU device reservation. See
that file's own header comment for the full reasoning.
"""

import os
import time
from pathlib import Path

import httpx
import pytest
from testcontainers.compose import DockerCompose

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CPU_OVERLAY = "tests/ci/docker-compose.cpu.yml"

# See the identical note in tests/integration/conftest.py: testcontainers'
# DockerCompose never sets a project name itself, so it'd otherwise collide
# with the integration layer (both point `context` at this same repo root).
os.environ["COMPOSE_PROJECT_NAME"] = "docling-serve-ray-e2e"


def _compose_files(*base_files: str) -> list[str]:
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
def stack_url():
    compose = DockerCompose(
        context=str(_REPO_ROOT),
        compose_file_name=_compose_files("docker-compose.yml"),
        pull=False,
        build=False,
    )
    try:
        compose.start()
    except Exception:
        # docker compose up --wait treats a normally-exited one-shot init
        # container (minio-init, exit 0) as a wait failure regardless of its
        # exit code -- ignore and rely on explicit HTTP polling below.
        pass
    try:
        host, port = compose.get_service_host_and_port("nginx", 5001)
        base_url = f"http://{host}:{port}"
        _wait_for_http(f"{base_url}/health", timeout=180)
        yield base_url
    finally:
        compose.stop()


@pytest.fixture(scope="session")
def client(stack_url):
    with httpx.Client(base_url=stack_url, timeout=180.0) as c:
        yield c
