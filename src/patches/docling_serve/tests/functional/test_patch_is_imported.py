"""Integration-level check that the app.py patch is what actually gets imported.

The container structure test in this package proves the patched app.py
exists at the right filesystem path in the real (Docker-merged) image. It
cannot prove Python's import machinery actually resolves to that file rather
than a shadowed duplicate -- that's a real, distinct failure mode: Artemis
(a sister project patching this same docling-serve image the same way) hit
exactly this gap, where a container_structure_test using driver="tar" passed
on a build where the patch file existed in the tarball but was never
imported by the running container. This runs the actual check they replaced
it with: import the patched module inside a real container and assert on its
resolved content, not just its filesystem presence.
"""

import os

import docker
import pytest

pytestmark = pytest.mark.integration

# CI loads only the CPU-patched variant (see tests/ci/docker-compose.cpu.yml
# and BUILD.bazel's :tarball.cpu) since GitHub-hosted runners have no GPU --
# the GPU tag is never `docker load`-ed there and isn't pushed to a registry,
# so pulling it fails outright. Same opt-in env var the compose-based
# integration/e2e conftest.py fixtures key off of.
_IMAGE = (
    "docling-serve-ray-patched:v1.36.0-bazel-cpu"
    if os.environ.get("DOCLING_COMPOSE_CPU_OVERLAY")
    else "docling-serve-ray-patched:v1.36.0-bazel"
)

_CHECK_SCRIPT = (
    "import inspect, docling_serve.app as m; "
    "src = inspect.getsource(m); "
    "assert 'queue_limit_exceeded_error_handler' in src, "
    "'patched app.py not imported: handler missing from source'; "
    "assert 'HTTP_429_TOO_MANY_REQUESTS' in src, "
    "'patched app.py not imported: 429 status missing from source'; "
    "print('OK: patch is live')"
)


def test_patched_app_module_is_actually_imported():
    client = docker.from_env()
    output = client.containers.run(
        _IMAGE,
        ["python3", "-c", _CHECK_SCRIPT],
        entrypoint=[],
        remove=True,
    )
    assert b"OK: patch is live" in output, output
