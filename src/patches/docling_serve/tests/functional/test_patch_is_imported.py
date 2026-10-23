"""Integration-level check that the app.py patch is what actually gets imported.

Same reasoning as the docling_jobkit models.py patch's own
test_patch_is_imported.py: the container structure test proves the patched
file exists at the right filesystem path, not that Python's import machinery
actually resolves to it over a shadowed duplicate. This runs the actual check
inside a real container.
"""

import os

import docker
import pytest

pytestmark = pytest.mark.integration

_IMAGE = (
    "docling-serve-ray-patched:v1.29.0-jobkit-fix-bazel-cpu"
    if os.environ.get("DOCLING_COMPOSE_CPU_OVERLAY")
    else "docling-serve-ray-patched:v1.29.0-jobkit-fix-bazel"
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
