"""Integration-level check that the patch is what actually gets imported.

The container structure test in this directory proves the patched models.py
exists at the right filesystem path in the real (Docker-merged) image. It
cannot prove Python's import machinery actually resolves to that file rather
than a shadowed duplicate -- that's a real, distinct failure mode: Artemis
(a sister project patching this same docling-serve image the same way) hit
exactly this gap, where a container_structure_test using driver="tar" passed
on a build where the patch file existed in the tarball but was never
imported by the running container. This test runs the actual check they
replaced it with: execute the patched module inside a real container and
assert on its resolved content, not just its filesystem presence.
"""

import docker
import pytest

pytestmark = pytest.mark.integration

_IMAGE = "docling-serve-ray-patched:v1.29.0-jobkit-fix-bazel"

_CHECK_SCRIPT = (
    "import inspect, docling_jobkit.orchestrators.ray.models as m; "
    "src = inspect.getsource(m.SourceChunkConvertRequest); "
    "assert 'chunk: DocumentChunk = Field(' in src, src; "
    "print('OK: patch is live')"
)


def test_patched_models_module_is_actually_imported():
    client = docker.from_env()
    output = client.containers.run(
        _IMAGE,
        ["python3", "-c", _CHECK_SCRIPT],
        entrypoint=[],
        remove=True,
    )
    assert b"OK: patch is live" in output, output
