"""Shared fixtures: a pinned Temporal time-skipping test server, and fakes.

Under Bazel, TEMPORAL_TEST_SERVER points at the binary MODULE.bazel pins
(http_archive + sha256), so the workflow tests never download anything at
runtime. Without it (a plain local pytest run) the SDK falls back to its own
download.
"""

import os
import shutil
import stat
from pathlib import Path

import pytest
import pytest_asyncio
from temporalio.testing import WorkflowEnvironment

from orchestration.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        docling_serve_url="http://docling.test",
        s3_endpoint="minio:9000",
        s3_access_key="ak",
        s3_secret_key="sk",
        output_bucket="out",
        output_prefix="temporal/",
        status_poll_wait_seconds=0.01,
    )


@pytest.fixture(scope="session")
def test_server_path(tmp_path_factory) -> str | None:
    pinned = os.environ.get("TEMPORAL_TEST_SERVER")
    if not pinned:
        return None
    # The release archive ships the binary as mode 644 and Bazel's runfiles
    # are read-only, so run a private executable copy instead.
    exe = tmp_path_factory.mktemp("temporal") / "temporal-test-server"
    shutil.copyfile(Path(pinned).resolve(), exe)
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    return str(exe)


@pytest_asyncio.fixture(scope="session")
async def workflow_env(test_server_path):
    env = await WorkflowEnvironment.start_time_skipping(
        test_server_existing_path=test_server_path
    )
    try:
        yield env
    finally:
        await env.shutdown()
