"""Worker and starter settings.

Everything a workflow run needs that is either environment-specific (where
docling-serve, MinIO and Temporal live) or secret (S3 credentials) comes from
here, read by activities and the starter -- never passed as workflow input.
Workflow inputs end up in Temporal's event history (Postgres, the UI, `temporal
workflow show`), so they carry only document coordinates and conversion
options.

Defaults target a host-side run (the starter CLI, published ports on
localhost). The worker container overrides them via ORCH_* env vars in
src/orchestration/docker-compose.yml.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ORCH_")

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"

    # For the main task queue only (polling, output listing) -- the submit
    # queue's limit is fixed at 1, see models.SUBMIT_TASK_QUEUE.
    max_concurrent_activities: int = 16

    docling_serve_url: str = "http://localhost:5001"
    tenant_id: str | None = None
    tenant_id_header: str = "X-Tenant-Id"
    # Server-side long-poll window per /v1/status/poll call. Must stay well
    # under the wait activity's heartbeat timeout (workflows.py), since the
    # activity heartbeats once per poll.
    status_poll_wait_seconds: float = 30.0

    # How docling-serve's Ray workers reach S3 (compose DNS) -- goes into the
    # S3 coordinates of every submit request.
    s3_endpoint: str = "minio:9000"
    s3_verify_ssl: bool = False
    # How this process reaches S3 itself (listing inputs and outputs). Differs
    # from s3_endpoint for the host-side starter (published port).
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"

    output_bucket: str = "docling-output"
    output_prefix: str = "temporal/"


@lru_cache
def get_settings() -> Settings:
    return Settings()
