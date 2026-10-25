"""Request shapes for docling-serve's async S3-batch API.

Same S3Coordinates/S3Target body the facade submits (see
src/facade/service.py:submit_file_upload), minus the facade's own upload step:
inputs here are objects already sitting in S3.
"""

from typing import Any
from urllib.parse import quote

from orchestration.config import Settings

CALLBACK_TOKEN_HEADER = "X-Callback-Token"


def convert_output_prefix(settings: Settings, bucket: str, key: str) -> str:
    """Per-document output prefix: re-converting the same object overwrites
    its previous outputs instead of accumulating per-run copies. jobkit adds
    its own 12-char source-hash directory underneath, so outputs must be
    listed recursively, never addressed by a predicted path."""
    return f"{settings.output_prefix}{bucket}/{key}/convert/"


def _s3_coords(settings: Settings, bucket: str, key_prefix: str) -> dict[str, Any]:
    return {
        "kind": "s3",
        "endpoint": settings.s3_endpoint,
        "verify_ssl": settings.s3_verify_ssl,
        "access_key": settings.s3_access_key,
        "secret_key": settings.s3_secret_key,
        "bucket": bucket,
        "key_prefix": key_prefix,
    }


def callback_specs(settings: Settings, workflow_id: str | None) -> list[dict[str, Any]]:
    """Where docling-serve reports the task's completion. The workflow id
    rides in the URL, so the relay can signal the right workflow without a
    lookup table; escaped, since it contains '/' and ':'. No relay
    configured, or no workflow to signal: no callback."""
    if not settings.callback_base_url or not workflow_id:
        return []
    base = settings.callback_base_url.rstrip("/")
    return [
        {
            "url": f"{base}/v1/callbacks/{quote(workflow_id, safe='')}",
            "headers": {CALLBACK_TOKEN_HEADER: settings.callback_token},
        }
    ]


def build_batch_convert_body(
    settings: Settings,
    *,
    bucket: str,
    key: str,
    options: dict[str, Any],
    callbacks: list[dict[str, Any]],
) -> dict[str, Any]:
    # key_prefix is a prefix match, not an exact key: the object's full key
    # also matches any sibling that merely extends it (`a.pdf` vs
    # `a.pdf.bak`). Acceptable for one-workflow-per-object dispatch from a
    # listing; worth knowing if keys can be prefixes of each other.
    return {
        "sources": [_s3_coords(settings, bucket, key)],
        "target": _s3_coords(
            settings,
            settings.output_bucket,
            convert_output_prefix(settings, bucket, key),
        ),
        # "options", not "convert_options": BatchConvertSourcesRequest has no
        # convert_options field, and an unknown key is silently ignored
        # (falling back to to_formats=["md"]) rather than rejected. See the
        # matching note in src/facade/service.py.
        "options": options,
        "callbacks": callbacks,
    }


def tenant_headers(settings: Settings) -> dict[str, str]:
    return {settings.tenant_id_header: settings.tenant_id} if settings.tenant_id else {}
