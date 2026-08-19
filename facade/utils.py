"""Format mapping and S3 artifact discovery/reassembly helpers.

Format is derived from the leaf filename's extension, not a parent directory
-- confirmed empirically (against a live MinIO-backed run) that the Ray
orchestrator's actual S3 output layout is flat under an extra hash directory
(`{our_prefix}/{12-char-hash}/{stem}.{ext}`), not the per-format subfolder
layout `convert/results_processor.py` (a different, non-Ray code path)
suggested. Deriving from the extension instead of a folder name sidesteps
that layout entirely -- works regardless of how many directory levels sit
above the leaf file.
"""

import io
import json
import zipfile

from facade.schemas import ExportDocumentResponse

# File extension (longest/most-specific first, since ".doctags.txt" is also
# a suffix match for ".txt") -> ExportDocumentResponse field.
EXTENSION_TO_FIELD: list[tuple[str, str]] = [
    (".doctags.txt", "doctags_content"),
    (".json", "json_content"),
    (".md", "md_content"),
    (".html", "html_content"),
    (".dclg", "doclang_content"),
    (".txt", "text_content"),
]

DEFAULT_TO_FORMATS = ["md"]


def parse_artifact_key(prefix: str, key: str) -> tuple[str, str, str] | None:
    """Extract (extension, field_name, filename_stem) from a listed S3 key."""
    if not key.startswith(prefix):
        return None
    filename = key[len(prefix) :].rsplit("/", 1)[-1]
    for ext, field_name in EXTENSION_TO_FIELD:
        if filename.endswith(ext):
            return ext, field_name, filename[: -len(ext)]
    return None


def list_artifacts(s3_client, *, bucket: str, prefix: str) -> list[str]:
    """Recursively list every object under `prefix`, returning raw S3 keys."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def fetch_artifacts_by_stem(
    s3_client, *, bucket: str, prefix: str
) -> dict[str, dict[str, tuple[str, bytes]]]:
    """Recursively list+fetch every artifact under `prefix`, grouped by
    filename stem then field name: {stem: {field_name: (ext, body_bytes)}}.
    """
    grouped: dict[str, dict[str, tuple[str, bytes]]] = {}
    for key in list_artifacts(s3_client, bucket=bucket, prefix=prefix):
        parsed = parse_artifact_key(prefix, key)
        if parsed is None:
            continue
        ext, field_name, stem = parsed
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        grouped.setdefault(stem, {})[field_name] = (ext, body)
    return grouped


def build_export_document_response(
    filename: str, artifacts_by_field: dict[str, tuple[str, bytes]]
) -> ExportDocumentResponse:
    fields: dict[str, object] = {"filename": filename}
    for field_name, (_ext, body) in artifacts_by_field.items():
        text = body.decode("utf-8")
        fields[field_name] = json.loads(text) if field_name == "json_content" else text
    return ExportDocumentResponse(**fields)


def build_zip_archive(
    documents: list[tuple[str, dict[str, tuple[str, bytes]]]],
) -> bytes:
    """Mirror docling-serve's own multi-file zip layout: one flat file per
    (document, format) pair, named `{stem}{ext}` -- confirmed empirically
    against a live 2-file /v1/convert/file/async request (default inbody
    target auto-falls-back to a zip when more than one file is submitted).
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for stem, artifacts_by_field in documents:
            for _field_name, (ext, body) in artifacts_by_field.items():
                zf.writestr(f"{stem}{ext}", body)
    return buf.getvalue()
