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

from facade.schemas import DocumentStatus, ExportDocumentResponse, ZipResult

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

# Fields the facade must intercept itself rather than forward -- "files" gets
# claim-checked to S3, "target_type" drives the facade's own inbody/zip
# decision on the *result* side, it was never a docling-serve conversion
# option to begin with (native docling-serve's own multipart target_type
# Form field is disjoint from ConvertDocumentsOptions).
_INTERCEPTED_FIELDS = frozenset({"files", "target_type"})

# docling-serve's ConvertDocumentsOptions has 47 fields as of the version
# this repo targets (do_ocr, page_range, ocr_lang, pipeline, ...) and the
# facade has no business knowing what most of them mean -- hardcoding all of
# them as individual FastAPI Form() parameters would both be a lot of
# fragile duplication and silently drop anything upstream adds later. Only
# these four are array-typed in the schema (confirmed via docling-serve's
# own live OpenAPI document); every other field is a scalar. This is the one
# piece of schema knowledge build_convert_options needs to correctly turn a
# single repeated form field into a length-1 list instead of a bare scalar
# where the target field expects an array.
_ARRAY_FIELDS = frozenset({"from_formats", "to_formats", "ocr_lang", "page_range"})


def build_convert_options(form) -> dict[str, object]:
    """Turn a multipart form (anything with `.keys()`/`.getlist()`, matching
    Starlette's FormData) into a `convert_options` dict for docling-serve's
    JSON API, forwarding every field the facade doesn't itself need to
    intercept untouched -- not just `to_formats`, which is all the facade
    used to declare explicitly (silently dropping everything else a client
    might send, e.g. `do_ocr`, `page_range`, `ocr_lang`).
    """
    options: dict[str, object] = {}
    for key in dict.fromkeys(form.keys()):
        if key in _INTERCEPTED_FIELDS:
            continue
        values = form.getlist(key)
        options[key] = values if (key in _ARRAY_FIELDS or len(values) > 1) else values[0]
    return options


def parse_artifact_key(prefix: str, key: str) -> tuple[str, str, str] | None:
    """Extract (extension, field_name, filename_stem) from a listed S3 key."""
    if not key.startswith(prefix):
        return None
    filename = key[len(prefix) :].rsplit("/", 1)[-1]
    for ext, field_name in EXTENSION_TO_FIELD:
        if filename.endswith(ext):
            return ext, field_name, filename[: -len(ext)]
    return None


async def list_artifacts(s3_client, *, bucket: str, prefix: str) -> list[str]:
    """Recursively list every object under `prefix`, returning raw S3 keys."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


async def fetch_artifacts_by_stem(
    s3_client, *, bucket: str, prefix: str
) -> dict[str, dict[str, tuple[str, bytes]]]:
    """Recursively list+fetch every artifact under `prefix`, grouped by
    filename stem then field name: {stem: {field_name: (ext, body_bytes)}}.
    """
    grouped: dict[str, dict[str, tuple[str, bytes]]] = {}
    for key in await list_artifacts(s3_client, bucket=bucket, prefix=prefix):
        parsed = parse_artifact_key(prefix, key)
        if parsed is None:
            continue
        ext, field_name, stem = parsed
        obj = await s3_client.get_object(Bucket=bucket, Key=key)
        async with obj["Body"] as body:
            content = await body.read()
        grouped.setdefault(stem, {})[field_name] = (ext, content)
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
    documents: list[tuple[str, str, dict[str, tuple[str, bytes]]]],
) -> ZipResult:
    """Mirror docling-serve's own multi-file zip layout: one flat file per
    (document, format) pair, named `{stem}{ext}` -- confirmed empirically
    against a live 2-file /v1/convert/file/async request (default inbody
    target auto-falls-back to a zip when more than one file is submitted).

    `documents` is `(original_filename, stem, artifacts_by_field)`. A missing
    document (empty `artifacts_by_field` -- nothing found in S3 for it, e.g.
    it individually failed conversion while sibling documents in the same
    batch succeeded) contributes no zip entries but still gets a "failed"
    status in the manifest -- native docling-serve's own zip response has no
    per-document status concept at all to preserve here (ZipArchiveResult is
    raw bytes, nothing else), so this is a facade-only addition, not a
    compatibility break.
    """
    buf = io.BytesIO()
    statuses: list[DocumentStatus] = []
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, stem, artifacts_by_field in documents:
            for _field_name, (ext, body) in artifacts_by_field.items():
                zf.writestr(f"{stem}{ext}", body)
            statuses.append(
                DocumentStatus(filename=filename, status="success" if artifacts_by_field else "failed")
            )
        zf.writestr(
            "_status.json",
            json.dumps({"documents": [s.model_dump() for s in statuses]}, indent=2),
        )
    return ZipResult(content=buf.getvalue(), document_statuses=statuses)
