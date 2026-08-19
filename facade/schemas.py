"""Wire-shape models for the facade.

The reconstructed response models here mirror the field names of
docling-serve's own `ConvertDocumentResponse`/`ExportDocumentResponse`
(docling.datamodel.service.responses) so existing multipart clients see an
unchanged contract. They're redefined locally rather than imported from
`docling`/`docling_core` to avoid pulling the full ML dependency stack into
an otherwise thin proxy service -- the facade never touches document
internals, it only relays already-rendered text/JSON content fetched from S3.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class RecordKind(str, Enum):
    FILE_UPLOAD = "file_upload"


class TaskRecord(BaseModel):
    """Facade-side bookkeeping for a task it originated, stored in Redis.

    Only tasks the facade itself submits get a record. Any task_id with no
    record (native /v1/convert/source/batch submissions, or anything else)
    falls through to a plain passthrough of docling-serve's own response --
    the facade has no target info to act on otherwise, and the caller who
    supplied their own S3 target already knows where their output landed.
    """

    kind: RecordKind
    request_id: str
    tenant_id: Optional[str] = None
    filenames: list[str]
    to_formats: list[str]
    as_zip: bool
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ExportDocumentResponse(BaseModel):
    filename: str
    md_content: Optional[str] = None
    json_content: Optional[dict[str, Any]] = None
    html_content: Optional[str] = None
    text_content: Optional[str] = None
    doctags_content: Optional[str] = None
    doclang_content: Optional[str] = None


class ConvertDocumentResponse(BaseModel):
    document: ExportDocumentResponse
    status: str
    errors: list[Any] = []
    processing_time: float
    timings: dict[str, Any] = {}
    confidence: Optional[dict[str, Any]] = None


class TaskStatusResponse(BaseModel):
    task_id: str
    task_type: str
    task_status: str
    task_position: Optional[int] = None
    task_meta: Optional[dict[str, Any]] = None
    error_message: Optional[str] = None
    failure: Optional[dict[str, Any]] = None
