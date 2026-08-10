## Title
`SourceChunkConvertRequest.chunk: DocumentChunk[Any, Any]` breaks every S3-source batch conversion under the Ray engine

## Summary

Any conversion submitted via `/v1/convert/source/batch` with an S3 (or presumably
Azure Blob / GCS — anything routed through the generic connector-chunking path)
source fails instantly under the Ray orchestrator, before the converter replica ever
runs — before any object is even fetched from storage. Every source chunk fails with:

```
ray.exceptions.RaySystemError: System error: 'type'
  At least one of the input arguments for this task could not be computed:
  File ".../ray/_private/serialization.py", line 582, in deserialize_objects
  File ".../ray/_private/serialization.py", line 431, in _deserialize_object
  File ".../ray/_private/serialization.py", line 378, in _deserialize_msgpack_data
  File ".../ray/_private/serialization.py", line 360, in _deserialize_pickle5_data
    obj = pickle.loads(in_band)
KeyError: 'type'
```

The task itself reports `task_status: success` at the top level (the coordinator
completes without raising), but every individual document fails
(`num_succeeded: 0, num_failed: N`), and nothing is written to the target.

## Environment

- `docling-jobkit` 3.2.0 and 3.3.1 (both confirmed — this is not version-specific)
- `docling-serve` v1.29.0 (`ghcr.io/docling-project/docling-serve-cu128:v1.29.0`)
- Ray 2.55.1, `ray[serve]~=2.52`
- Reproduced with MinIO as the S3-compatible backend, single-node Ray cluster
  (head + one worker), but the root cause is unrelated to storage backend or
  cluster topology — see below.

## Root cause

`docling_jobkit/orchestrators/ray/models.py`:

```python
class SourceChunkConvertRequest(BaseModel):
    kind: str = Field(default="source_chunk_convert")
    task: Task = Field(description="Parent task metadata")
    chunk: DocumentChunk[Any, Any] = Field(
        description="Data-only source chunk to fetch and convert"
    )
    expected_doc_count: int = Field(...)
```

`DocumentChunk` (`docling_jobkit/connectors/source_processor.py`) is a Pydantic
`Generic[SourceT, FileIdentifierT]` model. Annotating the field as
`DocumentChunk[Any, Any]` rather than the bare `DocumentChunk` looks like an
innocuous, idiomatic type-checker nicety (the kind of thing Pyright's default mode
nudges you to write for any bare generic) — but for a **Pydantic** generic model it
has a real runtime effect: Pydantic coerces the field's value into the specific,
dynamically-created parameterized class produced by `DocumentChunk.__class_getitem__(Any,
Any)`. That class has no stable, importable qualname — it's not a real attribute of
`docling_jobkit.connectors.source_processor` that `pickle` can resolve by reference.

`_execute_source_chunk` (`serve_deployment.py`) constructs exactly this object and
sends it from the coordinator replica to the converter replica via
`self.converter_handle.process_converter_request.remote(request)`. Ray has to
serialize the call arguments across that Serve-replica process boundary, and the
receiving side's deserialization of the dynamically-parameterized generic class fails.

This is why only the S3 (generic connector-chunking) fan-out path is affected:
`PassthroughTaskRequest`, `MaterializedConvertRequest`, and `SliceConvertRequest` (the
multipart/HTTP and single-PDF-slicing request variants) never construct a
`DocumentChunk`, let alone the subscripted form — they carry the whole `Task` directly
or a `ray.ObjectRef` (which Ray natively, robustly serializes). `SourceChunkConvertRequest`
is the only request type that touches this generic model at all, and S3 is currently
the only source type routed through it (`_is_s3_fanout_task` gates on
`isinstance(source, S3Coordinates)`).

## Minimal reproduction (no S3/MinIO/network involved)

Run inside any node with a live Ray connection:

```python
import ray
from typing import Any
from pydantic import BaseModel, Field, ConfigDict
from docling_jobkit.connectors.source_processor import DocumentChunk, SourceDocumentRef

class Request(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    chunk: DocumentChunk[Any, Any] = Field(...)  # reproduces with any Source/FileIdentifier types

ref = SourceDocumentRef(id="x", source_index=0, source_uri="s3://bucket/key", filename="key")
chunk = DocumentChunk(source="whatever", refs=[ref], chunk_index=0)
req = Request(chunk=chunk)

ray.init(address="ray://<head>:10001")

@ray.remote
def echo(x):
    return repr(x)

ray.get(echo.remote(req))
# ray.exceptions.RaySystemError: System error: 'type'
```

Confirmed independently:
- Plain `pickle.dumps`/`pickle.loads` of the *bare* `DocumentChunk` (all its
  constituent pieces) round-trips fine.
- Plain `pickle.dumps` of the full `SourceChunkConvertRequest` (with the real
  `S3SourceRequest`/`S3FileIdentifier` types) fails immediately with
  `PicklingError: Can't pickle <class '...DocumentChunk[Any, Any]'>: attribute lookup
  DocumentChunk[Any, Any] on docling_jobkit.connectors.source_processor failed` —
  confirming the dynamically-parameterized class itself is the unpicklable object,
  independent of Ray.
- The exact same object round-trips through real `ray.put`/`ray.get` and a real
  `@ray.remote` call **fine** when the receiving field is typed as the bare
  `DocumentChunk` (or `Any`) instead of `DocumentChunk[Any, Any]` — the runtime type
  of the value becomes the plain `DocumentChunk` class in both passing cases, vs. the
  subscripted alias in the failing case.

## Not specific to `Any` — this generalizes to every connector, present and future

It's tempting to read this as "don't use `Any` as the type param, use concrete types
for real type safety instead." That doesn't avoid the bug — it hits the *same* failure
with *any* explicit subscription of a Pydantic generic model, concrete types included.
Reproduced with fake concrete Azure-shaped types (no `Any` anywhere):

```python
class FakeAzureCoords(BaseModel):
    account_name: str
    container: str

class FakeAzureIdentifier(BaseModel):
    blob_name: str

class Chunk(BaseModel, Generic[SourceT, IdT]):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    source: SourceT
    refs: Sequence[Ref[IdT]]

class RequestConcrete(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    chunk: Chunk[FakeAzureCoords, FakeAzureIdentifier] = Field(...)
```

```
runtime type of req.chunk: <class '__main__.Chunk[FakeAzureCoords, FakeAzureIdentifier]'>
pickle round-trip: FAILED -> PicklingError: Can't pickle <class
'__main__.Chunk[FakeAzureCoords, FakeAzureIdentifier]'>: attribute lookup
Chunk[FakeAzureCoords, FakeAzureIdentifier] on __main__ failed
```

Practical implication: only S3 is routed through `SourceChunkConvertRequest` today
(`_is_s3_fanout_task` gates on `isinstance(source, S3Coordinates)`), but a future PR
wiring up Azure Blob or GCS fan-out that reaches for `DocumentChunk[AzureBlobCoordinates,
AzureBlobIdentifier]` — reasonably, for better static typing — would silently
reintroduce this exact bug for that connector too. The bare `DocumentChunk` isn't a
narrow patch for the `Any, Any` case specifically; it's the correct general pattern for
"a Pydantic generic model used as a field on something that crosses a Ray Serve replica
boundary," regardless of which concrete types would otherwise go there.

## Fix

```diff
- chunk: DocumentChunk[Any, Any] = Field(
+ chunk: DocumentChunk = Field(
      description="Data-only source chunk to fetch and convert"
  )
```

`DocumentChunk` already has `model_config = ConfigDict(arbitrary_types_allowed=True)`,
so the bare annotation still validates that the field holds a real `DocumentChunk`
instance — this isn't a loosening to `Any`, just removing the subscript that triggers
the broken coercion.

Verified fixed end-to-end: built a patched image with this one-line change, ran the
same S3 batch conversion (5 documents, MinIO backend) that failed 5/5 before the
patch — succeeded 5/5 after, with converted output correctly written to the target
bucket.

Happy to open a PR with this change plus a regression test (e.g. a unit test that
constructs a `SourceChunkConvertRequest` and asserts it round-trips through
`ray.cloudpickle`/a real `ray.remote` call) if that's useful — let me know if there's
a preferred contribution process.
