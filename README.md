# docling-serve-ray

A GPU-accelerated [docling-serve](https://github.com/docling-project/docling-serve) deployment backed by [Ray Serve](https://docs.ray.io/en/latest/serve/index.html), with S3/MinIO-source batch conversion and a reproducible Bazel/rules_oci image build.

## What's here

- **`docker-compose.yml`** — full local stack: MinIO (S3-compatible object store), Redis, a Ray head/worker pair, and docling-serve itself configured with the `ray` engine, GPU passthrough, and PDF page-slice fan-out tuned for a single-GPU VRAM budget.
- **`patches/docling_jobkit/orchestrators/ray/models.py`** — a one-line upstream fix for a real `docling-jobkit` bug: `SourceChunkConvertRequest.chunk` is typed as the dynamically-parameterized generic alias `DocumentChunk[Any, Any]`, which Ray's cross-process Serve-replica argument (de)serialization cannot reconstruct (`ray.exceptions.RaySystemError: System error: 'type'`). This breaks *every* S3-source batch conversion under the Ray engine, before the converter ever runs. Fixed here by using the bare `DocumentChunk` instead. Verified end-to-end against a live MinIO + Ray cluster; upstream issue draft in [`upstream-issue-draft.md`](upstream-issue-draft.md).
- **`MODULE.bazel` / `patches/docling_jobkit/orchestrators/ray/BUILD.bazel`** — reproducible Bazel/rules_oci build of the patched docling-serve image (`bazel run //patches/docling_jobkit/orchestrators/ray:tarball`), pinned to the upstream base image by digest.
- **`Containerfile`** — a plain `docker build` fallback producing the same patched image, for anyone without Bazel set up.
- **`scripts/send_pdf.py`** — exercises the S3-source batch path end-to-end: uploads every PDF in a local directory to MinIO, submits a `/v1/convert/source/batch` job, polls until it terminates, and lists the converted output objects.

## Quickstart

```bash
# Preferred: build the patched image reproducibly with Bazel
bazel run //patches/docling_jobkit/orchestrators/ray:tarball

# Fallback: plain docker build
docker build -t docling-serve-ray-patched:v1.29.0-jobkit-fix .

docker compose up
```

Then drop some PDFs in `/tmp/docling-pdfs` and run:

```bash
python scripts/send_pdf.py
```

## Requirements

- Docker Compose, an NVIDIA GPU with drivers + the NVIDIA Container Toolkit.
- Bazel (via `bazelisk`), for the reproducible image build — optional if you use the `Containerfile` fallback instead.
- Python 3 with the packages in `requirements.in`, to run `scripts/send_pdf.py`.

## License

[MIT](LICENSE)
