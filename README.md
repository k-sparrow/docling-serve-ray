# docling-serve-ray

A GPU-accelerated [docling-serve](https://github.com/docling-project/docling-serve) deployment backed by [Ray Serve](https://docs.ray.io/en/latest/serve/index.html), with S3/MinIO-source batch conversion, a claim-check facade fixing a real architectural gap in the multipart upload path, and a reproducible Bazel/rules_oci image build for the whole stack.

## Architecture

`nginx` is the one public entrypoint (port 5001). It routes most traffic straight through to `docling-serve` unmodified — including `/v1/convert/source/batch`, which is already S3-native and byte-free through Redis. Three routes get intercepted by a small FastAPI facade instead:

- `POST /v1/convert/file` and `/v1/convert/file/async` — docling-serve's own multipart handler embeds the *entire uploaded file* as base64 bytes into the `Task` object it durably persists (`RPUSH`) into Redis's AOF log, with no size cap. The facade uploads the file to S3 itself and resubmits via `/v1/convert/source/batch` instead — measured on a real 50MB PDF: ~63.6MB written to Redis's AOF through the raw endpoint, versus ~7.7KB through the facade.
- `GET /v1/result/{task_id}` — for tasks the facade originated, reconstructs docling-serve's native inline response shape from the S3 output (including a per-document status manifest for partially-failed batches, something docling-serve's own zip response can't express). Everything else falls through to a plain passthrough of docling-serve's own response.

The facade also deletes its S3 objects and Redis record once a result has been delivered — that storage is single-use, not meant to persist past the one delivery.

## What's here

- **`docker-compose.yml`** — the full stack: MinIO, Redis, a Ray head/worker pair, `docling-serve` (`ray` engine, GPU passthrough, PDF page-slice fan-out tuned for a single-GPU VRAM budget), the `facade`, and `nginx` in front of both.
- **`facade/`** — the claim-check facade (FastAPI: `main.py`/`service.py`/`dependencies.py`/`utils.py`/`schemas.py`). Streams uploads to S3 via `aioboto3` instead of buffering them in memory, so peak RAM per upload is bounded by the client's part size, not file size.
- **`nginx/`** — `nginx.conf` baked into an image via Bazel, routing the three facade-owned paths there and everything else to `docling-serve` directly.
- **`patches/docling_jobkit/orchestrators/ray/models.py`** — a one-line upstream fix for a real `docling-jobkit` bug: `SourceChunkConvertRequest.chunk` is typed as the dynamically-parameterized generic alias `DocumentChunk[Any, Any]`, which Ray's cross-process Serve-replica argument (de)serialization cannot reconstruct (`ray.exceptions.RaySystemError: System error: 'type'`). This breaks *every* S3-source batch conversion under the Ray engine, before the converter ever runs. Fixed here by using the bare `DocumentChunk` instead. A second, related finding — Redis durably persisting document bytes and connector credentials in plaintext — is drafted but not yet fixed; see [`upstream-issue-draft.md`](upstream-issue-draft.md).
- **`MODULE.bazel`** plus each component's own `BUILD.bazel` — reproducible Bazel/rules_oci builds for all three images, pinned to their upstream bases by digest. Each image also has its own `Containerfile` (`patches/docling_jobkit/orchestrators/ray/` and `facade/`) as a plain-`docker build` fallback for anyone without Bazel set up.
- **`scripts/send_pdf.py`** — exercises the S3-source batch path end-to-end: uploads every PDF in a local directory to MinIO, submits a `/v1/convert/source/batch` job through nginx, polls until it terminates, and lists the converted output objects.

## Quickstart

```bash
# Build and load all three images (patched docling-serve, facade, nginx)
bazel run //:load.all

docker compose up
```

Then drop some PDFs in `/tmp/docling-pdfs` and run:

```bash
python scripts/send_pdf.py
```

Or hit the facade's own multipart endpoint directly (through nginx, at `http://localhost:5001`) — `POST /v1/convert/file/async` with a `files` field, same contract as native docling-serve, no client-side changes needed.

Pre-built images are also published to GHCR on every merge to `main`: `ghcr.io/k-sparrow/docling-serve-ray-{patched,facade,nginx}:latest`.

## Testing

```bash
bazel test //...              # fast, hermetic: container structure tests + facade unit suite + format check, no Docker needed
bazel test //:test.docker     # patch-import check + testcontainers-driven integration/e2e, needs a real Docker daemon and a GPU
```

The Docker-dependent suite is tagged `manual` on purpose (mirrors Artemis's own "never run per-PR, nightly or pre-deploy" principle) — it's excluded from `bazel test //...` and only runs when named explicitly, or on a nightly CI schedule. Since GitHub-hosted CI runners have no GPU, CI runs it against a CPU-only variant instead (`DOCLING_COMPOSE_CPU_OVERLAY=1`, see `tests/ci/`) — locally, against real GPU hardware, it uses the real stack unmodified.

Each of `facade/`, `nginx/`, and the ray patch has its tests split into `tests/functional/` (real test code) and `tests/structural/` (`container_structure_test` configs).

### Formatting and linting

Mirrors Artemis's own `tools/format/` and `tools/lint/` convention (ruff for Python, buildifier for Starlark, yamlfmt for YAML) — minus flake8, which needs a newer `rules_python` than this repo's pin allows; ruff alone covers the same ground.

```bash
bazel run //:format                                # auto-fix formatting in place
bazel test //tools/format:format_test               # check only (also part of bazel test //...)
bazel build --aspects=//tools/lint:linters.bzl%ruff --output_groups=rules_lint_human \
    --@aspect_rules_lint//lint:fail_on_violation //...   # ruff lint, repo-wide
```

`docker-compose.yml` is excluded from yamlfmt via `.gitattributes` (`rules-lint-ignored`) — it hand-wraps a couple of long commands across multiple lines for readability, which YAML's folded-scalar syntax treats as equivalent to one line but a formatter would happily collapse.

## CI/CD

- **`.github/workflows/ci.yml`** — on every PR and push to `main`: `lint` (the ruff aspect above) runs first and gates `test` (`bazel test //...`) and `test-docker` (the Docker-dependent suite, CPU-only), which then run in parallel with each other. On push to `main`, once all three pass, also pushes the three production images to GHCR.
- **`.github/workflows/nightly-docker-tests.yml`** — the Docker-dependent suite again, on a daily cron plus manual dispatch, as an environment-drift canary independent of code changes.

## Requirements

- Docker Compose, an NVIDIA GPU with drivers + the NVIDIA Container Toolkit, for the real stack.
- Bazel (via `bazelisk`), for the reproducible image builds — optional if you use the `Containerfile` fallbacks instead.
- Python 3 with the packages in `requirements.in`, to run `scripts/send_pdf.py`.

## License

[MIT](LICENSE)
