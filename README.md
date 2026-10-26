# docling-serve-ray

A GPU-accelerated [docling-serve](https://github.com/docling-project/docling-serve) deployment backed by [Ray Serve](https://docs.ray.io/en/latest/serve/index.html), with S3/MinIO-source batch conversion, a claim-check facade fixing a real architectural gap in the multipart upload path, and a reproducible Bazel/rules_oci image build for the whole stack.

## Architecture

`nginx` is the one public entrypoint (port 5001). It routes most traffic straight through to `docling-serve` unmodified — including `/v1/convert/source/batch`, which is already S3-native and byte-free through Redis. Three routes get intercepted by a small FastAPI facade instead:

- `POST /v1/convert/file` and `/v1/convert/file/async` — docling-serve's own multipart handler embeds the *entire uploaded file* as base64 bytes into the `Task` object it durably persists (`RPUSH`) into Redis's AOF log, with no size cap. The facade uploads the file to S3 itself and resubmits via `/v1/convert/source/batch` instead — measured on a real 50MB PDF: ~63.6MB written to Redis's AOF through the raw endpoint, versus ~7.7KB through the facade.
- `GET /v1/result/{task_id}` — for tasks the facade originated, reconstructs docling-serve's native inline response shape from the S3 output (including a per-document status manifest for partially-failed batches, something docling-serve's own zip response can't express). Everything else falls through to a plain passthrough of docling-serve's own response.

The facade also deletes its S3 objects and Redis record once a result has been delivered — that storage is single-use, not meant to persist past the one delivery.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design write-up — the two upstream bugs that motivate this repo, a component diagram, and a step-by-step flowchart of the claim-check request path.

## What's here

- **`docker-compose.yml`** — the full stack: MinIO, Redis, a Ray head/worker pair, `docling-serve` (`ray` engine, GPU passthrough, PDF page-slice fan-out tuned for a single-GPU VRAM budget), the `facade`, and `nginx` in front of both.
- **`src/facade/`** — the claim-check facade (FastAPI: `main.py`/`service.py`/`dependencies.py`/`utils.py`/`schemas.py`). Streams uploads to S3 via `aioboto3` instead of buffering them in memory, so peak RAM per upload is bounded by the client's part size, not file size.
- **`src/nginx/`** — `nginx.conf` baked into an image via Bazel, routing the three facade-owned paths there and everything else to `docling-serve` directly.
- **`src/patches/docling_serve/app.py`** — the patched `docling-serve` image (base: official `docling-serve-cu128:v1.36.0`). Registers the missing `QueueLimitExceededError` → 429 exception handler; stock docling-serve lets a full queue surface as an unhandled 500 ([docling-serve#581](https://github.com/docling-project/docling-serve/issues/581), fix [#583](https://github.com/docling-project/docling-serve/pull/583) unmerged).
- **Upstreamed:** this repo previously also patched a `docling-jobkit` bug — `SourceChunkConvertRequest.chunk` typed as the dynamically-parameterized generic `DocumentChunk[Any, Any]`, which Ray's cross-process Serve-replica (de)serialization cannot reconstruct (`ray.exceptions.RaySystemError: System error: 'type'`), breaking *every* S3-source batch conversion under the Ray engine. Fixed upstream in [docling-jobkit#263](https://github.com/docling-project/docling-jobkit/pull/263) (jobkit v3.8.1, first shipped in docling-serve v1.36.0), so the overlay is gone; the image's structure test still asserts the base carries the fix. A second, related finding — Redis durably persisting document bytes and connector credentials in plaintext — is drafted but not yet fixed; see [`upstream-issue-draft.md`](upstream-issue-draft.md).
- **`MODULE.bazel`** plus each component's own `BUILD.bazel` — reproducible Bazel/rules_oci builds for all five images, pinned to their upstream bases by digest. The patched docling-serve and the facade also have a `Containerfile` as a plain-`docker build` fallback; the orchestration worker and the callback relay are built by Bazel only.
- **`src/orchestration/`** — durable conversion with Temporal: one `ConvertDocumentWorkflow` per document already in S3, submitted serially to docling-serve, sleeping until docling calls back, surviving worker/server restarts without re-converting. Its own `docker-compose.yml` (Temporal + Postgres + UI + worker + callback relay). See [Durable conversion with Temporal](#durable-conversion-with-temporal).
- **`src/callback_relay/`** — a small Go service that receives docling-serve's task-completion callbacks and signals the workflow waiting on each task. Built with rules_go/gazelle and rules_oci onto `distroless/static`, following rules_oci's Go guide.
- **`scripts/send_pdf.py`** — exercises the S3-source batch path end-to-end: uploads every PDF in a local directory to MinIO, submits a `/v1/convert/source/batch` job through nginx, polls until it terminates, and lists the converted output objects.

## Quickstart

```bash
# Build and load all five images (patched docling-serve, facade, nginx, orchestration worker, callback relay)
bazel run //:load.all

docker compose up
```

Then drop some PDFs in `/tmp/docling-pdfs` and run:

```bash
python scripts/send_pdf.py
```

Or hit the facade's own multipart endpoint directly (through nginx, at `http://localhost:5001`) — `POST /v1/convert/file/async` with a `files` field, same contract as native docling-serve, no client-side changes needed.

Pre-built images are also published to GHCR on every merge to `main`: `ghcr.io/k-sparrow/docling-serve-ray-{patched,facade,nginx,orchestration,callback-relay}:latest`.

**Upgrading an existing checkout:** MinIO now runs from Chainguard's image as uid 65532 (MinIO's own images are no longer publicly pullable). A `minio-data` volume created by the old root-run image is root-owned, so the new server can't write to it. Fix it once, with the stack down:

```bash
docker run --rm --user 0 --entrypoint sh -v docling-serve-ray_minio-data:/data \
  cgr.dev/chainguard/minio@sha256:73297936408b731668ca0e835ac9add1a35cb6026a125c2f0cd476d9ef48222c \
  -c 'chown -R 65532:65532 /data'
```

Fresh volumes (new clones, CI) need nothing.

## Durable conversion with Temporal

For documents already in S3: one workflow per object, which survives worker and Temporal restarts without re-converting, absorbs docling's 429 backpressure as retries, and rejects a duplicate dispatch while a document is still in flight.

While docling converts, the workflow sleeps and holds no worker slot. Each submit registers a callback with docling-serve; when the task finishes, the callback relay turns it into a `task_completed` signal that wakes the workflow within seconds. Callback delivery is best-effort (docling tries 3 times over about 3 s), so a safety net stays on: each time a sleep of about 5 minutes (jittered ±20%) passes without a signal, the workflow checks the task's status once. A task still in progress counts one poll and the workflow sleeps again, up to `max_safety_polls` (600, about 50 h). Both knobs are per-run fields of `ConvertInput`.

```bash
# Main stack first -- the Temporal worker joins its network.
docker compose up -d
docker compose -f src/orchestration/docker-compose.yml up -d

# Dispatch every object under a prefix (json is always produced, alongside --to-formats).
bazel run //src/orchestration:start -- convert --prefix incoming/ --wait
```

Outputs land under `docling-output/temporal/{bucket}/{key}/convert/`. The Temporal UI is at `http://localhost:8080`. If another local Temporal already holds `7233`/`8080`, set `TEMPORAL_HOST_PORT` / `TEMPORAL_UI_HOST_PORT` on the compose command and point the starter at it with `ORCH_TEMPORAL_ADDRESS=localhost:<port>`. Run **one** worker replica: submits are serialized per worker process. The relay is stateless and can run several. Set `CALLBACK_TOKEN` on the compose command outside local development: the worker and the relay share it, and docling-serve keeps it in its Redis task records, which is why the relay is never published to the host. Each activity's start-to-close timeout (one attempt) is a worker setting: `ORCH_SUBMIT_CONVERSION_START_TO_CLOSE_SECONDS` (default 120), `ORCH_CHECK_TASK_START_TO_CLOSE_SECONDS` (120), `ORCH_FETCH_OUTCOME_START_TO_CLOSE_SECONDS` (120) and `ORCH_COLLECT_OUTPUTS_START_TO_CLOSE_SECONDS` (300). Set them in the shell or in `src/orchestration/.env` (gitignored); a worker restart applies them to in-flight workflows too. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#request-flow-the-temporal-path) for the failure semantics.

The relay's Go toolchain and dependencies come from the root `go.mod`, fetched by Bazel; a Go installed on the host is never used. After changing Go imports or `go.mod`:

```bash
bazel run @rules_go//go -- mod tidy   # go.mod/go.sum, plus `bazel mod tidy` for MODULE.bazel's use_repo
bazel run //:gazelle                  # regenerate the go_* BUILD rules
```

## Testing

```bash
bazel test //...              # fast, hermetic: structure tests, Python and Go unit suites, format and gazelle checks
bazel test //:test.docker     # patch-import check + testcontainers-driven integration/e2e, needs a real Docker daemon and a GPU
```

The Docker-dependent suite is tagged `manual` on purpose (mirrors Artemis's own "never run per-PR, nightly or pre-deploy" principle) — it's excluded from `bazel test //...` and only runs when named explicitly, or on a nightly CI schedule. Since GitHub-hosted CI runners have no GPU, CI runs it against a CPU-only variant instead (`DOCLING_COMPOSE_CPU_OVERLAY=1`, see `tests/ci/`) — locally, against real GPU hardware, it uses the real stack unmodified.

Each of `src/facade/`, `src/nginx/`, and the ray patch has its tests split into `tests/functional/` (real test code) and `tests/structural/` (`container_structure_test` configs).

### Formatting and linting

Mirrors Artemis's own `tools/format/` and `tools/lint/` convention (ruff for Python, buildifier for Starlark, yamlfmt for YAML) — minus flake8, which needs a newer `rules_python` than this repo's pin allows; ruff alone covers the same ground. Go adds gofumpt to the same format targets, and nogo, which runs `go vet` inside every Go compile, so a finding fails the build itself with no separate lint step.

```bash
bazel run //:format                                # auto-fix formatting in place
bazel test //tools/format:format_test               # check only (also part of bazel test //...)
bazel build --aspects=//tools/lint:linters.bzl%ruff --output_groups=rules_lint_human \
    --@aspect_rules_lint//lint:fail_on_violation //...   # ruff lint, repo-wide
```

`docker-compose.yml` is excluded from yamlfmt via `.gitattributes` (`rules-lint-ignored`) — it hand-wraps a couple of long commands across multiple lines for readability, which YAML's folded-scalar syntax treats as equivalent to one line but a formatter would happily collapse.

## CI/CD

- **`.github/workflows/ci.yml`** — on every PR and push to `main`: `lint` (the ruff aspect above) runs first and gates `test` (`bazel test //...`) and `test-docker` (the Docker-dependent suite, CPU-only), which then run in parallel with each other. On push to `main`, once all three pass, also pushes the five production images to GHCR.
- **`.github/workflows/nightly-docker-tests.yml`** — the Docker-dependent suite again, on an every-other-day cron plus manual dispatch, as an environment-drift canary independent of code changes.

## Requirements

- Docker Compose, an NVIDIA GPU with drivers + the NVIDIA Container Toolkit, for the real stack.
- Bazel (via `bazelisk`), for the reproducible image builds. Required for the orchestration worker and the callback relay; optional for the patched docling-serve and facade images, which have `Containerfile` fallbacks.
- Python 3 with the packages in `requirements.in`, to run `scripts/send_pdf.py`.

## License

[MIT](LICENSE)
