# Non-Bazel fallback. The primary, reproducible build is
# `bazel run //patches/docling_jobkit/orchestrators/ray:tarball` (rules_oci,
# base image pinned by digest in MODULE.bazel) -- docker-compose.yml points at
# that build's output tag. This file is kept as a plain `docker build`
# equivalent producing the same fix under a different tag
# (docling-serve-ray-patched:v1.29.0-jobkit-fix).
#
# Patched docling-serve image: fixes docling-jobkit's SourceChunkConvertRequest.chunk
# field being annotated as the dynamically-parameterized generic alias
# `DocumentChunk[Any, Any]`, which Ray's cross-process Serve-replica argument
# (de)serialization cannot reconstruct (ray.exceptions.RaySystemError: System error:
# 'type' -- a KeyError: 'type' inside pickle.loads on the receiving replica). This
# breaks every S3-source batch conversion under the Ray engine, instantly, before the
# converter ever runs. See patches/docling_jobkit/orchestrators/ray/models.py for the
# one-line fix (bare `DocumentChunk` instead of `DocumentChunk[Any, Any]`), verified
# fixed end-to-end against a live MinIO + Ray cluster.
#
# Root cause + fix pending upstream: https://github.com/docling-project/docling-jobkit

ARG BASE_IMAGE=ghcr.io/docling-project/docling-serve-cu128:v1.29.0
FROM ${BASE_IMAGE}

COPY --chown=1001:0 patches/docling_jobkit/orchestrators/ray/models.py \
    /opt/app-root/lib64/python3.12/site-packages/docling_jobkit/orchestrators/ray/models.py
