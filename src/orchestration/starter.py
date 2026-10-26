"""CLI: dispatch documents already stored in S3 to ConvertDocumentWorkflow.

    bazel run //src/orchestration:start -- convert --prefix incoming/ [--wait]

Starts one workflow per object under the prefix. Re-running it while
conversions are still in flight is safe: each workflow id is derived from the
object's coordinates, so Temporal rejects the duplicate instead of converting
the document twice (reported as "already running"). Once a workflow has
finished, starting it again re-converts the document.
"""

import argparse
import asyncio
import sys

import boto3
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import WorkflowAlreadyStartedError

from orchestration.config import get_settings
from orchestration.dispatch import start_conversion
from orchestration.models import (
    DEFAULT_TO_FORMATS,
    ConvertInput,
    ConvertResult,
    workflow_id_for_convert,
)


def _list_objects(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        keys.extend(
            obj["Key"]
            for obj in page.get("Contents", [])
            if not obj["Key"].endswith("/")
        )
    return keys


async def _convert(args: argparse.Namespace) -> int:
    settings = get_settings()
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )
    keys = _list_objects(s3, args.bucket, args.prefix)
    if not keys:
        print(f"no objects under s3://{args.bucket}/{args.prefix}", file=sys.stderr)
        return 1

    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    to_formats = [f.strip() for f in args.to_formats.split(",") if f.strip()]
    handles = {}
    started = 0
    for key in keys:
        workflow_id = workflow_id_for_convert(args.bucket, key)
        try:
            handles[key] = await start_conversion(
                client, ConvertInput(bucket=args.bucket, key=key, to_formats=to_formats)
            )
            started += 1
            print(f"started          {workflow_id}")
        except WorkflowAlreadyStartedError:
            handles[key] = client.get_workflow_handle(
                workflow_id, result_type=ConvertResult
            )
            print(f"already running  {workflow_id}")

    print(f"\n{started} started, {len(handles) - started} already running.")
    if not args.wait:
        print("Follow them in the Temporal UI.")
        return 0

    print("\nwaiting for results...")
    failed = 0
    for key, handle in handles.items():
        try:
            result: ConvertResult = await handle.result()
        except WorkflowFailureError as err:
            failed += 1
            print(f"FAILED  s3://{args.bucket}/{key}: {err.cause}")
            continue
        formats = ", ".join(
            f"{fmt}={len(k)}" for fmt, k in sorted(result.outputs.items())
        )
        print(
            f"ok      s3://{args.bucket}/{key} -> s3://{result.output_bucket}/ "
            f"[{formats}] task={result.task_id} submissions={result.submissions} "
            f"safety_polls={result.safety_polls}"
        )
    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="start", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    convert = sub.add_parser("convert", help="convert every object under a prefix")
    convert.add_argument("--bucket", default="docling-input")
    convert.add_argument("--prefix", required=True)
    convert.add_argument(
        "--to-formats",
        default=",".join(DEFAULT_TO_FORMATS),
        help="comma-separated; json is always added (chunking consumes it)",
    )
    convert.add_argument(
        "--wait", action="store_true", help="block until every workflow finishes"
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(_convert(args)))


if __name__ == "__main__":
    main()
