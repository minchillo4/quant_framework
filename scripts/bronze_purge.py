#!/usr/bin/env python3
"""
Purge all data from MinIO bronze bucket, including _checkpoints.
SAFE: prompts for confirmation unless --yes is provided.
"""

import argparse
import os
import sys

import boto3


def purge_bucket(
    bucket: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    prefix: str | None = None,
) -> int:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        objs = page.get("Contents", [])
        if not objs:
            continue
        to_delete = {"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": True}
        s3.delete_objects(Bucket=bucket, Delete=to_delete)
        deleted += len(objs)
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge MinIO bronze bucket")
    parser.add_argument(
        "--yes", action="store_true", help="Confirm purge without prompt"
    )
    parser.add_argument("--prefix", help="Optional prefix to purge", default=None)
    args = parser.parse_args()

    bucket = os.getenv("MINIO_BUCKET_NAME", "bronze")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    if not args.yes:
        resp = input(
            f"This will DELETE ALL objects under s3://{bucket}/{args.prefix or ''}. Continue? [y/N] "
        )
        if resp.strip().lower() != "y":
            print("Aborted.")
            sys.exit(1)

    count = purge_bucket(bucket, endpoint, access_key, secret_key, args.prefix)
    print(f"Deleted {count} objects from s3://{bucket}/{args.prefix or ''}")


if __name__ == "__main__":
    main()
