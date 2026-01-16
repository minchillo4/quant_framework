"""
File-based BronzeCheckpointManager for MinIO.

Stores JSON checkpoints under s3://{bucket}/_checkpoints/{source}/{data_type}/{symbol}_{timeframe}.json

Includes:
- last_successful, last_written_file, files_written
- consecutive_failures, last_error, backoff_until
- versioning and provenance metadata
- adaptive_stats persisted from rate limiter
"""

import json
import os
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError


class BronzeCheckpointManager:
    def __init__(
        self,
        bucket: str | None = None,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        self.bucket = bucket or os.getenv("MINIO_BUCKET_NAME", "bronze")
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "http://minio:9000")

        access_key = access_key or os.getenv("MINIO_ROOT_USER", "admin")
        secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "password123")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=os.getenv("MINIO_REGION", "us-east-1"),
        )

    @staticmethod
    def _key(source: str, data_type: str, symbol: str, timeframe: str) -> str:
        return f"_checkpoints/{source}/{data_type}/{symbol}_{timeframe}.json"

    def load(
        self, source: str, data_type: str, symbol: str, timeframe: str
    ) -> dict[str, Any] | None:
        key = self._key(source, data_type, symbol, timeframe)
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            raw = resp["Body"].read().decode("utf-8")
            data = json.loads(raw)
            return data
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "NoSuchKey":
                return None
            raise

    def save(
        self,
        source: str,
        data_type: str,
        symbol: str,
        timeframe: str,
        checkpoint: dict[str, Any],
    ) -> None:
        key = self._key(source, data_type, symbol, timeframe)
        # Ensure required metadata
        now = datetime.utcnow().isoformat() + "Z"
        checkpoint.setdefault("version", "1.0")
        checkpoint.setdefault("source", source)
        checkpoint.setdefault("data_type", data_type)
        checkpoint.setdefault("symbol", symbol)
        checkpoint.setdefault("timeframe", timeframe)
        checkpoint.setdefault("status", "active")
        checkpoint.setdefault("files_written", 0)
        checkpoint.setdefault("consecutive_failures", 0)
        checkpoint.setdefault("metadata", {})
        checkpoint["metadata"].setdefault("created_at", now)
        checkpoint["metadata"]["updated_at"] = now
        checkpoint.setdefault(
            "provenance",
            {
                "adapter": source,
                "producer_version": os.getenv("PRODUCER_VERSION", "unknown"),
                "api_point_limit": 1000,
            },
        )

        body = json.dumps(checkpoint, ensure_ascii=False).encode("utf-8")
        self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType="application/json"
        )

    def delete(self, source: str, data_type: str, symbol: str, timeframe: str) -> None:
        key = self._key(source, data_type, symbol, timeframe)
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def list_failing_symbols(self, source: str, data_type: str) -> list[str]:
        prefix = f"_checkpoints/{source}/{data_type}/"
        try:
            resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            failing = []
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                get = self.s3.get_object(Bucket=self.bucket, Key=key)
                data = json.loads(get["Body"].read().decode("utf-8"))
                if data.get("status") in {"failed", "rate_limited", "inactive"}:
                    failing.append(key)
            return failing
        except ClientError:
            return []
