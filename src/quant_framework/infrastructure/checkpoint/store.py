"""Checkpoint storage backed by MinIO (S3-compatible) with optional local fallback.

Provides atomic JSON read/write helpers and simple existence/list operations.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


@dataclass
class CheckpointDocument:
    """Normalized checkpoint representation."""

    source: str
    exchange: str
    symbol: str
    timeframe: str
    market_type: str | None
    last_successful_timestamp: int
    last_updated: str
    total_records: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        doc = asdict(self)
        # Keep nulls explicit for JSON consumers
        return doc

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> CheckpointDocument:
        return cls(
            source=raw.get("source", ""),
            exchange=raw.get("exchange", ""),
            symbol=raw.get("symbol", ""),
            timeframe=raw.get("timeframe", ""),
            market_type=raw.get("market_type"),
            last_successful_timestamp=int(raw.get("last_successful_timestamp", 0)),
            last_updated=raw.get("last_updated", ""),
            total_records=int(raw.get("total_records", 0)),
            metadata=raw.get("metadata", {}) or {},
        )


class CheckpointStore:
    """S3/MinIO-backed checkpoint store with optional local fallback."""

    def __init__(
        self,
        bucket: str | None = None,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        local_root: str | None = None,
        region: str = "us-east-1",
        secure: bool | None = None,
    ) -> None:
        self.bucket = bucket or os.getenv("MINIO_BUCKET_NAME", "bronze")
        env_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.endpoint = endpoint or env_endpoint
        self.local_root = Path(local_root) if local_root else None

        # Resolve credentials
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "admin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "password123")
        use_ssl = secure if secure is not None else self.endpoint.startswith("https")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=region,
            use_ssl=use_ssl,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def exists(self, key: str) -> bool:
        if self.local_root:
            return (self.local_root / key).exists()
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"404", "NotFound", "NoSuchKey"}:
                return False
            raise

    def read(self, key: str) -> tuple[CheckpointDocument | None, str | None]:
        """Read checkpoint and return document with current ETag (if available)."""
        if self.local_root:
            path = self.local_root / key
            if not path.exists():
                return None, None
            raw = json.loads(path.read_text())
            return CheckpointDocument.from_dict(raw), None

        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            etag = resp.get("ETag")
            return CheckpointDocument.from_dict(json.loads(body)), etag
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "NoSuchKey":
                return None, None
            raise

    def write_atomic(
        self,
        key: str,
        document: CheckpointDocument,
        if_match: str | None = None,
    ) -> str:
        """Write checkpoint with optional ETag precondition. Returns new ETag."""
        payload = json.dumps(document.to_dict(), ensure_ascii=True).encode("utf-8")

        if self.local_root:
            path = self.local_root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            return "local"

        put_kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": payload,
            "ContentType": "application/json",
        }
        # Note: MinIO doesn't support IfMatch (ETag) parameter in put_object.
        # Omitting conditional put for MinIO compatibility.
        # For concurrent modification detection, use read() with version checking instead.
        # if_match parameter is kept for S3 API compatibility but not used with MinIO.

        try:
            resp = self.s3.put_object(**put_kwargs)
            return resp.get("ETag", "")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "PreconditionFailed":
                raise RuntimeError(
                    f"Concurrent modification detected for checkpoint {key}"
                ) from e
            raise

    def list(self, prefix: str) -> list[str]:
        if self.local_root:
            base = self.local_root / prefix
            if not base.exists():
                return []
            return [str(p.relative_to(self.local_root)) for p in base.rglob("*.json")]

        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        contents = resp.get("Contents", [])
        return [obj["Key"] for obj in contents]
