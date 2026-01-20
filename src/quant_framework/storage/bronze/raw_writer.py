# src/quant_framework/storage/bronze/raw_writer.py
import hashlib
import json
import pickle
from datetime import datetime
from io import BytesIO
from typing import Any


class RawBronzeWriter:
    """Writes raw data to MinIO Bronze layer with minimal processing"""

    def __init__(self, minio_client):
        self.minio_client = minio_client
        # Allow bucket override via env for flexibility
        import os

        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze")

    async def write_raw(
        self,
        source: str,
        data_type: str,
        symbol: str,
        raw_data: Any,
        timestamp: datetime,
        file_format: str = "raw_json",
        compression: str = "none",
    ) -> dict[str, Any]:
        """
        Write raw data to MinIO with standard partitioning

        Args:
            source: Data source (e.g., 'binance', 'coinmetrics')
            data_type: Type of data (e.g., 'ohlcv', 'onchain')
            symbol: Trading symbol (e.g., 'BTC', 'ETH')
            raw_data: The actual data to write
            timestamp: Data timestamp
            file_format: Format of the data (raw_json, raw_pickle, etc.)
            compression: Compression algorithm (none, gzip, snappy, etc.)

        Returns:
            Dictionary with write result metadata
        """

        # 1. Generate partition path (source/data_type/symbol/date)
        date_str = timestamp.strftime("%Y-%m-%d")
        partition = f"{source}/{data_type}/{symbol}/{date_str}"

        # 2. Serialize data based on format
        if file_format == "raw_json":
            serialized_data = json.dumps(raw_data, default=str).encode("utf-8")
            extension = "json"
        elif file_format == "raw_pickle":
            serialized_data = pickle.dumps(raw_data)
            extension = "pkl"
        else:
            # Default to JSON
            serialized_data = json.dumps(raw_data, default=str).encode("utf-8")
            extension = file_format

        # 3. Generate file key with timestamp
        file_key = f"{partition}/data_{timestamp.strftime('%Y%m%d_%H%M%S')}.{extension}"

        # 4. Calculate checksums
        md5_hash = hashlib.md5(serialized_data).hexdigest()
        sha256_hash = hashlib.sha256(serialized_data).hexdigest()

        # 5. Prepare metadata for S3 object
        metadata = {
            "source": source,
            "data_type": data_type,
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "file_format": file_format,
            "compression": compression,
            "md5": md5_hash,
            "sha256": sha256_hash,
        }

        # 6. Write to MinIO
        file_bytes = BytesIO(serialized_data)
        try:
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=file_key,
                data=file_bytes,
                length=len(serialized_data),
                metadata=metadata,
            )
        except Exception as e:
            raise Exception(f"Failed to write to MinIO: {str(e)}")

        # 7. Return result metadata
        return {
            "success": True,
            "s3_key": file_key,
            "partition": partition,
            "bytes_written": len(serialized_data),
            "md5": md5_hash,
            "sha256": sha256_hash,
            "timestamp": timestamp.isoformat(),
        }
