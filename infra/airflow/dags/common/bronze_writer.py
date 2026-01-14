"""
Schema-safe BronzeWriter for MinIO bronze layer.
Handles strict type validation, daily batch accumulation, and Parquet writes.
"""

import logging
import os
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


# ============================================================================
# PYDANTIC SCHEMAS FOR STRICT TYPE VALIDATION
# ============================================================================


class OHLCVSchema(BaseModel):
    """Strict schema for OHLCV (OHLC + Volume) data."""

    timestamp: datetime = Field(..., description="Candle open time")
    open: float = Field(..., description="Open price", gt=0)
    high: float = Field(..., description="High price", gt=0)
    low: float = Field(..., description="Low price", gt=0)
    close: float = Field(..., description="Close price", gt=0)
    volume: float = Field(..., description="Trading volume", ge=0)
    canonical_symbol: str = Field(..., description="Symbol (e.g., BTC, ETH)")
    exchange: str = Field(..., description="Exchange name (e.g., binance)")
    timeframe: str = Field(..., description="Timeframe (e.g., 1h, 1d)")

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp_is_datetime(cls, v):
        """Validate that timestamp is a datetime object."""
        if not isinstance(v, datetime):
            raise ValueError(f"timestamp must be datetime, got {type(v).__name__}")
        return v

    @field_validator("open", "high", "low", "close", mode="before")
    @classmethod
    def validate_prices_are_numeric(cls, v):
        """Validate that prices are numeric (will be cast to float64)."""
        if isinstance(v, str):
            raise ValueError(f"price must be numeric, got string: {v}")
        try:
            return float(v)
        except (TypeError, ValueError):
            raise ValueError(f"price must be convertible to float, got {v}")

    @model_validator(mode="after")
    def high_must_be_gte_low(self):
        """Validate that high >= low."""
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        return self


class OpenInterestSchema(BaseModel):
    """Strict schema for Open Interest data."""

    timestamp: datetime = Field(..., description="Data point time")
    open_interest: float = Field(..., description="Open interest value", ge=0)
    canonical_symbol: str = Field(..., description="Symbol (e.g., BTC, ETH)")
    exchange: str = Field(..., description="Exchange name")
    timeframe: str = Field(..., description="Timeframe (e.g., 1h, 1d)")

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp_is_datetime(cls, v):
        """Validate that timestamp is a datetime object."""
        if not isinstance(v, datetime):
            raise ValueError(f"timestamp must be datetime, got {type(v).__name__}")
        return v

    @field_validator("open_interest", mode="before")
    @classmethod
    def validate_oi_is_numeric(cls, v):
        """Validate that open_interest is numeric."""
        if isinstance(v, str):
            raise ValueError(f"open_interest must be numeric, got string: {v}")
        try:
            return float(v)
        except (TypeError, ValueError):
            raise ValueError(f"open_interest must be convertible to float, got {v}")


# ============================================================================
# BRONZE WRITER CLASS
# ============================================================================


class BronzeWriter:
    """
    Writes validated data to MinIO bronze layer as Parquet files.

    Features:
    - Strict Pydantic schema validation before write
    - Daily partitioning: one Parquet file per (symbol, timeframe, exchange, date)
    - S3/MinIO compatible backend
    - Idempotent writes (overwrite existing files safely)
    - Snappy compression
    """

    def __init__(
        self,
        bucket: str | None = None,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        """
        Initialize BronzeWriter.

        Reads configuration from environment variables with fallbacks:
        - MINIO_BUCKET_NAME -> bucket (default: bronze)
        - MINIO_ENDPOINT -> endpoint (default: http://localhost:9002)
        - MINIO_ROOT_USER -> access_key (default: admin)
        - MINIO_ROOT_PASSWORD -> secret_key (default: password123)
        """
        # Read from environment or use provided parameters
        self.bucket = bucket or os.getenv("MINIO_BUCKET_NAME", "bronze")
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "http://localhost:9002")

        # Try to get credentials from Airflow Variables first, then env, then defaults
        if access_key is None:
            try:
                from airflow.models import Variable

                access_key = Variable.get("MINIO_ROOT_USER", default_var=None)
            except Exception:
                access_key = None
            access_key = access_key or os.getenv("MINIO_ROOT_USER", "admin")

        if secret_key is None:
            try:
                from airflow.models import Variable

                secret_key = Variable.get("MINIO_ROOT_PASSWORD", default_var=None)
            except Exception:
                secret_key = None
            secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "password123")

        # Initialize S3 client (MinIO is S3-compatible)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

        logger.info(
            f"🟢 BronzeWriter initialized: bucket={self.bucket}, endpoint={self.endpoint}"
        )

    async def write_daily_batch(
        self,
        records: list[dict[str, Any]],
        symbol: str,
        timeframe: str,
        data_type: str,
        date: datetime,
        schema_class: type | None = None,
    ) -> dict[str, Any]:
        """
        Write daily batch of records to Parquet file in MinIO bronze layer.

        Args:
            records: List of data records (dicts or models)
            symbol: Canonical symbol (e.g., BTC, ETH)
            timeframe: Timeframe (e.g., 1h, 1d)
            data_type: Data type (ohlc or oi)
            date: Date for this batch (used in filename)
            schema_class: Pydantic schema class for validation (auto-selected if None)

        Returns:
            {
                "success": bool,
                "file_path": str,
                "records_written": int,
                "error": str | None
            }
        """
        try:
            # Auto-select schema class if not provided
            if schema_class is None:
                schema_class = (
                    OHLCVSchema if data_type == "ohlc" else OpenInterestSchema
                )

            # Step 1: Validate all records against schema
            logger.info(
                f"🔍 Validating {len(records)} records for {symbol}/{timeframe}/{data_type}"
            )
            validated_records = []
            for i, record in enumerate(records):
                try:
                    if isinstance(record, schema_class):
                        validated = record
                    else:
                        validated = schema_class(**record)
                    validated_records.append(validated)
                except Exception as e:
                    logger.error(f"❌ Validation failed for record {i}: {e}")
                    raise ValueError(f"Record {i} failed validation: {e}") from e

            if not validated_records:
                logger.warning(
                    f"⚠️ No valid records to write for {symbol}/{timeframe}/{data_type}"
                )
                return {
                    "success": True,
                    "file_path": None,
                    "records_written": 0,
                    "error": "No valid records",
                }

            # Step 2: Convert to DataFrame
            logger.info(f"📊 Converting {len(validated_records)} records to DataFrame")
            df_records = [r.dict() for r in validated_records]
            df = pd.DataFrame(df_records)

            # Ensure timestamp is datetime64[ns]
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Ensure numeric columns are float64
            numeric_cols = ["open", "high", "low", "close", "volume", "open_interest"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype("float64")

            logger.info(f"✅ DataFrame schema validated: {df.dtypes.to_dict()}")

            # Step 3: Construct MinIO path
            date_str = date.strftime("%Y%m%d")
            exchange = df["exchange"].iloc[0] if "exchange" in df.columns else "unknown"

            file_key = (
                f"source=coinalyze/data_type={data_type}/"
                f"coinalyze-{data_type}-{symbol}-{timeframe}-{exchange}-{date_str}.parquet"
            )

            # Step 4: Convert DataFrame to Parquet in memory
            logger.info(f"📦 Creating Parquet file: {file_key}")
            table = pa.Table.from_pandas(df)
            parquet_bytes = table.to_batches()

            # Use pyarrow to write with snappy compression
            import io

            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            # Step 5: Upload to MinIO (idempotent: overwrite if exists)
            logger.info(f"⬆️ Uploading to MinIO: s3://{self.bucket}/{file_key}")
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=file_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )

            logger.info(
                f"✅ Successfully wrote {len(validated_records)} records to {file_key}"
            )

            return {
                "success": True,
                "file_path": f"s3://{self.bucket}/{file_key}",
                "records_written": len(validated_records),
                "error": None,
            }

        except Exception as e:
            logger.error(f"❌ BronzeWriter error: {e}", exc_info=True)
            return {
                "success": False,
                "file_path": None,
                "records_written": 0,
                "error": str(e),
            }

    def list_bronze_files(self, data_type: str, symbol: str | None = None) -> list[str]:
        """
        List Parquet files in bronze layer for given data_type (and optional symbol).

        Args:
            data_type: Data type (ohlc or oi)
            symbol: Optional symbol filter (e.g., BTC)

        Returns:
            List of file keys in MinIO
        """
        try:
            prefix = f"source=coinalyze/data_type={data_type}/"
            if symbol:
                prefix += f"coinalyze-{data_type}-{symbol}-"

            logger.info(f"🔍 Listing bronze files: s3://{self.bucket}/{prefix}")

            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"✅ Found {len(files)} bronze files")

            return files

        except ClientError as e:
            logger.error(f"❌ MinIO error: {e}")
            return []
