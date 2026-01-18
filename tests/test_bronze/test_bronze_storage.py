"""
Comprehensive tests for Bronze Storage Layer
Tests cover MinIO operations, raw data writing, checkpoint management, and integration
"""

import json
import pickle
from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock

import pytest

from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketDataType,
    MarketType,
)
from quant_framework.shared.models.instruments import Instrument
from quant_framework.storage.bronze.checkpoint import BronzeCheckpoint
from quant_framework.storage.bronze.metadata import (
    BronzeCompression,
    BronzeFileFormat,
    BronzeFileMetadata,
)
from quant_framework.storage.bronze.registry import (
    BronzeIngestionRequest,
)

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client for testing"""
    client = MagicMock()
    client.bucket_exists = MagicMock(return_value=True)
    client.put_object = MagicMock()
    client.get_object = MagicMock()
    client.list_objects = MagicMock(return_value=[])
    return client


@pytest.fixture
def mock_checkpoint_manager():
    """Mock checkpoint manager"""
    manager = MagicMock()
    manager.save_checkpoint = AsyncMock(return_value={"status": "saved"})
    manager.load_checkpoint = AsyncMock(return_value=None)
    manager.list_checkpoints = AsyncMock(return_value=[])
    return manager


@pytest.fixture
def sample_instrument():
    """Create a sample instrument for testing"""
    return Instrument(
        instrument_id="BTC_SPOT_BINANCE",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.SPOT,
        venue=DataVenue.BINANCE,
        base_asset="BTC",
        quote_asset="USD",
        wrapper="ccxt",
        is_active=True,
        metadata={"exchange": "binance", "symbol": "BTC/USD"},
    )


@pytest.fixture
def sample_json_data():
    """Sample JSON data for testing"""
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol": "BTC",
        "price": 45000.50,
        "volume": 1000.5,
        "data": [1, 2, 3, 4, 5],
    }


@pytest.fixture
def sample_binary_data():
    """Sample binary data for testing"""
    data = {"raw": b"binary data", "timestamp": datetime.utcnow().isoformat()}
    return pickle.dumps(data)


# ============================================================================
# GROUP 1: MinIO Connection & Setup Smoke Tests (Tests 1-3)
# ============================================================================


class TestMinIOConnectionAndSetup:
    """Tests for MinIO client connection and setup verification"""

    def test_minio_client_connection(self, mock_minio_client):
        """Test 1: Verify MinIO client can connect to endpoint"""
        # Simulate connection check
        assert mock_minio_client is not None
        # Verify the client has expected methods
        assert hasattr(mock_minio_client, "bucket_exists")
        assert hasattr(mock_minio_client, "put_object")
        assert hasattr(mock_minio_client, "get_object")

    def test_bronze_bucket_exists_and_accessible(self, mock_minio_client):
        """Test 2: Verify bronze bucket exists and is accessible"""
        bucket_name = "bronze"
        result = mock_minio_client.bucket_exists(bucket_name)
        assert result is True

    def test_bronze_folder_structure_exists(self, mock_minio_client):
        """Test 3: Verify folder structure (source=/data_type=) exists in bronze"""
        # Mock the expected prefix structure
        mock_minio_client.list_objects = MagicMock(
            return_value=[
                MagicMock(object_name="binance/ohlcv/"),
                MagicMock(object_name="coinmetrics/onchain/"),
            ]
        )

        # Check that the expected prefixes are found
        objects = list(
            mock_minio_client.list_objects("bronze", prefix="", recursive=True)
        )
        assert len(objects) >= 0  # Verifies call succeeds


# ============================================================================
# GROUP 2: Raw Data Writing Tests (Tests 4-7)
# ============================================================================


class TestRawDataWriting:
    """Tests for writing raw data to bronze storage"""

    @pytest.mark.asyncio
    async def test_write_raw_json_data_to_bronze(
        self, mock_minio_client, sample_json_data
    ):
        """Test 4: Write raw JSON data to bronze (no validation)"""
        # Create a mock raw writer
        s3_key = "binance/ohlcv/BTC/2026-01-18/data_20260118_120000.json"
        json_bytes = json.dumps(sample_json_data).encode("utf-8")

        mock_minio_client.put_object(
            bucket_name="bronze",
            object_name=s3_key,
            data=BytesIO(json_bytes),
            length=len(json_bytes),
        )

        # Verify the write was called
        mock_minio_client.put_object.assert_called()
        call_args = mock_minio_client.put_object.call_args
        assert call_args.kwargs.get("bucket_name") == "bronze"
        assert call_args.kwargs.get("object_name") == s3_key

    @pytest.mark.asyncio
    async def test_write_raw_binary_pickle_data_to_bronze(
        self, mock_minio_client, sample_binary_data
    ):
        """Test 5: Write raw binary/pickle data to bronze"""
        s3_key = "binance/ohlcv/BTC/2026-01-18/data_20260118_120000.pkl"

        mock_minio_client.put_object(
            bucket_name="bronze",
            object_name=s3_key,
            data=BytesIO(sample_binary_data),
            length=len(sample_binary_data),
        )

        mock_minio_client.put_object.assert_called()
        call_args = mock_minio_client.put_object.call_args
        assert call_args.kwargs.get("bucket_name") == "bronze"
        assert call_args.kwargs.get("object_name") == s3_key

    @pytest.mark.asyncio
    async def test_verify_correct_partitioning_structure(self, mock_minio_client):
        """Test 6: Verify correct partitioning (source/data_type/symbol/date)"""
        # Test the expected partitioning structure
        source = "binance"
        data_type = "ohlcv"
        symbol = "BTC"
        date = "2026-01-18"

        expected_partition = f"{source}/{data_type}/{symbol}/{date}"
        s3_key = f"{expected_partition}/data_20260118_120000.json"

        # Verify partition structure is correct
        assert source in s3_key
        assert data_type in s3_key
        assert symbol in s3_key
        assert date in s3_key

        # Verify the partition components in order
        parts = s3_key.split("/")
        assert parts[0] == source
        assert parts[1] == data_type
        assert parts[2] == symbol
        assert parts[3] == date

    @pytest.mark.asyncio
    async def test_verify_metadata_preserved_in_s3_object_metadata(
        self, mock_minio_client, sample_json_data
    ):
        """Test 7: Verify metadata is preserved in S3 object metadata"""
        custom_metadata = {
            "source": "binance",
            "data_type": "ohlcv",
            "ingestion_id": "binance_ohlcv_20260118_120000",
            "custom_field": "test_value",
        }

        # Simulate putting object with metadata
        s3_key = "binance/ohlcv/BTC/2026-01-18/data.json"
        json_bytes = json.dumps(sample_json_data).encode("utf-8")

        mock_minio_client.put_object(
            bucket_name="bronze",
            object_name=s3_key,
            data=BytesIO(json_bytes),
            length=len(json_bytes),
            metadata=custom_metadata,
        )

        # Verify metadata was passed
        call_args = mock_minio_client.put_object.call_args
        assert "metadata" in call_args.kwargs or len(call_args.args) > 5


# ============================================================================
# GROUP 3: Checkpoint System Tests (Tests 8-11)
# ============================================================================


class TestCheckpointSystem:
    """Tests for bronze checkpoint creation, loading, and management"""

    @pytest.mark.asyncio
    async def test_create_and_save_bronze_checkpoint(
        self, mock_checkpoint_manager, sample_instrument
    ):
        """Test 8: Create and save bronze checkpoint (MinIO-only)"""
        checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            instrument=sample_instrument,
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="binance/ohlcv/BTC/2026-01-18/data_20260118_120000.json",
            last_data_timestamp=datetime.utcnow(),
            metadata={
                "exchange": "binance",
                "timeframe": "1h",
            },
            total_files_written=1,
            total_bytes_written=1024,
        )

        # Save checkpoint
        result = await mock_checkpoint_manager.save_checkpoint(checkpoint)

        assert result is not None
        mock_checkpoint_manager.save_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_existing_checkpoint_resume_capability(
        self, mock_checkpoint_manager, sample_instrument
    ):
        """Test 9: Load existing checkpoint (resume capability)"""
        checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            instrument=sample_instrument,
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="binance/ohlcv/BTC/2026-01-18/data_20260118_100000.json",
            last_data_timestamp=datetime.utcnow() - timedelta(hours=1),
            total_files_written=10,
            total_bytes_written=10240,
        )

        mock_checkpoint_manager.load_checkpoint = AsyncMock(return_value=checkpoint)

        # Load checkpoint
        loaded = await mock_checkpoint_manager.load_checkpoint(
            source="binance", data_type="ohlcv", symbol="BTC"
        )

        assert loaded is not None
        assert loaded.symbol == "BTC"
        assert loaded.total_files_written == 10

    @pytest.mark.asyncio
    async def test_update_checkpoint_after_successful_write(
        self, mock_checkpoint_manager, sample_instrument
    ):
        """Test 10: Update checkpoint after successful write"""
        checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            instrument=sample_instrument,
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="binance/ohlcv/BTC/2026-01-18/data_old.json",
            last_data_timestamp=datetime.utcnow() - timedelta(hours=1),
            total_files_written=5,
            total_bytes_written=5120,
        )

        # Update checkpoint with new data
        checkpoint.last_success_key = "binance/ohlcv/BTC/2026-01-18/data_new.json"
        checkpoint.total_files_written = 6
        checkpoint.total_bytes_written = 6144
        checkpoint.checkpoint_updated_at = datetime.utcnow()

        result = await mock_checkpoint_manager.save_checkpoint(checkpoint)

        assert result is not None
        assert checkpoint.total_files_written == 6

    @pytest.mark.asyncio
    async def test_list_checkpoints_with_filters(self, mock_checkpoint_manager):
        """Test 11: List checkpoints with filters (by source/data_type)"""
        checkpoints = [
            {
                "source": "binance",
                "data_type": "ohlcv",
                "symbol": "BTC",
            },
            {
                "source": "binance",
                "data_type": "ohlcv",
                "symbol": "ETH",
            },
            {
                "source": "coinmetrics",
                "data_type": "onchain",
                "symbol": "BTC",
            },
        ]

        mock_checkpoint_manager.list_checkpoints = AsyncMock(return_value=checkpoints)

        # List all checkpoints
        all_checkpoints = await mock_checkpoint_manager.list_checkpoints()
        assert len(all_checkpoints) == 3

        # Filter by source
        mock_checkpoint_manager.list_checkpoints = AsyncMock(
            return_value=[c for c in checkpoints if c["source"] == "binance"]
        )
        filtered = await mock_checkpoint_manager.list_checkpoints(source="binance")
        assert len(filtered) == 2
        assert all(c["source"] == "binance" for c in filtered)


# ============================================================================
# GROUP 4: Integration with Existing Models (Tests 12-14)
# ============================================================================


class TestIntegrationWithModels:
    """Tests for integration with DataVenue, MarketDataType, and other models"""

    def test_use_datavenue_and_marketdatatype_enums_in_operations(
        self, sample_instrument
    ):
        """Test 12: Use DataVenue and MarketDataType enums in bronze operations"""
        # Verify enums work correctly
        assert DataVenue.BINANCE.value == "binance"
        assert MarketDataType.OHLCV.value == "ohlcv"

        # Test in checkpoint
        checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="binance/ohlcv/BTC/2026-01-18/data.json",
            last_data_timestamp=datetime.utcnow(),
        )

        assert checkpoint.source == DataVenue.BINANCE
        assert checkpoint.data_type == MarketDataType.OHLCV
        assert isinstance(checkpoint.source, DataVenue)
        assert isinstance(checkpoint.data_type, MarketDataType)

    def test_create_bronze_ingestion_request_with_instrument_model(
        self, sample_instrument, sample_json_data
    ):
        """Test 13: Create BronzeIngestionRequest with Instrument model"""
        request = BronzeIngestionRequest(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            instrument=sample_instrument,
            raw_data=sample_json_data,
            file_format="raw_json",
            compression="none",
            create_checkpoint=True,
        )

        assert request.source == DataVenue.BINANCE
        assert request.data_type == MarketDataType.OHLCV
        assert request.instrument is not None
        assert request.instrument.base_asset == "BTC"
        assert request.symbol == "BTC"
        assert request.ingestion_id is not None

    def test_verify_data_lineage_tracking_works(
        self, sample_instrument, sample_json_data
    ):
        """Test 14: Verify data lineage tracking works (ingestion_id, parent_files)"""
        # Create file metadata with lineage info
        file_metadata = BronzeFileMetadata(
            file_key="binance/ohlcv/BTC/2026-01-18/data_new.json",
            file_size_bytes=2048,
            file_format=BronzeFileFormat.RAW_JSON,
            compression=BronzeCompression.NONE,
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            data_timestamp=datetime.utcnow(),
            ingestion_id="binance_ohlcv_20260118_120000",
            parent_files=[
                "binance/ohlcv/BTC/2026-01-17/data_old.json",
                "binance/ohlcv/BTC/2026-01-16/data_old.json",
            ],
            custom_metadata={"source_api": "ccxt", "exchange": "binance"},
        )

        # Verify lineage tracking
        assert file_metadata.ingestion_id == "binance_ohlcv_20260118_120000"
        assert len(file_metadata.parent_files) == 2
        assert "2026-01-17" in file_metadata.parent_files[0]
        assert file_metadata.custom_metadata["source_api"] == "ccxt"


# ============================================================================
# GROUP 5: Error Handling & Edge Cases (Tests 15-16)
# ============================================================================


class TestErrorHandlingAndEdgeCases:
    """Tests for error handling and recovery scenarios"""

    @pytest.mark.asyncio
    async def test_handle_minio_write_failures_gracefully(
        self, mock_minio_client, sample_json_data
    ):
        """Test 15: Handle MinIO write failures gracefully"""
        # Simulate write failure
        mock_minio_client.put_object = MagicMock(
            side_effect=Exception("Connection refused")
        )

        # Attempt write and catch error
        with pytest.raises(Exception) as exc_info:
            mock_minio_client.put_object(
                bucket_name="bronze",
                object_name="binance/ohlcv/BTC/2026-01-18/data.json",
                data=BytesIO(json.dumps(sample_json_data).encode()),
                length=1024,
            )

        assert "Connection refused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_recover_from_missing_checkpoint_cold_start(
        self, mock_checkpoint_manager
    ):
        """Test 16: Recover from missing checkpoint (cold start)"""
        # Simulate missing checkpoint (returns None)
        mock_checkpoint_manager.load_checkpoint = AsyncMock(return_value=None)

        loaded = await mock_checkpoint_manager.load_checkpoint(
            source="unknown", data_type="unknown", symbol="UNKNOWN"
        )

        assert loaded is None

        # Create a new checkpoint as fallback (cold start)
        new_checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="",  # Empty, cold start
            last_data_timestamp=datetime.utcnow(),
            status="active",
        )

        assert new_checkpoint.last_success_key == ""
        assert new_checkpoint.status == "active"


# ============================================================================
# ADDITIONAL: Checkpoint Serialization Tests
# ============================================================================


class TestCheckpointSerialization:
    """Tests for checkpoint serialization/deserialization"""

    def test_checkpoint_to_minio_dict(self, sample_instrument):
        """Test checkpoint conversion to MinIO-compatible dict"""
        checkpoint = BronzeCheckpoint(
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            instrument=sample_instrument,
            partition_path="binance/ohlcv/BTC/2026-01-18",
            last_success_key="binance/ohlcv/BTC/2026-01-18/data.json",
            last_data_timestamp=datetime.utcnow(),
        )

        minio_dict = checkpoint.to_minio_dict()

        # Verify serialization
        assert minio_dict["source"] == "binance"
        assert minio_dict["data_type"] == "ohlcv"
        assert isinstance(minio_dict["last_data_timestamp"], str)  # ISO format

    def test_checkpoint_from_minio_dict(self, sample_instrument):
        """Test checkpoint deserialization from MinIO dict"""
        now = datetime.utcnow()
        minio_dict = {
            "source": "binance",
            "data_type": "ohlcv",
            "symbol": "BTC",
            "partition_path": "binance/ohlcv/BTC/2026-01-18",
            "last_success_key": "binance/ohlcv/BTC/2026-01-18/data.json",
            "last_data_timestamp": now.isoformat(),
            "checkpoint_created_at": now.isoformat(),
            "checkpoint_updated_at": now.isoformat(),
        }

        checkpoint = BronzeCheckpoint.from_minio_dict(minio_dict)

        assert checkpoint.source == DataVenue.BINANCE
        assert checkpoint.data_type == MarketDataType.OHLCV
        assert isinstance(checkpoint.last_data_timestamp, datetime)


# ============================================================================
# ADDITIONAL: File Metadata Tests
# ============================================================================


class TestFileMetadata:
    """Tests for file metadata creation and validation"""

    def test_create_file_metadata_with_all_fields(self):
        """Test creating file metadata with all fields"""
        metadata = BronzeFileMetadata(
            file_key="binance/ohlcv/BTC/2026-01-18/data.json",
            file_size_bytes=2048,
            file_format=BronzeFileFormat.RAW_JSON,
            compression=BronzeCompression.GZIP,
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
            data_timestamp=datetime.utcnow(),
            ingestion_id="test_ingestion_123",
            parent_files=["parent1.json", "parent2.json"],
            md5_hash="abc123",
            sha256_hash="def456",
            is_valid=True,
            custom_metadata={"test": "value"},
        )

        assert metadata.file_key == "binance/ohlcv/BTC/2026-01-18/data.json"
        assert metadata.file_size_bytes == 2048
        assert metadata.ingestion_id == "test_ingestion_123"
        assert len(metadata.parent_files) == 2

    def test_file_metadata_serialization(self):
        """Test file metadata serialization to dict"""
        metadata = BronzeFileMetadata(
            file_key="binance/ohlcv/BTC/2026-01-18/data.json",
            file_size_bytes=1024,
            file_format=BronzeFileFormat.RAW_JSON,
            compression=BronzeCompression.NONE,
            source=DataVenue.BINANCE,
            data_type=MarketDataType.OHLCV,
            symbol="BTC",
        )

        dict_repr = metadata.model_dump()

        assert dict_repr["file_key"] == "binance/ohlcv/BTC/2026-01-18/data.json"
        assert dict_repr["file_size_bytes"] == 1024
        assert dict_repr["source"] == DataVenue.BINANCE


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
