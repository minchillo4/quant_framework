# scripts/ingestion/get_onchain_v2_demo.py
"""
Demonstration of the Bronze ingestion workflow using the get_onchain_v2 script
This version shows the complete workflow without requiring actual external adapters
"""

import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from quant_framework.shared.models.enums import DataVenue, MarketDataType
from quant_framework.shared.models.instruments import AssetClass, Instrument, MarketType
from quant_framework.storage.bronze.registry import (
    BronzeIngestionRequest,
    BronzeRegistry,
)


async def demo_main():
    """Complete workflow demonstration using all Bronze models"""

    print("=" * 80)
    print("🚀 Bronze Storage Layer - Complete Workflow Demonstration")
    print("=" * 80)

    # 1. Setup - Create mock clients for demonstration
    print("\n[Step 1] Setting up Bronze Registry...")

    # Create mock MinIO client
    mock_minio_client = MagicMock()
    mock_minio_client.bucket_exists = MagicMock(return_value=True)
    mock_minio_client.put_object = MagicMock()

    # Create mock checkpoint manager
    mock_checkpoint_manager = AsyncMock()
    mock_checkpoint_manager.save_checkpoint = AsyncMock(
        return_value={"status": "saved"}
    )

    # In real implementation, would be:
    # registry = BronzeRegistry.get_default()
    # For demo, we'll manually construct the registry
    registry = BronzeRegistry(mock_minio_client, mock_checkpoint_manager)
    print("✅ Registry initialized")

    # 2. Create instrument using the Instrument model
    print("\n[Step 2] Creating Instrument...")
    btc_instrument = Instrument(
        instrument_id="BTC_COINMETRICS",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.SPOT,
        venue=DataVenue.COINMETRICS,
        base_asset="BTC",
        quote_asset="USD",
        wrapper="coinmetrics",
        is_active=True,
        metadata={
            "description": "Bitcoin on-chain metrics",
            "data_source": "CoinMetrics CSV",
            "update_frequency": "daily",
        },
    )
    print(f"✅ Created Instrument: {btc_instrument.instrument_id}")
    print(f"   - Asset Class: {btc_instrument.asset_class}")
    print(f"   - Market Type: {btc_instrument.market_type}")
    print(f"   - Venue: {btc_instrument.venue}")

    # 3. Fetch/Create mock data
    print("\n[Step 3] Fetching on-chain data...")
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)

    # Create mock on-chain data
    raw_data = {
        "data": [
            {
                "timestamp": (start_date + timedelta(days=i)).isoformat(),
                "asset": "BTC",
                "metrics": {
                    "price_usd": 45000.0 + (i * 100),
                    "active_addresses": 1000000 + (i * 50000),
                    "hash_rate": 500e18 + (i * 1e17),
                },
            }
            for i in range(7)
        ]
    }
    print(f"✅ Fetched {len(raw_data['data'])} days of on-chain data")
    print(f"   - Date Range: {start_date.date()} to {end_date.date()}")
    print("   - Metrics: price_usd, active_addresses, hash_rate")

    # 4. Create BronzeIngestionRequest with all models
    print("\n[Step 4] Creating BronzeIngestionRequest...")
    request = BronzeIngestionRequest(
        source=DataVenue.COINMETRICS,
        data_type=MarketDataType.ONCHAIN,
        instrument=btc_instrument,
        raw_data=raw_data,
        file_format="raw_json",
        compression="snappy",
        ingestion_id=f"cm_btc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        custom_metadata={
            "metrics_included": ["price_usd", "active_addresses", "hash_rate"],
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "quality_score": 0.95,
        },
    )
    print("✅ Created Ingestion Request")
    print(f"   - Source: {request.source.value}")
    print(f"   - Data Type: {request.data_type.value}")
    print(f"   - Ingestion ID: {request.ingestion_id}")
    print(f"   - File Format: {request.file_format}")
    print(f"   - Compression: {request.compression}")

    # 5. Simulate ingestion process
    print("\n[Step 5] Simulating Bronze Ingestion...")
    print("   - Validating request structure...")
    assert request.source == DataVenue.COINMETRICS
    assert request.data_type == MarketDataType.ONCHAIN
    assert request.instrument is not None
    print("   ✅ Request validation passed")

    print("   - Calculating data partitioning...")
    partition = (
        f"{request.source.value}/{request.data_type.value}/{request.symbol}/2026-01-18"
    )
    s3_key = f"{partition}/data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    print(f"   ✅ Partition: {partition}")
    print(f"   ✅ S3 Key: {s3_key}")

    print("   - Serializing data to JSON...")
    json_data = json.dumps(raw_data, default=str).encode("utf-8")
    print(f"   ✅ Data size: {len(json_data)} bytes")

    print("   - Writing to MinIO...")
    # Simulate MinIO write
    mock_minio_client.put_object(
        bucket_name="bronze",
        object_name=s3_key,
        data=json_data,
        length=len(json_data),
        metadata={
            "source": request.source.value,
            "data_type": request.data_type.value,
            "ingestion_id": request.ingestion_id,
        },
    )
    print("   ✅ Written to MinIO bucket: bronze")

    print("   - Creating checkpoint...")
    checkpoint_result = await mock_checkpoint_manager.save_checkpoint(
        {
            "source": request.source,
            "data_type": request.data_type,
            "symbol": request.symbol,
            "partition_path": partition,
            "last_success_key": s3_key,
            "total_files_written": 1,
            "total_bytes_written": len(json_data),
        }
    )
    print("   ✅ Checkpoint saved")

    # 6. Display results
    print("\n" + "=" * 80)
    print("📊 INGESTION RESULTS")
    print("=" * 80)
    result = {
        "success": True,
        "ingestion_id": request.ingestion_id,
        "instrument": {
            "id": btc_instrument.instrument_id,
            "asset_class": str(btc_instrument.asset_class),
            "market_type": str(btc_instrument.market_type),
            "venue": btc_instrument.venue.value,
            "symbol": f"{btc_instrument.base_asset}/{btc_instrument.quote_asset}",
        },
        "file_result": {
            "s3_key": s3_key,
            "partition": partition,
            "bytes_written": len(json_data),
            "format": request.file_format,
            "compression": request.compression,
        },
        "checkpoint": checkpoint_result,
        "metadata": {
            "rows_ingested": len(raw_data["data"]),
            "date_range": f"{start_date.date()} to {end_date.date()}",
            "quality_score": request.custom_metadata["quality_score"],
        },
    }

    print(f"""
✅ Ingestão Bronze Completed Successfully!

📦 Ingestion Details:
   ID: {result['ingestion_id']}
   Status: {result['success']}

📍 Location:
   S3 Bucket: bronze
   S3 Key: {result['file_result']['s3_key']}
   Partition: {result['file_result']['partition']}

📋 Instrument:
   ID: {result['instrument']['id']}
   Asset Class: {result['instrument']['asset_class']}
   Market Type: {result['instrument']['market_type']}
   Venue: {result['instrument']['venue']}
   Symbol: {result['instrument']['symbol']}

📊 Data:
   Rows Ingested: {result['metadata']['rows_ingested']}
   Date Range: {result['metadata']['date_range']}
   Format: {result['file_result']['format']}
   Compression: {result['file_result']['compression']}
   File Size: {result['file_result']['bytes_written']} bytes
   Quality Score: {result['metadata']['quality_score']}

✅ Checkpoint Status: {result['checkpoint'].get('status', 'saved')}
    """)

    print("=" * 80)
    print("🎉 Complete Bronze Workflow Demonstration Successful!")
    print("=" * 80)

    return result


if __name__ == "__main__":
    result = asyncio.run(demo_main())
