# src/quant_framework/storage/bronze/bronze_tasks.py
from datetime import datetime
from typing import Any

from airflow.decorators import task

from quant_framework.shared.models.enums import DataVenue, MarketDataType
from quant_framework.storage.bronze.registry import (
    BronzeIngestionRequest,
    BronzeRegistry,
)


@task
def ingest_coinmetrics_onchain(
    symbols: list[str] = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> dict[str, Any]:
    """Task Airflow para ingerir dados CoinMetrics"""

    async def _ingest_async():
        # 1. Setup
        from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
            CoinMetricsCsvAdapter,
        )

        registry = BronzeRegistry.get_default()

        results = []

        # 2. Para cada símbolo
        for symbol in symbols or ["BTC", "ETH"]:
            try:
                # 3. Busca dados da API
                adapter = CoinMetricsCsvAdapter()
                await adapter.connect()

                raw_data = await adapter.fetch_onchain_data(
                    asset=symbol, start_date=start_date, end_date=end_date
                )

                # 4. Cria request usando SEUS models
                request = BronzeIngestionRequest(
                    source=DataVenue.COINMETRICS,
                    data_type=MarketDataType.ONCHAIN,
                    raw_data=raw_data,
                    ingestion_id=f"coinmetrics_{symbol}_{datetime.utcnow().strftime('%Y%m%d')}",
                    custom_metadata={
                        "start_date": start_date.isoformat() if start_date else None,
                        "end_date": end_date.isoformat() if end_date else None,
                        "adapter_version": "1.0.0",
                    },
                )

                # 5. Ingere no bronze
                result = await registry.ingest_raw_data(request)
                results.append(result)

                await adapter.close()

            except Exception as e:
                logger.error(f"Failed to ingest {symbol}: {e}")
                results.append({"symbol": symbol, "success": False, "error": str(e)})

        return {"results": results}

    # Executa async no Airflow
    import asyncio

    return asyncio.run(_ingest_async())


@task
def ingest_coinalyze_oi(
    symbols: list[str] = None, timeframes: list[str] = None
) -> dict[str, Any]:
    """Task para ingerir Open Interest do Coinalyze"""

    async def _ingest_async():
        from quant_framework.ingestion.adapters.coinalyze_plugin.open_interest_adapter import (
            CoinalyzeOpenInterestAdapter,
        )

        registry = BronzeRegistry.get_default()
        results = []

        for symbol in symbols or ["BTC", "ETH"]:
            try:
                # Cria instrumento apropriado
                from quant_framework.shared.models.enums import AssetClass, MarketType
                from quant_framework.shared.models.instruments import Instrument

                instrument = Instrument(
                    instrument_id=f"{symbol}_PERP",
                    asset_class=AssetClass.CRYPTO,
                    market_type=MarketType.LINEAR_PERPETUAL,
                    venue=DataVenue.COINALYZE,
                    base_asset=symbol,
                    quote_asset="USD",
                    wrapper="coinalyze",
                    is_active=True,
                )

                # Busca dados
                adapter = CoinalyzeOpenInterestAdapter()
                raw_data = await adapter.fetch_open_interest(
                    instrument=instrument, timeframe="1h", limit=1000
                )

                # Cria request
                request = BronzeIngestionRequest(
                    source=DataVenue.COINALYZE,
                    data_type=MarketDataType.OPEN_INTEREST,
                    instrument=instrument,
                    raw_data=raw_data,
                    ingestion_id=f"coinalyze_oi_{symbol}_{datetime.utcnow().strftime('%H%M%S')}",
                )

                # Ingere
                result = await registry.ingest_raw_data(request)
                results.append(result)

            except Exception as e:
                logger.error(f"Failed Coinalyze OI for {symbol}: {e}")
                results.append({"symbol": symbol, "success": False, "error": str(e)})

        return {"results": results}

    return asyncio.run(_ingest_async())
