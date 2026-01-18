# scripts/ingestion/get_onchain_integrated.py
import asyncio
from datetime import datetime, timedelta

from quant_framework.shared.models.enums import DataVenue, MarketDataType
from quant_framework.shared.models.instruments import AssetClass, Instrument, MarketType
from quant_framework.storage.bronze.registry import (
    BronzeIngestionRequest,
    BronzeRegistry,
)


async def main():
    """Exemplo completo usando TODOS seus modelos"""

    # 1. Setup do registry
    registry = BronzeRegistry.get_default()

    # 2. Cria instrumento usando SEU modelo
    btc_instrument = Instrument(
        instrument_id="BTC_COINMETRICS",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.SPOT,  # Para dados on-chain
        venue=DataVenue.COINMETRICS,
        base_asset="BTC",
        quote_asset="USD",
        wrapper="coinmetrics_csv",
        is_active=True,
        metadata={
            "description": "Bitcoin on-chain metrics",
            "data_source": "CoinMetrics CSV",
            "update_frequency": "daily",
        },
    )

    # 3. Busca dados (seu código existente)
    from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
        CoinMetricsCsvAdapter,
    )

    adapter = CoinMetricsCsvAdapter()
    await adapter.connect()

    # Busca últimos 7 dias
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)

    raw_data = await adapter.get_onchain_data(
        asset="BTC",
        metrics=["price_usd", "active_addresses", "hash_rate"],
        start_date=start_date,
        end_date=end_date,
    )

    await adapter.close()

    # 4. Cria request de ingestão
    request = BronzeIngestionRequest(
        source=DataVenue.COINMETRICS,
        data_type=MarketDataType.ONCHAIN,
        instrument=btc_instrument,  # ✅ Usando SEU modelo
        raw_data=raw_data,  # ✅ Dados brutos
        file_format="raw_json",
        compression="snappy",
        ingestion_id=f"cm_btc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        custom_metadata={
            "metrics_included": ["price_usd", "active_addresses", "hash_rate"],
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "quality_score": 0.95,  # Metadado customizado
        },
    )

    # 5. Executa ingestão
    result = await registry.ingest_raw_data(request)

    print(f"""
    ✅ Ingestão Bronze Completa:
    
    Arquivo: {result['file_result']['s3_key']}
    Instrumento: {result['instrument']['instrument_id']}
    Checkpoint: {result.get('checkpoint', 'N/A')}
    
    Metadados:
    - Source: {request.source.value}
    - Data Type: {request.data_type.value}
    - File Format: {request.file_format}
    - Compression: {request.compression}
    - Custom Fields: {len(request.custom_metadata)}
    """)

    return result


if __name__ == "__main__":
    asyncio.run(main())
