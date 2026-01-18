# src/quant_framework/storage/bronze/bronze_registry.py
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from quant_framework.shared.models.enums import DataVenue, MarketDataType
from quant_framework.shared.models.instruments import Instrument

from .checkpoint import BronzeCheckpoint
from .metadata import BronzeFileMetadata
from .raw_writer import RawBronzeWriter


@dataclass
class BronzeIngestionRequest:
    """Request para ingestão bronze - usando SEUS models"""

    # Identificação
    source: DataVenue
    data_type: MarketDataType
    instrument: Instrument | None = None

    # Dados
    raw_data: Any = None
    raw_file_path: str | None = None  # Alternativa: path para arquivo local

    # Configuração
    file_format: str = "raw_json"
    compression: str = "none"

    # Metadados
    ingestion_id: str | None = None
    custom_metadata: dict[str, Any] = None

    # Controle
    create_checkpoint: bool = True
    validate_schema: bool = False  # Apenas validação básica, não conteúdo

    def __post_init__(self):
        if self.custom_metadata is None:
            self.custom_metadata = {}

        # Gera ID se não fornecido
        if not self.ingestion_id:
            self.ingestion_id = f"{self.source.value}_{self.data_type.value}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    @property
    def symbol(self) -> str:
        """Extrai símbolo do instrumento ou usa default"""
        if self.instrument and self.instrument.base_asset:
            return self.instrument.base_asset
        return "UNKNOWN"


class BronzeRegistry:
    """Registry central para operações bronze"""

    def __init__(self, minio_client, checkpoint_manager):
        self.minio_client = minio_client
        self.checkpoint_manager = checkpoint_manager
        self.raw_writer = RawBronzeWriter(minio_client)

        # Cache de instrumentos ativos
        self._instruments_cache: dict[str, Instrument] = {}

    async def register_instrument(self, instrument: Instrument):
        """Registra instrumento no cache do registry"""
        key = f"{instrument.venue.value}_{instrument.base_asset}"
        self._instruments_cache[key] = instrument

    async def get_or_create_instrument(
        self, venue: DataVenue, base_asset: str, quote_asset: str = "USD"
    ) -> Instrument:
        """Obtém ou cria instrumento"""
        key = f"{venue.value}_{base_asset}"

        if key in self._instruments_cache:
            return self._instruments_cache[key]

        # Cria novo instrumento (simplificado)
        instrument = Instrument(
            instrument_id=f"{base_asset}_{venue.value}",
            asset_class="crypto",  # Supondo crypto
            market_type="spot",  # Default
            venue=venue,
            base_asset=base_asset,
            quote_asset=quote_asset,
            wrapper="unknown",
            is_active=True,
        )

        self._instruments_cache[key] = instrument
        return instrument

    async def ingest_raw_data(self, request: BronzeIngestionRequest) -> dict[str, Any]:
        """Ingere dados raw no bronze - método principal"""

        # 1. Prepara instrumento se não fornecido
        if not request.instrument:
            request.instrument = await self.get_or_create_instrument(
                venue=request.source, base_asset=request.symbol
            )

        # 2. Determina timestamp dos dados
        data_timestamp = datetime.utcnow()

        # 3. Escreve dados brutos
        result = await self.raw_writer.write_raw(
            source=request.source.value,
            data_type=request.data_type.value,
            symbol=request.symbol,
            raw_data=request.raw_data,
            timestamp=data_timestamp,
            file_format=request.file_format,
            compression=request.compression,
        )

        # 4. Cria metadados do arquivo
        file_metadata = BronzeFileMetadata(
            file_key=result["s3_key"],
            file_size_bytes=result["bytes_written"],
            file_format=request.file_format,
            compression=request.compression,
            source=request.source,
            data_type=request.data_type,
            symbol=request.symbol,
            data_timestamp=data_timestamp,
            ingestion_id=request.ingestion_id,
            custom_metadata=request.custom_metadata,
        )

        # 5. Salva metadados no MinIO (opcional)
        await self._save_file_metadata(file_metadata)

        # 6. Atualiza checkpoint se solicitado
        checkpoint_result = None
        if request.create_checkpoint:
            checkpoint = BronzeCheckpoint(
                source=request.source,
                data_type=request.data_type,
                symbol=request.symbol,
                instrument=request.instrument,
                partition_path=result["partition"],
                last_success_key=result["s3_key"],
                last_data_timestamp=data_timestamp,
                metadata={
                    "ingestion_id": request.ingestion_id,
                    "file_metadata_key": file_metadata.file_key,
                    **request.custom_metadata,
                },
                total_files_written=1,
                total_bytes_written=result["bytes_written"],
            )

            checkpoint_result = await self.checkpoint_manager.save_checkpoint(
                checkpoint
            )

        # 7. Retorna resultado completo
        return {
            "success": True,
            "file_result": result,
            "file_metadata": file_metadata.model_dump(),
            "checkpoint": checkpoint_result,
            "instrument": request.instrument.model_dump()
            if request.instrument
            else None,
            "ingestion_id": request.ingestion_id,
        }
