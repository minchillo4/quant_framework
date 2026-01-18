# src/quant_framework/storage/bronze/metadata/bronze_metadata.py
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from quant_framework.shared.models.enums import DataVenue, MarketDataType


class BronzeFileFormat(str, Enum):
    """Formatos de arquivo suportados no Bronze"""

    RAW_JSON = "raw_json"
    RAW_PICKLE = "raw_pickle"
    RAW_MSGPACK = "raw_msgpack"
    RAW_AVRO = "raw_avro"
    RAW_PARQUET = "raw_parquet"
    RAW_CSV = "raw_csv"


class BronzeCompression(str, Enum):
    """Algoritmos de compressão"""

    NONE = "none"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    GZIP = "gzip"
    ZSTD = "zstd"


class BronzeFileMetadata(BaseModel):
    """Metadados de um arquivo Bronze individual"""

    # Identificação
    file_key: str = Field(..., description="S3 key completa")
    file_size_bytes: int = Field(..., ge=0, description="Tamanho em bytes")

    # Formato
    file_format: BronzeFileFormat = Field(
        default=BronzeFileFormat.RAW_JSON, description="Formato do arquivo"
    )
    compression: BronzeCompression = Field(
        default=BronzeCompression.NONE, description="Algoritmo de compressão"
    )

    # Proveniência
    source: DataVenue = Field(..., description="Fonte original")
    data_type: MarketDataType = Field(..., description="Tipo de dado")
    symbol: str = Field(..., description="Símbolo")

    # Timestamps
    data_timestamp: datetime | None = Field(
        None, description="Timestamp dos dados (se conhecido)"
    )
    ingested_at: datetime = Field(
        default_factory=datetime.utcnow, description="Quando foi ingerido"
    )

    # Hash/Checksum
    md5_hash: str | None = Field(None, description="MD5 do conteúdo")
    sha256_hash: str | None = Field(None, description="SHA256 do conteúdo")

    # Linhagem (data lineage)
    ingestion_id: str | None = Field(None, description="ID da execução de ingestão")
    parent_files: list[str] = Field(
        default_factory=list, description="Arquivos pais (se derivado)"
    )

    # Qualidade
    is_valid: bool = Field(default=True, description="Arquivo considerado válido")
    validation_errors: list[str] = Field(
        default_factory=list, description="Erros de validação (se houver)"
    )

    # Custom metadata
    custom_metadata: dict[str, Any] = Field(
        default_factory=dict, description="Metadados customizados da fonte"
    )
