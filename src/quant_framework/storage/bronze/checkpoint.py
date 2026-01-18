# src/quant_framework/storage/bronze/checkpoints/bronze_checkpoint.py
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from quant_framework.shared.models.enums import DataVenue, MarketDataType
from quant_framework.shared.models.instruments import Instrument


class BronzeCheckpoint(BaseModel):
    """Checkpoint Pydantic model usando SEUS enums"""

    # Identificação usando SEUS enums
    source: DataVenue = Field(..., description="Fonte dos dados")
    data_type: MarketDataType = Field(..., description="Tipo de dado")
    symbol: str = Field(..., description="Símbolo (BTC, ETH)")

    # Referência ao instrumento (opcional, mas recomendado)
    instrument: Instrument | None = Field(
        None, description="Instrumento associado (se disponível)"
    )

    # Localização no MinIO
    partition_path: str = Field(..., description="Caminho da partição S3")
    last_success_key: str = Field(..., description="Última key S3 escrita")

    # Timestamps
    last_data_timestamp: datetime = Field(
        ..., description="Timestamp do último dado processado"
    )
    checkpoint_created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Quando o checkpoint foi criado"
    )
    checkpoint_updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Quando o checkpoint foi atualizado",
    )

    # Metadados
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Metadados customizados"
    )

    # Estatísticas
    total_files_written: int = Field(
        default=0, description="Total de arquivos escritos"
    )
    total_bytes_written: int = Field(
        default=0, ge=0, description="Total de bytes escritos"
    )

    # Status
    status: str = Field(
        default="active", description="Status: active, paused, failed, completed"
    )

    model_config = ConfigDict()

    @field_serializer(
        "last_data_timestamp", "checkpoint_created_at", "checkpoint_updated_at"
    )
    def _serialize_dt(self, value: datetime) -> str:
        return value.isoformat()

    @field_serializer("source", "data_type")
    def _serialize_enums(self, value) -> str:
        return value.value

    def to_minio_dict(self) -> dict[str, Any]:
        """Converte para dict compatível com MinIO (JSON serializable)"""
        data = self.model_dump()

        # Converte enums para strings
        data["source"] = self.source.value
        data["data_type"] = self.data_type.value

        # Converte instrumento se existir
        if self.instrument:
            data["instrument"] = self.instrument.model_dump()

        # Converte timestamps para ISO string
        data["last_data_timestamp"] = self.last_data_timestamp.isoformat()
        data["checkpoint_created_at"] = self.checkpoint_created_at.isoformat()
        data["checkpoint_updated_at"] = self.checkpoint_updated_at.isoformat()

        return data

    @classmethod
    def from_minio_dict(cls, data: dict[str, Any]) -> "BronzeCheckpoint":
        """Cria checkpoint a partir de dict do MinIO"""

        # Converte strings de volta para enums
        if "source" in data:
            data["source"] = DataVenue(data["source"])
        if "data_type" in data:
            data["data_type"] = MarketDataType(data["data_type"])

        # Converte strings para datetime
        datetime_fields = [
            "last_data_timestamp",
            "checkpoint_created_at",
            "checkpoint_updated_at",
        ]

        for field in datetime_fields:
            if field in data and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])

        # Converte dict para Instrument se existir
        if "instrument" in data and isinstance(data["instrument"], dict):
            data["instrument"] = Instrument(**data["instrument"])

        return cls(**data)
