"""OHLCV transformation pipeline.

Orchestrates: OHLCV fetch → normalize → validate → store to market.ohlc
"""

from typing import Any

from quant_framework.transformation.pipelines.base import BaseTransformationPipeline
from quant_framework.transformation.ports import (
    IOHLCVNormalizer,
    IOHLCVValidator,
)


class OHLCVRepository:
    """Placeholder for OHLCVRepository type hint."""

    pass


class OHLCVTransformationPipeline(BaseTransformationPipeline):
    """Pipeline for OHLCV candlestick data transformation.

    Orchestrates:
    1. Normalize: Convert API OHLCV → OHLCVRecord
    2. Validate: Check price relationships, volume, timestamps
    3. Store: Persist to market.ohlc hypertable

    Example:
        ```python
        from transformation.adapters.ccxt import CCXTOHLCVNormalizer
        from transformation.validators import BaseOHLCVValidator
        from storage.repositories import OHLCVRepository

        pipeline = OHLCVTransformationPipeline(
            normalizer=CCXTOHLCVNormalizer(symbol="BTC/USDT", exchange="binance"),
            validator=BaseOHLCVValidator(),
            repository=OHLCVRepository(db_pool),
        )

        # Fetch raw OHLCV from CCXT
        raw_ohlcv = exchange.fetch_ohlcv("BTC/USDT", "1h")

        # Transform and store
        result = await pipeline.transform_and_store(raw_ohlcv)
        print(f"Stored {result['records_stored']} OHLCV records")
        ```
    """

    def __init__(
        self,
        normalizer: IOHLCVNormalizer,
        validator: IOHLCVValidator,
        repository: Any,  # OHLCVRepository
        discard_invalid: bool = False,
    ):
        """Initialize OHLCV pipeline.

        Args:
            normalizer: OHLCV normalizer (source-specific)
            validator: OHLCV validator
            repository: OHLCVRepository instance
            discard_invalid: If True, discard invalid records. If False, raise error.
        """
        super().__init__(
            normalizer=normalizer,
            validator=validator,
            repository=repository,
            discard_invalid=discard_invalid,
        )

    @property
    def data_type(self) -> str:
        """Type of data being processed."""
        return "ohlcv"
