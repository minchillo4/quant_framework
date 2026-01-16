"""Trade transformation pipeline.

Orchestrates: Trade fetch → normalize → validate → store to market.trades
"""

from typing import Any

from quant_framework.transformation.pipelines.base import BaseTransformationPipeline
from quant_framework.transformation.ports import (
    IDataValidator,
    ITradeNormalizer,
)


class TradeRepository:
    """Placeholder for TradeRepository type hint."""

    pass


class TradeTransformationPipeline(BaseTransformationPipeline):
    """Pipeline for trade data transformation.

    Orchestrates:
    1. Normalize: Convert API trade → TradeRecord
    2. Validate: Check price, quantity, side, timestamps
    3. Store: Persist to market.trades hypertable

    Example:
        ```python
        from transformation.adapters.ccxt import CCXTTradeNormalizer
        from transformation.validators import BaseTradeValidator
        from storage.repositories import TradeRepository

        pipeline = TradeTransformationPipeline(
            normalizer=CCXTTradeNormalizer(symbol="BTC/USDT", exchange="binance"),
            validator=BaseTradeValidator(),
            repository=TradeRepository(db_pool),
        )

        # Fetch raw trades from CCXT
        raw_trades = exchange.fetch_trades("BTC/USDT")

        # Transform and store
        result = await pipeline.transform_and_store(raw_trades)
        print(f"Stored {result['records_stored']} trade records")
        ```
    """

    def __init__(
        self,
        normalizer: ITradeNormalizer,
        validator: IDataValidator,
        repository: Any,  # TradeRepository
        discard_invalid: bool = False,
    ):
        """Initialize Trade pipeline.

        Args:
            normalizer: Trade normalizer (source-specific)
            validator: Trade validator
            repository: TradeRepository instance
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
        return "trade"
