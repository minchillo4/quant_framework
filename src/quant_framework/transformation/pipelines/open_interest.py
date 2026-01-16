"""Open Interest transformation pipeline.

Orchestrates: OI fetch → normalize → validate → store to market.open_interest
"""

from typing import Any

from quant_framework.transformation.pipelines.base import BaseTransformationPipeline
from quant_framework.transformation.ports import (
    IOpenInterestNormalizer,
    IOpenInterestValidator,
)


class OpenInterestRepository:
    """Placeholder for OpenInterestRepository type hint."""

    pass


class OpenInterestPipeline(BaseTransformationPipeline):
    """Pipeline for Open Interest data transformation.

    Orchestrates:
    1. Normalize: Convert API OI → OpenInterestRecord
    2. Validate: Check amounts, settlement currency, timestamps
    3. Store: Persist to market.open_interest hypertable

    Example:
        ```python
        from transformation.adapters.coinalyze import CoinalyzeOINormalizer
        from transformation.validators import BaseOpenInterestValidator
        from storage.repositories import OpenInterestRepository

        pipeline = OpenInterestPipeline(
            normalizer=CoinalyzeOINormalizer(symbol="BTC/USDT", exchange="binance"),
            validator=BaseOpenInterestValidator(strict_settlement_currency=True),
            repository=OpenInterestRepository(db_pool),
        )

        # Fetch raw OI from Coinalyze
        raw_oi = coinalyze_client.fetch_open_interest("BTCUSDT", "binance")

        # Transform and store
        result = await pipeline.transform_and_store(raw_oi)
        print(f"Stored {result['records_stored']} OI records")
        ```
    """

    def __init__(
        self,
        normalizer: IOpenInterestNormalizer,
        validator: IOpenInterestValidator,
        repository: Any,  # OpenInterestRepository
        discard_invalid: bool = False,
    ):
        """Initialize Open Interest pipeline.

        Args:
            normalizer: OI normalizer (source-specific)
            validator: OI validator
            repository: OpenInterestRepository instance
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
        return "open_interest"
