"""Port/Protocol definitions for transformation layer.

Defines abstract interfaces for:
- Normalizers: Convert API responses to storage schemas
- Validators: Data quality and integrity checks
- Pipelines: Orchestrate fetch → normalize → validate → persist

All ports use Protocol-based structural typing for maximum flexibility.
"""

from typing import Any, Protocol

from quant_framework.storage.schemas import OHLCVRecord, OpenInterestRecord, TradeRecord


class IDataNormalizer(Protocol):
    """Abstract normalizer for converting API responses to storage schemas.

    Responsibility: Transform raw API data into standardized schema objects.
    Does NOT validate or persist data.
    """

    async def normalize(self, raw_data: Any) -> Any:
        """Normalize raw API response to schema object.

        Args:
            raw_data: Raw API response (dict, list, or other format)

        Returns:
            Normalized schema object (OHLCVRecord, OpenInterestRecord, etc.)

        Raises:
            NormalizationError: If raw data cannot be normalized
        """
        ...


class IOHLCVNormalizer(IDataNormalizer, Protocol):
    """Normalizer specifically for OHLCV candlestick data.

    Converts API OHLCV response → OHLCVRecord.
    """

    async def normalize_single(self, raw_ohlcv: dict) -> OHLCVRecord:
        """Normalize single OHLCV record from API.

        Args:
            raw_ohlcv: Single OHLCV record from API (varies by source)

        Returns:
            OHLCVRecord with validated fields
        """
        ...

    async def normalize_batch(self, raw_ohlcvs: list[dict]) -> list[OHLCVRecord]:
        """Normalize batch of OHLCV records.

        Args:
            raw_ohlcvs: List of raw OHLCV records from API

        Returns:
            List of OHLCVRecord objects
        """
        ...


class IOpenInterestNormalizer(IDataNormalizer, Protocol):
    """Normalizer specifically for open interest data.

    Converts API OI response → OpenInterestRecord.
    """

    async def normalize_single(self, raw_oi: dict) -> OpenInterestRecord:
        """Normalize single OI record from API.

        Args:
            raw_oi: Single OI record from API (varies by source)

        Returns:
            OpenInterestRecord with validated fields
        """
        ...

    async def normalize_batch(self, raw_ois: list[dict]) -> list[OpenInterestRecord]:
        """Normalize batch of OI records.

        Args:
            raw_ois: List of raw OI records from API

        Returns:
            List of OpenInterestRecord objects
        """
        ...


class ITradeNormalizer(IDataNormalizer, Protocol):
    """Normalizer specifically for trade data.

    Converts API trade response → TradeRecord.
    """

    async def normalize_single(self, raw_trade: dict) -> TradeRecord:
        """Normalize single trade record from API.

        Args:
            raw_trade: Single trade record from API

        Returns:
            TradeRecord with validated fields
        """
        ...

    async def normalize_batch(self, raw_trades: list[dict]) -> list[TradeRecord]:
        """Normalize batch of trade records.

        Args:
            raw_trades: List of raw trade records from API

        Returns:
            List of TradeRecord objects
        """
        ...


class IDataValidator(Protocol):
    """Abstract validator for data quality checks.

    Responsibility: Validate normalized records before persistence.
    Does NOT modify data, only checks quality.
    """

    async def validate(self, record: Any) -> tuple[bool, list[str]]:
        """Validate a record and return quality assessment.

        Args:
            record: Normalized record to validate (OHLCVRecord, etc.)

        Returns:
            (is_valid: bool, errors: list[str])
            - is_valid: True if record passes all quality checks
            - errors: List of validation error messages (empty if valid)

        Raises:
            ValidationError: On unexpected validation failures
        """
        ...


class IOHLCVValidator(IDataValidator, Protocol):
    """Validator for OHLCV records.

    Checks:
    - Price relationships: open, high >= low, close in [low, high]
    - Volume: must be positive
    - Timestamp: must be valid UTC datetime
    - Fields: all required fields present and non-null
    """

    async def validate_single(self, record: OHLCVRecord) -> tuple[bool, list[str]]:
        """Validate single OHLCV record.

        Args:
            record: OHLCVRecord to validate

        Returns:
            (is_valid, errors)
        """
        ...

    async def validate_batch(
        self, records: list[OHLCVRecord]
    ) -> tuple[list[bool], list[list[str]]]:
        """Validate batch of OHLCV records.

        Args:
            records: List of OHLCVRecord objects

        Returns:
            (validities, error_lists) - Parallel lists of validation results
        """
        ...


class IOpenInterestValidator(IDataValidator, Protocol):
    """Validator for OpenInterest records.

    Checks:
    - open_interest_usd: must be positive
    - long_oi_usd, short_oi_usd: if present, both must be positive
    - long_short_ratio: if present, must be positive
    - settlement_currency: if present, must be 3-4 char code (USDT, USDC, etc.)
    - Timestamp: must be valid UTC datetime
    """

    async def validate_single(
        self, record: OpenInterestRecord
    ) -> tuple[bool, list[str]]:
        """Validate single OI record.

        Args:
            record: OpenInterestRecord to validate

        Returns:
            (is_valid, errors)
        """
        ...

    async def validate_batch(
        self, records: list[OpenInterestRecord]
    ) -> tuple[list[bool], list[list[str]]]:
        """Validate batch of OI records.

        Args:
            records: List of OpenInterestRecord objects

        Returns:
            (validities, error_lists)
        """
        ...


class ITransformationPipeline(Protocol):
    """End-to-end data transformation pipeline.

    Orchestrates: Fetch → Normalize → Validate → Persist

    Responsibility: Coordinate the flow and handle errors/logging.
    """

    async def transform_and_store(
        self,
        raw_data: Any,
        data_type: str,
    ) -> dict[str, Any]:
        """Execute complete transformation pipeline.

        Args:
            raw_data: Raw API response to transform
            data_type: Type of data (ohlcv, open_interest, trade)

        Returns:
            {
                "success": bool,
                "records_processed": int,
                "records_stored": int,
                "errors": list[str],
                "duration_seconds": float,
            }

        Raises:
            PipelineError: On fatal pipeline failures
        """
        ...
