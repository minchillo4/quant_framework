"""Data quality validators for normalized records.

Provides:
- BaseOHLCVValidator: Validates OHLCV records (prices, volume, timestamps)
- BaseOpenInterestValidator: Validates OI records (amounts, ratios, currencies)
- BaseTradeValidator: Validates trade records (price, quantity, side)

Validators check data quality without modifying data.
All violations are collected and returned as error messages.
"""

from quant_framework.storage.schemas import (
    OHLCVRecord,
    OpenInterestRecord,
    TradeRecord,
)


class ValidationError(Exception):
    """Raised when validation of record fails."""

    pass


class BaseOHLCVValidator:
    """Base validator for OHLCV records.

    Checks:
    - Price relationships: low <= open, close <= high (typically)
    - High >= Low always
    - Volume must be positive or zero (depends on exchange)
    - Timestamp must be valid Unix milliseconds
    - No required fields are null
    """

    def __init__(self, allow_zero_volume: bool = False):
        """Initialize validator.

        Args:
            allow_zero_volume: If True, volume of 0 is allowed
        """
        self.allow_zero_volume = allow_zero_volume

    async def validate_single(self, record: OHLCVRecord) -> tuple[bool, list[str]]:
        """Validate single OHLCV record.

        Args:
            record: OHLCVRecord to validate

        Returns:
            (is_valid, errors) - errors is empty if valid
        """
        errors = []

        # Validate required fields are not null
        if record.open is None:
            errors.append("open price is null")
        if record.high is None:
            errors.append("high price is null")
        if record.low is None:
            errors.append("low price is null")
        if record.close is None:
            errors.append("close price is null")
        if record.volume is None:
            errors.append("volume is null")
        if record.timestamp is None:
            errors.append("timestamp is null")

        # Skip further checks if critical fields are null
        if errors:
            return (False, errors)

        # Validate price relationships
        if record.high < record.low:
            errors.append(f"high ({record.high}) < low ({record.low})")

        # Validate that all prices are positive (or zero for some cases)
        for field, value in [
            ("open", record.open),
            ("high", record.high),
            ("low", record.low),
            ("close", record.close),
        ]:
            if value < 0:
                errors.append(f"{field} price is negative: {value}")

        # Validate volume
        if record.volume < 0:
            errors.append(f"volume is negative: {record.volume}")
        elif record.volume == 0 and not self.allow_zero_volume:
            errors.append("volume is zero")

        # Validate timestamp is reasonable (after year 2010, before year 2100)
        # Unix milliseconds: 2010-01-01 = 1262304000000, 2100-01-01 = 4102444800000
        if record.timestamp < 1262304000000 or record.timestamp > 4102444800000:
            errors.append(
                f"timestamp {record.timestamp} is outside valid range "
                "(1262304000000 to 4102444800000)"
            )

        return (len(errors) == 0, errors)

    async def validate_batch(
        self, records: list[OHLCVRecord]
    ) -> tuple[list[bool], list[list[str]]]:
        """Validate batch of OHLCV records.

        Args:
            records: List of OHLCVRecord objects

        Returns:
            (validities, error_lists) - Parallel lists of validation results
        """
        validities = []
        error_lists = []

        for record in records:
            is_valid, errors = await self.validate_single(record)
            validities.append(is_valid)
            error_lists.append(errors)

        return (validities, error_lists)


class BaseOpenInterestValidator:
    """Base validator for Open Interest records.

    Checks:
    - open_interest_usd must be positive
    - If long_oi_usd and short_oi_usd present, both must be positive
    - If long_short_ratio present, must be positive
    - settlement_currency if present must be 3-4 char code (USDT, USDC, BNB, etc.)
    - Timestamp must be valid Unix milliseconds
    - No required fields are null
    """

    VALID_SETTLEMENT_CURRENCIES = {
        "USDT",
        "USDC",
        "BUSD",
        "USDK",
        "TUSD",
        "DAI",
        "USDP",
        "FRAX",
        "BNB",
        "ETH",
        "BTC",
        "XRP",
        "SOL",
        "ADA",
        "DOGE",
        "XLM",
        "AVAX",
        "FTT",
        "LINK",
        "UNI",
        "MATIC",
        "ARB",
        "OP",
        "GMT",
    }

    def __init__(self, strict_settlement_currency: bool = False):
        """Initialize validator.

        Args:
            strict_settlement_currency: If True, only allow known settlement currencies
        """
        self.strict_settlement_currency = strict_settlement_currency

    async def validate_single(
        self, record: OpenInterestRecord
    ) -> tuple[bool, list[str]]:
        """Validate single OI record.

        Args:
            record: OpenInterestRecord to validate

        Returns:
            (is_valid, errors) - errors is empty if valid
        """
        errors = []

        # Validate required fields
        if record.open_interest_usd is None:
            errors.append("open_interest_usd is null")
        if record.timestamp is None:
            errors.append("timestamp is null")

        # Skip further checks if critical fields are null
        if errors:
            return (False, errors)

        # Validate open_interest_usd is positive
        if record.open_interest_usd <= 0:
            errors.append(
                f"open_interest_usd must be positive, got {record.open_interest_usd}"
            )

        # Validate optional long/short OI fields
        if record.long_oi_usd is not None:
            if record.long_oi_usd < 0:
                errors.append(f"long_oi_usd is negative: {record.long_oi_usd}")
        if record.short_oi_usd is not None:
            if record.short_oi_usd < 0:
                errors.append(f"short_oi_usd is negative: {record.short_oi_usd}")

        # Validate long/short ratio if present
        if record.long_short_ratio is not None:
            if record.long_short_ratio <= 0:
                errors.append(
                    f"long_short_ratio must be positive, got {record.long_short_ratio}"
                )

        # Validate settlement_currency if present
        if record.settlement_currency is not None:
            currency = record.settlement_currency.upper()
            if len(currency) < 3 or len(currency) > 4:
                errors.append(
                    f"settlement_currency must be 3-4 chars, got {record.settlement_currency}"
                )
            elif (
                self.strict_settlement_currency
                and currency not in self.VALID_SETTLEMENT_CURRENCIES
            ):
                errors.append(
                    f"settlement_currency '{currency}' is not in known currencies"
                )

        # Validate timestamp
        if record.timestamp < 1262304000000 or record.timestamp > 4102444800000:
            errors.append(
                f"timestamp {record.timestamp} is outside valid range "
                "(1262304000000 to 4102444800000)"
            )

        return (len(errors) == 0, errors)

    async def validate_batch(
        self, records: list[OpenInterestRecord]
    ) -> tuple[list[bool], list[list[str]]]:
        """Validate batch of OI records.

        Args:
            records: List of OpenInterestRecord objects

        Returns:
            (validities, error_lists)
        """
        validities = []
        error_lists = []

        for record in records:
            is_valid, errors = await self.validate_single(record)
            validities.append(is_valid)
            error_lists.append(errors)

        return (validities, error_lists)


class BaseTradeValidator:
    """Base validator for Trade records.

    Checks:
    - price must be positive
    - quantity must be positive
    - side must be 'buy' or 'sell'
    - timestamp must be valid Unix milliseconds
    - No required fields are null
    """

    def __init__(self, allow_zero_quantity: bool = False):
        """Initialize validator.

        Args:
            allow_zero_quantity: If True, quantity of 0 is allowed
        """
        self.allow_zero_quantity = allow_zero_quantity

    async def validate_single(self, record: TradeRecord) -> tuple[bool, list[str]]:
        """Validate single trade record.

        Args:
            record: TradeRecord to validate

        Returns:
            (is_valid, errors)
        """
        errors = []

        # Validate required fields
        if record.price is None:
            errors.append("price is null")
        if record.quantity is None:
            errors.append("quantity is null")
        if record.side is None:
            errors.append("side is null")
        if record.timestamp is None:
            errors.append("timestamp is null")

        # Skip further checks if critical fields are null
        if errors:
            return (False, errors)

        # Validate price
        if record.price <= 0:
            errors.append(f"price must be positive, got {record.price}")

        # Validate quantity
        if record.quantity < 0:
            errors.append(f"quantity is negative: {record.quantity}")
        elif record.quantity == 0 and not self.allow_zero_quantity:
            errors.append("quantity is zero")

        # Validate side
        if record.side not in ("buy", "sell"):
            errors.append(f"side must be 'buy' or 'sell', got '{record.side}'")

        # Validate timestamp
        if record.timestamp < 1262304000000 or record.timestamp > 4102444800000:
            errors.append(
                f"timestamp {record.timestamp} is outside valid range "
                "(1262304000000 to 4102444800000)"
            )

        return (len(errors) == 0, errors)

    async def validate_batch(
        self, records: list[TradeRecord]
    ) -> tuple[list[bool], list[list[str]]]:
        """Validate batch of trade records.

        Args:
            records: List of TradeRecord objects

        Returns:
            (validities, error_lists)
        """
        validities = []
        error_lists = []

        for record in records:
            is_valid, errors = await self.validate_single(record)
            validities.append(is_valid)
            error_lists.append(errors)

        return (validities, error_lists)
