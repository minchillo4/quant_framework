"""Strategy pattern implementations for CoinAlyze.

Replaces if/elif chains with extensible strategy registries.
Adding new exchanges/error types requires registration, not code modification.
"""

from typing import Any, Protocol


class ISymbolFormatter(Protocol):
    """Strategy for formatting symbols per exchange."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for target exchange.

        Args:
            base_asset: Base asset symbol (e.g., "BTC", "ETH")
            market_type: Market type (e.g., "spot", "linear_perpetual")
            settlement: Settlement currency for perpetuals (e.g., "USDT")

        Returns:
            Exchange-specific symbol format
        """
        ...


class SymbolFormatterRegistry:
    """Registry for symbol formatters per exchange.

    Replaces the 7 if/elif branches in CoinAlyzeSymbolRegistry.get_coinalyze_symbol().
    """

    def __init__(self):
        """Initialize empty registry."""
        self._formatters: dict[str, ISymbolFormatter] = {}
        self._default_formatter: ISymbolFormatter | None = None

    def register(self, exchange: str, formatter: ISymbolFormatter) -> None:
        """Register formatter for exchange.

        Args:
            exchange: Exchange name (e.g., "binance", "bybit")
            formatter: Formatter implementation
        """
        self._formatters[exchange] = formatter

    def set_default(self, formatter: ISymbolFormatter) -> None:
        """Set default formatter for unknown exchanges."""
        self._default_formatter = formatter

    def get_formatter(self, exchange: str) -> ISymbolFormatter:
        """Get formatter for exchange.

        Args:
            exchange: Exchange name

        Returns:
            Formatter for exchange, or default if not registered

        Raises:
            KeyError: If exchange not found and no default set
        """
        if exchange in self._formatters:
            return self._formatters[exchange]
        if self._default_formatter is not None:
            return self._default_formatter
        raise KeyError(f"No formatter registered for exchange: {exchange}")


class IErrorHandler(Protocol):
    """Strategy for handling specific error condition."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        """Check if this handler can handle the error.

        Args:
            status_code: HTTP status code
            body: Response body (may be JSON or text)

        Returns:
            True if this handler should process the error
        """
        ...

    def handle(
        self,
        status_code: int,
        body: Any,
        endpoint: str,
    ) -> Exception:
        """Convert error response to exception.

        Args:
            status_code: HTTP status code
            body: Response body
            endpoint: API endpoint

        Returns:
            Domain-specific exception
        """
        ...


class ErrorMapperChain:
    """Chain of Responsibility for error mapping.

    Replaces the 7 if/elif branches in error_mapper.py.
    Each error type has its own handler class.
    """

    def __init__(self):
        """Initialize empty chain."""
        self._handlers: list[IErrorHandler] = []

    def register(self, handler: IErrorHandler) -> None:
        """Register error handler.

        Handlers are tried in registration order; first match wins.

        Args:
            handler: Error handler
        """
        self._handlers.append(handler)

    def map_error(
        self,
        status_code: int,
        body: Any,
        endpoint: str,
    ) -> Exception:
        """Map error response to exception using chain.

        Args:
            status_code: HTTP status code
            body: Response body
            endpoint: API endpoint

        Returns:
            Domain-specific exception
        """
        for handler in self._handlers:
            if handler.can_handle(status_code, body):
                return handler.handle(status_code, body, endpoint)

        # Fallback to generic error
        return Exception(f"HTTP {status_code}: {endpoint}")
