"""
Foundational enums for multi-asset, pluggable ingestion framework.

These enums support asset-agnostic design and decouple the framework from specific
data providers (CCXT, Bloomberg, etc.).
"""

from enum import Enum


class DataProvider(str, Enum):
    """
    Multi-asset data provider enum.

    Replaces the crypto-specific Exchange enum to support equities, commodities,
    forex, and other asset classes beyond cryptocurrency.
    """

    # Cryptocurrency Exchanges
    BINANCE = "binance"
    BYBIT = "bybit"
    GATEIO = "gateio"
    HUOBI = "huobi"
    BITGET = "bitget"
    OKX = "okx"
    KRAKEN = "kraken"
    COINBASE = "coinbase"

    # Traditional Market Data Providers
    ICE = "ice"  # ICE Data Services
    BLOOMBERG = "bloomberg"
    REFINITIV = "refinitiv"
    REUTERS = "reuters"
    FACTSET = "factset"

    # Forex Providers
    OANDA = "oanda"
    FXCM = "fxcm"

    # Commodity Providers
    CME = "cme"

    # Generic
    INTERNAL = "internal"  # Internal data generation
    FILE = "file"  # File-based data

    # Onchain
    COINMETRICS = "coinmetrics"


class ClientType(str, Enum):
    """
    Type of client implementation used by adapter.

    Distinguishes between different integration patterns:
    - WRAPPER: Uses third-party wrapper (e.g., CCXT)
    - NATIVE: Direct integration with provider's API
    - VENDOR_SDK: Uses provider's official SDK
    - FILE_PARSER: Reads from files (CSV, Parquet, etc.)
    """

    WRAPPER = "wrapper"  # e.g., CCXT wrapping exchange APIs
    NATIVE = "native"  # Direct REST/WebSocket integration
    VENDOR_SDK = "vendor_sdk"  # Official SDK (e.g., Bloomberg Terminal API)
    FILE_PARSER = "file_parser"  # File-based ingestion


class ConnectionType(str, Enum):
    """
    Network connection type used by adapter.

    Specifies the protocol/mechanism for data retrieval.
    """

    REST = "rest"
    WEBSOCKET = "websocket"
    FIX = "fix"  # Financial Information eXchange protocol
    GRPC = "grpc"
    SFTP = "sftp"
    FILE = "file"  # Local file system
    DATABASE = "database"  # Direct database connection


class PipelineMode(str, Enum):
    """
    Execution mode for data ingestion pipeline.

    Used by orchestration layer to determine behavior:
    - BACKFILL: Historical data fetch with pagination
    - INCREMENTAL: Recent data fetch (e.g., last N candles)
    - STREAMING: Real-time continuous data feed
    """

    BACKFILL = "backfill"
    INCREMENTAL = "incremental"
    STREAMING = "streaming"


class IngestionStatus(str, Enum):
    """Status of data ingestion operation."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some data retrieved, some failed
    SKIPPED = "skipped"
