"""
Shared enumerations for the Quant Framework.

Refactored to separate data venues (WHERE) from wrappers (HOW).
Khraisha §2 p 44-54: Multi-asset, multi-venue support requires clear type definitions.
"""

import enum


# ============================================================================
# ASSET & INSTRUMENT CLASSIFICATION
# ============================================================================
class AssetClass(str, enum.Enum):
    """Layer 1-4: Fundamental asset classification."""

    CRYPTO = "crypto"
    EQUITY = "equity"
    FUTURE = "future"
    OPTION = "option"
    FOREX = "forex"
    COMMODITY = "commodity"
    RATES = "rates"
    INDEX = "index"
    OTHER = "other"


class ContractType(str, enum.Enum):
    """Layer 1-4: Contract settlement type - needed for ingestion!
    Khraisha: Preserve ingestion context in canonical forms."""

    LINEAR = "linear"
    INVERSE = "inverse"


class CandleType(str, enum.Enum):
    """Layer 1-4: Price feed types - needed during ingestion
    to distinguish what we're pulling from exchanges."""

    SPOT = "spot"
    FUTURES = "futures"
    MARK = "mark"
    INDEX = "index"
    FUNDING_RATE = "funding_rate"


class PriceType(str, enum.Enum):
    """Layer 1-4: Price reference types - exchanges provide different prices."""

    LAST = "last"
    MARK = "mark"
    INDEX = "index"


class InstrumentType(str, enum.Enum):
    """Layer 1-4: Instrument type classification."""

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    CALL = "call"
    PUT = "put"
    STOCK = "stock"
    PAIR = "pair"


class MarketType(str, enum.Enum):
    """
    Market type classification that maps to exchange market structures.
    Used for routing and configuration.
    """

    SPOT = "spot"
    LINEAR_PERPETUAL = "linear_perpetual"  # USD-margined perpetuals
    INVERSE_PERPETUAL = "inverse_perpetual"  # Coin-margined perpetuals
    LINEAR_FUTURE = "linear_future"  # Dated USD-margined futures
    INVERSE_FUTURE = "inverse_future"  # Dated coin-margined futures
    OPTION = "option"


class OrderType(str, enum.Enum):
    """Layer 1-4: Order type - fundamental market structure."""

    LIMIT = "limit"
    MARKET = "market"
    STOP_LOSS = "stop_loss"
    STOP_LOSS_LIMIT = "stop_loss_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class MarketDataType(str, enum.Enum):
    """Types of market data streams."""

    OHLC = "ohlc"
    OHLCV = "ohlcv"
    OPEN_INTEREST = "open_interest"
    FUNDING_RATE = "funding_rate"
    LIQUIDATION = "liquidation"
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    TICKER = "ticker"
    ONCHAIN = "onchain"


# ============================================================================
# DATA LINEAGE: WHERE (Venue) + HOW (Wrapper)
# ============================================================================
class DataVenue(str, enum.Enum):
    """The actual source where data originates.

    This represents the legal entity/trading venue that generates the data.
    Used to track data provenance and lineage.

    Examples:
    - BINANCE_USDM: Binance USD-M Futures exchange
    - ICE_DATA: ICE Data Services
    - NYSE: New York Stock Exchange
    """

    # ========== CRYPTO EXCHANGES ==========
    # Binance (separate venues for different market types)
    BINANCE = "binance"  # Spot
    BINANCE_USDM = "binance_usdm"  # USD-M Futures (linear perpetual/futures)
    BINANCE_COINM = "binance_coinm"  # COIN-M Futures (inverse perpetual/futures)

    # Other major crypto exchanges
    BYBIT = "bybit"
    GATEIO = "gateio"
    HUOBI = "huobi"
    BITGET = "bitget"
    OKX = "okx"
    KRAKEN = "kraken"
    COINBASE = "coinbase"
    DERIBIT = "deribit"

    # ========== TRADITIONAL EXCHANGES ==========
    NYSE = "nyse"
    NASDAQ = "nasdaq"
    LSE = "lse"  # London Stock Exchange

    # ========== FUTURES & DERIVATIVES EXCHANGES ==========
    CME = "cme"  # Chicago Mercantile Exchange
    CBOE = "cboe"  # Chicago Board Options Exchange
    ICE_FUTURES = "ice_futures"  # ICE Futures (as trading venue)
    EUREX = "eurex"

    # ========== DATA VENDORS (Terminal/Feed Providers) ==========
    BLOOMBERG = "bloomberg"  # Bloomberg Terminal
    REFINITIV = "refinitiv"  # Refinitiv Eikon/Workspace
    ICE_DATA = "ice_data"  # ICE Data Services (as data provider)
    FACTSET = "factset"

    # ========== FOREX VENUES ==========
    OANDA = "oanda"
    FXCM = "fxcm"

    # ========== SPECIAL CASES ==========
    AGGREGATED = "aggregated"  # When wrapper combines multiple venues
    INTERNAL = "internal"  # Internal data generation
    FILE = "file"  # File-based data
    UNKNOWN = "unknown"  # Unknown source

    COINMETRICS = "coinmetrics"
    ALPHA_VANTAGE = "alpha_vantage"  # Alpha Vantage API


class WrapperImplementation(str, enum.Enum):
    """How we access the data (wrapper/client type).

    This identifies the specific library/API/service used to fetch data.
    Separates the access method from the data source.

    Examples:
    - CCXT: Accessing Binance via CCXT library
    - COINALYZE: Accessing multiple exchanges via CoinAlyze API
    - YAHOO_FINANCE: Accessing various venues via Yahoo Finance
    - NONE: Direct native API access
    """

    # ========== CRYPTO WRAPPERS ==========
    CCXT = "ccxt"  # CCXT library (multi-exchange)
    COINALYZE = "coinalyze"  # CoinAlyze API (multi-exchange aggregator)

    # ========== MULTI-ASSET WRAPPERS ==========
    YAHOO_FINANCE = "yahoo_finance"  # Yahoo Finance (stocks, crypto, forex, futures)
    ALPHA_VANTAGE = "alpha_vantage"  # Alpha Vantage API
    POLYGON = "polygon"  # Polygon.io (stocks, forex, crypto)
    TWELVE_DATA = "twelve_data"  # Twelve Data API
    FINNHUB = "finnhub"  # Finnhub API

    # ========== BLOCKCHAIN DATA WRAPPERS ==========
    ETHERSCAN = "etherscan"  # Etherscan API
    ALCHEMY = "alchemy"  # Alchemy API
    INFURA = "infura"  # Infura API
    THEGRAPH = "thegraph"  # The Graph Protocol

    # ========== TRADITIONAL FINANCE WRAPPERS ==========
    IB_INSYNC = "ib_insync"  # Interactive Brokers wrapper
    ALPACA = "alpaca"  # Alpaca trading API
    ROBINHOOD = "robinhood"  # Robinhood API

    # ========== VENDOR SDKs ==========
    BLOOMBERG_SDK = "bloomberg_sdk"  # Bloomberg Terminal API
    REFINITIV_SDK = "refinitiv_sdk"  # Refinitiv SDK

    # ========== NATIVE (No Wrapper) ==========
    NONE = "none"  # Direct API access, no wrapper
    COINMETRICS = "coinmetrics"


class ClientType(str, enum.Enum):
    """Type of client implementation architecture.

    Describes the technical implementation pattern.
    """

    WRAPPER = "wrapper"  # Third-party wrapper library
    NATIVE = "native"  # Direct native API integration
    VENDOR_SDK = "vendor_sdk"  # Official vendor SDK
    FILE_PARSER = "file_parser"  # File-based ingestion


class ConnectionType(str, enum.Enum):
    """Network connection type used by adapter."""

    REST = "rest"
    WEBSOCKET = "websocket"
    FIX = "fix"  # Financial Information eXchange protocol
    GRPC = "grpc"
    SFTP = "sftp"
    FILE = "file"  # Local file system
    DATABASE = "database"  # Direct database connection


# ============================================================================
# PIPELINE & INGESTION
# ============================================================================
class PipelineMode(str, enum.Enum):
    """Execution mode for data ingestion pipeline."""

    BACKFILL = "backfill"
    INCREMENTAL = "incremental"
    STREAMING = "streaming"


class IngestionStatus(str, enum.Enum):
    """Status of data ingestion operation."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some data retrieved, some failed
    SKIPPED = "skipped"


# ============================================================================
# BACKWARD COMPATIBILITY ALIASES (Deprecated)
# ============================================================================
# Note: DataProvider and Exchange are deprecated in favor of DataVenue + WrapperImplementation
# Keep for migration period only


class Exchange(str, enum.Enum):
    """Trading venue identifiers (DEPRECATED - use DataVenue instead)."""

    BINANCE = "binance"
    BINANCEUSDM = "binance_usdm"
    BINANCECOINM = "binance_coinm"
    BYBIT = "bybit"
    GATEIO = "gateio"
    OKX = "okx"
    DERIBIT = "deribit"
    HUOBI = "huobi"
    BITGET = "bitget"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            value_lower = value.lower()
            for member in cls:
                if member.value == value_lower:
                    return member
        return None


# Alias for migration - maps old DataProvider to new DataVenue
DataProvider = DataVenue  # type: ignore
