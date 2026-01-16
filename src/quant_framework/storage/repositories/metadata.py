"""Metadata repository for relational reference data persistence.

Provides CRUD operations for reference data that changes infrequently:
- Instruments: Trading symbols and their properties
- Exchanges: Trading venues
- Markets: Market type definitions

All metadata is used for relational normalization to avoid duplication
across time-series tables and enable efficient joins.

Table Schemas:
  market.instruments:
    - id: SERIAL PRIMARY KEY
    - symbol: VARCHAR NOT NULL UNIQUE
    - asset_type: VARCHAR NOT NULL
    - name: VARCHAR
    - decimals: INTEGER
    - coingecko_id: VARCHAR
    - coinalyze_symbol: VARCHAR
    - ccxt_symbol_binance: VARCHAR
    - is_active: BOOLEAN DEFAULT true
    - is_major: BOOLEAN DEFAULT false
    - created_at: TIMESTAMP
    - updated_at: TIMESTAMP

  market.exchanges:
    - id: SERIAL PRIMARY KEY
    - name: VARCHAR NOT NULL UNIQUE
    - display_name: VARCHAR NOT NULL
    - is_cex: BOOLEAN DEFAULT true
    - is_active: BOOLEAN DEFAULT true
    - country: VARCHAR
    - website: VARCHAR
    - api_documentation: VARCHAR
    - created_at: TIMESTAMP
    - updated_at: TIMESTAMP

  market.market_types:
    - id: SERIAL PRIMARY KEY
    - market_type: VARCHAR NOT NULL UNIQUE
    - display_name: VARCHAR NOT NULL
    - description: TEXT
    - has_oi: BOOLEAN DEFAULT false
    - is_perpetual: BOOLEAN DEFAULT false
    - is_leveraged: BOOLEAN DEFAULT false
    - created_at: TIMESTAMP
    - updated_at: TIMESTAMP
"""

import logging
from datetime import datetime

from quant_framework.infrastructure.database.ports import DatabaseAdapter
from quant_framework.storage.schemas.relational import Exchange, Instrument, Market

logger = logging.getLogger(__name__)


class InstrumentRepository:
    """Repository for trading instrument reference data.

    Provides CRUD operations for instruments (symbols and their properties).
    Single source of truth for symbol normalization across data sources.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize instrument repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("InstrumentRepository initialized")

    async def create(self, instrument: Instrument) -> Instrument:
        """Create new instrument record.

        Args:
            instrument: Instrument data to persist

        Returns:
            Instrument with id populated from database

        Raises:
            UniqueConstraintError: If symbol already exists
        """
        query = """
            INSERT INTO market.instruments
            (symbol, asset_type, name, decimals, coingecko_id,
             coinalyze_symbol, ccxt_symbol_binance, is_active, is_major,
             created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
        """

        try:
            now = datetime.utcnow()
            result = await self.db.fetchval(
                query,
                instrument.symbol,
                instrument.asset_type,
                instrument.name,
                instrument.decimals,
                instrument.coingecko_id,
                instrument.coinalyze_symbol,
                instrument.ccxt_symbol_binance,
                instrument.is_active,
                instrument.is_major,
                now,
                now,
            )

            instrument.id = result
            instrument.created_at = now
            instrument.updated_at = now
            logger.debug(f"✅ Created instrument: {instrument.symbol}")
            return instrument

        except Exception as e:
            logger.error(f"❌ Failed to create instrument: {e}")
            raise

    async def find_by_symbol(self, symbol: str) -> Instrument | None:
        """Get instrument by canonical symbol.

        Args:
            symbol: Trading symbol (e.g., 'BTC', 'ETH')

        Returns:
            Instrument record if exists, None otherwise
        """
        query = """
            SELECT id, symbol, asset_type, name, decimals, coingecko_id,
                   coinalyze_symbol, ccxt_symbol_binance, is_active, is_major,
                   created_at, updated_at
            FROM market.instruments
            WHERE symbol = $1 LIMIT 1
        """

        try:
            row = await self.db.fetchrow(query, symbol)

            if not row:
                logger.debug(f"No instrument found for symbol {symbol}")
                return None

            return Instrument(
                instrument_id=row["id"],
                symbol=row["symbol"],
                asset_type=row["asset_type"],
                name=row["name"],
                decimals=row["decimals"],
                coingecko_id=row["coingecko_id"],
                coinalyze_symbol=row["coinalyze_symbol"],
                ccxt_symbol_binance=row["ccxt_symbol_binance"],
                is_active=row["is_active"],
                is_major=row["is_major"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        except Exception as e:
            logger.error(f"❌ Failed to find instrument: {e}")
            raise

    async def find_all_major(self) -> list[Instrument]:
        """Get all major trading instruments.

        Major instruments are primary assets like BTC, ETH, etc.

        Returns:
            List of major Instrument records
        """
        query = """
            SELECT id, symbol, asset_type, name, decimals, coingecko_id,
                   coinalyze_symbol, ccxt_symbol_binance, is_active, is_major,
                   created_at, updated_at
            FROM market.instruments
            WHERE is_major = true AND is_active = true
            ORDER BY symbol ASC
        """

        try:
            rows = await self.db.fetch(query)

            instruments = [
                Instrument(
                    instrument_id=row["id"],
                    symbol=row["symbol"],
                    asset_type=row["asset_type"],
                    name=row["name"],
                    decimals=row["decimals"],
                    coingecko_id=row["coingecko_id"],
                    coinalyze_symbol=row["coinalyze_symbol"],
                    ccxt_symbol_binance=row["ccxt_symbol_binance"],
                    is_active=row["is_active"],
                    is_major=row["is_major"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(instruments)} major instruments")
            return instruments
        except Exception as e:
            logger.error(f"❌ Failed to find major instruments: {e}")
            raise

    async def update(self, instrument: Instrument) -> None:
        """Update existing instrument record.

        Args:
            instrument: Instrument data to update (must have instrument_id)

        Raises:
            ValueError: If instrument has no id
        """
        if not instrument.instrument_id:
            raise ValueError("Cannot update instrument without id")

        query = """
            UPDATE market.instruments
            SET symbol = $1, asset_type = $2, name = $3, decimals = $4,
                coingecko_id = $5, coinalyze_symbol = $6,
                ccxt_symbol_binance = $7, is_active = $8, is_major = $9,
                updated_at = $10
            WHERE id = $11
        """

        try:
            await self.db.execute(
                query,
                instrument.symbol,
                instrument.asset_type,
                instrument.name,
                instrument.decimals,
                instrument.coingecko_id,
                instrument.coinalyze_symbol,
                instrument.ccxt_symbol_binance,
                instrument.is_active,
                instrument.is_major,
                datetime.utcnow(),
                instrument.instrument_id,
            )
            logger.debug(f"✅ Updated instrument id={instrument.instrument_id}")
        except Exception as e:
            logger.error(f"❌ Failed to update instrument: {e}")
            raise


class ExchangeRepository:
    """Repository for exchange/venue reference data.

    Provides CRUD operations for exchanges where trading occurs.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize exchange repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("ExchangeRepository initialized")

    async def create(self, exchange: Exchange) -> Exchange:
        """Create new exchange record.

        Args:
            exchange: Exchange data to persist

        Returns:
            Exchange with id populated from database

        Raises:
            UniqueConstraintError: If name already exists
        """
        query = """
            INSERT INTO market.exchanges
            (name, display_name, is_cex, is_active, country, website,
             api_documentation, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """

        try:
            now = datetime.utcnow()
            result = await self.db.fetchval(
                query,
                exchange.name,
                exchange.display_name,
                exchange.is_cex,
                exchange.is_active,
                exchange.country,
                exchange.website,
                exchange.api_documentation,
                now,
                now,
            )

            exchange.exchange_id = result
            exchange.created_at = now
            exchange.updated_at = now
            logger.debug(f"✅ Created exchange: {exchange.name}")
            return exchange

        except Exception as e:
            logger.error(f"❌ Failed to create exchange: {e}")
            raise

    async def find_by_name(self, name: str) -> Exchange | None:
        """Get exchange by name.

        Args:
            name: Exchange name (e.g., 'binance', 'coinalyze')

        Returns:
            Exchange record if exists, None otherwise
        """
        query = """
            SELECT id, name, display_name, is_cex, is_active, country,
                   website, api_documentation, created_at, updated_at
            FROM market.exchanges
            WHERE name = $1 LIMIT 1
        """

        try:
            row = await self.db.fetchrow(query, name)

            if not row:
                logger.debug(f"No exchange found with name {name}")
                return None

            return Exchange(
                exchange_id=row["id"],
                name=row["name"],
                display_name=row["display_name"],
                is_cex=row["is_cex"],
                is_active=row["is_active"],
                country=row["country"],
                website=row["website"],
                api_documentation=row["api_documentation"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        except Exception as e:
            logger.error(f"❌ Failed to find exchange: {e}")
            raise

    async def find_all_active(self) -> list[Exchange]:
        """Get all active exchanges.

        Returns:
            List of active Exchange records
        """
        query = """
            SELECT id, name, display_name, is_cex, is_active, country,
                   website, api_documentation, created_at, updated_at
            FROM market.exchanges
            WHERE is_active = true
            ORDER BY name ASC
        """

        try:
            rows = await self.db.fetch(query)

            exchanges = [
                Exchange(
                    exchange_id=row["id"],
                    name=row["name"],
                    display_name=row["display_name"],
                    is_cex=row["is_cex"],
                    is_active=row["is_active"],
                    country=row["country"],
                    website=row["website"],
                    api_documentation=row["api_documentation"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(exchanges)} active exchanges")
            return exchanges
        except Exception as e:
            logger.error(f"❌ Failed to find active exchanges: {e}")
            raise


class MarketTypeRepository:
    """Repository for market type reference data.

    Provides CRUD operations for market type categorization.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize market type repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("MarketTypeRepository initialized")

    async def create(self, market: Market) -> Market:
        """Create new market type record.

        Args:
            market: Market data to persist

        Returns:
            Market with id populated from database

        Raises:
            UniqueConstraintError: If market_type already exists
        """
        query = """
            INSERT INTO market.market_types
            (market_type, display_name, description, has_oi, is_perpetual,
             is_leveraged, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
        """

        try:
            now = datetime.utcnow()
            result = await self.db.fetchval(
                query,
                market.market_type,
                market.display_name,
                market.description,
                market.has_oi,
                market.is_perpetual,
                market.is_leveraged,
                now,
                now,
            )

            market.market_type_id = result
            market.created_at = now
            market.updated_at = now
            logger.debug(f"✅ Created market type: {market.market_type}")
            return market

        except Exception as e:
            logger.error(f"❌ Failed to create market type: {e}")
            raise

    async def find_by_type(self, market_type: str) -> Market | None:
        """Get market type by identifier.

        Args:
            market_type: Market type identifier (e.g., 'spot', 'linear_perpetual')

        Returns:
            Market record if exists, None otherwise
        """
        query = """
            SELECT id, market_type, display_name, description, has_oi,
                   is_perpetual, is_leveraged, created_at, updated_at
            FROM market.market_types
            WHERE market_type = $1 LIMIT 1
        """

        try:
            row = await self.db.fetchrow(query, market_type)

            if not row:
                logger.debug(f"No market type found for {market_type}")
                return None

            return Market(
                market_type_id=row["id"],
                market_type=row["market_type"],
                display_name=row["display_name"],
                description=row["description"],
                has_oi=row["has_oi"],
                is_perpetual=row["is_perpetual"],
                is_leveraged=row["is_leveraged"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        except Exception as e:
            logger.error(f"❌ Failed to find market type: {e}")
            raise

    async def find_all_with_oi(self) -> list[Market]:
        """Get all market types that support open interest.

        Returns:
            List of Market records where has_oi = true
        """
        query = """
            SELECT id, market_type, display_name, description, has_oi,
                   is_perpetual, is_leveraged, created_at, updated_at
            FROM market.market_types
            WHERE has_oi = true
            ORDER BY market_type ASC
        """

        try:
            rows = await self.db.fetch(query)

            markets = [
                Market(
                    market_type_id=row["id"],
                    market_type=row["market_type"],
                    display_name=row["display_name"],
                    description=row["description"],
                    has_oi=row["has_oi"],
                    is_perpetual=row["is_perpetual"],
                    is_leveraged=row["is_leveraged"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(markets)} market types with OI")
            return markets
        except Exception as e:
            logger.error(f"❌ Failed to find market types with OI: {e}")
            raise
