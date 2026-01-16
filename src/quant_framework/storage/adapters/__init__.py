"""Storage adapters for different backend implementations.

Provides abstraction over storage backends:
- PostgreSQL/TimescaleDB: Time-series and relational data (via asyncpg)
- MinIO/S3: Bronze layer Parquet files
"""

from .postgres import PostgresAdapter

__all__ = [
    "PostgresAdapter",
]
