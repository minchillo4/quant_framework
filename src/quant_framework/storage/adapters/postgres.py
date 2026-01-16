"""PostgreSQL/TimescaleDB adapter for storage layer.

Re-exports the database adapter from infrastructure layer with storage-specific
documentation and convenience methods.

Note: This is primarily a facade that re-exports the infrastructure.database.DatabaseAdapter.
Direct database interaction still goes through the existing infrastructure layer,
but the storage layer provides a clean abstraction boundary.

See infrastructure.database.ports.DatabaseAdapter for the full interface.
"""

from quant_framework.infrastructure.database.ports import DatabaseAdapter

__all__ = ["PostgresAdapter"]

# Type alias for clarity - PostgresAdapter is the same as DatabaseAdapter
# but positioned within the storage layer for architectural clarity
PostgresAdapter = DatabaseAdapter
