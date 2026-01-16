"""Storage layer migrations and schema versioning.

This directory is reserved for database schema migrations using Alembic.

Current Status: No migrations yet
- Existing database schema is versioned manually in db/schemas/
- All tables are pre-created and stable
- New storage layer repositories work with existing schema

Migration Strategy:
1. Use Alembic for future schema changes
2. Keep migrations organized by phase/feature
3. Always run migrations in staging before production
4. Document breaking changes in MIGRATION_NOTES.md

Future: When Alembic is integrated, migrations will live here with:
- versions/001_initial_schema.py
- versions/002_add_settlement_currency.py
- env.py (Alembic configuration)
- script.py.mako (migration template)
"""
