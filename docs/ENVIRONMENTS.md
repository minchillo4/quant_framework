# Multi-Environment Docker Guide

## What you get
- Clear separation of dev vs prod via Compose overrides and `MNEMO_ENV`.
- Environment variables loaded from `.env.<env>`; hardened secrets expected in prod.
- Dev mounts source for hot reload; prod bakes code into images.
- Ready-to-run commands for bringing stacks up/down, rebuilding, inspecting logs, and running health checks/tests.

## Core files and how they combine
- Base stack: [docker-compose.yml](docker-compose.yml) — defines services, healthchecks, networks, volumes; uses `env_file: .env.${MNEMO_ENV:-dev}`.
- Dev overlay: [docker-compose.dev.yml](docker-compose.dev.yml) — mounts source, faster DAG discovery, debug logging.
- Prod overlay: [docker-compose.prod.yml](docker-compose.prod.yml) — no source mounts, code baked in, slower DAG scans, info logging.
- App images: dev [docker/Dockerfile.dev](docker/Dockerfile.dev) (no code copy, installs deps), prod [docker/Dockerfile](docker/Dockerfile) (copies code, installs deps + lz4).
- Airflow images: dev [infra/airflow/Dockerfile.dev](infra/airflow/Dockerfile.dev) (no code copy), prod [infra/airflow/Dockerfile.prod](infra/airflow/Dockerfile.prod) (code copied into image).
- Helper scripts: [scripts/dev-up.sh](scripts/dev-up.sh), [scripts/dev-down.sh](scripts/dev-down.sh), [scripts/dev-rebuild.sh](scripts/dev-rebuild.sh), [scripts/dev-logs.sh](scripts/dev-logs.sh), [scripts/prod-up.sh](scripts/prod-up.sh), [scripts/prod-down.sh](scripts/prod-down.sh).
- Make targets (wrapper around compose): [Makefile](Makefile) — `make start`, `make stop`, `ENV=prod make start`, etc.

## Environment selection and .env usage
- Selector: `MNEMO_ENV` (defaults to `dev`). Compose loads `env_file: .env.${MNEMO_ENV:-dev}` in services.
- Dev defaults: [.env.dev](.env.dev) — contains Timescale, Airflow, Redis, MinIO, API keys, logging, PYTHONPATH.
- Prod: create `.env.prod` (not in repo). Never commit secrets; rotate and harden values. The prod helper script refuses `CHANGE_ME` placeholders.
- Runtime vs build: `.env.*` is injected at container run-time via compose; Dockerfiles themselves do not consume these unless passed as build args (not used here).

## Dev vs Prod at a glance
- Code handling: Dev mounts `./src`, `./infra/airflow/dags`, configs; Prod copies code into images, only mounts config/logs/plugins.
- Airflow tuning: Dev `AIRFLOW__CORE__MIN_FILE_PROCESS_INTERVAL=10`, `DAG_DISCOVERY_SAFE_MODE=false`, `LOG_LEVEL=DEBUG`; Prod interval=60, safe mode true, `LOG_LEVEL=INFO`.
- DB/Infra: Prod override tightens Timescale/Redis settings; consider managed services in real prod.
- Images: Dev uses `docker/Dockerfile.dev` and `infra/airflow/Dockerfile.dev`; Prod uses `docker/Dockerfile` and `infra/airflow/Dockerfile.prod`.

## Quick starts (recommended)
```bash
# Dev stack (uses .env.dev)
./scripts/dev-up.sh
# Stop dev
./scripts/dev-down.sh
# Rebuild dev images without cache
./scripts/dev-rebuild.sh
# Follow dev logs (all or a service)
./scripts/dev-logs.sh
./scripts/dev-logs.sh airflow-webserver

# Prod stack (expects .env.prod, no mounts)
./scripts/prod-up.sh
./scripts/prod-down.sh
```

## Direct docker compose (manual control)
```bash
# Dev
MNEMO_ENV=dev docker compose --env-file .env.dev \
  -f docker-compose.yml -f docker-compose.dev.yml up -d

MNEMO_ENV=dev docker compose --env-file .env.dev \
  -f docker-compose.yml -f docker-compose.dev.yml down

# Prod
MNEMO_ENV=prod docker compose --env-file .env.prod \
  -f docker-compose.yml -f docker-compose.prod.yml up -d

MNEMO_ENV=prod docker compose --env-file .env.prod \
  -f docker-compose.yml -f docker-compose.prod.yml down

# Rebuild (no cache)
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml build --no-cache
MNEMO_ENV=prod docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml build --no-cache
```

## Make targets (wrapper)
```bash
make start          # dev by default
ENV=prod make start # prod
make stop
make rebuild        # no-cache build for current ENV
make logs SERVICE=airflow-webserver
make shell SERVICE=mnemo_quant
make check-config   # verifies config loads inside mnemo_quant
```

## Service endpoints (dev defaults)
- Airflow UI: http://localhost:8080 (user/pass from `.env.dev`, defaults admin/admin)
- API: http://localhost:8000
- Grafana: http://localhost:3000 (admin/admin)
- pgAdmin: http://localhost:5050
- MinIO console: http://localhost:9001 (admin/password123)
- TimescaleDB: localhost:5432 (postgres/changeme123 unless overridden)
- Redis: localhost:6379

## Common operational commands
```bash
# List services and health
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml ps

# Follow logs
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml logs -f --tail=100 airflow-scheduler

# Exec shells
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec mnemo_quant bash
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec quant-airflow-webserver bash

# Stop and remove volumes (data loss!)
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml down -v
```

## Airflow-specific
```bash
# Create/upgrade DB, create user (normally done by airflow-init)
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml run --rm airflow-init

# List DAGs / trigger DAG / task logs
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec airflow-webserver airflow dags list
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec airflow-webserver airflow dags trigger coinalyze_ohlc_backfill
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-webserver

# Web UI healthcheck (already configured in compose)
curl -f http://localhost:8080/health
```

## MinIO checks
```bash
# Connectivity test script (uses env vars)
uv run python scripts/test_minio_connection.py

# Via compose service logs
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml logs -f minio
```

## TimescaleDB / Redis quick checks
```bash
# psql inside container
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec timescaledb psql -U postgres -d mnemo_quant -c "\dt"

# Redis ping
MNEMO_ENV=dev docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml exec redis redis-cli ping
```

## Validation and tests
```bash
# DAG validation (from repo root)
uv run python scripts/validate_coinalyze_dags.py

# MinIO connection test
uv run python scripts/test_minio_connection.py

# Schema-only test
uv run pytest tests/airflow/test_bronze_writer.py::TestBronzeWriterSchemaValidation -v

# Full tests (requires services up)
uv run pytest tests/airflow/test_bronze_writer.py -v

# Validate environment config inside container
make validate-env
```

## Pitfalls and tips
- Ensure `.env.dev` exists; `.env.prod` must be created with real secrets (not committed). `prod-up` refuses `CHANGE_ME` placeholders.
- Dev mounts source; changes are instant. Prod bakes code; rebuild images after code changes.
- Airflow in dev scans DAGs faster; in prod scans slower and safer. If DAGs don’t appear in prod, confirm images were rebuilt and restarted.
- `MNEMO_ENV` controls which env file compose loads; keep it consistent across commands.
- For clean rebuilds: `down -v` will drop DB/Redis/MinIO data. Use cautiously.
- Consider switching to managed DB/Redis in real production; update compose or use external services.

## Minimal prod checklist
- Create `.env.prod` with strong secrets, real endpoints, and SSL settings.
- Rebuild images with prod overrides.
- Backups configured for Timescale/MinIO; monitoring/alerting enabled.
- Resource limits and firewalls in place; consider CeleryExecutor if scaling Airflow.
