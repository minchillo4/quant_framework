
# mnemo-quant

Hedge fund-grade cryptocurrency market data ingestion and analysis platform. Provides real-time and historical OHLCV data collection, open interest tracking, and automated data validation across multiple exchanges and market types.

**Version:** 0.1.0 | **License:** MIT | **Author:** Lucas Minchillo

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
  - [Development Setup](#development-setup)
  - [Production Setup](#production-setup)
- [System Architecture](#system-architecture)
- [Services & Ports](#services--ports)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Common Operations](#common-operations)
- [Testing (TODO)](#testing)
- [Development Workflow](#development-workflow)
- [Documentation](#documentation)

---

## Overview

**mnemo-quant** is an enterprise-grade platform for ingesting, processing, and analyzing cryptocurrency market data at scale. It's designed for quantitative trading teams that need reliable, multi-source data pipelines with automated validation and flexible deployment options.

### Core Capabilities

- **Multi-Source Data Ingestion:** Real-time and historical market data from 100+ exchanges via CCXT
- **Multiple Market Types:** Spot, linear perpetuals, inverse perpetuals with configurable symbols and timeframes
- **Open Interest Tracking:** Integrated OI data from specialized providers (Coinalyze, CCXT incremental fetcher)
- **Intelligent Pipelines:** Automated backfill + incremental update systems with dynamic DAG generation
- **Data Validation:** Cross-source validation, quality checks, tolerance thresholds, gap detection
- **Async-First Architecture:** Non-blocking I/O throughout the stack for high throughput
- **Production Ready:** Health checks, circuit breakers, retry logic, comprehensive logging

---

## Key Features

✅ **Flexible Data Ingestion**
- OHLCV data from CCXT (100+ exchanges)
- Open Interest data with multi-source validation
- Configurable timeframes (5m, 15m, 1h, 4h, 1d, etc.)
- Automatic symbol mapping across exchanges

✅ **Scalable Pipeline Architecture**
- Apache Airflow orchestration with automatic DAG generation from YAML config
- Incremental fetching for continuous updates (configurable intervals)
- Backfill system for historical data initialization
- Batched data processing with configurable pool sizes

✅ **Data Quality & Reliability**
- Automated cross-source data validation
- Price tolerance and volume anomaly detection
- Gap detection and alerting
- Built-in retry logic and circuit breaker patterns

✅ **Developer Experience**
- Hot-reload development environment (code changes apply instantly)
- Type-hinted codebase with mypy validation
- Comprehensive test suite (unit, integration, verification)
- Easy container shell access and log inspection

✅ **Production Deployment**
- Dev/Prod environment separation (isolated compose configs)
- Secret management via environment variables
- Health checks on all services
- Structured logging with multiple handlers

---

## Technology Stack

### Core Framework
- **FastAPI** (0.115.0) - REST API framework
- **AsyncPG** (0.29.0) - Async PostgreSQL driver
- **SQLAlchemy** (1.4.24-1.9.x) - ORM with async support
- **Pydantic** (2.9.2) - Data validation

### Data Storage
- **TimescaleDB** (2.21.3) - Time-series PostgreSQL extension
- **Redis** (7.2) - Caching and real-time data
- **MinIO** - S3-compatible object storage for raw data

### Orchestration & Scheduling
- **Apache Airflow** (2.9.0) - Workflow orchestration and DAG scheduling
- **APScheduler** (3.10.4) - Task scheduling

### Data Sources
- **CCXT** (4.4.13) - Unified cryptocurrency exchange API (100+ exchanges)
- **Coinalyze API** - Specialized open interest data provider
- **Async I/O** - aiohttp, websockets for real-time data

### Data Processing
- **Pandas** (≥2.3.3) - Data manipulation
- **PyArrow** (≥22.0.0) - Columnar data format
- **aioboto3** (≥15.5.0) - Async S3/MinIO access

### Development & Quality
- **pytest** (8.3.3) + pytest-asyncio - Testing framework
- **ruff** (0.6.9) - Fast linting
- **black** (24.8.0) - Code formatting
- **isort** (5.13.2) - Import sorting
- **mypy** (1.11.2) - Static type checking
- **pre-commit** (3.8.0) - Git hooks

### Infrastructure
- **Docker & Docker Compose** - Containerization
- **pgAdmin** (7) - PostgreSQL administration interface

---

## Quick Start

### Prerequisites

- Docker & Docker Compose (v20.10+)
- Python 3.11+ (for local development)
- Git
- Linux/macOS (Windows with WSL2 recommended)

### Development Setup

**1. Clone the repository**
```bash
git clone <repository-url>
cd mnemo-quant
```

**2. Create environment file**
```bash
# Copy dev environment template (if available)
cp config/env/dev.yaml.example config/env/dev.yaml
# Or create minimal .env file
echo "MNEMO_ENV=dev" > .env
```

**3. Start development environment**
```bash
# Using convenience script (recommended)
./scripts/dev-up.sh

# Or using make
make dev-up

# Or direct docker compose
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

**4. Verify services are running**
```bash
docker-compose ps
# Expected services: mnemo_quant, mnemo_quant_api, airflow-webserver, 
# airflow-scheduler, timescaledb, redis, minio, pgadmin
```

**5. Access development interfaces**
- **Airflow UI:** http://localhost:8080 (credentials: airflow/airflow)
- **pgAdmin:** http://localhost:5050 (credentials: admin@admin.com/admin)
- **MinIO Console:** http://localhost:9001 (credentials: minioadmin/minioadmin)
- **Application logs:** `./scripts/dev-logs.sh` or `docker-compose logs -f mnemo_quant`

**6. Run a quick test**
```bash
# Execute test suite
make test

# Run integration tests (requires running services)
make test-integration

# Shell into main container for REPL access
make shell SERVICE=mnemo_quant
```

### Production Setup

**1. Prepare production environment**
```bash
# Create production env file with production settings
cp config/env/prod.yaml.example config/env/prod.yaml
# Edit prod.yaml with production database credentials, API keys, etc.

# Set environment variable
export MNEMO_ENV=prod
```

**2. Build production images**
```bash
# Docker will use prod Dockerfile variants automatically
./scripts/prod-up.sh

# Or direct docker compose
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

**3. Verify production deployment**
```bash
docker-compose ps
docker-compose logs -f mnemo_quant
```

**4. Access production services**
- **pgAdmin:** http://localhost:5050
- **Application:** Access via container shell or API (when implemented)

---

## System Architecture

### Service Topology

```
┌─────────────────────────────────────────────┐
│  ORCHESTRATION LAYER (Airflow)              │
├─────────────────────────────────────────────┤
│  • airflow-webserver (UI @ :8080)           │
│  • airflow-scheduler (DAG execution)        │
│  • airflow-triggerer (Async events)         │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│  APPLICATION SERVICES LAYER                 │
├─────────────────────────────────────────────┤
│  • mnemo_quant (Main service)               │
│  • mnemo_quant_api (FastAPI @ :8000)        │
│  • ccxt-incremental-fetcher (Data worker)   │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│  DATA & STORAGE LAYER                       │
├─────────────────────────────────────────────┤
│  • timescaledb (@ :5432)                    │
│  • redis (@ :6379)                          │
│  • minio (@ :9000 API, :9001 console)       │
└─────────────────────────────────────────────┘
```

### Data Pipeline Flow

```
External APIs (CCXT, Coinalyze, Exchanges)
        ↓
[Data Adapters] (exchange-specific integrations)
        ↓
[Normalization] (standardize formats)
        ↓
[Validation Layer] (quality checks, tolerance)
        ↓
[Storage]:
    ├─ MinIO (Bronze Layer - raw data archive)
    ├─ TimescaleDB (OHLC/OI tables - indexed queries)
    └─ Redis (Hot cache - real-time API access)
        ↓
[Airflow DAGs] (orchestrated pipelines)
        ↓
[Analysis/Backtesting] (strategy implementation)
```

---

## Services & Ports

| Service | Port(s) | Purpose | Credentials |
|---------|---------|---------|-------------|
| **airflow-webserver** | 8080 | DAG scheduling & monitoring UI | airflow / airflow |
| **mnemo_quant_api** | 8000 | FastAPI application server | (authentication TBD) |
| **timescaledb** | 5432 | Time-series database | postgres / postgres |
| **redis** | 6379 | Cache & session store | (no auth in dev) |
| **minio** | 9000 (API) / 9001 (console) | S3-compatible object storage | minioadmin / minioadmin |
| **pgadmin** | 5050 | PostgreSQL admin UI | admin@admin.com / admin |

---

## Configuration

### Configuration System

mnemo-quant uses a YAML-based configuration system with Python-based validation via Pydantic. Configurations are located in the `config/` directory.

**Environment Variable:** `MNEMO_ENV`
- `dev` (default) - Development settings with verbose logging
- `staging` - Staging environment configuration
- `prod` - Production settings

### Key Configuration Files

| File | Purpose |
|------|---------|
| `config/global.yaml` | Logging, retry, circuit breaker, rate limiting defaults |
| `config/assets.yaml` | Crypto universe (symbols, timeframes, market types) |
| `config/coinalyze.yaml` | Coinalyze API configuration |
| `config/database.yaml` | PostgreSQL connection pool settings |
| `config/redis.yaml` | Redis configuration |
| `config/dags/dag_configs.yaml` | Airflow DAG definitions (source-agnostic) |
| `config/bronze/sources.yaml` | MinIO bronze layer structure |
| `config/env/{dev,staging,prod}.yaml` | Environment-specific overrides |

### Loading Configuration

```python
from quant_framework.config import settings

# Access configuration
db_url = settings.database.url
redis_url = settings.redis.url
assets = settings.assets.crypto_universe
```

### Adding New Configuration

1. Define your config structure in `config/settings.py` using Pydantic models
2. Create corresponding YAML file in `config/`
3. Load via `settings.<section>`
4. Override via environment-specific files in `config/env/`

---

## Project Structure

```
mnemo-quant/
├── config/                    # Configuration files (YAML + Python)
│   ├── settings.py           # Pydantic config validation
│   ├── global.yaml           # Platform defaults
│   ├── assets.yaml           # Symbols, timeframes, markets
│   ├── database.yaml         # PostgreSQL settings
│   ├── redis.yaml            # Cache configuration
│   ├── coinalyze.yaml        # OI provider config
│   ├── dags/                 # Airflow DAG definitions
│   ├── env/                  # Environment-specific overrides
│   └── ...
├── src/quant_framework/       # Main application package
│   ├── __init__.py
│   ├── config/               # Config module
│   ├── ingestion/            # Data adapters & normalizers
│   │   ├── ccxt/             # CCXT exchange adapter
│   │   ├── coinalyze/        # Coinalyze OI adapter
│   │   └── ...
│   ├── infrastructure/       # Storage, database, checkpoints
│   ├── shared/               # Common models (enums, instruments)
│   ├── legacy/               # Deprecated code (to be removed)
│   └── examples/             # Usage examples
├── infra/
│   ├── airflow/              # Airflow configuration
│   │   ├── dags/             # DAG definitions
│   │   ├── plugins/          # Custom Airflow operators
│   │   ├── Dockerfile.*      # Airflow images
│   │   └── scripts/          # Entrypoint scripts
│   ├── logging/              # Logging infrastructure (Loki)
│   └── monitoring/           # Monitoring config (Prometheus, Grafana)
├── db/
│   ├── init/                 # Database initialization
│   │   ├── 00-init.databases.sql
│   │   ├── schemas/          # Table definitions
│   │   └── ...
├── docker/                    # Application Dockerfiles
│   ├── Dockerfile
│   ├── Dockerfile.dev
│   └── Dockerfile.prod
├── docs/                      # Detailed documentation
│   ├── DOCKER_SETUP.md       # Docker setup guide
│   ├── ENVIRONMENTS.md       # Environment configuration
│   └── ...
├── scripts/                   # Operational scripts
│   ├── dev-up.sh             # Start dev environment
│   ├── dev-down.sh           # Stop dev environment
│   ├── prod-up.sh            # Start prod environment
│   ├── prod-down.sh          # Stop prod environment
│   ├── backfill_*.sh         # Backfill helpers
│   └── ...
├── tests/                     # Test suite
│   ├── test_*.py             # Unit & integration tests
│   ├── fixtures/             # Test fixtures & mocks
│   └── ...
├── docker-compose.yml         # Base services definition
├── docker-compose.dev.yml     # Dev overlay (hot-reload, debug)
├── docker-compose.prod.yml    # Prod overlay (optimizations)
├── Makefile                   # Common commands
├── pyproject.toml             # Python package metadata & dependencies
├── pytest.ini                 # Pytest configuration
└── README.md                  # This file
```

---

## Common Operations

### Data Ingestion

**Run a single incremental fetch cycle**
```bash
make run-incremental
```

**Backfill historical data for symbols**
```bash
# Backfill BTC and ETH across all configured markets
make backfill SYMBOLS="BTC ETH"

# Alternative: use script directly
./scripts/backfill_inverse_contracts.sh
```

### Database Operations

**Access PostgreSQL directly**
```bash
docker-compose exec timescaledb psql -U postgres -d mnemo
```

**View database from pgAdmin UI**
1. Open http://localhost:5050
2. Login with admin@admin.com / admin
3. Add server: hostname=timescaledb, port=5432, username=postgres

**Run database migrations**
```bash
# Alembic migrations are applied on container startup
# To manually run migrations:
docker-compose exec mnemo_quant alembic upgrade head
```

### Cache Management

**Access Redis**
```bash
docker-compose exec redis redis-cli

# Example commands:
redis-cli> KEYS *
redis-cli> GET key_name
redis-cli> DEL key_name
```

**Clear all cache**
```bash
docker-compose exec redis redis-cli FLUSHALL
```

### Service Inspection

**View container logs**
```bash
# All services
./scripts/dev-logs.sh

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f mnemo_quant
```

**Shell into a container**
```bash
# Main application
make shell SERVICE=mnemo_quant

# Airflow scheduler
docker-compose exec airflow-scheduler bash

# Direct docker command
docker-compose exec timescaledb psql -U postgres
```

**Check service health**
```bash
docker-compose ps
curl http://localhost:8000/health  # API health check (when implemented)
```

### Airflow Operations

**Trigger a DAG manually**
1. Open Airflow UI: http://localhost:8080
2. Navigate to DAGs list
3. Click the "Trigger DAG" button
4. Monitor execution in Airflow

**View DAG source code**
```bash
# In Airflow UI, click on the DAG name → "Code" tab
# Or inspect directly:
ls infra/airflow/dags/ccxt_*.py
```

**Debug DAG parsing**
```bash
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags test <dag_id> <execution_date>
```

---

## Testing

### Test Structure

- **Unit Tests:** Individual function/class tests, no external dependencies
- **Integration Tests:** Tests with running services (database, redis, MinIO)
- **Verification Tests:** Data quality and end-to-end validation tests

### Running Tests

**All tests**
```bash
make test-all
```

**Unit tests only**
```bash
make test
```

**Integration tests** (requires running services)
```bash
make test-integration

# Or with specific marker:
pytest -m integration
```

**Specific test file**
```bash
pytest tests/test_phase1_abstractions.py -v
```

**With coverage report**
```bash
pytest --cov=src/quant_framework --cov-report=html
# Open htmlcov/index.html
```

### Test Markers

Available pytest markers:
- `@pytest.mark.unit` - Unit tests (run by default)
- `@pytest.mark.integration` - Integration tests (require services)
- `@pytest.mark.slow` - Slow-running tests
- `@pytest.mark.real_data` - Tests using real exchange data (skip in CI)

### Writing Tests

```python
import pytest
from quant_framework.config import settings

@pytest.mark.unit
def test_config_loading():
    """Test that configuration loads correctly."""
    assert settings.database.url is not None

@pytest.mark.integration
async def test_database_connection(db_session):
    """Test database connectivity."""
    result = await db_session.execute("SELECT 1")
    assert result is not None
```

---

## Development Workflow

### Setting Up for Development

**1. Install pre-commit hooks**
```bash
pre-commit install
```

**2. Code quality checks before commit**
```bash
# Lint check
make lint

# Format code
black src/
isort src/

# Type check
mypy src/
```

### Making Changes

**1. Create a feature branch**
```bash
git checkout -b feature/my-feature
```

**2. Make changes in hot-reload environment**
```bash
make dev-up
# Code changes automatically reload in container
```

**3. Run tests**
```bash
make test
make test-integration
```

**4. Format and lint**
```bash
make lint
black src/ && isort src/
```

**5. Commit and push**
```bash
git add .
git commit -m "feat: description of changes"
git push origin feature/my-feature
```

### Common Development Tasks

**Add a new data adapter**
1. Create module in `src/quant_framework/ingestion/<provider>/`
2. Implement adapter interface (inherit from base adapter)
3. Add configuration in `config/<provider>.yaml`
4. Write tests in `tests/test_<provider>_adapter.py`
5. Update DAG config to reference new adapter

**Add new configuration section**
1. Define Pydantic model in `config/settings.py`
2. Create YAML file in `config/` directory
3. Load in settings initialization
4. Add to environment overrides if needed

**Debug with container REPL**
```bash
make shell SERVICE=mnemo_quant
python

>>> from quant_framework.config import settings
>>> settings.database.url
>>> from quant_framework.ingestion import CCXTAdapter
>>> adapter = CCXTAdapter()
```

---

## Documentation

Detailed documentation is available in the `docs/` directory:

- **[DOCKER_SETUP.md](docs/DOCKER_SETUP.md)** - Docker & Docker Compose reference
- **[ENVIRONMENTS.md](docs/ENVIRONMENTS.md)** - Environment configuration guide

### Architecture Documentation

- **Configuration System:** See `config/settings.py` and `config/global.yaml`
- **Data Adapters:** See `src/quant_framework/ingestion/` modules
- **Database Schema:** See `db/schemas/` SQL files
- **Airflow DAGs:** See `infra/airflow/dags/` and `config/dags/dag_configs.yaml`

### API Documentation

When the REST API is fully implemented, Swagger/OpenAPI documentation will be available at:
```
http://localhost:8000/docs
```

---

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs <service_name>

# Rebuild images
./scripts/dev-rebuild.sh

# Verify ports are available
lsof -i :5432  # PostgreSQL
lsof -i :6379  # Redis
lsof -i :8080  # Airflow
```

### Database Connection Errors

```bash
# Verify database is running
docker-compose exec timescaledb pg_isready

# Check connection string
docker-compose exec mnemo_quant python -c "from quant_framework.config import settings; print(settings.database.url)"

# View database logs
docker-compose logs timescaledb
```

### Airflow DAG Not Appearing

```bash
# Verify DAG parsing
docker-compose exec airflow-webserver airflow dags list

# Check for syntax errors
docker-compose exec airflow-webserver airflow dags validate

# View logs
docker-compose logs airflow-scheduler
```

### Data Not Being Ingested

```bash
# Check if incremental fetcher is running
docker-compose ps ccxt-incremental-fetcher

# View service logs
docker-compose logs -f ccxt-incremental-fetcher

# Verify configuration
docker-compose exec mnemo_quant python -c "from quant_framework.config import settings; print(settings.assets.crypto_universe)"
```

### Out of Memory

```bash
# Increase Docker memory limit in docker-compose.override.yml
# Or reduce batch sizes in configuration files

# Check current usage
docker stats
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow code style (black, isort, mypy)
4. Write tests for new features
5. Ensure all tests pass (`make test-all`)
6. Commit with clear messages
7. Push to your fork and submit a pull request

---

## License

This project is licensed under the MIT License - see LICENSE file for details.

---

## Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation in `docs/`
- Review test examples in `tests/`
- Inspect configuration templates in `config/`
Readable README files

README files must be named README.md. The file name must end with the .md extension and is case sensitive.

For example, the file /README.md is rendered when you view the contents of the containing directory:

https://github.com/google/styleguide/tree/gh-pages

Also README.md at HEAD ref is rendered by Gitiles when displaying repository index:

https://gerrit.googlesource.com/gitiles/
Where to put your README

Unlike all other Markdown files, README.md files should not be located inside your product or library’s documentation directory. README.md files should be located in the top-level directory for your product or library’s actual codebase.

All top-level directories for a code package should have an up-to-date README.md file. This is especially important for package directories that provide interfaces for other teams.
What to put in your README

At a minimum, your README.md file should contain a link to your user- and/or team-facing documentation.

Every package-level README.md should include or point to the following information:

    What is in this package or library and what’s it used for.
    Points of contact.
    Status of whether this package or library is deprecated, or not for general release, etc.
    How to use the package or library. Examples include sample code, copyable bazel run or bazel test commands, etc.
    Links to relevant documentation.

