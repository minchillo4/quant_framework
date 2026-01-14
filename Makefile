# =====================================================================
#  GLOBAL CONFIG
# =====================================================================
# Default environment (override with: ENV=prod make start)
ENV ?= dev

# Project name based on folder
PROJECT_NAME := $(notdir $(CURDIR))

# Compose files - base + environment-specific overlay
BASE_COMPOSE := docker-compose.yml
DEV_COMPOSE := docker-compose.dev.yml
PROD_COMPOSE := docker-compose.prod.yml

# Docker compose command with environment-specific overlay
ifeq ($(ENV),prod)
	DOCKER_COMPOSE := MNEMO_ENV=prod docker compose --env-file .env.prod -f $(BASE_COMPOSE) -f $(PROD_COMPOSE)
else
	DOCKER_COMPOSE := MNEMO_ENV=dev docker compose --env-file .env.dev -f $(BASE_COMPOSE) -f $(DEV_COMPOSE)
endif

.PHONY: start stop restart build rebuild logs shell check-sync dev-up dev-down prod-up prod-down migrate backtest test lint clean code-prompt run-oi-ingest run-oi-store validate-env check-config kafka-setup help


# =====================================================================
#  QUICK START COMMANDS
# =====================================================================
# Quick development startup using helper script
dev-up:  ## Quick start development environment (uses script)
	@./scripts/dev-up.sh

dev-down:  ## Quick stop development environment (uses script)
	@./scripts/dev-down.sh

# Quick production startup using helper script
prod-up:  ## Quick start production environment (uses script)
	@./scripts/prod-up.sh

prod-down:  ## Quick stop production environment (uses script)
	@./scripts/prod-down.sh

# Check code synchronization
check-sync:  ## Verify code is syncing properly in dev containers
	@./scripts/check-code-sync.sh


# =====================================================================
#  START / STOP / RESTART
# =====================================================================
# Start environment using docker compose directly
# Example:
#   make start          # Starts dev
#   ENV=prod make start # Starts prod
start:  ## Start containers for specified environment
	@echo "🚀 Starting $(PROJECT_NAME) (ENV=$(ENV))..."
	@$(DOCKER_COMPOSE) up -d
	@echo "✅ Containers started."
	@$(MAKE) check-config

# Build images
build:  ## Build docker images
	@echo "🔨 Building images for ENV=$(ENV)..."
	@$(DOCKER_COMPOSE) build

# Rebuild images without cache
rebuild:  ## Rebuild docker images without cache
	@echo "🔨 Rebuilding images without cache for ENV=$(ENV)..."
	@$(DOCKER_COMPOSE) build --no-cache

# Example:
#   make stop
stop:  ## Stop containers
	@$(DOCKER_COMPOSE) down

# View logs
logs:  ## Follow logs for all services (or specific: make logs SERVICE=airflow-scheduler)
	@$(DOCKER_COMPOSE) logs -f --tail=50 $(SERVICE)

# Shell into a container
shell:  ## Open shell in container (make shell SERVICE=mnemo_quant)
	@$(DOCKER_COMPOSE) exec $(or $(SERVICE),mnemo_quant) bash

# Example:
#   make restart
restart: stop start  ## Restart containers


# =====================================================================
#  BRONZE & DAG VALIDATION
# =====================================================================
validate-bronze:  ## Validate bronze sources configuration
	@echo "Validating bronze configuration..."
	@BRONZE_VALIDATE_ONLY=1 bash src/quant_framework/infrastructure/minio/init_bronze.sh

generate-dags:  ## Generate Airflow DAGs from configuration
	@echo "Generating DAGs..."
	@uv run python infra/airflow/dags/factories/universal_dag_factory.py \
		--mode all \
		--validate \
		--config config/dags/dag_configs.yaml \
		--sources-config config/bronze/sources.yaml \
		--enforce-sources
	@echo "✅ DAGs generated and validated"

validate-all: validate-bronze generate-dags  ## Run all validations
	@echo "✅ All validations passed"


# =====================================================================
#  DATABASE / CONFIG
# =====================================================================
# Example:
#   make migrate
migrate:  ## Run database migrations
	@echo "Running migrations..."
	@$(DOCKER_COMPOSE) exec timescaledb bash -c 'for script in /docker-entrypoint-initdb.d/[0-9]*.sh; do \
		echo "Applying $$script..."; \
		bash "$$script"; \
	done'

# Example:
#   make validate-env
validate-env:  ## Validate environment configuration
	@echo "Validating environment configuration..."
	@$(DOCKER_COMPOSE) run --rm mnemo_quant \
		python scripts/utils/validate_env.py

# Example:
#   make check-config
check-config:  ## Check configuration loading in running container
	@echo "Checking configuration loading..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python -c "from config.settings import settings; print('✅ Configuration loaded successfully'); print(f'Environment: {settings.env}'); print(f'Crypto Assets: {settings.assets.crypto.full_universe}')"

# Example:
#   make kafka-setup
kafka-setup:  ## Setup Kafka topics for OI data
	@echo "Setting up Kafka topics..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python scripts/utils/kafka_setup.py

# Example:
#   make clear-market-data
clear-market-data:  ## Clear all market data from database (OHLC, OI, metadata)
	@./scripts/clear_market_data.sh

# Example:
#   make verify-backfill
verify-backfill:  ## Verify backfill data coverage
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python scripts/verify_backfill_coverage.py


# =====================================================================
#  INCREMENTAL FETCHER
# =====================================================================
.PHONY: start-incremental
start-incremental:  ## Start incremental fetcher service
	@echo "🚀 Starting CCXT 5m incremental fetcher..."
	@$(DOCKER_COMPOSE) up -d ccxt-incremental-fetcher
	@$(DOCKER_COMPOSE) logs -f ccxt-incremental-fetcher

.PHONY: stop-incremental
stop-incremental:  ## Stop incremental fetcher service
	@echo "⏹️  Stopping incremental fetcher..."
	@$(DOCKER_COMPOSE) stop ccxt-incremental-fetcher

.PHONY: restart-incremental
restart-incremental:  ## Restart incremental fetcher service
	@echo "🔄 Restarting incremental fetcher..."
	@$(DOCKER_COMPOSE) restart ccxt-incremental-fetcher

# Example:
#   make run-incremental SYMBOLS="BTC ETH" TIMEFRAMES="1h 4h"
run-incremental:  ## Run single incremental update cycle (manual trigger)
	@echo "▶️ Running single incremental update..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python -m mnemo_quant.pipelines.orchestration.scripts.incremental_runner \
		--symbols $(or $(SYMBOLS), BTC) \
		--timeframes $(or $(TIMEFRAMES), 1h) \
		--data-types $(or $(DATA_TYPES), ohlc oi) \
		--exchanges $(or $(EXCHANGES), binance) \
		--lookback-hours $(or $(LOOKBACK), 24)

# Example:
#   make backfill SYMBOLS="BTC ETH" START="2023-01-01" END="2024-01-01"
backfill:  ## Run historical backfill pipeline
	@echo "📜 Running historical backfill..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python -m mnemo_quant.pipelines.orchestration.scripts.backfill_runner \
		--symbols $(or $(SYMBOLS), BTC) \
		--timeframes $(or $(TIMEFRAMES), 1d 4h 1h) \
		--data-types $(or $(DATA_TYPES), ohlc oi) \
		--start-date $(or $(START), $(shell date -d '1 month ago' +%Y-%m-%d)) \
		--end-date $(or $(END), $(shell date +%Y-%m-%d)) \
		$(if $(VERBOSE),--verbose,)


# =====================================================================
#  PIPELINES
# =====================================================================
# Example:
#   make backtest
backtest:  ## Run backtesting pipeline
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		python -m mnemo_quant.pipelines.backtest_pipeline \
		--start 2024-01-01 --end 2024-07-01


# =====================================================================
#  TEST / LINT
# =====================================================================
# Example:
#   make test
test:  ## Run unit tests
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/unit -v --cov=mnemo_quant --cov-report=term-missing

# Example:
#   make test-integration
test-integration:  ## Run integration tests (requires DB, Kafka, and APIs)
	@echo "🧪 Running integration tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/integration -v -s

# Example:
#   make test-verification
test-verification:  ## Run verification/health tests
	@echo "🔍 Running pipeline verification tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/verification -v -s

# Example:
#   make test-all
test-all:  ## Run all tests (unit + integration + verification)
	@echo "🧪 Running all tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/ -v

# Example:
#   make test-ccxt
test-ccxt:  ## Run CCXT incremental tests only
	@echo "🧪 Running CCXT incremental tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/integration/test_ccxt_incremental.py -v -s

# Example:
#   make test-coinalyze
# Example:
#   make test-coinalyze
test-coinalyze:  ## Run CoinAlyze unit tests only
	@echo "🧪 Running CoinAlyze unit tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/unit/ingestion/adapters/coinalyze_plugin -v --tb=short

# Example:
#   make test-coinalyze-live
test-coinalyze-live:  ## Run CoinAlyze live API tests (requires COINALYZE_API_KEY and COINALYZE_LIVE_TESTS=1)
	@echo "🧪 Running CoinAlyze live API tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		env COINALYZE_LIVE_TESTS=1 COINALYZE_API_KEY=$(or $(COINALYZE_API_KEY), NOT_SET) \
		uv run pytest tests/unit/ingestion/adapters/coinalyze_plugin/test_live_api.py -v -s

# Example:
#   make test-coinalyze-all
test-coinalyze-all:  ## Run all CoinAlyze tests (unit + live if credentials available)
	@echo "🧪 Running all CoinAlyze tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/unit/ingestion/adapters/coinalyze_plugin -v --tb=short -m coinalyze
	@echo "🧪 Running CoinAlyze backfill tests..."
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		uv run pytest tests/integration/test_coinalyze_backfill.py -v -s

# Example:
#   make lint
lint:  ## Run linting and formatting checks
	@$(DOCKER_COMPOSE) exec mnemo_quant \
		ruff check . && black --check .

ps:  ## Run linting and formatting checks
	@$(DOCKER_COMPOSE) ps -a


# =====================================================================
#  CLEANING
# =====================================================================
# Example:
#   make clean
clean:  ## Remove all containers, volumes, and cached files
	@echo "🧹 Cleaning up..."
	@# Explicitly stop and remove containers to ensure no orphans
	@docker stop kafka zookeeper quant-mnemo quant-mnemo-api quant-storage-consumer quant-timescaledb quant-redis quant-prometheus quant-grafana quant-kafka-setup quant-airflow-webserver quant-airflow-scheduler quant-airflow-triggerer quant-pgadmin quant-kafka-ui quant-incremental-fetcher quant-aggregation-consumer 2>/dev/null || true
	@docker rm kafka zookeeper quant-mnemo quant-mnemo-api quant-storage-consumer quant-timescaledb quant-redis quant-prometheus quant-grafana quant-kafka-setup quant-airflow-webserver quant-airflow-scheduler quant-airflow-triggerer quant-pgadmin quant-kafka-ui quant-incremental-fetcher quant-aggregation-consumer 2>/dev/null || true
	@$(DOCKER_COMPOSE) down -v --rmi all --remove-orphans
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name ".mypy_cache" -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@echo "✅ Clean complete"

# Example:
#   make rebuild
rebuild:  ## Rebuild all services from scratch (use for dependency or Dockerfile changes)
	@$(DOCKER_COMPOSE) down -v
	@$(DOCKER_COMPOSE) build --no-cache
	@$(DOCKER_COMPOSE) up -d

.DEFAULT_GOAL := help


# =====================================================================
#  SYSTEM CLEAN
# =====================================================================
# Example:
#   make clean-all-up
clean-all-up:  ## Clean up Docker system (prune unused resources)
	@docker system df          
	@docker system prune -af   
	@docker volume prune -f    


# =====================================================================
#  CODE PROMPT GENERATOR
# =====================================================================
# Example:
#   make code-prompt-all
code-prompt-all:  ## Run all code2prompt commands and join into final.txt
	@echo "Generating combined code prompt for $(PROJECT_NAME)..."
	@code2prompt config -O config.txt
	@code2prompt src -O src.txt \
	-e "mnemo_quant.egg-info/*" \
	-e "*.pyc*" \


	@code2prompt . -O ret.txt \
	-e "final.txt" \
	-e "*.lock" \
	-e "*.log" \
	-e "*.md" \
	-e "*.txt" \
	-e "src/*" \
	-e "*.py" \
	-e "infra/*" \
	-e ".logs/*"
	@cat  config.txt  src.txt ret.txt  > final.txt
	@rm  config.txt src.txt ret.txt 
	@echo "✅ Combined prompt saved to final.txt"


# =====================================================================
#  LOGS
# =====================================================================
# Examples:
#   make logs                   → logs of all services
#   SERVICE=mnemo_quant make logs  → logs of one service
logs: ## Show logs for all or specific service(s)
	@echo "📜 Fetching logs for environment: $(ENV)"
ifeq ($(SERVICE),)
	@$(DOCKER_COMPOSE) logs -f 
else
	@echo "Showing logs for service: $(SERVICE)"
	@$(DOCKER_COMPOSE) logs -f  $(SERVICE)
endif


# =====================================================================
#  SERVICE MANAGEMENT
# =====================================================================
# Example:
#   SERVICE=storage_consumer make restart-service
restart-service:  ## Restart specific service without rebuild (e.g., make restart-service SERVICE=storage_consumer)
	@if [ -z "$(SERVICE)" ]; then echo "Error: SERVICE required (e.g., SERVICE=storage_consumer)"; exit 1; fi
	@$(DOCKER_COMPOSE) restart $(SERVICE)
	@echo "✅ Service $(SERVICE) restarted"

# =====================================================================
#  KAFKA MANAGEMENT
# =====================================================================
# Example:
#   make kafka-clean-restart
kafka-clean-restart:  ## Clean restart Kafka and Zookeeper (removes volumes to fix cluster ID issues)
	@echo "🛑 Stopping Kafka and Zookeeper..."
	@docker stop kafka zookeeper 2>/dev/null || true
	@docker rm kafka zookeeper 2>/dev/null || true
	@echo "🗑️  Removing Kafka and Zookeeper volumes..."
	@docker volume rm $(PROJECT_NAME)_kafka-data $(PROJECT_NAME)_zookeeper-data 2>/dev/null || true
	@echo "🚀 Starting fresh Kafka and Zookeeper..."
	@$(DOCKER_COMPOSE) up -d zookeeper kafka
	@echo "⏳ Waiting for Kafka to be healthy..."
	@sleep 10
	@echo "✅ Kafka and Zookeeper restarted with clean state"

# Example:
#   make kafka-restart
kafka-restart:  ## Restart Kafka (preserves data, entrypoint handles cluster ID mismatch)
	@echo "🔄 Restarting Kafka..."
	@$(DOCKER_COMPOSE) restart kafka
	@echo "✅ Kafka restarted"

# Example:
#   SERVICE=mnemo_quant_api make rebuild-service
rebuild-service:  ## Rebuild and restart specific service without cache (e.g., for dep changes: make rebuild-service SERVICE=mnemo_quant_api)
	@if [ -z "$(SERVICE)" ]; then echo "Error: SERVICE required (e.g., SERVICE=mnemo_quant_api)"; exit 1; fi
	@$(DOCKER_COMPOSE) build --no-cache $(SERVICE)
	@$(DOCKER_COMPOSE) up -d --force-recreate $(SERVICE)
	@echo "✅ Service $(SERVICE) rebuilt and restarted"

# Example:
#   SERVICE=mnemo_quant make exec
exec:  ## Exec into service shell (e.g., make exec SERVICE=mnemo_quant)
	@if [ -z "$(SERVICE)" ]; then echo "Error: SERVICE required (e.g., SERVICE=mnemo_quant)"; exit 1; fi
	@$(DOCKER_COMPOSE) exec $(SERVICE) bash

clean-pyc: ## Clean Python cache files (safe)
	@sudo find . -name "*.pyc" -delete
	@sudo find . -name "__pycache__" -type d -exec rm -rf {} +


.DEFAULT_GOAL := help


# =====================================================================
#  HELP MENU
# =====================================================================
help:  ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
