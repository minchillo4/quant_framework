# Docker Infrastructure Organization
# ===================================

## Directory Structure

```
docker/
├── Dockerfile              # Main application container
├── Dockerfile.dev          # Development container
├── docker-compose.yml      # Orchestration
│
├── airflow/               # Airflow-specific Docker configs
│   ├── Dockerfile.dev     # Development Airflow environment
│   ├── Dockerfile.prod    # Production Airflow environment
│   └── README.md          # Airflow setup documentation
│
└── storage/               # Storage-specific Docker configs
    ├── init_bronze.sh     # MinIO bucket initialization
    └── README.md          # Storage setup documentation
```

## Moved Files

### docker/storage/init_bronze.sh
**From:** `src/quant_framework/infrastructure/minio/init_bronze.sh`
**Purpose:** MinIO bronze layer initialization
**Used by:** Docker Compose during MinIO startup

## Container Organization

### Main Containers (docker-compose.yml)
- **api**: Main application API
- **airflow-webserver**: Airflow UI
- **airflow-scheduler**: Airflow task scheduler
- **postgres**: TimescaleDB
- **minio**: S3-compatible storage
- **redis**: Caching and task queue

### Initialization Hooks
- **MinIO**: Uses `docker/storage/init_bronze.sh` via entrypoint
- **PostgreSQL**: Uses `db/init/` scripts via volume mount

## Development vs Production

- `docker/Dockerfile.dev`: Development image with hot reload
- `docker/Dockerfile.prod`: Production image with optimization
- `docker/airflow/Dockerfile.dev`: Airflow dev environment
- `docker/airflow/Dockerfile.prod`: Airflow prod environment
