# Docker Environment Setup - Dev/Prod Separation

This document explains the clean dev/prod Docker environment setup for the mnemo-quant project.

## 📁 File Structure

```
├── docker-compose.yml           # Base configuration (all services)
├── docker-compose.dev.yml       # Development overrides (volume mounts, hot-reload)
├── docker-compose.prod.yml      # Production overrides (baked code, optimizations)
├── docker-compose.old.yml       # Historical backup (working baseline)
│
├── .env.dev                     # Development environment variables
├── .env.prod                    # Production environment variables (⚠️ keep secure!)
├── .env.example                 # Template for new environments
│
├── infra/airflow/
│   ├── Dockerfile.dev          # Airflow dev image (no code copy, uses mounts)
│   └── Dockerfile.prod         # Airflow prod image (code baked in)
│
├── docker/
│   ├── Dockerfile.dev          # App dev image (watchdog for hot-reload)
│   └── Dockerfile              # App prod image (optimized, code baked in)
│
└── scripts/
    ├── dev-up.sh               # Quick dev startup
    ├── dev-down.sh             # Quick dev shutdown
    ├── dev-rebuild.sh          # Rebuild dev images
    ├── dev-logs.sh             # View dev logs
    ├── prod-up.sh              # Quick prod startup
    ├── prod-down.sh            # Quick prod shutdown
    └── check-code-sync.sh      # Verify code synchronization
```

## 🚀 Quick Start

### Development Environment

```bash
# Option 1: Using helper script (recommended)
./scripts/dev-up.sh

# Option 2: Using Makefile
make dev-up

# Option 3: Direct docker compose
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

### Production Environment

```bash
# Option 1: Using helper script (recommended)
./scripts/prod-up.sh

# Option 2: Using Makefile
make prod-up

# Option 3: Direct docker compose
MNEMO_ENV=prod docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## 🔧 Architecture

### Three-File Compose Structure

1. **docker-compose.yml** (Base)
   - Contains ALL services (Airflow, app, databases, monitoring, storage)
   - Defines common configuration
   - Uses `.env.${MNEMO_ENV}` for environment-specific variables
   - No environment-specific overrides

2. **docker-compose.dev.yml** (Development Overlay)
   - Extends base configuration
   - Mounts source code for hot-reloading
   - Uses `Dockerfile.dev` images
   - Fast DAG discovery (10s interval)
   - Debug logging enabled
   - Mounts: `./src`, `./infra/airflow/dags`, `./config`, etc.

3. **docker-compose.prod.yml** (Production Overlay)
   - Extends base configuration
   - Uses `Dockerfile.prod` images with baked-in code
   - Only mounts `./config` (read-only)
   - Slower DAG discovery (60s interval) for stability
   - Info-level logging
   - Production-optimized database settings

### Dockerfile Strategy

#### Development Dockerfiles
- **infra/airflow/Dockerfile.dev**
  - Installs dependencies only
  - NO source code copying (relies on volume mounts)
  - Includes entrypoint scripts
  - Installs watchdog for file monitoring

- **docker/Dockerfile.dev**
  - Uses `uv` for fast dependency installation
  - NO source code copying
  - Watchmedo for auto-restart on code changes
  - Development dependencies included

#### Production Dockerfiles
- **infra/airflow/Dockerfile.prod**
  - Copies ALL source code into image
  - Includes entrypoint scripts
  - Optimized for production
  - No dev dependencies

- **docker/Dockerfile**
  - Full code baked into image
  - Production dependencies only
  - Optimized layers for caching

## 📊 Service Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin/admin (dev) |
| API | http://localhost:8000 | N/A |
| Grafana | http://localhost:3000 | admin/admin (dev) |
| pgAdmin | http://localhost:5050 | See .env file |
| MinIO Console | http://localhost:9001 | admin/password123 (dev) |
| Prometheus | http://localhost:9090 | N/A |

## 🛠️ Common Commands

### Development

```bash
# Start dev environment
./scripts/dev-up.sh

# Stop dev environment
./scripts/dev-down.sh

# View logs (all services)
./scripts/dev-logs.sh

# View logs (specific service)
./scripts/dev-logs.sh airflow-scheduler

# Rebuild images
./scripts/dev-rebuild.sh

# Check code synchronization
./scripts/check-code-sync.sh

# Shell into container
make shell SERVICE=mnemo_quant
make shell SERVICE=airflow-scheduler

# Run tests
make test
make test-integration
```

### Using Makefile

```bash
# Development
make dev-up              # Start dev
make dev-down            # Stop dev
make check-sync          # Verify code sync

# Production
make prod-up             # Start prod
make prod-down           # Stop prod

# Environment-specific (ENV=dev by default)
make start               # Start dev
ENV=prod make start      # Start prod

make build               # Build images
make rebuild             # Rebuild without cache
make logs                # View all logs
make logs SERVICE=redis  # View specific service logs
make shell SERVICE=mnemo_quant  # Open shell
```

## 🔍 Troubleshooting

### Code Changes Not Reflecting

1. **Check if volumes are mounted properly:**
   ```bash
   ./scripts/check-code-sync.sh
   ```

2. **Verify you're using dev compose:**
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml ps
   ```

3. **Check container logs:**
   ```bash
   docker logs quant-airflow-scheduler -f --tail=50
   ```

### Import Errors in Airflow

1. **Check PYTHONPATH:**
   ```bash
   docker exec quant-airflow-scheduler env | grep PYTHONPATH
   # Should show: /opt/airflow:/opt/airflow/src:/opt/airflow/dags
   ```

2. **Verify source is mounted:**
   ```bash
   docker exec quant-airflow-scheduler ls -la /opt/airflow/src/
   ```

3. **Test import directly:**
   ```bash
   docker exec quant-airflow-scheduler python -c "from quant_framework.ingestion.adapters.ccxt_plugin.base import CCXTBaseAdapter; print('✅ Import successful')"
   ```

### Containers Won't Start

1. **Check for port conflicts:**
   ```bash
   lsof -i :8080  # Airflow
   lsof -i :5432  # PostgreSQL
   lsof -i :6379  # Redis
   ```

2. **Check Docker logs:**
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml logs
   ```

3. **Verify environment file exists:**
   ```bash
   ls -la .env.dev
   ```

## 🔒 Security Notes for Production

Before deploying to production:

- [ ] Rotate ALL passwords and API keys in `.env.prod`
- [ ] Generate new Airflow Fernet key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- [ ] Generate new secret key: `openssl rand -hex 32`
- [ ] Add `.env.prod` to `.gitignore`
- [ ] Use secrets management (AWS Secrets Manager, Vault, etc.)
- [ ] Enable SSL/TLS for all connections
- [ ] Set `networks.default.internal: true` in docker-compose.prod.yml
- [ ] Implement IP whitelisting
- [ ] Add resource limits to all services
- [ ] Set up proper backup strategies
- [ ] Configure monitoring and alerting
- [ ] Review and apply database security best practices

## 📋 Migration from Old Setup

If you're migrating from `docker-compose.old.yml`:

1. **Backup existing setup:**
   ```bash
   docker compose -f docker-compose.old.yml down
   # Keep docker-compose.old.yml as reference
   ```

2. **Start with new dev setup:**
   ```bash
   ./scripts/dev-up.sh
   ```

3. **Verify everything works:**
   ```bash
   ./scripts/check-code-sync.sh
   make check-config
   ```

4. **Update your workflows:**
   - Replace `docker-compose up` → `./scripts/dev-up.sh`
   - Replace `docker-compose down` → `./scripts/dev-down.sh`

## 🎯 Benefits of This Setup

✅ **Clear Separation**: Dev and prod are completely isolated
✅ **Hot Reloading**: Code changes reflect immediately in dev
✅ **No Code Duplication**: Single source of truth in base compose
✅ **Easy Switching**: Simple scripts for environment switching
✅ **Production Ready**: Optimized images with baked-in code
✅ **Maintainable**: Clear structure and documentation
✅ **Safe**: Production requires explicit confirmation
✅ **Flexible**: Use scripts, Makefile, or direct docker compose

## 📚 Additional Resources

- [Docker Compose Extends Documentation](https://docs.docker.com/compose/extends/)
- [Airflow in Docker Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Multi-stage Docker Builds](https://docs.docker.com/build/building/multi-stage/)

## 🆘 Need Help?

If you encounter issues:

1. Check this README troubleshooting section
2. Run `./scripts/check-code-sync.sh` to diagnose issues
3. Check container logs: `docker logs <container-name> --tail=100`
4. Verify environment variables: `docker exec <container> env`
5. Test imports manually: `docker exec <container> python -c "import module"`
