import logging
from typing import Any

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter()


async def check_database() -> bool:
    """Check database connectivity."""
    try:
        # This will be implemented after we set up async database layer
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


async def check_kafka() -> bool:
    """Check Kafka connectivity."""
    try:
        # This will be implemented after we set up Kafka properly
        return True
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return False


async def check_redis() -> bool:
    """Check Redis connectivity."""
    try:
        # This will be implemented after we set up Redis properly
        return True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False


@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Comprehensive health check endpoint."""
    health_status = {
        "status": "healthy",
        "services": {
            "database": await check_database(),
            "kafka": await check_kafka(),
            "redis": await check_redis(),
        },
        "version": "0.1.0",
    }

    # If any service is unhealthy, return 503
    if not all(health_status["services"].values()):
        raise HTTPException(status_code=503, detail=health_status)

    return health_status


@router.get("/ready")
async def readiness_check() -> dict[str, Any]:
    """Readiness check for Kubernetes."""
    return {"status": "ready"}
