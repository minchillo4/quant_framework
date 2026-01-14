#!/usr/bin/env python3
"""
Environment validation script for Mnemo Quant.
Works both inside Docker containers and locally.
"""

import logging
import os
import sys
from pathlib import Path

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def validate_environment():
    """Validate all required environment variables are set."""
    required_vars = [
        "POSTGRES_PASSWORD",
        "QUANT_APP_PASSWORD",
        "COINALYZE_API_KEYS",
    ]

    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        elif var == "COINALYZE_API_KEYS":
            # Expect comma-separated format
            keys = [k.strip() for k in value.split(",") if k.strip()]
            if not keys:
                missing.append(f"{var} (empty or invalid format)")
            else:
                logger.info(f"Found {len(keys)} Coinalyze API keys")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error(
            "COINALYZE_API_KEYS should be comma-separated, e.g.: key1,key2,key3"
        )
        return False

    logger.info("✅ Environment validation passed")
    return True


def check_config_files():
    """Check if required configuration files exist."""
    config_dir = Path("config")
    required_files = [
        config_dir / "assets.yaml",
        config_dir / "coinalyze.yaml",
        config_dir / "database.yaml",
        config_dir / "kafka.yaml",
    ]

    missing_files = []
    for file_path in required_files:
        if not file_path.exists():
            missing_files.append(file_path.name)

    if missing_files:
        logger.error(f"Missing configuration files: {', '.join(missing_files)}")
        return False

    logger.info("✅ All configuration files found")
    return True


if __name__ == "__main__":
    logger.info("Starting environment validation...")

    success = True
    success &= validate_environment()
    success &= check_config_files()

    if success:
        logger.info("🎉 All validation checks passed!")
        sys.exit(0)
    else:
        logger.error("❌ Validation failed")
        sys.exit(1)
