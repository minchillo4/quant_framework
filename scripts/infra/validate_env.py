#!/usr/bin/env python3
"""Environment validation script for Mnemo Quant platform."""

import asyncio
import os
import sys

from aiokafka import AIOKafkaProducer

REQUIRED_ENV_VARS = {
    "KAFKA_BROKERS": "kafka:9092",
    "TIMESCALE_URL": "postgresql+psycopg2://postgres:devpassword@timescaledb:5432/quant_dev",
    "REDIS_URL": "redis://redis:6379/0",
    "MNEMO_ENV": "dev",
}


async def validate_kafka():
    """Validate Kafka connection and configuration."""
    brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
    print(f"🔍 Testing Kafka connection to: {brokers}")

    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=brokers,
            request_timeout_ms=10000,
        )
        await producer.start()

        metadata = await producer.client.fetch_all_metadata()
        print(f"✅ Kafka: Connected to {len(metadata.brokers)} broker(s)")

        await producer.stop()
        return True
    except Exception as e:
        print(f"❌ Kafka: Connection failed - {e}")
        return False


def validate_environment():
    """Validate all required environment variables."""
    print("🔍 Validating environment configuration...")

    all_valid = True

    for var, default in REQUIRED_ENV_VARS.items():
        value = os.getenv(var, default)
        if value:
            print(f"✅ {var}: Set")
        else:
            print(f"❌ {var}: Missing")
            all_valid = False

    return all_valid


async def main():
    """Run all validations."""
    print("🚀 Mnemo Quant Environment Validation")
    print("=" * 50)

    # Validate environment variables
    env_valid = validate_environment()

    # Validate Kafka
    kafka_valid = await validate_kafka()

    print("=" * 50)

    if env_valid and kafka_valid:
        print("🎉 All validations passed! System is ready.")
        return True
    else:
        print("💥 Validation failed. Please check the errors above.")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
