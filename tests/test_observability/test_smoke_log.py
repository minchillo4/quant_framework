"""
Smoke tests for observability module.
Quick sanity checks that verify basic functionality.

These tests should ALWAYS pass and run in < 1 second total.
If any of these fail, there's a fundamental problem.
"""

import logging

import pytest
import structlog


def test_smoke_can_import_module():
    """Smoke test 1/6: Verify module can be imported without errors."""
    try:
        from quant_framework.infrastructure.observability import (
            get_database_logger,
            get_infrastructure_logger,
            get_ingestion_logger,
            get_logger,
            get_orchestration_logger,
            get_processing_logger,
            get_storage_logger,
            get_transformation_logger,
            setup_logging,
        )

        # Verify all imports are callable
        assert callable(setup_logging)
        assert callable(get_logger)
        assert callable(get_infrastructure_logger)
        assert callable(get_ingestion_logger)
        assert callable(get_orchestration_logger)
        assert callable(get_transformation_logger)
        assert callable(get_storage_logger)
        assert callable(get_database_logger)
        assert callable(get_processing_logger)

    except ImportError as e:
        pytest.fail(f"Failed to import observability module: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error during import: {e}")


def test_smoke_can_setup_logging():
    """Smoke test 2/6: Verify setup_logging() can be called without errors."""
    from quant_framework.infrastructure.observability import setup_logging

    try:
        # Clear any existing configuration first
        logging.root.handlers = []
        structlog.reset_defaults()

        # Should not raise any exceptions
        setup_logging(level="INFO", json_logs=True, include_timestamp=True)

        # Verify structlog is configured
        assert structlog.is_configured()

    except Exception as e:
        pytest.fail(f"setup_logging() failed: {e}")


def test_smoke_can_create_all_loggers():
    """Smoke test 3/6: Verify all logger factories can create loggers."""
    from quant_framework.infrastructure.observability import (
        get_database_logger,
        get_infrastructure_logger,
        get_ingestion_logger,
        get_logger,
        get_orchestration_logger,
        get_processing_logger,
        get_storage_logger,
        get_transformation_logger,
        setup_logging,
    )

    # Clear and setup first
    logging.root.handlers = []
    structlog.reset_defaults()
    setup_logging(level="INFO", json_logs=True)

    try:
        # Create one logger of each type
        logger_base = get_logger(__name__)
        logger_infra = get_infrastructure_logger("database-adapter")
        logger_ingest = get_ingestion_logger("ccxt-adapter")
        logger_orch = get_orchestration_logger()
        logger_trans = get_transformation_logger("validator")
        logger_storage = get_storage_logger("timescale-writer")
        logger_db = get_database_logger()
        logger_proc = get_processing_logger("normalizer")

        # Verify all are logger instances
        assert isinstance(logger_base, structlog.stdlib.BoundLogger)
        assert isinstance(logger_infra, structlog.stdlib.BoundLogger)
        assert isinstance(logger_ingest, structlog.stdlib.BoundLogger)
        assert isinstance(logger_orch, structlog.stdlib.BoundLogger)
        assert isinstance(logger_trans, structlog.stdlib.BoundLogger)
        assert isinstance(logger_storage, structlog.stdlib.BoundLogger)
        assert isinstance(logger_db, structlog.stdlib.BoundLogger)
        assert isinstance(logger_proc, structlog.stdlib.BoundLogger)

    except Exception as e:
        pytest.fail(f"Failed to create loggers: {e}")


def test_smoke_logger_has_expected_methods():
    """Smoke test 4/6: Verify loggers have expected methods."""
    from quant_framework.infrastructure.observability import get_logger, setup_logging

    # Clear and setup
    logging.root.handlers = []
    structlog.reset_defaults()
    setup_logging(level="INFO", json_logs=True)

    # Create logger
    logger = get_logger("smoke_test")

    # Check that logger has all expected logging methods
    expected_methods = [
        "debug",
        "info",
        "warning",
        "error",
        "critical",
        "exception",
        "log",
    ]
    for method in expected_methods:
        assert hasattr(logger, method), f"Logger missing method: {method}"
        assert callable(
            getattr(logger, method)
        ), f"Logger method {method} is not callable"

    # Verify context binding works
    bound_logger = logger.bind(test_key="test_value")
    assert isinstance(bound_logger, structlog.stdlib.BoundLogger)
    assert bound_logger._context.get("test_key") == "test_value"


def test_smoke_logger_can_emit_without_errors():
    """Smoke test 5/6: Verify loggers can emit logs without raising exceptions."""
    from quant_framework.infrastructure.observability import get_logger, setup_logging

    # Clear and setup
    logging.root.handlers = []
    structlog.reset_defaults()
    setup_logging(level="INFO", json_logs=True)

    # Create logger
    logger = get_logger("smoke_test")

    try:
        # Call all logging methods - should not raise exceptions
        logger.debug("debug_message", test="smoke")
        logger.info("info_message", test="smoke")
        logger.warning("warning_message", test="smoke")
        logger.error("error_message", test="smoke")
        logger.critical("critical_message", test="smoke")

        # Test with exception info
        try:
            raise ValueError("Test exception for logging")
        except ValueError:
            logger.exception("exception_message", test="smoke")

        # Test structured logging
        logger.info(
            "structured_event",
            action="test",
            result="success",
            count=42,
            metrics={"duration": 0.123, "items": 100},
        )

    except Exception as e:
        pytest.fail(f"Logging raised unexpected exception: {e}")


def test_smoke_json_mode_can_be_configured():
    """Smoke test 6/6: Verify JSON logging mode can be configured."""
    from quant_framework.infrastructure.observability import setup_logging

    try:
        # Clear any existing configuration
        logging.root.handlers = []
        structlog.reset_defaults()

        # Setup JSON logging - this should work without errors
        setup_logging(
            level="INFO",
            json_logs=True,  # ← Testing JSON mode specifically
            include_timestamp=True,
        )

        # Verify structlog is configured
        assert structlog.is_configured()

        # Get a logger through structlog directly
        logger = structlog.get_logger("json_test")

        # Log with complex data structures that JSON can handle
        logger.info(
            "json_test_event",
            numeric_value=123,
            float_value=45.67,
            string_value="test",
            list_value=[1, 2, 3],
            dict_value={"key": "value"},
            nested={"level1": {"level2": "deep"}},
        )

        # If we get here without exceptions, JSON mode works
        assert True

    except Exception as e:
        pytest.fail(f"JSON logging configuration failed: {e}")


# Mark all tests in this file as smoke tests
pytestmark = pytest.mark.smoke


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v"])
