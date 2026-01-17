"""
SMOKE TESTS for CoinMetricsCsvAdapter.

These tests verify fundamental functionality and should:
1. Run quickly (< 5 seconds total)
2. Always pass if system is working
3. Test basic import, instantiation, and core functionality
4. Provide early warning of system breakage
"""

import sys
from pathlib import Path

import pytest

# Add the ingestion module to path if needed
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# ============================================
# TEST 1: Module Import
# ============================================
def test_smoke_can_import_coinmetrics_module():
    """Smoke test 1/6: Verify CoinMetrics adapter module can be imported."""
    try:
        from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
            CoinMetricsCsvAdapter,
        )
        from quant_framework.ingestion.adapters.coinmetrics_plugin.base import (
            CoinMetricsAdapterBase,
        )

        assert CoinMetricsCsvAdapter is not None
        assert CoinMetricsAdapterBase is not None

        # Verify it's a proper class
        assert isinstance(CoinMetricsCsvAdapter, type)
        assert isinstance(CoinMetricsAdapterBase, type)

        print("✓ CoinMetrics module imports successfully")

    except ImportError as e:
        pytest.fail(f"Failed to import CoinMetrics module: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error during import: {e}")


# ============================================
# TEST 2: Adapter Instantiation
# ============================================
def test_smoke_can_instantiate_adapter():
    """Smoke test 2/6: Verify adapter can be instantiated."""
    from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
        CoinMetricsCsvAdapter,
    )

    try:
        # Test default instantiation
        adapter = CoinMetricsCsvAdapter()

        # Verify basic attributes
        assert hasattr(adapter, "venue")
        assert hasattr(adapter, "client_type")
        assert hasattr(adapter, "connection_type")
        assert hasattr(adapter, "supported_asset_classes")
        assert hasattr(adapter, "capabilities")

        # Verify from base adapter
        assert hasattr(adapter, "validate_instrument")
        assert hasattr(adapter, "supports_asset_class")
        assert hasattr(adapter, "supports_capability")

        print("✓ CoinMetrics adapter instantiates successfully")

    except Exception as e:
        pytest.fail(f"Failed to instantiate CoinMetrics adapter: {e}")


# ============================================
# TEST 3: Adapter Configuration
# ============================================
def test_smoke_adapter_has_correct_configuration():
    """Smoke test 3/6: Verify adapter has correct configuration."""
    from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
        CoinMetricsCsvAdapter,
    )
    from quant_framework.ingestion.models.enums import ClientType, ConnectionType
    from quant_framework.ingestion.ports.data_ports import OnChainDataPort
    from quant_framework.shared.models.enums import DataVenue

    adapter = CoinMetricsCsvAdapter()

    # Verify venue
    assert adapter.venue == DataVenue.COINMETRICS
    assert isinstance(adapter.venue, DataVenue)

    # Verify client and connection types
    assert adapter.client_type == ClientType.FILE_PARSER
    assert adapter.connection_type == ConnectionType.FILE

    # Verify capabilities
    assert OnChainDataPort in adapter.capabilities
    assert len(adapter.capabilities) == 1

    print("✓ CoinMetrics adapter has correct configuration")


# ============================================
# TEST 4: Async Interface
# ============================================
@pytest.mark.asyncio
async def test_smoke_async_context_manager():
    """Smoke test 4/6: Verify adapter supports async context manager."""
    from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
        CoinMetricsCsvAdapter,
    )

    try:
        # Test async context manager
        async with CoinMetricsCsvAdapter() as adapter:
            # Verify it works inside context
            assert adapter is not None
            assert hasattr(adapter, "connect")
            assert hasattr(adapter, "close")

        print("✓ CoinMetrics adapter works as async context manager")

    except Exception as e:
        pytest.fail(f"Async context manager failed: {e}")


# ============================================
# TEST 5: Core Methods Exist
# ============================================
def test_smoke_core_methods_exist():
    """Smoke test 5/6: Verify all core methods exist and are callable."""
    from quant_framework.ingestion.adapters.coinmetrics_plugin.adapter import (
        CoinMetricsCsvAdapter,
    )

    adapter = CoinMetricsCsvAdapter()

    # Check required async methods
    required_methods = [
        "connect",
        "close",
        "fetch_onchain_history",
        "list_available_assets",
    ]

    for method_name in required_methods:
        assert hasattr(adapter, method_name), f"Missing method: {method_name}"

        method = getattr(adapter, method_name)
        assert callable(method), f"Method {method_name} is not callable"

    # Check inherited methods
    inherited_methods = [
        "validate_instrument",
        "supports_asset_class",
        "supports_capability",
    ]

    for method_name in inherited_methods:
        assert hasattr(adapter, method_name), f"Missing inherited method: {method_name}"

        method = getattr(adapter, method_name)
        assert callable(method), f"Inherited method {method_name} is not callable"

    print("✓ All core methods exist and are callable")


# ============================================
# TEST 6: Optional - Quick Dependency Check
# ============================================
def test_smoke_dependencies_available():
    """Optional smoke test: Verify required dependencies are available."""
    try:
        import asyncio

        import aiohttp
        import pandas as pd

        # Check versions if needed
        assert aiohttp.__version__ is not None
        assert pd.__version__ is not None

        print("✓ Required dependencies (aiohttp, pandas) are available")

    except ImportError as e:
        pytest.skip(f"Dependency not available: {e}")


# ============================================
# RUN ALL SMOKE TESTS
# ============================================
if __name__ == "__main__":
    # Run smoke tests directly
    import sys

    import pytest

    print("=" * 60)
    print("RUNNING COINMETRICS SMOKE TESTS")
    print("=" * 60)

    # Run all smoke tests
    exit_code = pytest.main(
        [
            __file__,
            "-v",  # verbose
            "--tb=short",  # short traceback
            "--disable-warnings",  # disable warnings for clean output
            "-k",
            "smoke",  # run only smoke tests
        ]
    )

    if exit_code == 0:
        print("\n" + "=" * 60)
        print("✅ ALL SMOKE TESTS PASSED")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ SOME SMOKE TESTS FAILED")
        print("=" * 60)
        sys.exit(1)
