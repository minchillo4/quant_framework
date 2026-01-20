"""Test consistency between pandas and polars feature engineering implementations."""

import numpy as np
import pandas as pd
import polars as pl
import pytest


def generate_realistic_ohlcv(n_rows: int = 1000) -> pd.DataFrame:
    """Generate realistic OHLCV financial data."""
    np.random.seed(42)

    base_price = 100.0

    # Generate realistic price returns with volatility clustering
    returns = np.random.normal(0, 0.02, n_rows)
    prices = base_price * np.cumprod(1 + returns)

    # Generate OHLC with proper relationships
    close = prices
    open_price = np.roll(close, 1)
    open_price[0] = close[0]

    # Generate high and low with realistic intraday ranges
    intraday_volatility = np.abs(returns) * 0.5
    high = np.maximum(open_price, close) * (1 + np.random.uniform(0, 0.01, n_rows))
    low = np.minimum(open_price, close) * (1 - np.random.uniform(0, 0.01, n_rows))

    # Ensure proper OHLC relationships
    high = np.maximum(high, np.maximum(open_price, close))
    low = np.minimum(low, np.minimum(open_price, close))

    # Generate volume with some correlation to price movement
    volume = np.random.lognormal(mean=12, sigma=0.5, size=n_rows)
    volume = volume * (1 + np.abs(returns) * 2)  # Higher volume with larger moves

    # Create DataFrame with proper column names
    df = pd.DataFrame(
        {
            "o": open_price,
            "h": high,
            "l": low,
            "c": close,
            "v": volume,
        },
        index=pd.date_range(start="2020-01-01", periods=n_rows, freq="D"),
    )

    return df


def compare_series(
    test_name: str,
    pandas_series: pd.Series,
    polars_series: pl.Series,
    tolerance: float = 1e-10,
) -> bool:
    """Compare pandas Series with polars Series for numerical consistency."""
    pandas_vals = pandas_series.values
    polars_vals = polars_series.to_numpy()

    # Check shape
    if pandas_vals.shape != polars_vals.shape:
        print(
            f"  ❌ {test_name}: Shape mismatch - pandas: {pandas_vals.shape}, polars: {polars_vals.shape}"
        )
        return False

    # Create masks for valid (non-NaN) values
    mask = ~(np.isnan(pandas_vals) | np.isnan(polars_vals))

    if mask.sum() == 0:
        # All NaN - considered a match
        print(f"  ✓ {test_name}: All NaN values match")
        return True

    # Calculate differences for valid values only
    diff = np.abs(pandas_vals[mask] - polars_vals[mask])
    max_diff = np.max(diff)
    mean_diff = np.mean(diff)

    # Check tolerance
    if max_diff < tolerance:
        print(f"  ✓ {test_name}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}")
        return True
    else:
        print(f"  ❌ {test_name}: max_diff={max_diff:.2e} > {tolerance:.2e}")
        return False


def compare_dataframes(
    test_name: str,
    pandas_df: pd.DataFrame,
    polars_df: pl.DataFrame,
    tolerance: float = 1e-10,
) -> bool:
    """Compare pandas DataFrame with polars DataFrame for numerical consistency."""
    pandas_vals = pandas_df.values
    polars_vals = polars_df.to_numpy()

    # Check shape
    if pandas_vals.shape != polars_vals.shape:
        print(
            f"  ❌ {test_name}: Shape mismatch - pandas: {pandas_vals.shape}, polars: {polars_vals.shape}"
        )
        return False

    # Create masks for valid (non-NaN) values
    mask = ~(np.isnan(pandas_vals) | np.isnan(polars_vals))

    if mask.sum() == 0:
        # All NaN - considered a match
        print(f"  ✓ {test_name}: All NaN values match")
        return True

    # Calculate differences for valid values only
    diff = np.abs(pandas_vals[mask] - polars_vals[mask])
    max_diff = np.max(diff)
    mean_diff = np.mean(diff)

    # Check tolerance
    if max_diff < tolerance:
        print(f"  ✓ {test_name}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}")
        return True
    else:
        print(f"  ❌ {test_name}: max_diff={max_diff:.2e} > {tolerance:.2e}")
        return False


class TestFeatureEngineeringConsistency:
    """Test consistency between pandas and polars feature engineering implementations."""

    def setup_method(self):
        """Generate consistent mock data for each test."""
        self.mock_data = generate_realistic_ohlcv(n_rows=1000)
        self.mock_data_pl = pl.from_pandas(self.mock_data)

    def test_basic_returns(self):
        """Test all return calculation methods."""
        print("\n=== Testing Basic Returns ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test candle return
        feat_name_pandas = fe_pandas.calculate_candle_return(lookback=1)
        feat_name_polars = fe_polars.calculate_candle_return(lookback=1)
        assert compare_series(
            "candle_return",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

        # Test log return
        feat_name_pandas = fe_pandas.calculate_log_return()
        feat_name_polars = fe_polars.calculate_log_return()
        assert compare_series(
            "log_return",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

        # Test gap return
        feat_name_pandas = fe_pandas.calculate_gap_return(lookback=1)
        feat_name_polars = fe_polars.calculate_gap_return(lookback=1)
        assert compare_series(
            "gap_return",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

        # Test past return with different lookback
        feat_name_pandas = fe_pandas.calculate_past_return("c", lookback=5)
        feat_name_polars = fe_polars.calculate_past_return("c", lookback=5)
        assert compare_series(
            "past_return_5",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

    def test_log_transformations(self):
        """Test log transformation methods."""
        print("\n=== Testing Log Transformations ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test log of close
        feat_name_pandas = fe_pandas.calculate_log("c")
        feat_name_polars = fe_polars.calculate_log("c")
        assert compare_series(
            "log_close",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

        # Test log of volume
        feat_name_pandas = fe_pandas.calculate_log("v")
        feat_name_polars = fe_polars.calculate_log("v")
        assert compare_series(
            "log_volume",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

    def test_differencing(self):
        """Test differencing methods."""
        print("\n=== Testing Differencing ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test volume diff with different periods
        for periods in [1, 5, 20]:
            feat_name_pandas = fe_pandas.calculate_diff("v", periods=periods)
            feat_name_polars = fe_polars.calculate_diff("v", periods=periods)
            assert compare_series(
                f"diff_volume_{periods}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
            )

    def test_rate_of_change(self):
        """Test percentage change methods."""
        print("\n=== Testing Rate of Change ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test pct_change with different periods
        for periods in [1, 5, 20]:
            feat_name_pandas = fe_pandas.calculate_pct_change("v", periods=periods)
            feat_name_polars = fe_polars.calculate_pct_change("v", periods=periods)
            assert compare_series(
                f"pct_change_volume_{periods}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
            )

    def test_moving_averages(self):
        """Test moving average calculations."""
        print("\n=== Testing Moving Averages ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test SMA with different windows
        for window in [5, 20, 50, 200]:
            feat_name_pandas = fe_pandas.calculate_moving_average("v", window, "sma")
            feat_name_polars = fe_polars.calculate_moving_average("v", window, "sma")
            # Use slightly higher tolerance for rolling operations
            assert compare_series(
                f"sma_volume_{window}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
                tolerance=1e-10,
            )

        # Test EMA with different windows
        for window in [10, 50]:
            feat_name_pandas = fe_pandas.calculate_moving_average("c", window, "ema")
            feat_name_polars = fe_polars.calculate_moving_average("c", window, "ema")
            # EMA can have slightly larger numerical differences
            assert compare_series(
                f"ema_close_{window}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
                tolerance=1e-6,
            )

        # Test price vs MA
        for window in [20, 200]:
            feat_name_pandas = fe_pandas.calculate_price_vs_ma("c", window, "sma")
            feat_name_polars = fe_polars.calculate_price_vs_ma("c", window, "sma")
            assert compare_series(
                f"price_vs_ma_{window}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
            )

        # Test volume vs MA
        feat_name_pandas = fe_pandas.calculate_volume_vs_ma(window=200)
        feat_name_polars = fe_polars.calculate_volume_vs_ma(window=200)
        assert compare_series(
            "volume_vs_ma_200",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

    def test_zscores(self):
        """Test z-score calculations."""
        print("\n=== Testing Z-Scores ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test zscore with different windows
        for window in [50, 200]:
            feat_name_pandas = fe_pandas.calculate_zscore("c", window=window)
            feat_name_polars = fe_polars.calculate_zscore("c", window=window)
            # Use higher tolerance for rolling z-scores
            assert compare_series(
                f"zscore_close_{window}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
                tolerance=1e-6,
            )

    def test_percentile_ranks(self):
        """Test percentile rank calculations."""
        print("\n=== Testing Percentile Ranks ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test rolling percentile with different windows
        for window in [50, 200]:
            feat_name_pandas = fe_pandas.calculate_rolling_percentile(
                "v", window=window
            )
            feat_name_polars = fe_polars.calculate_rolling_percentile(
                "v", window=window
            )
            # Percentile calculations might have slight differences
            assert compare_series(
                f"rolling_percentile_volume_{window}",
                fe_pandas.features[feat_name_pandas],
                fe_polars.features[feat_name_polars],
                tolerance=1e-10,
            )

    def test_lag_features(self):
        """Test lag feature creation."""
        print("\n=== Testing Lag Features ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test individual lag features
        for column in ["c", "v"]:
            for periods in [1, 5, 10]:
                feat_name_pandas = fe_pandas.calculate_lag(column, periods=periods)
                feat_name_polars = fe_polars.calculate_lag(column, periods=periods)
                assert compare_series(
                    f"lag_{column}_{periods}",
                    fe_pandas.features[feat_name_pandas],
                    fe_polars.features[feat_name_polars],
                )

        # Test batch lag feature creation
        pandas_features = fe_pandas.create_lag_features(
            columns=["c", "v"], periods=[1, 2, 5]
        )
        polars_features = fe_polars.create_lag_features(
            columns=["c", "v"], periods=[1, 2, 5]
        )

        assert len(pandas_features) == len(polars_features), (
            f"Mismatch in number of lag features: {len(pandas_features)} vs {len(polars_features)}"
        )

        # Compare each lag feature
        for i, (pandas_feat, polars_feat) in enumerate(
            zip(pandas_features, polars_features)
        ):
            assert compare_series(
                f"batch_lag_{i}",
                fe_pandas.features[pandas_feat],
                fe_polars.features[polars_feat],
            )

    def test_sign_and_sum(self):
        """Test sign calculation and sum operations."""
        print("\n=== Testing Sign and Sum ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test sign calculation
        feat_name_pandas = fe_pandas.calculate_sign("c")
        feat_name_polars = fe_polars.calculate_sign("c")
        assert compare_series(
            "sign_close",
            fe_pandas.features[feat_name_pandas],
            fe_polars.features[feat_name_polars],
        )

        # Test plus/minus sum
        feat_name_pandas_sum = fe_pandas.calculate_plus_minus(
            feat_name_pandas, window=20
        )
        feat_name_polars_sum = fe_polars.calculate_plus_minus(
            feat_name_polars, window=20
        )
        assert compare_series(
            "plus_minus_sum",
            fe_pandas.features[feat_name_pandas_sum],
            fe_polars.features[feat_name_polars_sum],
            tolerance=1e-10,
        )

    def test_rolling_quantile_bins(self):
        """Test rolling quantile binning."""
        print("\n=== Testing Rolling Quantile Bins ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Test with different parameters
        for window in [100, 252]:
            for n_bins in [5, 10]:
                feat_name_pandas = fe_pandas.calculate_rolling_quantile_bins(
                    "c", window=window, n_bins=n_bins
                )
                feat_name_polars = fe_polars.calculate_rolling_quantile_bins(
                    "c", window=window, n_bins=n_bins
                )
                # Quantile bins should be exact match
                assert compare_series(
                    f"rolling_quantile_bins_{window}_{n_bins}",
                    fe_pandas.features[feat_name_pandas],
                    fe_polars.features[feat_name_polars],
                    tolerance=1e-10,
                )

    def test_comprehensive_feature_set(self):
        """Test building the complete feature set."""
        print("\n=== Testing Comprehensive Feature Set ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Build comprehensive feature sets
        pandas_features = fe_pandas.build_comprehensive_feature_set()
        polars_features = fe_polars.build_comprehensive_feature_set()

        # Check number of features
        assert pandas_features.shape[1] == polars_features.width, (
            f"Mismatch in number of features: {pandas_features.shape[1]} vs {polars_features.width}"
        )

        print(
            f"\n  Total features created: pandas={pandas_features.shape[1]}, polars={polars_features.width}"
        )

        # Compare each feature
        all_passed = True
        for col_name in pandas_features.columns:
            if col_name in polars_features.columns:
                if not compare_series(
                    f"comprehensive_{col_name}",
                    pandas_features[col_name],
                    polars_features[col_name],
                    tolerance=1e-6,
                ):
                    all_passed = False

        assert all_passed, "Some features in comprehensive set did not match"

    def test_scaling_operations(self):
        """Test feature scaling operations."""
        print("\n=== Testing Scaling Operations ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Build some features first
        fe_pandas.calculate_candle_return()
        fe_pandas.calculate_log_return()
        fe_pandas.calculate_diff("v", 1)

        fe_polars.calculate_candle_return()
        fe_polars.calculate_log_return()
        fe_polars.calculate_diff("v", 1)

        # Test standard scaling
        pandas_scaled = fe_pandas.scale_features(method="standard", copy=True)
        polars_scaled = fe_polars.scale_features(method="standard", copy=True)

        # Convert polars to pandas for comparison
        polars_scaled_pd = polars_scaled.to_pandas()

        # Compare scaled features
        assert compare_dataframes(
            "standard_scaler",
            pandas_scaled,
            polars_scaled,
            tolerance=1e-6,
        )

        # Test normalize scaling
        fe_pandas2 = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars2 = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        fe_pandas2.calculate_candle_return()
        fe_polars2.calculate_candle_return()

        pandas_normalized = fe_pandas2.scale_features(method="normalize", copy=True)
        polars_normalized = fe_polars2.scale_features(method="normalize", copy=True)

        assert compare_dataframes(
            "normalize_scaler",
            pandas_normalized,
            polars_normalized,
            tolerance=1e-6,
        )

    def test_feature_descriptions(self):
        """Test that feature descriptions match between implementations."""
        print("\n=== Testing Feature Descriptions ===")

        from quant_framework.features.generators.fe_pandas import (
            FeatureEngineer as FeatureEngineerPandas,
        )
        from quant_framework.features.generators.fe_polars import (
            FeatureEngineer as FeatureEngineerPolars,
        )

        fe_pandas = FeatureEngineerPandas(self.mock_data, validate=False)
        fe_polars = FeatureEngineerPolars(self.mock_data_pl, validate=False)

        # Create same set of features in both
        fe_pandas.calculate_candle_return()
        fe_polars.calculate_candle_return()

        fe_pandas.calculate_log_return()
        fe_polars.calculate_log_return()

        fe_pandas.calculate_log("v")
        fe_polars.calculate_log("v")

        fe_pandas.calculate_diff("v", 1)
        fe_polars.calculate_diff("v", 1)

        # Get feature descriptions
        pandas_desc = fe_pandas.get_feature_descriptions()
        polars_desc = fe_polars.get_feature_descriptions()

        # Check that descriptions match
        assert pandas_desc == polars_desc, (
            "Feature descriptions do not match between implementations"
        )

        # Get feature names
        pandas_names = fe_pandas.get_feature_names()
        polars_names = fe_polars.get_feature_names()

        assert pandas_names == polars_names, (
            "Feature names do not match between implementations"
        )

        print(f"  ✓ Feature descriptions match ({len(pandas_desc)} features)")
        print(f"  ✓ Feature names match ({len(pandas_names)} features)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
