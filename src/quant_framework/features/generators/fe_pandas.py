"""Feature engineering module for financial data."""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import Normalizer, StandardScaler


class FeatureEngineer:
    """Feature engineering class for financial time series data."""

    def __init__(
        self,
        df: pd.DataFrame,
        feature_prefix: str = "f",
        validate: bool = False,
        config: dict | None = None,
    ) -> None:
        """Initialize with validated OHLCV DataFrame."""
        # Setup project paths
        current_dir = Path(__file__).resolve().parent
        project_root = current_dir.parent.parent.parent
        import sys

        sys.path.append(str(project_root))

        # Validation removed - DataFrame is assumed to be pre-validated
        self.df = df.copy()

        self.feature_prefix = feature_prefix
        self._counter = 1
        self._feature_descriptions = {}

        # Initialize empty features DataFrame with same index
        self.features = pd.DataFrame(index=self.df.index)

        # Default configuration
        self.config = {
            "windows": [5, 10, 20, 50, 200],
            "min_periods": 20,
            "zscore_window": 200,
            "percentile_window": 200,
            "n_bins": 10,
            "mfi_period": 14,
        }

    def _next_feature_name(self) -> str:
        """Generate next feature name dynamically."""
        name = f"{self.feature_prefix}{self._counter:02d}"
        self._counter += 1
        return name

    def _add_feature(
        self,
        series: pd.Series,
        name: str | None = None,
        description: str = "",
    ) -> str:
        """Add a feature series to the features DataFrame and track description."""
        if name is None:
            name = self._next_feature_name()

        # Direct assignment - index should already be aligned from calling methods
        self.features[name] = series

        # Store description
        self._feature_descriptions[name] = description

        return name

    def _is_multi_index(self) -> bool:
        """Check if DataFrame has MultiIndex."""
        return isinstance(self.df.index, pd.MultiIndex)

    # ==================== Feature Descriptions ====================

    def get_feature_descriptions(self) -> dict:
        """Return dictionary of feature descriptions."""
        return self._feature_descriptions.copy()

    def print_feature_summary(self) -> None:
        """Print a summary of all features created."""
        print(f"\n{'=' * 60}")
        print("FEATURE SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total features: {len(self.features.columns)}")
        print(f"{'-' * 60}")

        for i, (feature_name, description) in enumerate(
            self._feature_descriptions.items(),
            1,
        ):
            print(f"{i:3d}. {feature_name}: {description}")

        print(f"{'=' * 60}")

        # Add statistics
        print("\nFEATURE STATISTICS:")
        print(f"{'-' * 60}")
        if not self.features.empty:
            print(f"Feature matrix shape: {self.features.shape}")
            print(f"Missing values: {self.features.isna().sum().sum():,} total")

            # Show first few non-null percentages
            non_null_pct = (self.features.count() / len(self.features) * 100).round(1)
            print("\nNon-null percentage (top 10 features):")
            for feat, pct in non_null_pct.head(10).items():
                print(f"  {feat}: {pct}%")

    # ==================== Basic Returns ====================
    # ==================== Return Calculations (PAST RETURNS ONLY) ====================

    def calculate_past_return(self, column: str = "c", lookback: int = 1) -> str:
        """Calculate return over past N periods: price_t / price_{t-lookback} - 1."""
        if self._is_multi_index():
            past_return = self.df.groupby(level="symbol")[column].pct_change(lookback)
        else:
            past_return = self.df[column].pct_change(lookback)

        desc = f"{lookback}-period return of {column}"
        return self._add_feature(past_return, description=desc)

    def calculate_past_log_return(
        self,
        column: str = "c",
        lookback: int = 1,
        epsilon: float = 1e-10,
        clip_absolute: tuple | None = (-5.0, 5.0),
    ) -> str:
        """Calculate log return over past N periods with robust infinity protection.

        Formula: ln((price_t + epsilon) / (price_{t-lookback} + epsilon)).

        Parameters
        ----------
        column : str
            Column to calculate returns on
        lookback : int
            Number of periods to look back
        epsilon : float
            Small value added to prevent log(0) and division by zero (default: 1e-10)
        clip_absolute : tuple or None
            Absolute (min, max) values to clip log returns (default: (-5, 5) ≈ -99% to +14,800%)
            Set to None to disable clipping

        Returns
        -------
        str : Feature name

        """
        if self._is_multi_index():
            shifted = self.df.groupby(level="symbol")[column].shift(lookback)
        else:
            shifted = self.df[column].shift(lookback)

        # Add epsilon to both numerator and denominator to prevent division by zero
        numerator = self.df[column] + epsilon
        denominator = shifted + epsilon

        # Calculate log return
        log_return = np.log(numerator / denominator)

        # Replace any remaining inf/nan with 0 (from first rows or data issues)
        log_return = log_return.replace([np.inf, -np.inf], np.nan)

        # Clip extreme values to reasonable range
        if clip_absolute is not None:
            log_return = log_return.clip(lower=clip_absolute[0], upper=clip_absolute[1])

        desc = f"{lookback}-period log return of {column} (epsilon={epsilon}, clipped={clip_absolute})"
        return self._add_feature(log_return, description=desc)

    def calculate_candle_return(self, lookback: int = 1) -> str:
        """Candle return over N periods: close_t / close_{t-lookback} - 1."""
        return self.calculate_past_return("c", lookback)

    def calculate_gap_return(self, lookback: int = 1) -> str:
        """Gap return: open_t / close_{t-lookback} - 1."""
        if self._is_multi_index():
            prev_close = self.df.groupby(level="symbol")["c"].shift(lookback)
        else:
            prev_close = self.df["c"].shift(lookback)

        gap = self.df["o"] / prev_close - 1
        desc = f"{lookback}-period gap return: open/prev_close - 1"
        return self._add_feature(gap, description=desc)

    # Keep for backward compatibility (defaults to daily)
    def calculate_log_return(self) -> str:
        """Daily log return: ln(close/open) - kept for backward compatibility."""
        return self.calculate_past_log_return("c", lookback=1)

    # ==================== Logs ====================

    def calculate_log(self, column: str) -> str:
        """Log transform of specified column."""
        log_val = self.df[column].apply(np.log)
        return self._add_feature(log_val, description=f"Log of {column}")

    # ==================== Differencing ====================
    def calculate_diff(self, column: str, periods: int = 1) -> str:
        """Change in column value since N periods ago."""
        if self._is_multi_index():
            diff = self.df.groupby(level="symbol")[column].diff(periods)
        else:
            diff = self.df[column].diff(periods)
        desc = f"{column} change over {periods} period(s)"
        return self._add_feature(diff, description=desc)

    # ==================== Rate of Change ====================

    def calculate_pct_change(
        self,
        column: str,
        periods: int = 1,
        epsilon: float = 1e-10,
        clip_range: tuple | None = (-10.0, 10.0),
    ) -> str:
        """Percent change with infinity protection for zero-prone features.

        Formula: (x_t - x_{t-periods}) / (abs(x_{t-periods}) + epsilon).

        Parameters
        ----------
        column : str
            Column to calculate percent change on
        periods : int
            Number of periods for comparison
        epsilon : float
            Small value added to denominator to prevent division by zero (default: 1e-10)
        clip_range : tuple or None
            (min, max) values to clip percentage changes (default: (-10, 10) = -1000% to +1000%)
            Set to None to disable clipping

        Returns
        -------
        str : Feature name

        """
        if self._is_multi_index():
            current = self.df[column]
            previous = self.df.groupby(level="symbol")[column].shift(periods)
        else:
            current = self.df[column]
            previous = self.df[column].shift(periods)

        # Calculate percent change with epsilon protection
        # Use abs() on denominator to handle negative values properly
        pct_change = (current - previous) / (previous.abs() + epsilon)

        # Clip extreme values if specified
        if clip_range is not None:
            pct_change = pct_change.clip(lower=clip_range[0], upper=clip_range[1])

        desc = f"{periods}-period % change of {column} (epsilon={epsilon}, clipped={clip_range})"
        return self._add_feature(pct_change, description=desc)

    # ==================== Moving Averages ====================

    def calculate_moving_average(
        self,
        column: str,
        window: int,
        ma_type: str = "sma",
    ) -> str:
        """Calculate moving average using lambda wrapper."""

        def sma_fn(x):
            return x.rolling(window).mean()

        def ema_fn(x):
            return x.ewm(span=window).mean()

        if ma_type == "sma":
            ma_fn = sma_fn
            type_desc = "simple"
        else:  # ema
            ma_fn = ema_fn
            type_desc = "exponential"

        if self._is_multi_index():
            result = self.df[column].groupby(level="symbol").apply(ma_fn)
            if result.index.nlevels > 2:
                result = result.droplevel(0)
        else:
            result = ma_fn(self.df[column])

        desc = f"{window}-day {type_desc} MA of {column}"
        return self._add_feature(result, description=desc)

    def calculate_price_vs_ma(
        self,
        column: str = "c",
        window: int = 200,
        ma_type: str = "sma",
    ) -> str:
        """Calculate daily price vs moving average: price/MA - 1."""

        def sma_fn(x):
            return x.rolling(window).mean()

        def ema_fn(x):
            return x.ewm(span=window).mean()

        if ma_type == "sma":
            ma_fn = sma_fn
            type_desc = "SMA"
        else:
            ma_fn = ema_fn
            type_desc = "EMA"

        if self._is_multi_index():
            ma = self.df[column].groupby(level="symbol").apply(ma_fn)
            if ma.index.nlevels > 2:
                ma = ma.droplevel(0)
        else:
            ma = ma_fn(self.df[column])

        ratio = self.df[column] / ma - 1
        desc = f"{column} vs {window}-day {type_desc}: (price/MA - 1)"
        return self._add_feature(ratio, description=desc)

    def calculate_volume_vs_ma(self, window: int = 200) -> str:
        """Calculate daily volume vs moving average."""

        def ma_fn(x):
            return x.rolling(window).mean()

        if self._is_multi_index():
            ma = self.df["v"].groupby(level="symbol").apply(ma_fn)
            if ma.index.nlevels > 2:
                ma = ma.droplevel(0)
        else:
            ma = ma_fn(self.df["v"])

        ratio = self.df["v"] / ma - 1
        desc = f"Volume vs {window}-day MA: (volume/MA - 1)"
        return self._add_feature(ratio, description=desc)

    # ==================== Z-Scores ====================

    def calculate_zscore(
        self,
        column: str,
        window: int | None = None,
        min_periods: int | None = None,
    ) -> str:
        """Calculate rolling z-score avoiding future peeking."""
        if window is None:
            window = self.config["zscore_window"]
        if min_periods is None:
            min_periods = self.config["min_periods"]

        def zscore_fn(x):
            return (
                x - x.rolling(window=window, min_periods=min_periods).mean()
            ) / x.rolling(window=window, min_periods=min_periods).std()

        if self._is_multi_index():
            result = self.df.groupby(level="symbol")[column].apply(zscore_fn)
            if result.index.nlevels > 2:
                result = result.droplevel(0)
        else:
            result = zscore_fn(self.df[column])

        desc = f"{window}-day rolling z-score of {column}"
        return self._add_feature(result, description=desc)

    # ==================== Percentile Rank ====================

    def calculate_rolling_percentile(
        self,
        column: str,
        window: int | None = None,
        min_periods: int | None = None,
    ) -> str:
        """Calculate rolling percentile rank (longitudinal)."""
        if window is None:
            window = self.config["percentile_window"]
        if min_periods is None:
            min_periods = self.config["min_periods"]

        def rollrank_fn(x):
            return x.rolling(window, min_periods=min_periods).apply(
                lambda y: pd.Series(y).rank(pct=True).iloc[-1],
            )

        if self._is_multi_index():
            result = self.df.groupby(level="symbol")[column].apply(rollrank_fn)
            if result.index.nlevels > 2:
                result = result.droplevel(0)
        else:
            result = rollrank_fn(self.df[column])

        desc = f"{window}-day percentile rank of {column}"
        return self._add_feature(result, description=desc)

    def calculate_cross_sectional_rank(self, feature_name: str) -> str:
        """Calculate cross-sectional rank (rank stocks against each other on each day)."""
        if not self._is_multi_index():
            msg = "Cross-sectional rank requires MultiIndex (date, symbol)"
            raise ValueError(msg)

        ranked = (
            self.features[feature_name].dropna().groupby(level="date").rank(pct=True)
        )

        desc = f"Cross-sectional rank of {feature_name} against peers"
        return self._add_feature(ranked, description=desc)

    # ==================== Lag Features (NEW) ====================

    def calculate_lag(self, column: str, periods: int = 1) -> str:
        """Create lagged version of a column."""
        if self._is_multi_index():
            lagged = self.df.groupby(level="symbol")[column].shift(periods)
        else:
            lagged = self.df[column].shift(periods)

        desc = f"{column} lagged by {periods} period(s)"
        return self._add_feature(lagged, description=desc)

    def create_lag_features(
        self,
        columns: list[str] | None = None,
        periods: list[int] | None = None,
    ) -> list[str]:
        """Create multiple lag features at once."""
        if columns is None:
            columns = ["c", "v", "o", "h", "l"]  # Default: price and volume columns
        if periods is None:
            periods = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 24]  # Default lag periods

        created_features = []
        for column in columns:
            if column not in self.df.columns:
                print(f"Warning: Column '{column}' not found in DataFrame")
                continue

            for period in periods:
                feat_name = self.calculate_lag(column, period)
                created_features.append(feat_name)

        return created_features

    # ==================== Alternative Representations ====================

    def calculate_rolling_quantile_bins(
        self,
        column: str,
        window: int = 252,  # 1 year lookback
        n_bins: int = 5,
        min_periods: int | None = None,
    ) -> str:
        """Assign bins based on rolling quantiles (NO FUTURE LEAKAGE).

        For each time point, bins are determined by quantiles from the
        previous 'window' periods only.

        Parameters
        ----------
        column : str
            Column to bin
        window : int
            Lookback window for calculating quantiles
        n_bins : int
            Number of bins (e.g., 5 = quintiles)
        min_periods : int
            Minimum observations needed (default: window // 2)

        Returns
        -------
        str : Feature name

        """
        if min_periods is None:
            min_periods = max(n_bins * 2, window // 2)

        def assign_bin_from_rolling_quantiles(series: pd.Series) -> pd.Series:
            """Assign current value to bin based on past quantiles."""
            bins = []

            for i in range(len(series)):
                if i < min_periods:
                    bins.append(np.nan)
                    continue

                # Use only PAST data to calculate quantiles
                lookback_start = max(0, i - window)
                historical_data = series.iloc[
                    lookback_start:i
                ]  # Exclude current point!

                if len(historical_data) < min_periods:
                    bins.append(np.nan)
                    continue

                # Calculate quantile thresholds from historical data
                quantiles = np.linspace(0, 1, n_bins + 1)
                thresholds = historical_data.quantile(quantiles).to_numpy()

                # Assign current value to bin
                current_value = series.iloc[i]
                # 1 to n_bins
                bin_assigned = np.digitize(current_value, thresholds[1:-1]) + 1
                bins.append(bin_assigned)

            return pd.Series(bins, index=series.index)

        if self._is_multi_index():
            result = self.df.groupby(level="symbol")[column].apply(
                assign_bin_from_rolling_quantiles,
            )
            if result.index.nlevels > 2:
                result = result.droplevel(0)
        else:
            result = assign_bin_from_rolling_quantiles(self.df[column])

        desc = (
            f"{n_bins}-bin rolling quantile of {column} (window={window}, NO LEAKAGE)"
        )
        return self._add_feature(result, description=desc)

    def calculate_sign(self, column: str) -> str:
        """Convert values to sign (+1, 0, -1)."""
        sign = self.df[column].apply(np.sign)
        desc = f"Sign of {column} (+1, 0, or -1)"
        return self._add_feature(sign, description=desc)

    def calculate_plus_minus(self, sign_feature: str, window: int = 20) -> str:
        """Calculate net positive/negative days (sum of signs over window)."""
        if self._is_multi_index():
            result = (
                self.features[sign_feature]
                .groupby(level="symbol")
                .rolling(window)
                .sum()
                .reset_index(0, drop=True)
            )
        else:
            result = self.features[sign_feature].rolling(window).sum()

        desc = f"{window}-day sum of signs from {sign_feature}"
        return self._add_feature(result, description=desc)

    # ==================== One-Hot Encoding ====================

    def create_time_features(self) -> list[str]:
        """Create one-hot encoded time features (month of year)."""
        if not self._is_multi_index():
            msg = "Time features require MultiIndex (date, symbol)"
            raise ValueError(msg)

        month_values = self.df.index.get_level_values(level="date").month
        one_hot_df = pd.get_dummies(month_values)
        one_hot_df.index = self.df.index

        feature_names = []
        for col in one_hot_df.columns:
            name = self._add_feature(
                one_hot_df[col],
                description=f"Month={col} indicator",
            )
            feature_names.append(name)

        return feature_names

    # ==================== Batch Operations ====================

    def build_comprehensive_feature_set(self) -> pd.DataFrame:
        """Build a comprehensive feature set following the notebook."""
        print("Building comprehensive feature set...")

        # Basic returns
        print("  - Basic returns...")
        self.calculate_candle_return()
        self.calculate_log_return()
        self.calculate_gap_return()

        # Logs
        print("  - Logarithms...")
        self.calculate_log("v")  # Changed from calculate_log_volume()

        # Differencing
        print("  - Differencing...")
        self.calculate_diff("v", 1)  # Changed from calculate_volume_diff(1)
        self.calculate_diff("v", 50)  # Changed from calculate_volume_diff(50)

        # Rate of change
        print("  - Rate of change...")
        self.calculate_pct_change("v", periods=1)

        # Moving averages
        print("  - Moving averages...")
        for window in [5, 50, 200]:
            self.calculate_moving_average("v", window, "sma")
            self.calculate_price_vs_ma("c", window, "sma")

        # EMA
        self.calculate_price_vs_ma("c", 50, "ema")

        # Volume vs MA
        self.calculate_volume_vs_ma(200)

        # Z-scores
        print("  - Z-scores...")
        self.calculate_zscore("c", window=200)

        # Percentile ranks
        print("  - Percentile ranks...")
        self.calculate_rolling_percentile("v", window=200)

        # Lag features
        print("  - Lag features...")
        self.create_lag_features(columns=["c", "v"], periods=[1, 2, 5, 10])

        # Try cross-sectional if multi-index
        if self._is_multi_index():
            try:
                vol_ma_feature = self.features.columns[-1]  # Last created feature
                self.calculate_cross_sectional_rank(vol_ma_feature)
            except Exception as e:
                print(f"    Skipping cross-sectional rank: {e}")

        # Technical indicators
        print("  - Technical indicators...")
        # self.calculate_money_flow_index()
        # self.calculate_mfi_mean_centered()

        # Alternative representations
        print("  - Alternative representations...")
        sign_feat = self.calculate_sign("c")
        self.calculate_plus_minus(sign_feat, window=20)

        # Time features
        if self._is_multi_index():
            print("  - Time features...")
            try:
                self.create_time_features()
            except Exception as e:
                print(f"    Skipping time features: {e}")

        print(f"\n✓ Created {len(self.features.columns)} features")
        return self.get_all_features()

    def get_all_features(self) -> pd.DataFrame:
        """Return the complete features DataFrame."""
        return self.features.copy()

    def get_feature_names(self) -> list[str]:
        """Return list of all feature names."""
        return self.features.columns.tolist()

    # ==================== Feature Scaling & Persistence ====================

    def scale_features(
        self,
        method: str = "standard",
        copy: bool = True,
        handle_inf: str = "remove",
        clip_quantile: float = 0.01,
    ) -> pd.DataFrame:
        """Scale all features using specified method with proper infinity handling.

        Parameters
        ----------
        method : str
            Scaling method: 'standard' (StandardScaler) or 'normalize' (Normalizer)
        copy : bool
            If True, return scaled copy. If False, modify self.features in place
        handle_inf : str
            How to handle infinities:
            - 'remove': Drop rows with any inf values (safest)
            - 'clip': Clip to min/max of finite values
            - 'nan': Replace inf with NaN then drop
        clip_quantile : float
            If handle_inf='clip', clip to this quantile (e.g., 0.01 = 1st/99th percentile)

        Returns
        -------
        pd.DataFrame : Scaled features with same index/columns

        """
        print(
            f"\nScaling features with method='{method}', handle_inf='{handle_inf}'...",
        )
        # Ensure we start fresh
        self.features = self.features.dropna()
        # Select only numeric columns
        features_numeric = self.features.select_dtypes(include=[np.number])
        print(f"  Starting shape: {features_numeric.shape}")

        # Handle infinities
        if handle_inf == "remove":
            # Remove rows with any infinity
            mask_finite = np.all(np.isfinite(features_numeric), axis=1)
            features_clean = features_numeric[mask_finite]
            n_removed = (~mask_finite).sum()
            if n_removed > 0:
                print(f"  ⚠️  Removed {n_removed:,} rows with infinity values")

        elif handle_inf == "clip":
            # Clip each column to quantile range
            features_clean = features_numeric.copy()
            for col in features_clean.columns:
                col_data = features_clean[col]
                if np.isinf(col_data).any():
                    finite_vals = col_data[np.isfinite(col_data)]
                    if len(finite_vals) > 0:
                        lower = finite_vals.quantile(clip_quantile)
                        upper = finite_vals.quantile(1 - clip_quantile)
                        features_clean[col] = col_data.clip(lower=lower, upper=upper)
                        print(f"  ⚠️  Clipped {col} to [{lower:.2e}, {upper:.2e}]")

        elif handle_inf == "nan":
            # Replace inf with NaN
            features_clean = features_numeric.replace([np.inf, -np.inf], np.nan)
            n_inf = np.isinf(features_numeric).sum().sum()
            if n_inf > 0:
                print(f"  ⚠️  Replaced {n_inf:,} infinity values with NaN")
        else:
            raise ValueError(f"Unknown handle_inf: {handle_inf}")

        # Drop remaining NaN values
        features_clean = features_clean.dropna()
        print(f"  After cleaning: {features_clean.shape}")

        if features_clean.empty:
            print("  ❌ ERROR: No valid features remaining after cleaning!")
            return features_clean

        # Check for remaining issues
        if np.isinf(features_clean.values).any():
            print("  ❌ ERROR: Infinities still present after cleaning!")
            return features_clean

        # Select scaler
        if method == "standard":
            scaler = StandardScaler()
        elif method == "normalize":
            scaler = Normalizer()
        else:
            raise ValueError(f"Unknown method: {method}. Use 'standard' or 'normalize'")

        # Scale
        try:
            features_scaled = scaler.fit_transform(features_clean)
        except ValueError as e:
            print(f"  ❌ ERROR during scaling: {e}")
            print("  Run features.check_feature_quality() to diagnose issues")
            raise

        # Convert back to DataFrame with original index/columns
        df_scaled = pd.DataFrame(
            features_scaled,
            index=features_clean.index,
            columns=features_clean.columns,
        )

        # Store scaler for later use
        self._scaler = scaler
        self._scaling_method = method

        if not copy:
            self.features = df_scaled
            return self.features

        print(f"\n✓ Successfully scaled {df_scaled.shape[1]} features")
        print(f"  Final shape: {df_scaled.shape}")
        print(
            f"  Rows retained: {len(df_scaled):,} / {len(self.features):,} ({len(df_scaled) / len(self.features) * 100:.1f}%)",
        )

        return df_scaled

    def check_feature_quality(self) -> pd.DataFrame:
        """Diagnose feature quality issues (NaN, inf, extreme values).

        Returns
        -------
        pd.DataFrame : Summary statistics for each feature

        """
        summary = []

        for col in self.features.columns:
            if not pd.api.types.is_numeric_dtype(self.features[col]):
                continue

            col_data = self.features[col]

            summary.append(
                {
                    "feature": col,
                    "n_total": len(col_data),
                    "n_nan": col_data.isna().sum(),
                    "n_inf": np.isinf(col_data).sum(),
                    "n_neg_inf": np.isneginf(col_data).sum(),
                    "n_pos_inf": np.isposinf(col_data).sum(),
                    "pct_valid": ((~col_data.isna()) & (~np.isinf(col_data))).sum()
                    / len(col_data)
                    * 100,
                    "min": col_data[np.isfinite(col_data)].min()
                    if np.isfinite(col_data).any()
                    else np.nan,
                    "max": col_data[np.isfinite(col_data)].max()
                    if np.isfinite(col_data).any()
                    else np.nan,
                    "mean": col_data[np.isfinite(col_data)].mean()
                    if np.isfinite(col_data).any()
                    else np.nan,
                },
            )

        df_summary = pd.DataFrame(summary)

        # Highlight problematic features
        print("\n" + "=" * 80)
        print("FEATURE QUALITY REPORT")
        print("=" * 80)

        problematic = df_summary[
            (df_summary["n_inf"] > 0) | (df_summary["pct_valid"] < 50)
        ]

        if len(problematic) > 0:
            print(f"\n⚠️  Found {len(problematic)} problematic features:")
            print(
                problematic[["feature", "n_nan", "n_inf", "pct_valid"]].to_string(
                    index=False,
                ),
            )
        else:
            print("\n✓ No problematic features detected")

        print("\nOverall Statistics:")
        print(f"  Total features: {len(df_summary)}")
        print(f"  Features with infinities: {(df_summary['n_inf'] > 0).sum()}")
        print(f"  Features with >50% missing: {(df_summary['pct_valid'] < 50).sum()}")
        print("=" * 80 + "\n")

        return df_summary

    def transform_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply previously fitted scaler to new data (e.g., test set).

        Parameters
        ----------
        df : pd.DataFrame
            Features to transform (must have same columns as training data)

        Returns
        -------
        pd.DataFrame : Transformed features

        """
        if not hasattr(self, "_scaler"):
            msg = "No scaler fitted. Call scale_features() first."
            raise ValueError(msg)

        features_clean = df.dropna()
        features_scaled = self._scaler.transform(features_clean)

        return pd.DataFrame(
            features_scaled,
            index=features_clean.index,
            columns=features_clean.columns,
        )

    def save_features(
        self,
        filepath: str,
        scaled: bool = False,
        file_format: str = "parquet",
    ) -> None:
        """Save features to disk.

        Parameters
        ----------
        filepath : str
            Path to save file (without extension)
        scaled : bool
            If True, scale before saving
        file_format : str
            File format: 'parquet' (default), 'csv', or 'pickle'

        """
        # Determine which features to save
        if scaled:
            if not hasattr(self, "_scaler"):
                print("Scaling features before saving...")
                df_to_save = self.scale_features(copy=True)
            else:
                df_to_save = self.scale_features(copy=True)
        else:
            df_to_save = self.features.copy()

        # Create directory if needed
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Save based on format
        if file_format == "parquet":
            save_path = filepath.with_suffix(".parquet")
            df_to_save.to_parquet(save_path)
        elif file_format == "csv":
            save_path = filepath.with_suffix(".csv")
            df_to_save.to_csv(save_path)
        elif file_format == "pickle":
            save_path = filepath.with_suffix(".pkl")
            df_to_save.to_pickle(save_path)
        else:
            raise ValueError(f"Unknown format: {file_format}")

        print(f"✓ Saved {df_to_save.shape[1]} features to: {save_path}")
        print(f"  Shape: {df_to_save.shape}")
        print(f"  Scaled: {scaled}")

    def save_feature_metadata(self, filepath: str) -> None:
        """Save feature descriptions and metadata to JSON.

        Parameters
        ----------
        filepath : str
            Path to save JSON file

        """
        filepath = Path(filepath).with_suffix(".json")
        filepath.parent.mkdir(parents=True, exist_ok=True)

        metadata = {
            "n_features": len(self.features.columns),
            "feature_names": list(self.features.columns),
            "feature_descriptions": self._feature_descriptions,
            "shape": list(self.features.shape),
            "config": self.config,
        }

        if hasattr(self, "_scaling_method"):
            metadata["scaling_method"] = self._scaling_method

        with filepath.open("w") as f:
            json.dump(metadata, f, indent=2)

        print(f"✓ Saved feature metadata to: {filepath}")

    def load_features(
        self, filepath: str, file_format: str = "parquet"
    ) -> pd.DataFrame:
        """Load features from disk.

        Parameters
        ----------
        filepath : str
            Path to load file (with or without extension)
        file_format : str
            File format: 'parquet', 'csv', or 'pickle'

        Returns
        -------
        pd.DataFrame : Loaded features

        """
        filepath = Path(filepath)

        if file_format == "parquet":
            filepath = filepath.with_suffix(".parquet")
            df = pd.read_parquet(filepath)
        elif file_format == "csv":
            filepath = filepath.with_suffix(".csv")
            df = pd.read_csv(filepath, index_col=0)
        elif file_format == "pickle":
            filepath = filepath.with_suffix(".pkl")
            df = pd.read_pickle(filepath)
        else:
            raise ValueError(f"Unknown format: {file_format}")

        self.features = df
        print(f"✓ Loaded {df.shape[1]} features from: {filepath}")
        print(f"  Shape: {df.shape}")

        return df
