import pytest
from pyspark.sql import SparkSession

from src.streaming.gold.transformations import (
    transform_main_data,
    transform_rolling_average,
)
from src.schemas.crypto_schema import CRYPTO_SILVER_SCHEMA
from tests.data.silver_data import (
    VALID_SILVER_RECORDS,
    STRONG_UP_SILVER_RECORDS,
    STRONG_DOWN_SILVER_RECORDS,
    STABLE_SILVER_RECORDS,
    NO_RANGE_SILVER_RECORDS,
    ROLLING_AVG_SILVER_RECORDS,
    ZERO_PRICE_SILVER_RECORDS,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestGoldTransformations")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark_session
    spark_session.stop()


class TestMainDataTransformations:
    """Test suite for gold layer main data transformations."""

    def test_timestamp_parsing(self, spark: SparkSession):
        """Test that last_updated string is correctly parsed to timestamp."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Check that last_updated_ts column exists and is valid
        assert "last_updated_ts" in transformed_df.columns, (
            "last_updated_ts column should exist"
        )

        for row in result:
            assert row["last_updated_ts"] is not None, (
                "last_updated_ts should not be null"
            )
            # Verify timestamp is from 2026
            assert row["last_updated_ts"].year == 2026, "Year should be 2026"

    def test_price_volatility_calculation(self, spark: SparkSession):
        """Test price volatility calculation (24h range as % of current price)."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Bitcoin: high=46000, low=44000, current=45000.50
        # Volatility = ((46000 - 44000) / 45000.50) * 100 ≈ 4.44%
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        assert btc["price_volatility_24h"] == pytest.approx(4.44, abs=0.01), (
            "Bitcoin volatility should match"
        )

        # Ethereum: high=2600, low=2450, current=2500.75
        # Volatility = ((2600 - 2450) / 2500.75) * 100 ≈ 6.00%
        eth = [r for r in result if r["id"] == "ethereum"][0]
        assert eth["price_volatility_24h"] == pytest.approx(6.00, abs=0.01), (
            "Ethereum volatility should match"
        )

    def test_price_volatility_zero_price_handling(self, spark: SparkSession):
        """Test that zero price doesn't cause division by zero."""
        df = spark.createDataFrame(
            ZERO_PRICE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Should return 0.0 for zero price instead of error
        assert result[0]["price_volatility_24h"] == 0.0, (
            "Volatility should be 0.0 for zero price"
        )

    def test_price_position_calculation(self, spark: SparkSession):
        """Test price position within 24h range calculation."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Bitcoin: current=45000.50, high=46000, low=44000
        # Position = ((45000.50 - 44000) / (46000 - 44000)) * 100 ≈ 50.03%
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        assert btc["price_position_24h"] == pytest.approx(50.03, abs=0.01), (
            "Bitcoin position should match"
        )

        # Cardano: current=0.55, high=0.58, low=0.53
        # Position = ((0.55 - 0.53) / (0.58 - 0.53)) * 100 = 40.00%
        ada = [r for r in result if r["id"] == "cardano"][0]
        assert ada["price_position_24h"] == pytest.approx(40.00, abs=0.01), (
            "Cardano position should match"
        )

    def test_price_position_no_range_handling(self, spark: SparkSession):
        """Test that no price range (high == low) defaults to 50%."""
        df = spark.createDataFrame(NO_RANGE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # When high_24h == low_24h, position should be 50%
        assert result[0]["price_position_24h"] == 50.0, (
            "Position should be 50% when no range exists"
        )

    def test_price_trend_strong_up(self, spark: SparkSession):
        """Test price trend categorization for strong upward movement."""
        df = spark.createDataFrame(
            STRONG_UP_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Price change > 5% should be "strong_up"
        assert result[0]["price_trend_24h"] == "strong_up", (
            "Price trend should be strong_up"
        )

    def test_price_trend_up(self, spark: SparkSession):
        """Test price trend categorization for moderate upward movement."""
        # Create test data with 1% < change <= 5%
        test_data = VALID_SILVER_RECORDS.copy()
        test_data[1]["price_change_percentage_24h"] = 2.5  # Ethereum

        df = spark.createDataFrame(test_data, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        eth = [r for r in result if r["id"] == "ethereum"][0]
        assert eth["price_trend_24h"] == "up", "Price trend should be up"

    def test_price_trend_stable(self, spark: SparkSession):
        """Test price trend categorization for stable movement."""
        df = spark.createDataFrame(STABLE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # -1% <= change <= 1% should be "stable"
        assert result[0]["price_trend_24h"] == "stable", "Price trend should be stable"

    def test_price_trend_down(self, spark: SparkSession):
        """Test price trend categorization for moderate downward movement."""
        # Create test data with -5% < change <= -1%
        test_data = VALID_SILVER_RECORDS.copy()
        test_data[0]["price_change_percentage_24h"] = -2.5  # Bitcoin

        df = spark.createDataFrame(test_data, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        btc = [r for r in result if r["id"] == "bitcoin"][0]
        assert btc["price_trend_24h"] == "down", "Price trend should be down"

    def test_price_trend_strong_down(self, spark: SparkSession):
        """Test price trend categorization for strong downward movement."""
        df = spark.createDataFrame(
            STRONG_DOWN_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        # Price change < -5% should be "strong_down"
        assert result[0]["price_trend_24h"] == "strong_down", (
            "Price trend should be strong_down"
        )

    def test_all_columns_present(self, spark: SparkSession):
        """Test that all expected columns are present in output."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)

        expected_new_columns = [
            "last_updated_ts",
            "price_volatility_24h",
            "price_position_24h",
            "price_trend_24h",
        ]

        for col_name in expected_new_columns:
            assert col_name in transformed_df.columns, (
                f"{col_name} should be in output columns"
            )

    def test_no_null_values_in_calculated_fields(self, spark: SparkSession):
        """Test that calculated fields don't produce null values."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_volatility_24h"] is not None, (
                "price_volatility_24h should not be null"
            )
            assert row["price_position_24h"] is not None, (
                "price_position_24h should not be null"
            )
            assert row["price_trend_24h"] is not None, (
                "price_trend_24h should not be null"
            )


class TestRollingAverageTransformations:
    """Test suite for gold layer rolling average transformations."""

    def test_rolling_average_grouping(self, spark: SparkSession):
        """Test that rolling averages are calculated per coin."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )

        # Transform using rolling average
        rolling_df = transform_rolling_average(df)

        # Collect results
        result = rolling_df.collect()

        # Should have separate averages for bitcoin and ethereum
        coin_ids = [row["id"] for row in result]
        assert "bitcoin" in coin_ids, "bitcoin should be in rolling average results"
        assert "ethereum" in coin_ids, "ethereum should be in rolling average results"

    def test_rolling_average_price_calculation(self, spark: SparkSession):
        """Test that average price is correctly calculated."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Bitcoin prices: 45000, 46000, 45500
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        expected_avg_price = (45000.0 + 46000.0 + 45500.0) / 3
        assert btc["avg_price_2min"] == pytest.approx(expected_avg_price, abs=0.01)

        # Ethereum prices: 2500, 2550
        eth = [r for r in result if r["id"] == "ethereum"][0]
        expected_avg_price = (2500.0 + 2550.0) / 2
        assert eth["avg_price_2min"] == pytest.approx(expected_avg_price, abs=0.01)

    def test_rolling_average_market_cap(self, spark: SparkSession):
        """Test that average market cap is correctly calculated."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Bitcoin market caps: 850B, 860B, 855B
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        expected_avg = (850000000000.0 + 860000000000.0 + 855000000000.0) / 3
        assert btc["avg_market_cap_2min"] == pytest.approx(expected_avg, abs=1.0)

    def test_rolling_average_volume(self, spark: SparkSession):
        """Test that average volume is correctly calculated."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Bitcoin volumes: 25B, 26B, 25.5B
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        expected_avg = (25000000000.0 + 26000000000.0 + 25500000000.0) / 3
        assert btc["avg_volume_2min"] == pytest.approx(expected_avg, abs=1.0)

    def test_rolling_average_price_change_pct(self, spark: SparkSession):
        """Test that average price change percentage is correctly calculated."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Bitcoin price changes: 1.12, 1.32, 1.22
        btc = [r for r in result if r["id"] == "bitcoin"][0]
        expected_avg = (1.12 + 1.32 + 1.22) / 3
        assert btc["avg_price_change_pct_2min"] == pytest.approx(expected_avg, abs=0.01)

    def test_window_columns_present(self, spark: SparkSession):
        """Test that window start and end columns are present."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)

        assert "window_start" in rolling_df.columns, (
            "window_start column should be present"
        )
        assert "window_end" in rolling_df.columns, "window_end column should be present"

    def test_aggregated_at_timestamp(self, spark: SparkSession):
        """Test that aggregated_at timestamp is present."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            assert row["aggregated_at"] is not None, (
                "aggregated_at timestamp should not be null"
            )

    def test_coin_metadata_preserved(self, spark: SparkSession):
        """Test that coin metadata (symbol, name) is preserved in aggregation."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        btc = [r for r in result if r["id"] == "bitcoin"][0]
        assert btc["symbol"] == "btc", "Bitcoin symbol should be btc"
        assert btc["name"] == "Bitcoin", "Bitcoin name should be Bitcoin"

        eth = [r for r in result if r["id"] == "ethereum"][0]
        assert eth["symbol"] == "eth", "Ethereum symbol should be eth"
        assert eth["name"] == "Ethereum", "Ethereum name should be Ethereum"

    def test_rounding_precision(self, spark: SparkSession):
        """Test that values are rounded to correct precision."""
        df = spark.createDataFrame(
            ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA
        )
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            # Price should have 8 decimal places max
            price_str = str(row["avg_price_2min"])
            if "." in price_str:
                decimals = len(price_str.split(".")[1])
                assert decimals <= 8, "Price should have max 8 decimal places"

            # Market cap and volume should have 2 decimal places max
            mc_str = str(row["avg_market_cap_2min"])
            if "." in mc_str:
                decimals = len(mc_str.split(".")[1])
                assert decimals <= 2, "Market cap should have max 2 decimal places"
