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
    UP_SILVER_RECORDS,
    DOWN_SILVER_RECORDS,
    STRONG_DOWN_SILVER_RECORDS,
    STABLE_SILVER_RECORDS,
    NO_RANGE_SILVER_RECORDS,
    ZERO_PRICE_SILVER_RECORDS,
    ROLLING_AVG_SILVER_RECORDS,
)
from tests.data.expected_fields import (
    GOLD_MAIN_EXPECTED_FIELDS,
    GOLD_ROLLING_AVG_EXPECTED_FIELDS,
    GOLD_MAIN_CALCULATED_FIELDS,
    GOLD_ROLLING_AVG_EXPECTED_CALCULATED_FIELDS,
    PRICE_TREND_CATEGORIES,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestGoldTransformations")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark_session
    spark_session.stop()


class TestMainDataTransformations:
    """Test suite for gold layer main data transformations."""

    def test_all_expected_columns_present(self, spark: SparkSession):
        """Test that all expected gold main columns are present in output."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)

        missing_cols = set(GOLD_MAIN_EXPECTED_FIELDS) - set(transformed_df.columns)
        assert not missing_cols, f"Missing columns in output: {missing_cols}"

    def test_timestamp_parsing_works(self, spark: SparkSession):
        """Test that last_updated string is parsed to timestamp."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert "last_updated_ts" in transformed_df.columns, "last_updated_ts column missing"
        for row in result:
            assert row["last_updated_ts"] is not None, "Parsed timestamp is None"

    def test_price_volatility_calculated(self, spark: SparkSession):
        """Test that price volatility is calculated and not null."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert "price_volatility_24h" in transformed_df.columns, "price_volatility_24h column missing"
        for row in result:
            assert row["price_volatility_24h"] is not None, "Price volatility is None"
            assert isinstance(row["price_volatility_24h"], (int, float)), "Price volatility is not a number"

    def test_price_volatility_handles_zero_price(self, spark: SparkSession):
        """Test that zero price doesn't cause errors in volatility calculation."""
        # Validate test data assumption
        assert any(r["current_price"] == 0 for r in ZERO_PRICE_SILVER_RECORDS), (
            "ZERO_PRICE_SILVER_RECORDS must contain at least one record with current_price == 0"
        )
        
        df = spark.createDataFrame(ZERO_PRICE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            if ZERO_PRICE_SILVER_RECORDS[i]["current_price"] == 0:
                assert row["price_volatility_24h"] == 0.0, "Volatility should be 0 for zero price"

    def test_price_position_calculated(self, spark: SparkSession):
        """Test that price position is calculated and within valid range."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert "price_position_24h" in transformed_df.columns, "price_position_24h column missing"
        for row in result:
            assert row["price_position_24h"] is not None, "Price position is None"
            assert 0 <= row["price_position_24h"] <= 100, "Price position out of range (0-100)"

    def test_price_position_handles_no_range(self, spark: SparkSession):
        """Test that no price range defaults to middle position."""
        # Validate test data assumption
        assert all(r["high_24h"] == r["low_24h"] for r in NO_RANGE_SILVER_RECORDS), (
            "NO_RANGE_SILVER_RECORDS must have all records where high_24h == low_24h"
        )
        
        df = spark.createDataFrame(NO_RANGE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_position_24h"] == 50.0, "Position should be 50% when no range"

    def test_price_trend_is_valid_category(self, spark: SparkSession):
        """Test that price trend is one of the valid categories."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert "price_trend_24h" in transformed_df.columns, "price_trend_24h column missing"
        for row in result:
            assert row["price_trend_24h"] in PRICE_TREND_CATEGORIES, "Invalid price trend category"

    def test_strong_up_trend_categorization(self, spark: SparkSession):
        """Test that strong upward movement is categorized correctly."""
        # Validate test data assumption
        assert all(r["price_change_percentage_24h"] > 5 for r in STRONG_UP_SILVER_RECORDS), (
            "STRONG_UP_SILVER_RECORDS must have all records with price_change_percentage_24h > 5"
        )
        
        df = spark.createDataFrame(STRONG_UP_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "strong_up", "Should be categorized as strong_up"

    def test_strong_down_trend_categorization(self, spark: SparkSession):
        """Test that strong downward movement is categorized correctly."""
        # Validate test data assumption
        assert all(r["price_change_percentage_24h"] < -5 for r in STRONG_DOWN_SILVER_RECORDS), (
            "STRONG_DOWN_SILVER_RECORDS must have all records with price_change_percentage_24h < -5"
        )
        
        df = spark.createDataFrame(STRONG_DOWN_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "strong_down", "Should be categorized as strong_down"

    def test_stable_trend_categorization(self, spark: SparkSession):
        """Test that stable movement is categorized correctly."""
        # Validate test data assumption
        assert all(-1 <= r["price_change_percentage_24h"] <= 1 for r in STABLE_SILVER_RECORDS), (
            "STABLE_SILVER_RECORDS must have all records with -1 <= price_change_percentage_24h <= 1"
        )
        
        df = spark.createDataFrame(STABLE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "stable", "Should be categorized as stable"

    def test_up_trend_categorization(self, spark: SparkSession):
        """Test that upward movement (1% < x <= 5%) is categorized correctly."""
        # Validate test data assumption
        assert all(1 < r["price_change_percentage_24h"] <= 5 for r in UP_SILVER_RECORDS), (
            "UP_SILVER_RECORDS must have all records with 1 < price_change_percentage_24h <= 5"
        )
        df = spark.createDataFrame(UP_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert result[0]["price_trend_24h"] == "up", "Should be categorized as up"

    def test_down_trend_categorization(self, spark: SparkSession):
        """Test that downward movement (-5% <= x < -1%) is categorized correctly."""
        # Validate test data assumption
        assert all(-5 <= r["price_change_percentage_24h"] < -1 for r in DOWN_SILVER_RECORDS), (
            "DOWN_SILVER_RECORDS must have all records with -5 <= price_change_percentage_24h < -1"
        )
        df = spark.createDataFrame(DOWN_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert result[0]["price_trend_24h"] == "down", "Should be categorized as down"

    def test_no_nulls_in_calculated_fields(self, spark: SparkSession):
        """Test that calculated fields don't produce null values."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        calculated_fields = GOLD_MAIN_CALCULATED_FIELDS
        for row in result:
            for field in calculated_fields:
                assert row[field] is not None, f"Field {field} should not be None."

    def test_transformation_preserves_record_count(self, spark: SparkSession):
        """Test that transformation doesn't filter out records."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert len(result) == len(VALID_SILVER_RECORDS), "Record count should be preserved after transformation."

    def test_price_volatility_calculation_accuracy(self, spark: SparkSession):
        """Test that price volatility is calculated correctly based on input data."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            source = VALID_SILVER_RECORDS[i]
            if source["current_price"] > 0:
                expected_volatility = ((source["high_24h"] - source["low_24h"]) / source["current_price"]) * 100
                assert abs(row["price_volatility_24h"] - expected_volatility) < 0.01, (
                    f"Price volatility mismatch for {source['id']}: "
                    f"expected {expected_volatility:.2f}, got {row['price_volatility_24h']}"
                )

    def test_price_position_calculation_accuracy(self, spark: SparkSession):
        """Test that price position is calculated correctly based on input data."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            source = VALID_SILVER_RECORDS[i]
            price_range = source["high_24h"] - source["low_24h"]
            
            if price_range > 0:
                expected_position = ((source["current_price"] - source["low_24h"]) / price_range) * 100
                assert abs(row["price_position_24h"] - expected_position) < 0.01, (
                    f"Price position mismatch for {source['id']}: "
                    f"expected {expected_position:.2f}, got {row['price_position_24h']}"
                )


class TestRollingAverageTransformations:
    """Test suite for gold layer rolling average transformations."""

    def test_all_expected_columns_present(self, spark: SparkSession):
        """Test that all expected rolling average columns are present."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)

        missing_cols = set(GOLD_ROLLING_AVG_EXPECTED_FIELDS) - set(rolling_df.columns)
        assert not missing_cols, f"Missing columns in output: {missing_cols}"

    def test_aggregation_produces_results(self, spark: SparkSession):
        """Test that rolling average aggregation produces results."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        assert len(result) > 0, "Rolling average should produce aggregated results."

    def test_window_columns_populated(self, spark: SparkSession):
        """Test that window columns are populated with values."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            assert row["window_start"] is not None, "window_start should not be None"
            assert row["window_end"] is not None, "window_end should not be None"
            assert row["aggregated_at"] is not None, "aggregated_at should not be None"

    def test_coin_metadata_preserved(self, spark: SparkSession):
        """Test that coin metadata is preserved in aggregation."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        coin_fields = ["id", "symbol", "name"]
        for row in result:
            for field in coin_fields:
                assert row[field] is not None, f"Field {field} should not be None."
                assert row[field] != "", f"Field {field} should not be an empty string."

    def test_average_metrics_calculated(self, spark: SparkSession):
        """Test that all average metrics are calculated."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        avg_fields = GOLD_ROLLING_AVG_EXPECTED_CALCULATED_FIELDS
        
        for row in result:
            for field in avg_fields:
                assert row[field] is not None, f"Field {field} should not be None."
                assert isinstance(row[field], (int, float)), f"Field {field} should be a number."

    def test_averages_are_positive_or_zero(self, spark: SparkSession):
        """Test that price and volume averages are non-negative."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            assert row["avg_price_2min"] >= 0, "avg_price_2min should be non-negative"
            assert row["avg_market_cap_2min"] >= 0, "avg_market_cap_2min should be non-negative"
            assert row["avg_volume_2min"] >= 0, "avg_volume_2min should be non-negative"

    def test_grouping_by_coin(self, spark: SparkSession):
        """Test that aggregation groups by individual coins."""
        # Validate test data has multiple coins
        unique_coins = set(r["id"] for r in ROLLING_AVG_SILVER_RECORDS)
        assert len(unique_coins) > 1, (
            "ROLLING_AVG_SILVER_RECORDS must contain at least 2 different coins for this test"
        )
        
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Get unique coins from results
        result_coins = set(row["id"] for row in result)
        assert len(result_coins) > 0, "Should have aggregated data for at least one coin."
        
        # Each coin should appear in results
        for coin in unique_coins:
            assert coin in result_coins, f"Coin {coin} should appear in aggregated results"

    def test_no_nulls_in_output(self, spark: SparkSession):
        """Test that rolling average output contains no null values."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            for field in GOLD_ROLLING_AVG_EXPECTED_FIELDS:
                assert row[field] is not None, f"Field {field} should not be None."

    def test_rolling_average_price_calculation_accuracy(self, spark: SparkSession):
        """Test that rolling average price is calculated correctly."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Group source data by coin
        coin_prices = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            if coin_id not in coin_prices:
                coin_prices[coin_id] = []
            coin_prices[coin_id].append(record["current_price"])

        # Verify average prices
        for row in result:
            coin_id = row["id"]
            expected_avg = sum(coin_prices[coin_id]) / len(coin_prices[coin_id])
            assert abs(row["avg_price_2min"] - expected_avg) < 0.01, (
                f"Average price mismatch for {coin_id}: "
                f"expected {expected_avg:.2f}, got {row['avg_price_2min']}"
            )

    def test_rolling_average_market_cap_calculation_accuracy(self, spark: SparkSession):
        """Test that rolling average market cap is calculated correctly."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Group source data by coin
        coin_market_caps = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            if coin_id not in coin_market_caps:
                coin_market_caps[coin_id] = []
            coin_market_caps[coin_id].append(record["market_cap"])

        # Verify average market caps
        for row in result:
            coin_id = row["id"]
            expected_avg = sum(coin_market_caps[coin_id]) / len(coin_market_caps[coin_id])
            assert abs(row["avg_market_cap_2min"] - expected_avg) < 1.0, (
                f"Average market cap mismatch for {coin_id}: "
                f"expected {expected_avg:.2f}, got {row['avg_market_cap_2min']}"
            )

    def test_rolling_average_volume_calculation_accuracy(self, spark: SparkSession):
        """Test that rolling average volume is calculated correctly."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Group source data by coin
        coin_volumes = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            if coin_id not in coin_volumes:
                coin_volumes[coin_id] = []
            coin_volumes[coin_id].append(record["total_volume"])

        # Verify average volumes
        for row in result:
            coin_id = row["id"]
            expected_avg = sum(coin_volumes[coin_id]) / len(coin_volumes[coin_id])
            assert abs(row["avg_volume_2min"] - expected_avg) < 1.0, (
                f"Average volume mismatch for {coin_id}: "
                f"expected {expected_avg:.2f}, got {row['avg_volume_2min']}"
            )