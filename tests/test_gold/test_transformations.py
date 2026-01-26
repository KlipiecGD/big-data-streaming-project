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
    ZERO_PRICE_SILVER_RECORDS,
    ROLLING_AVG_SILVER_RECORDS,
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

    def test_timestamp_parsing(self, spark: SparkSession):
        """Test that last_updated string is correctly parsed to timestamp."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        assert "last_updated_ts" in transformed_df.columns, "last_updated_ts column missing in output."
        for row in result:
            assert row["last_updated_ts"] is not None, "Parsed timestamp should not be None."

    def test_price_volatility_calculation(self, spark: SparkSession):
        """Test price volatility calculation for any valid records."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            source = VALID_SILVER_RECORDS[i]
            
            # Calculate expected volatility
            if source["current_price"] > 0:
                expected = ((source["high_24h"] - source["low_24h"]) / source["current_price"]) * 100
                assert row["price_volatility_24h"] == pytest.approx(expected, abs=0.01), "Price volatility calculation mismatch."
            else:
                assert row["price_volatility_24h"] == 0.0, "Price volatility should be 0.0 for zero current price."

    def test_price_volatility_zero_price_handling(self, spark: SparkSession):
        """Test that zero price doesn't cause division by zero."""
        # Validate test data assumption
        assert any(r["current_price"] == 0 for r in ZERO_PRICE_SILVER_RECORDS), (
            "ZERO_PRICE_SILVER_RECORDS must contain at least one record with current_price == 0"
        )
        
        df = spark.createDataFrame(ZERO_PRICE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            if ZERO_PRICE_SILVER_RECORDS[i]["current_price"] == 0:
                assert row["price_volatility_24h"] == 0.0, "Price volatility should be 0.0 for zero current price."

    def test_price_position_calculation(self, spark: SparkSession):
        """Test price position within 24h range calculation."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            source = VALID_SILVER_RECORDS[i]
            price_range = source["high_24h"] - source["low_24h"]
            
            if price_range > 0:
                expected = ((source["current_price"] - source["low_24h"]) / price_range) * 100
                assert row["price_position_24h"] == pytest.approx(expected, abs=0.01), "Price position calculation mismatch."
            else:
                assert row["price_position_24h"] == 50.0, "Price position should be 50.0 when no price range."

    def test_price_position_no_range_handling(self, spark: SparkSession):
        """Test that no price range (high == low) defaults to 50%."""
        # Validate test data assumption
        assert any(r["high_24h"] == r["low_24h"] for r in NO_RANGE_SILVER_RECORDS), (
            "NO_RANGE_SILVER_RECORDS must contain at least one record where high_24h == low_24h"
        )
        
        df = spark.createDataFrame(NO_RANGE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for i, row in enumerate(result):
            if NO_RANGE_SILVER_RECORDS[i]["high_24h"] == NO_RANGE_SILVER_RECORDS[i]["low_24h"]:
                assert row["price_position_24h"] == 50.0, "Price position should be 50.0 when no price range."

    def test_price_trend_strong_up(self, spark: SparkSession):
        """Test price trend categorization for strong upward movement (> 5%)."""
        # Validate test data assumption
        assert all(r["price_change_percentage_24h"] > 5 for r in STRONG_UP_SILVER_RECORDS), (
            "STRONG_UP_SILVER_RECORDS must have all records with price_change_percentage_24h > 5"
        )
        
        df = spark.createDataFrame(STRONG_UP_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "strong_up", "Price trend should be categorized as strong_up"

    def test_price_trend_strong_down(self, spark: SparkSession):
        """Test price trend categorization for strong downward movement (< -5%)."""
        # Validate test data assumption
        assert all(r["price_change_percentage_24h"] < -5 for r in STRONG_DOWN_SILVER_RECORDS), (
            "STRONG_DOWN_SILVER_RECORDS must have all records with price_change_percentage_24h < -5"
        )
        
        df = spark.createDataFrame(STRONG_DOWN_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "strong_down", "Price trend should be categorized as strong_down"

    def test_price_trend_stable(self, spark: SparkSession):
        """Test price trend categorization for stable movement (-1% to 1%)."""
        # Validate test data assumption
        assert all(-1 <= r["price_change_percentage_24h"] <= 1 for r in STABLE_SILVER_RECORDS), (
            "STABLE_SILVER_RECORDS must have all records with -1 <= price_change_percentage_24h <= 1"
        )
        
        df = spark.createDataFrame(STABLE_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_trend_24h"] == "stable", "Price trend should be categorized as stable"

    def test_price_trend_all_boundaries(self, spark: SparkSession):
        """Test all price trend boundaries with dynamically created data."""
        # Create test data with exact boundary values
        base_record = VALID_SILVER_RECORDS[0].copy()
        
        test_cases = [
            (6.0, "strong_up"),     # > 5
            (5.0, "up"),            # = 5 (boundary)
            (2.0, "up"),            # 1 < x < 5
            (1.0, "stable"),        # = 1 (boundary)
            (0.0, "stable"),        # -1 < x < 1
            (-1.0, "stable"),       # = -1 (boundary)
            (-2.0, "down"),         # -5 < x < -1
            (-5.0, "down"),         # = -5 (boundary)
            (-6.0, "strong_down"),  # < -5
        ]

        for pct_change, expected_trend in test_cases:
            record = base_record.copy()
            record["price_change_percentage_24h"] = pct_change
            
            df = spark.createDataFrame([record], schema=CRYPTO_SILVER_SCHEMA)
            transformed_df = transform_main_data(df)
            result = transformed_df.collect()

            assert result[0]["price_trend_24h"] == expected_trend, (
                f"Price change of {pct_change}% should be categorized as {expected_trend}"
            )

    def test_all_columns_present(self, spark: SparkSession):
        """Test that all expected columns are present in output."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)

        expected_columns = [
            "last_updated_ts",
            "price_volatility_24h",
            "price_position_24h",
            "price_trend_24h",
        ]

        for col in expected_columns:
            assert col in transformed_df.columns, f"{col} column missing in output."

    def test_no_null_in_calculated_fields(self, spark: SparkSession):
        """Test that calculated fields don't produce null values."""
        df = spark.createDataFrame(VALID_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        transformed_df = transform_main_data(df)
        result = transformed_df.collect()

        for row in result:
            assert row["price_volatility_24h"] is not None, "Price volatility should not be None"
            assert row["price_position_24h"] is not None, "Price position should not be None"
            assert row["price_trend_24h"] is not None, "Price trend should not be None"


class TestRollingAverageTransformations:
    """Test suite for gold layer rolling average transformations."""

    def test_rolling_average_grouping_by_coin(self, spark: SparkSession):
        """Test that rolling averages are calculated separately per coin."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Get unique coins from source and result
        source_coins = set(r["id"] for r in ROLLING_AVG_SILVER_RECORDS)
        result_coins = set(row["id"] for row in result)

        # Each coin should have aggregated results
        for coin in source_coins:
            assert coin in result_coins, f"Coin {coin} missing in rolling average results."

    def test_rolling_average_price_calculation(self, spark: SparkSession):
        """Test that average prices are calculated correctly per coin."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Group source by coin
        coin_prices = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            coin_prices.setdefault(coin_id, []).append(record["current_price"])

        # Verify averages
        for row in result:
            expected_avg = sum(coin_prices[row["id"]]) / len(coin_prices[row["id"]])
            assert row["avg_price_2min"] == pytest.approx(expected_avg, abs=0.01), f"Average price mismatch for coin {row['id']}."

    def test_rolling_average_all_metrics(self, spark: SparkSession):
        """Test that all metrics (price, market_cap, volume, price_change_pct) are averaged correctly."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Group source data by coin
        coin_data = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            if coin_id not in coin_data:
                coin_data[coin_id] = {
                    "prices": [],
                    "market_caps": [],
                    "volumes": [],
                    "price_changes": []
                }
            coin_data[coin_id]["prices"].append(record["current_price"])
            coin_data[coin_id]["market_caps"].append(record["market_cap"])
            coin_data[coin_id]["volumes"].append(record["total_volume"])
            coin_data[coin_id]["price_changes"].append(record["price_change_percentage_24h"])

        # Verify all metrics for each coin
        for row in result:
            coin_id = row["id"]
            data = coin_data[coin_id]
            
            expected_price = sum(data["prices"]) / len(data["prices"])
            expected_mc = sum(data["market_caps"]) / len(data["market_caps"])
            expected_vol = sum(data["volumes"]) / len(data["volumes"])
            expected_pct = sum(data["price_changes"]) / len(data["price_changes"])

            assert row["avg_price_2min"] == pytest.approx(expected_price, abs=0.01), f"Average price mismatch for coin {row['id']}."
            assert row["avg_market_cap_2min"] == pytest.approx(expected_mc, abs=1.0), f"Average market cap mismatch for coin {row['id']}."
            assert row["avg_volume_2min"] == pytest.approx(expected_vol, abs=1.0), f"Average volume mismatch for coin {row['id']}."
            assert row["avg_price_change_pct_2min"] == pytest.approx(expected_pct, abs=0.01), f"Average price change percentage mismatch for coin {row['id']}."

    def test_window_columns_exist(self, spark: SparkSession):
        """Test that window columns are present."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)

        assert "window_start" in rolling_df.columns, "window_start column missing in output."
        assert "window_end" in rolling_df.columns, "window_end column missing in output."
        assert "aggregated_at" in rolling_df.columns, "aggregated_at column missing in output."

    def test_coin_metadata_preserved(self, spark: SparkSession):
        """Test that coin metadata is preserved in aggregation."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        # Build expected metadata from source
        coin_metadata = {}
        for record in ROLLING_AVG_SILVER_RECORDS:
            coin_id = record["id"]
            if coin_id not in coin_metadata:
                coin_metadata[coin_id] = {
                    "symbol": record["symbol"],
                    "name": record["name"]
                }

        # Verify metadata for each result
        for row in result:
            expected = coin_metadata[row["id"]]
            assert row["symbol"] == expected["symbol"], f"Symbol mismatch for coin {row['id']}."
            assert row["name"] == expected["name"], f"Name mismatch for coin {row['id']}."

    def test_rounding_precision(self, spark: SparkSession):
        """Test that values are rounded to specified precision."""
        df = spark.createDataFrame(ROLLING_AVG_SILVER_RECORDS, schema=CRYPTO_SILVER_SCHEMA)
        rolling_df = transform_rolling_average(df)
        result = rolling_df.collect()

        for row in result:
            # Price: max 8 decimals
            price_str = str(row["avg_price_2min"])
            if "." in price_str:
                assert len(price_str.split(".")[1]) <= 8, "Average price exceeds 8 decimal places."

            # Market cap and volume: max 2 decimals
            for field in ["avg_market_cap_2min", "avg_volume_2min"]:
                field_str = str(row[field])
                if "." in field_str:
                    assert len(field_str.split(".")[1]) <= 2, f"{field} exceeds 2 decimal places."
