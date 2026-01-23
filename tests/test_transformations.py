import pytest
from typing import Generator
from pyspark.sql import SparkSession
from src.schemas.crypto_schema import CRYPTO_PRICES_SCHEMA
from src.streaming.gold.transformations import transform_main_data, transform_rolling_average
from tests.data.sample_data import MAIN_DATA, AVERAGES_DATA, ZERO_PRICE_DATA, ZERO_RANGE_DATA

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("CryptoTestSession") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()  

class TestTransformMainData:
    """Test suite for transform_main_data function."""

    def test_filters_null_prices(self, spark: SparkSession) -> None:
        """Test that records with null current_price are filtered out."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_main_data(data)
        assert result.count() == 5, "Expected 5 records after filtering null prices"
        assert result.filter("id = 'null-coin'").count() == 0, "Null price record was not filtered out"

    def test_adds_timestamp_columns(self, spark: SparkSession) -> None:
        """Test that timestamp columns are added."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_main_data(data)
        columns = result.columns
        assert "last_updated_ts" in columns, "last_updated_ts column was not added"
        assert "processed_at" in columns, "processed_at column was not added"

    def test_price_volatility_calculation(self, spark: SparkSession) -> None:
        """Test price volatility calculation (24h range as percentage)."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_main_data(data) 

        # Bitcoin: (52000 - 48000) / 50000 * 100 = 8.0%
        btc_row = result.filter("id = 'bitcoin'").select("price_volatility_24h").first()
        assert btc_row is not None, "Bitcoin volatility row was not found"
        btc_volatility = btc_row[0]
        assert btc_volatility == 8.0, f"Bitcoin volatility was not calculated correctly, expected 8.0 but got {btc_volatility}"
        
        # Ethereum: (3200 - 2900) / 3000 * 100 = 10.0%
        eth_row = result.filter("id = 'ethereum'").select("price_volatility_24h").first()
        assert eth_row is not None, "Ethereum volatility row was not found"
        eth_volatility = eth_row[0]
        assert eth_volatility == 10.0, f"Ethereum volatility was not calculated correctly, expected 10.0 but got {eth_volatility}"

    def test_price_position_calculation(self, spark: SparkSession) -> None:
        """Test price position within 24h range."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_main_data(data)
        
        # Bitcoin: (50000 - 48000) / (52000 - 48000) * 100 = 50.0%
        btc_row = result.filter("id = 'bitcoin'").select("price_position_24h").first()
        assert btc_row is not None, "Bitcoin position row was not found"
        btc_position = btc_row[0]
        assert btc_position == 50.0, f"Bitcoin position was not calculated correctly, expected 50.0 but got {btc_position}"
        
        # Ethereum: (3000 - 2900) / (3200 - 2900) * 100 = 33.33%
        eth_row = result.filter("id = 'ethereum'").select("price_position_24h").first()
        assert eth_row is not None, "Ethereum position row was not found"
        eth_position = eth_row[0]
        assert eth_position == 33.33, f"Ethereum position was not calculated correctly, expected 33.33 but got {eth_position}"

    def test_price_trend_categorization(self, spark: SparkSession) -> None:
        """Test price trend categorization based on 24h change."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_main_data(data)
        
        trends = result.select("id", "price_trend_24h").collect()
        trend_dict = {row["id"]: row["price_trend_24h"] for row in trends}
        
        assert trend_dict["bitcoin"] == "up", f"Bitcoin trend incorrect, expected 'up' (3.5%) but got {trend_dict['bitcoin']}" 
        assert trend_dict["ethereum"] == "up", f"Ethereum trend incorrect, expected 'up' (2.0%) but got {trend_dict['ethereum']}"  
        assert trend_dict["cardano"] == "stable", f"Cardano trend incorrect, expected 'stable' (-0.5%) but got {trend_dict['cardano']}" 
        assert trend_dict["ripple"] == "down", f"Ripple trend incorrect, expected 'down' (-3.0%) but got {trend_dict['ripple']}"  
        assert trend_dict["dogecoin"] == "strong_down", f"Dogecoin trend incorrect, expected 'strong_down' (-6.0%) but got {trend_dict['dogecoin']}" 

    def test_handles_zero_price(self, spark: SparkSession) -> None:
        """Test handling of zero prices."""
        data = ZERO_PRICE_DATA
        data = spark.createDataFrame(data, schema=CRYPTO_PRICES_SCHEMA)
        
        result = transform_main_data(data)
        row = result.select("price_volatility_24h").first()
        assert row is not None, "Volatility row was not found"
        volatility = row[0]
        assert volatility == 0.0, f"Volatility for zero price should be 0.0 but got {volatility}"

    def test_handles_zero_range(self, spark: SparkSession) -> None:
        """Test handling when 24h high equals low."""
        data = ZERO_RANGE_DATA
        data = spark.createDataFrame(data, schema=CRYPTO_PRICES_SCHEMA)
        
        result = transform_main_data(data)
        row = result.select("price_position_24h").first()
        assert row is not None, "Position row was not found"
        position = row[0]
        assert position == 50.0, f"Position for zero range should be by default 50.0 but got {position}"


class TestTransformRollingAverage:
    """Test suite for transform_rolling_average function."""

    def test_filters_null_prices(self, spark: SparkSession) -> None:
        """Test that records with null current_price are filtered."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_rolling_average(data)
        assert result.filter("id = 'null-coin'").count() == 0, "Null price record was not filtered out"

    def test_output_columns(self, spark: SparkSession) -> None:
        """Test that the output contains expected columns."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_rolling_average(data)
        expected_columns = [
            "window_start", "window_end", "id", "symbol", "name",
            "avg_price_2min", "avg_market_cap_2min", "avg_volume_2min",
            "avg_price_change_pct_2min", "processed_at"
        ]
        assert all(col in result.columns for col in expected_columns), "Output columns do not match expected columns"

    def test_groups_by_coin(self, spark: SparkSession) -> None:
        """Test that data is grouped by cryptocurrency."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_rolling_average(data)
        # Each coin should have its own aggregated record
        coin_ids = [row["id"] for row in result.select("id").distinct().collect()]
        assert "bitcoin" in coin_ids, "Bitcoin record is missing"
        assert "ethereum" in coin_ids, "Ethereum record is missing"

    def test_calculates_averages(self, spark: SparkSession) -> None:
        """Test that averages are calculated correctly."""
        # Create data with multiple records for the same coin
        data = AVERAGES_DATA
        data = spark.createDataFrame(data, schema=CRYPTO_PRICES_SCHEMA)

        result = transform_rolling_average(data)
        btc_row = result.filter("id = 'bitcoin'").first()
        assert btc_row is not None, "Bitcoin average row was not found"
        
        # Average price should be (50000 + 51000) / 2 = 50500
        assert btc_row["avg_price_2min"] == 50500.0, f"Average price incorrect, expected 50500.0 but got {btc_row['avg_price_2min']}"

    def test_rounds_values(self, spark: SparkSession) -> None:
        """Test that values are rounded to appropriate decimal places."""
        data = spark.createDataFrame(MAIN_DATA, schema=CRYPTO_PRICES_SCHEMA)
        result = transform_rolling_average(data)
        
        # Check that values are properly rounded
        for row in result.collect():
            # Price should be rounded to 8 decimals
            price_str = str(row["avg_price_2min"])
            if "." in price_str:
                decimals = len(price_str.split(".")[1])
                assert decimals <= 8, f"Price not rounded to 8 decimals: {row['avg_price_2min']}"
            
            # Market cap and volume should be rounded to 2 decimals
            assert isinstance(row["avg_market_cap_2min"], float), f"Market cap not a float: {row['avg_market_cap_2min']}"
            assert isinstance(row["avg_volume_2min"], float), f"Volume not a float: {row['avg_volume_2min']}"
