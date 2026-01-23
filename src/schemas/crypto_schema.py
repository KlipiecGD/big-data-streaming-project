from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    DateType,
)

CRYPTO_BRONZE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("market_cap_rank", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", StringType(), True),
    ]
)

CRYPTO_SILVER_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("market_cap_rank", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("date", DateType(), True),
    ]
)
