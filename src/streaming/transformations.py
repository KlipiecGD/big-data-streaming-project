from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp, round, when, avg, window

def transform_main_data(df: DataFrame) -> DataFrame:
    """
    Transform the incoming DataFrame by parsing timestamps, filtering invalid data and performing additional transformations.
    Args:
        df (DataFrame): Input DataFrame with raw data
    Returns:
        DataFrame: Transformed DataFrame with additional columns
    """
    # Filter out records with null 'current_price' and add processing timestamp
    df = df.withColumn(
        "last_updated_ts", 
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    ).withColumn("processed_at", current_timestamp()) \
    .filter(col("current_price").isNotNull())

    # Price volatility calculation (24h range as percentage of current price)
    df = df.withColumn(
        "price_volatility_24h",
        round(((col("high_24h") - col("low_24h")) / col("current_price")) * 100, 2)
    )   

    # Price position within 24h range
    df = df.withColumn(
        "price_position_24h",
        round(
            ((col("current_price") - col("low_24h")) / (col("high_24h") - col("low_24h"))) * 100,
            2
        )
    )

    # Categorize price movement
    df = df.withColumn(
        "price_trend_24h",
        when(col("price_change_percentage_24h") > 5, "strong_up")
        .when(col("price_change_percentage_24h") > 1, "up")
        .when(col("price_change_percentage_24h") > -1, "stable")
        .when(col("price_change_percentage_24h") > -5, "down")
        .otherwise("strong_down")
    )

    return df

def transform_rolling_average(df: DataFrame) -> DataFrame:
    """
    Calculate 3-minute rolling average of prices for each cryptocurrency.
    Args:
        df (DataFrame): Input DataFrame with raw data
    Returns:
        DataFrame: DataFrame with rolling average prices per coin
    """
    # Add timestamp and filter valid data
    df = df.withColumn(
        "last_updated_ts", 
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    ).filter(col("current_price").isNotNull())

    # Define 3-minute tumbling window and calculate averages
    windowed_df = df.withWatermark("last_updated_ts", "5 minutes") \
        .groupBy(
            window(col("last_updated_ts"), "3 minutes"),
            col("id"),
            col("symbol"),
            col("name")
        ).agg(
            avg("current_price").alias("avg_price_3min"),
            avg("market_cap").alias("avg_market_cap_3min"),
            avg("total_volume").alias("avg_volume_3min"),
            avg("price_change_percentage_24h").alias("avg_price_change_pct_3min"),
            current_timestamp().alias("processed_at")
        )

    # Flatten window structure and round values
    rolling_avg_df = windowed_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("id"),
        col("symbol"),
        col("name"),
        round(col("avg_price_3min"), 8).alias("avg_price_3min"),
        round(col("avg_market_cap_3min"), 2).alias("avg_market_cap_3min"),
        round(col("avg_volume_3min"), 2).alias("avg_volume_3min"),
        round(col("avg_price_change_pct_3min"), 2).alias("avg_price_change_pct_3min"),
        col("processed_at")
    )

    return rolling_avg_df