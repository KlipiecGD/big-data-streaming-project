from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    round,
    when,
    avg,
    window,
)


def transform_main_data(df: DataFrame) -> DataFrame:
    """
    Transform the incoming DataFrame by parsing timestamps and performing additional transformations.
    Expects DataFrame from silver layer (already filtered for nulls and has processed_at + date).
    Args:
        df (DataFrame): Input DataFrame from silver layer
    Returns:
        DataFrame: Transformed DataFrame with additional columns
    """
    # Parse last_updated timestamp
    df = df.withColumn(
        "last_updated_ts",
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
    )

    # Price volatility calculation (24h range as percentage of current price)
    df = df.withColumn(
        "price_volatility_24h",
        when(
            col("current_price") > 0,
            round(((col("high_24h") - col("low_24h")) / col("current_price")) * 100, 2),
        ).otherwise(0.0),
    )

    # Price position within 24h range
    price_range_24h = col("high_24h") - col("low_24h")
    df = df.withColumn(
        "price_position_24h",
        when(
            price_range_24h > 0,
            round(((col("current_price") - col("low_24h")) / price_range_24h) * 100, 2),
        ).otherwise(50.0),  # If no range, set to middle (50%)
    )

    # Categorize price movement
    df = df.withColumn(
        "price_trend_24h",
        when(col("price_change_percentage_24h") > 5, "strong_up")
        .when(col("price_change_percentage_24h") > 1, "up")
        .when(col("price_change_percentage_24h") > -1, "stable")
        .when(col("price_change_percentage_24h") > -5, "down")
        .otherwise("strong_down"),
    )

    return df


def transform_rolling_average(df: DataFrame) -> DataFrame:
    """
    Calculate 2-minute rolling average of prices for each cryptocurrency.
    Expects DataFrame from silver layer (already filtered for nulls).
    Args:
        df (DataFrame): Input DataFrame from silver layer
    Returns:
        DataFrame: DataFrame with rolling average prices per coin
    """
    # Use the processed_at from silver layer for windowing
    # Define 2-minute tumbling window and calculate averages
    windowed_df = (
        df.withWatermark("processed_at", "3 minutes")
        .groupBy(
            window(col("processed_at"), "2 minutes"),
            col("id"),
            col("symbol"),
            col("name"),
        )
        .agg(
            avg("current_price").alias("avg_price_2min"),
            avg("market_cap").alias("avg_market_cap_2min"),
            avg("total_volume").alias("avg_volume_2min"),
            avg("price_change_percentage_24h").alias("avg_price_change_pct_2min"),
            current_timestamp().alias("aggregated_at"),
        )
    )

    # Flatten window structure and round values
    rolling_avg_df = windowed_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("id"),
        col("symbol"),
        col("name"),
        round(col("avg_price_2min"), 8).alias("avg_price_2min"),
        round(col("avg_market_cap_2min"), 2).alias("avg_market_cap_2min"),
        round(col("avg_volume_2min"), 2).alias("avg_volume_2min"),
        round(col("avg_price_change_pct_2min"), 2).alias("avg_price_change_pct_2min"),
        col("aggregated_at"),
    )

    return rolling_avg_df
