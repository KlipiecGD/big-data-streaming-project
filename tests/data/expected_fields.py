# Expected fields for each layer

# Bronze layer - raw data from API
BRONZE_EXPECTED_FIELDS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "last_updated",
]

# Silver layer - cleaned bronze data with additional metadata
SILVER_EXPECTED_FIELDS = BRONZE_EXPECTED_FIELDS + [
    "processed_at",
    "date",
]

# Gold layer - main data with calculated metrics
GOLD_MAIN_EXPECTED_FIELDS = SILVER_EXPECTED_FIELDS + [
    "last_updated_ts",
    "price_volatility_24h",
    "price_position_24h",
    "price_trend_24h",
]

# Gold layer - calculated fields
GOLD_MAIN_CALCULATED_FIELDS = [
    "price_volatility_24h",
    "price_position_24h",
    "price_trend_24h",
]

# Gold layer - rolling average aggregations
GOLD_ROLLING_AVG_EXPECTED_FIELDS = [
    "window_start",
    "window_end",
    "id",
    "symbol",
    "name",
    "avg_price_2min",
    "avg_market_cap_2min",
    "avg_volume_2min",
    "avg_price_change_pct_2min",
    "aggregated_at",
]

GOLD_ROLLING_AVG_EXPECTED_CALCULATED_FIELDS = [
    "avg_price_2min",
    "avg_market_cap_2min",
    "avg_volume_2min",
    "avg_price_change_pct_2min",
]

# Price trend categories
PRICE_TREND_CATEGORIES = [
    "strong_up",
    "up",
    "stable",
    "down",
    "strong_down",
]