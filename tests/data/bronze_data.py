# Complete valid records
VALID_BRONZE_RECORDS = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45000.50,
        "market_cap": 850000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 25000000000.0,
        "high_24h": 46000.0,
        "low_24h": 44000.0,
        "price_change_24h": 500.50,
        "price_change_percentage_24h": 1.12,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 2500.75,
        "market_cap": 300000000000.0,
        "market_cap_rank": 2.0,
        "total_volume": 15000000000.0,
        "high_24h": 2600.0,
        "low_24h": 2450.0,
        "price_change_24h": 50.75,
        "price_change_percentage_24h": 2.07,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    {
        "id": "cardano",
        "symbol": "ada",
        "name": "Cardano",
        "current_price": 0.55,
        "market_cap": 19000000000.0,
        "market_cap_rank": 10.0,
        "total_volume": 500000000.0,
        "high_24h": 0.58,
        "low_24h": 0.53,
        "price_change_24h": 0.02,
        "price_change_percentage_24h": 3.77,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
]

# Records with null values in different fields
BRONZE_RECORDS_WITH_NULLS = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": None,  # Null price
        "market_cap": 850000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 25000000000.0,
        "high_24h": 46000.0,
        "low_24h": 44000.0,
        "price_change_24h": 500.50,
        "price_change_percentage_24h": 1.12,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": None,  # Null name
        "current_price": 2500.75,
        "market_cap": 300000000000.0,
        "market_cap_rank": 2.0,
        "total_volume": 15000000000.0,
        "high_24h": 2600.0,
        "low_24h": 2450.0,
        "price_change_24h": 50.75,
        "price_change_percentage_24h": 2.07,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    {
        "id": "ripple",
        "symbol": "xrp",
        "name": "XRP",
        "current_price": 0.50,
        "market_cap": None,  # Null market cap
        "market_cap_rank": 6.0,
        "total_volume": 1000000000.0,
        "high_24h": 0.52,
        "low_24h": 0.48,
        "price_change_24h": 0.01,
        "price_change_percentage_24h": 2.04,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
]

# Mixed dataset with both valid and invalid records
MIXED_BRONZE_RECORDS = [
    # Valid records
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45000.50,
        "market_cap": 850000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 25000000000.0,
        "high_24h": 46000.0,
        "low_24h": 44000.0,
        "price_change_24h": 500.50,
        "price_change_percentage_24h": 1.12,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    # Invalid - null price
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": None,
        "market_cap": 300000000000.0,
        "market_cap_rank": 2.0,
        "total_volume": 15000000000.0,
        "high_24h": 2600.0,
        "low_24h": 2450.0,
        "price_change_24h": 50.75,
        "price_change_percentage_24h": 2.07,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    # Valid record
    {
        "id": "cardano",
        "symbol": "ada",
        "name": "Cardano",
        "current_price": 0.55,
        "market_cap": 19000000000.0,
        "market_cap_rank": 10.0,
        "total_volume": 500000000.0,
        "high_24h": 0.58,
        "low_24h": 0.53,
        "price_change_24h": 0.02,
        "price_change_percentage_24h": 3.77,
        "last_updated": "2026-01-15T10:30:45.123Z",
    },
    # Invalid - null last_updated
    {
        "id": "polkadot",
        "symbol": "dot",
        "name": "Polkadot",
        "current_price": 7.50,
        "market_cap": 9000000000.0,
        "market_cap_rank": 12.0,
        "total_volume": 300000000.0,
        "high_24h": 7.80,
        "low_24h": 7.20,
        "price_change_24h": 0.30,
        "price_change_percentage_24h": 4.17,
        "last_updated": None,
    },
]

# Empty dataset
EMPTY_BRONZE_RECORDS = []
