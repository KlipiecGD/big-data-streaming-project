from datetime import datetime, date

# Valid silver records with different price scenarios
VALID_SILVER_RECORDS = [
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
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
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
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
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
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    },
]

# Records with strong upward price movement
STRONG_UP_SILVER_RECORDS = [
    {
        "id": "altcoin1",
        "symbol": "alt1",
        "name": "AltCoin1",
        "current_price": 100.0,
        "market_cap": 1000000000.0,
        "market_cap_rank": 20.0,
        "total_volume": 50000000.0,
        "high_24h": 110.0,
        "low_24h": 95.0,
        "price_change_24h": 6.0,
        "price_change_percentage_24h": 6.38,  # strong_up
        "last_updated": "2026-01-15T10:30:45.123Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    }
]

# Records with strong downward price movement
STRONG_DOWN_SILVER_RECORDS = [
    {
        "id": "altcoin2",
        "symbol": "alt2",
        "name": "AltCoin2",
        "current_price": 50.0,
        "market_cap": 500000000.0,
        "market_cap_rank": 30.0,
        "total_volume": 25000000.0,
        "high_24h": 60.0,
        "low_24h": 48.0,
        "price_change_24h": -4.0,
        "price_change_percentage_24h": -7.41,  # strong_down
        "last_updated": "2026-01-15T10:30:45.123Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    }
]

# Records with stable price movement
STABLE_SILVER_RECORDS = [
    {
        "id": "stablecoin",
        "symbol": "stbl",
        "name": "StableCoin",
        "current_price": 1.0,
        "market_cap": 10000000000.0,
        "market_cap_rank": 5.0,
        "total_volume": 1000000000.0,
        "high_24h": 1.01,
        "low_24h": 0.99,
        "price_change_24h": 0.005,
        "price_change_percentage_24h": 0.5,  # stable
        "last_updated": "2026-01-15T10:30:45.123Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    }
]

# Records with no price range (high_24h == low_24h)
NO_RANGE_SILVER_RECORDS = [
    {
        "id": "norange",
        "symbol": "nrg",
        "name": "NoRangeCoin",
        "current_price": 10.0,
        "market_cap": 100000000.0,
        "market_cap_rank": 50.0,
        "total_volume": 5000000.0,
        "high_24h": 10.0,
        "low_24h": 10.0,
        "price_change_24h": 0.0,
        "price_change_percentage_24h": 0.0,
        "last_updated": "2026-01-15T10:30:45.123Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    }
]

# Records for rolling average testing - multiple timestamps
ROLLING_AVG_SILVER_RECORDS = [
    # First batch at 10:30
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45000.0,
        "market_cap": 850000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 25000000000.0,
        "high_24h": 46000.0,
        "low_24h": 44000.0,
        "price_change_24h": 500.0,
        "price_change_percentage_24h": 1.12,
        "last_updated": "2026-01-15T10:30:00.000Z",
        "processed_at": datetime(2026, 1, 15, 10, 30, 0),
        "date": date(2026, 1, 15),
    },
    # Second batch at 10:30 (same window)
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 46000.0,
        "market_cap": 860000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 26000000000.0,
        "high_24h": 46500.0,
        "low_24h": 44500.0,
        "price_change_24h": 600.0,
        "price_change_percentage_24h": 1.32,
        "last_updated": "2026-01-15T10:30:30.000Z",
        "processed_at": datetime(2026, 1, 15, 10, 30, 30),
        "date": date(2026, 1, 15),
    },
    # Third batch at 10:31 (same window)
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45500.0,
        "market_cap": 855000000000.0,
        "market_cap_rank": 1.0,
        "total_volume": 25500000000.0,
        "high_24h": 46200.0,
        "low_24h": 44200.0,
        "price_change_24h": 550.0,
        "price_change_percentage_24h": 1.22,
        "last_updated": "2026-01-15T10:31:00.000Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    },
    # Different coin in same window
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 2500.0,
        "market_cap": 300000000000.0,
        "market_cap_rank": 2.0,
        "total_volume": 15000000000.0,
        "high_24h": 2600.0,
        "low_24h": 2450.0,
        "price_change_24h": 50.0,
        "price_change_percentage_24h": 2.04,
        "last_updated": "2026-01-15T10:30:15.000Z",
        "processed_at": datetime(2026, 1, 15, 10, 30, 15),
        "date": date(2026, 1, 15),
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 2550.0,
        "market_cap": 305000000000.0,
        "market_cap_rank": 2.0,
        "total_volume": 15500000000.0,
        "high_24h": 2620.0,
        "low_24h": 2470.0,
        "price_change_24h": 60.0,
        "price_change_percentage_24h": 2.41,
        "last_updated": "2026-01-15T10:31:30.000Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 30),
        "date": date(2026, 1, 15),
    },
]

# Edge case: zero current price (division by zero scenario)
ZERO_PRICE_SILVER_RECORDS = [
    {
        "id": "zerocoin",
        "symbol": "zero",
        "name": "ZeroCoin",
        "current_price": 0.0,
        "market_cap": 0.0,
        "market_cap_rank": 1000.0,
        "total_volume": 0.0,
        "high_24h": 0.01,
        "low_24h": 0.0,
        "price_change_24h": 0.0,
        "price_change_percentage_24h": 0.0,
        "last_updated": "2026-01-15T10:30:45.123Z",
        "processed_at": datetime(2026, 1, 15, 10, 31, 0),
        "date": date(2026, 1, 15),
    }
]
