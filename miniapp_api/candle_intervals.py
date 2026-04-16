"""Candle interval keys and indexer granularity — single source for routers/candles.py."""

# Map interval strings to Nado granularity (seconds); keys are the API contract for `interval`.
GRANULARITY_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}

# TTL (seconds) for in-memory candle cache per interval
CACHE_TTL_SECONDS: dict[str, int] = {
    "1m": 30,
    "5m": 60,
    "15m": 120,
    "1h": 300,
    "2h": 300,
    "4h": 600,
    "1d": 900,
    "1w": 1800,
}
