"""Prompt constants for Strategy Studio extraction."""

SYSTEM_PROMPT = """You extract structured trading intents for Nadobro, a Telegram bot for the Nado Perp DEX.

Hard rules:
- If a field is not stated, set it to null. Never invent values.
- Return JSON only, no commentary.
- Resolve symbols to base tickers like BTC, ETH, SOL. Do not append USDT.
- All times are UTC. Convert relative phrases to ISO 8601 using the supplied current time.
- Use action buy for long/open long and sell for short/open short.
- Use order_type conditional when the user describes indicator/price/funding triggers.
- Use Quantity.type contracts for base-asset contract amounts, usdt for quote notional, percent_balance for account percentage.

Schema keys:
action, symbol, order_type, entry_price, quantity, take_profit, stop_loss, leverage, time_limit, conditions, network, notes, raw_input.
"""

FEW_SHOTS = [
    {
        "user": "Long ETH 0.1 at 10x",
        "json": {
            "action": "buy",
            "symbol": "ETH",
            "order_type": "market",
            "entry_price": None,
            "quantity": {"type": "contracts", "value": 0.1},
            "take_profit": None,
            "stop_loss": None,
            "leverage": 10,
            "time_limit": None,
            "conditions": [],
            "network": None,
            "notes": None,
        },
    },
    {
        "user": "Buy BTC limit 62000 with TP +5% and SL -2%",
        "json": {
            "action": "buy",
            "symbol": "BTC",
            "order_type": "limit",
            "entry_price": 62000,
            "quantity": None,
            "take_profit": {"type": "percent", "value": 5, "trailing": False},
            "stop_loss": {"type": "percent", "value": -2, "trailing": False},
            "leverage": None,
            "time_limit": None,
            "conditions": [],
            "network": None,
            "notes": None,
        },
    },
    {
        "user": "Buy BTC when RSI(14) on 1h drops below 30, close by Friday 17:00 UTC",
        "json": {
            "action": "buy",
            "symbol": "BTC",
            "order_type": "conditional",
            "entry_price": None,
            "quantity": None,
            "take_profit": None,
            "stop_loss": None,
            "leverage": None,
            "time_limit": "RESOLVE_ISO_UTC",
            "conditions": [{"indicator": "RSI", "timeframe": "1h", "period": 14, "params": None, "operator": "<", "value": 30}],
            "network": None,
            "notes": None,
        },
    },
    {
        "user": "Short ETH when MACD crosses below signal, 0.5 contracts, trailing 1% stop",
        "json": {
            "action": "sell",
            "symbol": "ETH",
            "order_type": "conditional",
            "entry_price": None,
            "quantity": {"type": "contracts", "value": 0.5},
            "take_profit": None,
            "stop_loss": {"type": "percent", "value": 1, "trailing": True},
            "leverage": None,
            "time_limit": None,
            "conditions": [{"indicator": "MACD", "timeframe": "1h", "period": None, "params": {"fast": 12, "slow": 26, "signal": 9}, "operator": "crosses_below", "value": 0}],
            "network": None,
            "notes": None,
        },
    },
    {
        "user": "Auto-close my SOL trade in 6h",
        "json": {
            "action": "modify",
            "symbol": "SOL",
            "order_type": None,
            "entry_price": None,
            "quantity": None,
            "take_profit": None,
            "stop_loss": None,
            "leverage": None,
            "time_limit": "RESOLVE_ISO_UTC",
            "conditions": [],
            "network": None,
            "notes": "modify existing auto-close",
        },
    },
    {
        "user": "Take a small BTC position",
        "json": {
            "action": "buy",
            "symbol": "BTC",
            "order_type": "market",
            "entry_price": None,
            "quantity": None,
            "take_profit": None,
            "stop_loss": None,
            "leverage": None,
            "time_limit": None,
            "conditions": [],
            "network": None,
            "notes": "size was ambiguous",
        },
    },
]
