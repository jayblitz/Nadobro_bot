"""Required-field clarifier for Strategy Studio intents."""

from __future__ import annotations

from typing import Optional

from src.nadobro.studio.intent import TradingIntent


def next_question(intent: TradingIntent) -> Optional[str]:
    if not intent.action:
        return "Do you want to buy, sell, close, or modify a strategy?"
    if intent.action in ("buy", "sell", "modify") and not intent.symbol:
        return "Which market should I use, for example BTC, ETH, or SOL?"
    if intent.action in ("buy", "sell") and intent.quantity is None:
        return "What size should I use: contracts, USDT notional, or percent of balance?"
    if intent.action in ("buy", "sell") and intent.order_type is None:
        return "Should this be a market, limit, conditional, or trailing order?"
    if intent.order_type == "limit" and intent.entry_price is None:
        return "What limit entry price should I use?"
    if intent.action in ("buy", "sell") and intent.leverage is None:
        return "What leverage should I use?"
    if intent.action in ("buy", "sell") and intent.take_profit is None:
        return "What take-profit should I set? You can say a price or a percent."
    if intent.action in ("buy", "sell") and intent.stop_loss is None:
        return "What stop-loss should I set? You can say a price or a percent."
    if intent.order_type == "conditional" and not intent.conditions:
        return "What condition should trigger this strategy?"
    return None
