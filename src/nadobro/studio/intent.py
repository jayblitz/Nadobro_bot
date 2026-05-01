"""Pydantic schemas for Strategy Studio trading intents."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, root_validator, validator

from src.nadobro.config import get_product_max_leverage

QuantityType = Literal["percent_balance", "usdt", "contracts"]
PriceLevelType = Literal["percent", "absolute_price"]
Operator = Literal["<", "<=", ">", ">=", "==", "crosses_above", "crosses_below"]
Timeframe = Literal["1m", "5m", "15m", "1h", "4h", "1d"]


class Quantity(BaseModel):
    type: QuantityType
    value: float

    @validator("value")
    def value_positive(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("quantity value must be greater than 0")
        return value


class PriceLevel(BaseModel):
    type: PriceLevelType
    value: float
    trailing: bool = False


class Condition(BaseModel):
    indicator: Literal["RSI", "EMA", "SMA", "MACD", "BBANDS", "ATR", "STOCH", "VWAP", "PRICE", "FUNDING"]
    timeframe: Timeframe = "1h"
    period: Optional[int] = None
    params: Optional[dict] = None
    operator: Operator
    value: float

    @validator("indicator", pre=True)
    def normalize_indicator(cls, value: str) -> str:
        return str(value or "").upper()


class TradingIntent(BaseModel):
    action: Literal["buy", "sell", "close", "modify"]
    symbol: Optional[str] = None
    order_type: Optional[Literal["market", "limit", "conditional", "trailing"]] = None
    entry_price: Optional[float] = None
    quantity: Optional[Quantity] = None
    take_profit: Optional[PriceLevel] = None
    stop_loss: Optional[PriceLevel] = None
    leverage: Optional[int] = None
    time_limit: Optional[datetime] = None
    conditions: list[Condition] = Field(default_factory=list)
    network: Optional[Literal["testnet", "mainnet"]] = None
    notes: Optional[str] = None
    raw_input: str

    @validator("symbol", pre=True)
    def normalize_symbol(cls, value):
        if value is None:
            return None
        text = str(value).upper().strip()
        return text.replace("-PERP", "").replace("USDT", "").strip() or None

    @validator("time_limit")
    def reject_past_time_limit(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        if value <= datetime.now(timezone.utc):
            raise ValueError("time_limit must be in the future")
        return value

    @root_validator(skip_on_failure=True)
    def validate_cross_fields(cls, values: dict) -> dict:
        conditions = values.get("conditions") or []
        order_type = values.get("order_type")
        if conditions and order_type != "trailing":
            values["order_type"] = "conditional"
        if values.get("order_type") == "limit" and values.get("entry_price") is None:
            raise ValueError("entry_price is required for limit orders")
        if values.get("entry_price") is not None and values["entry_price"] <= 0:
            raise ValueError("entry_price must be greater than 0")
        leverage = values.get("leverage")
        symbol = values.get("symbol")
        network = values.get("network")
        if leverage is not None:
            if leverage < 1:
                raise ValueError("leverage must be at least 1")
            if symbol and network:
                max_leverage = get_product_max_leverage(symbol, network=network)
                if leverage > max_leverage:
                    raise ValueError(f"leverage exceeds max {max_leverage}x for {symbol}")
        return values


def intent_from_json(payload: dict, raw_fallback: str = "") -> TradingIntent:
    """Build an intent while preserving raw text when mocked LLM payloads omit it."""
    data = dict(payload or {})
    data.setdefault("raw_input", raw_fallback)
    return TradingIntent.parse_obj(data)
