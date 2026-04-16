"""Pydantic request/response models for the Mini App API."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------

class UserResponse(BaseModel):
    telegram_id: int
    username: str = ""
    language: str = "en"
    network: str = "mainnet"
    main_address: Optional[str] = None
    tos_accepted: bool = False
    is_new: bool = False
    total_trades: int = 0
    total_volume_usd: float = 0.0


class UpdateSettingsRequest(BaseModel):
    language: Optional[str] = None
    tos_accepted: Optional[bool] = None


class SwitchNetworkRequest(BaseModel):
    network: str = Field(..., pattern=r"^(testnet|mainnet)$")


# ---------------------------------------------------------------------------
# Market
# ---------------------------------------------------------------------------

class ProductInfo(BaseModel):
    id: int
    name: str
    symbol: str = ""
    type: str = "perp"
    max_leverage: int = 20
    isolated_only: bool = False
    # Nado-style tab: perps, spot, memes, defi, chains, commodities
    category: str = "perps"


class PriceResponse(BaseModel):
    product: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    timestamp: float = 0.0


class AllPricesResponse(BaseModel):
    prices: dict[str, PriceResponse]


# ---------------------------------------------------------------------------
# Trade
# ---------------------------------------------------------------------------

class MarketOrderRequest(BaseModel):
    product: str
    side: str = Field(..., pattern=r"^(long|short)$")
    size_usd: float = Field(..., gt=0)
    leverage: float = Field(1.0, ge=1)
    take_profit_pct: Optional[float] = None
    stop_loss_pct: Optional[float] = None
    take_profit_price: Optional[float] = None
    stop_loss_price: Optional[float] = None


class LimitOrderRequest(BaseModel):
    product: str
    side: str = Field(..., pattern=r"^(long|short)$")
    size_usd: float = Field(..., gt=0)
    price: float = Field(..., gt=0)
    leverage: float = Field(1.0, ge=1)
    take_profit_pct: Optional[float] = None
    stop_loss_pct: Optional[float] = None


class TradeResponse(BaseModel):
    ok: bool
    trade_id: Optional[int] = None
    digest: Optional[str] = None
    fill_price: Optional[float] = None
    size: Optional[float] = None
    side: Optional[str] = None
    product: Optional[str] = None
    error: Optional[str] = None


class ParseIntentRequest(BaseModel):
    text: str


class ParseIntentResponse(BaseModel):
    intent: Optional[str] = None
    product: Optional[str] = None
    side: Optional[str] = None
    size_usd: Optional[float] = None
    price: Optional[float] = None
    leverage: Optional[float] = None
    raw: Optional[dict[str, object]] = None


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

class PositionResponse(BaseModel):
    product_id: int
    product_name: str = ""
    side: str = ""
    size: float = 0.0
    entry_price: float = 0.0
    mark_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    leverage: Optional[float] = None
    liquidation_price: Optional[float] = None
    margin: Optional[float] = None


class ClosePositionRequest(BaseModel):
    product: str
    close_pct: float = Field(100.0, gt=0, le=100)


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------

class PortfolioSummary(BaseModel):
    equity: float = 0.0
    balance_usd: float = 0.0
    available_balance: float = 0.0
    total_unrealized_pnl: float = 0.0
    unrealized_spot_pnl: float = 0.0
    total_margin_used: float = 0.0
    margin_utilization: Optional[float] = None
    total_volume_usd: float = 0.0
    fee_tier_display: str = "—"
    nlp_balance_usd: float = 0.0
    positions: list[PositionResponse] = []
    open_orders_count: int = 0


class TradeHistoryItem(BaseModel):
    id: int
    product_name: str
    side: str
    size: float
    price: Optional[float] = None
    leverage: float = 1.0
    status: str = "pending"
    pnl: Optional[float] = None
    fees: float = 0.0
    created_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Strategies (bot_runtime: grid, rgrid, dn, vol, bro)
# ---------------------------------------------------------------------------

class StrategyStartRequest(BaseModel):
    strategy: str = Field(..., pattern=r"^(grid|rgrid|dn|vol|bro)$")
    product: str = Field(default="BTC", min_length=1, max_length=32)
    leverage: float = Field(default=3.0, ge=1)
    slippage_pct: float = Field(default=1.0, ge=0, le=100)
    direction: str = Field(default="long", pattern=r"^(long|short)$")


class StrategyActionResponse(BaseModel):
    ok: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

class CreateAlertRequest(BaseModel):
    product: str
    condition: str = Field(..., pattern=r"^(above|below|funding_above|funding_below|pnl_above|pnl_below)$")
    target_value: float


class AlertResponse(BaseModel):
    id: int
    product_name: str
    condition: str
    target_value: float
    is_active: bool = True
    triggered_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------

class OkResponse(BaseModel):
    ok: bool = True
    message: str = ""
