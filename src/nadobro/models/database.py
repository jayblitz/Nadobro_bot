"""
Nadobro data layer: enums and Supabase-backed storage.
No SQLAlchemy; all persistence via supabase_client.
"""
import enum
import json
from datetime import datetime
from typing import Any, Optional

from src.nadobro.supabase_client import get_supabase


# --- Enums (kept for compatibility) ---
class NetworkMode(enum.Enum):
    TESTNET = "testnet"
    MAINNET = "mainnet"


class OrderSide(enum.Enum):
    LONG = "long"
    SHORT = "short"


class OrderTypeEnum(enum.Enum):
    MARKET = "market"
    LIMIT = "limit"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"


class TradeStatus(enum.Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AlertCondition(enum.Enum):
    ABOVE = "above"
    BELOW = "below"
    FUNDING_ABOVE = "funding_above"
    FUNDING_BELOW = "funding_below"
    PNL_ABOVE = "pnl_above"
    PNL_BELOW = "pnl_below"


# --- Init (Supabase) ---
def init_db():
    """Initialize Supabase client. Tables must exist (run migrations in Supabase dashboard)."""
    from src.nadobro.supabase_client import init_supabase
    init_supabase()


# --- Bot state (key-value) ---
def get_bot_state(key: str) -> Optional[dict]:
    """Get bot_state row by key. Returns parsed value dict or None."""
    sb = get_supabase()
    r = sb.table("bot_state").select("value").eq("key", key).execute()
    if not r.data or len(r.data) == 0:
        return None
    raw = r.data[0].get("value")
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None


def set_bot_state(key: str, value: Any):
    """Upsert bot_state row. value will be JSON-serialized."""
    sb = get_supabase()
    payload = json.dumps(value) if not isinstance(value, str) else value
    now = datetime.utcnow().isoformat()
    existing = sb.table("bot_state").select("id").eq("key", key).execute()
    if existing.data and len(existing.data) > 0:
        sb.table("bot_state").update({"value": payload, "updated_at": now}).eq("key", key).execute()
    else:
        sb.table("bot_state").insert({"key": key, "value": payload}).execute()
    return None


def get_bot_state_raw(key: str) -> Optional[str]:
    """Get raw value string for bot_state key."""
    sb = get_supabase()
    r = sb.table("bot_state").select("value").eq("key", key).execute()
    if not r.data or len(r.data) == 0:
        return None
    return r.data[0].get("value")


def insert_trade(data: dict) -> Optional[int]:
    """Insert trade row; return new id."""
    sb = get_supabase()
    r = sb.table("trades").insert(data).execute()
    if r.data and len(r.data) > 0:
        return r.data[0].get("id")
    return None


def update_trade(trade_id: int, data: dict):
    sb = get_supabase()
    sb.table("trades").update(data).eq("id", trade_id).execute()


def get_last_trade_for_rate_limit(telegram_id: int) -> Optional[dict]:
    """Get most recent PENDING or FILLED trade for user."""
    sb = get_supabase()
    r = (
        sb.table("trades")
        .select("created_at")
        .eq("user_id", telegram_id)
        .in_("status", ["pending", "filled"])
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if r.data and len(r.data) > 0:
        return r.data[0]
    return None


def get_trades_by_user(telegram_id: int, limit: int = 50) -> list:
    r = get_supabase().table("trades").select("*").eq("user_id", telegram_id).order("created_at", desc=True).limit(limit).execute()
    return r.data or []


def insert_alert(data: dict) -> Optional[int]:
    r = get_supabase().table("alerts").insert(data).execute()
    if r.data and len(r.data) > 0:
        return r.data[0].get("id")
    return None


def get_alerts_by_user(telegram_id: int, active_only: bool = True) -> list:
    q = get_supabase().table("alerts").select("*").eq("user_id", telegram_id)
    if active_only:
        q = q.eq("is_active", True)
    r = q.order("created_at", desc=True).execute()
    return r.data or []


def update_alert(alert_id: int, data: dict):
    get_supabase().table("alerts").update(data).eq("id", alert_id).execute()


def get_alert_by_id_and_user(alert_id: int, telegram_id: int) -> Optional[dict]:
    r = get_supabase().table("alerts").select("*").eq("id", alert_id).eq("user_id", telegram_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]
    return None


def get_all_active_alerts() -> list:
    r = get_supabase().table("alerts").select("*").eq("is_active", True).execute()
    return r.data or []


def update_alert_triggered(alert_id: int):
    get_supabase().table("alerts").update({"is_active": False, "triggered_at": datetime.utcnow().isoformat()}).eq("id", alert_id).execute()


def insert_admin_log(data: dict):
    get_supabase().table("admin_logs").insert(data).execute()


def get_trades_count() -> int:
    r = get_supabase().table("trades").select("id", count="exact").execute()
    return r.count or 0


def get_trades_count_filled() -> int:
    r = get_supabase().table("trades").select("id", count="exact").eq("status", "filled").execute()
    return r.count or 0


def get_trades_count_failed() -> int:
    r = get_supabase().table("trades").select("id", count="exact").eq("status", "failed").execute()
    return r.count or 0


def get_total_volume_filled() -> float:
    r = get_supabase().table("trades").select("size, price").eq("status", "filled").execute()
    total = 0.0
    for row in (r.data or []):
        size = float(row.get("size") or 0)
        price = float(row.get("price") or 0)
        total += size * price
    return total


def get_recent_trades(limit: int = 20) -> list:
    r = get_supabase().table("trades").select("*").order("created_at", desc=True).limit(limit).execute()
    return r.data or []


def get_recent_admin_logs(limit: int = 20) -> list:
    r = get_supabase().table("admin_logs").select("*").order("created_at", desc=True).limit(limit).execute()
    return r.data or []


class UserRow:
    """Simple user row for compatibility (attribute access)."""
    def __init__(self, data: dict):
        self._data = data or {}
        tid = self._data.get("telegram_id")
        self.telegram_id = int(tid) if tid is not None else None
        self.telegram_username = self._data.get("telegram_username")
        self.main_address = self._data.get("main_address")
        self.linked_signer_address = self._data.get("linked_signer_address")
        self.encrypted_linked_signer_pk = self._data.get("encrypted_linked_signer_pk")  # bytes or b64
        self.salt = self._data.get("salt")
        self.language = self._data.get("language") or "en"
        self.strategy_settings = self._data.get("strategy_settings") or {}
        nm = self._data.get("network_mode") or "mainnet"
        self.network_mode = NetworkMode.MAINNET if nm == "mainnet" else NetworkMode.TESTNET
        self.created_at = self._data.get("created_at")
        self.last_active = self._data.get("last_active")
        self.last_trade_at = self._data.get("last_trade_at")
        self.total_trades = int(self._data.get("total_trades") or 0)
        self.total_volume_usd = float(self._data.get("total_volume_usd") or 0)
        # Legacy compat: single main_address; "has key" = linked signer set
        self.wallet_address_testnet = self.main_address
        self.wallet_address_mainnet = self.main_address
        linked = bool(self.linked_signer_address and self.encrypted_linked_signer_pk)
        self.encrypted_private_key_testnet = "linked" if linked else None
        self.encrypted_private_key_mainnet = "linked" if linked else None
