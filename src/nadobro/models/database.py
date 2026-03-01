import enum
import json
from datetime import datetime
from typing import Any, Optional

from src.nadobro.db import query_one, query_all, execute, execute_returning, query_count


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


def init_db():
    from src.nadobro.db import init_db as _init
    _init()


def get_bot_state(key: str) -> Optional[dict]:
    row = query_one("SELECT value FROM bot_state WHERE key = %s", (key,))
    if not row:
        return None
    raw = row.get("value")
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None


def set_bot_state(key: str, value: Any):
    payload = json.dumps(value) if not isinstance(value, str) else value
    now = datetime.utcnow().isoformat()
    execute(
        """INSERT INTO bot_state (key, value, updated_at) VALUES (%s, %s, %s)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at""",
        (key, payload, now),
    )


def get_bot_state_raw(key: str) -> Optional[str]:
    row = query_one("SELECT value FROM bot_state WHERE key = %s", (key,))
    if not row:
        return None
    return row.get("value")


def insert_trade(data: dict) -> Optional[int]:
    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    vals = [data[c] for c in cols]
    row = execute_returning(
        f"INSERT INTO trades ({col_names}) VALUES ({placeholders}) RETURNING id",
        vals,
    )
    return row["id"] if row else None


_TRADE_UPDATE_ALLOWED_COLS = frozenset({
    "status", "order_digest", "price", "filled_at", "error_message",
})


def update_trade(trade_id: int, data: dict):
    disallowed = set(data.keys()) - _TRADE_UPDATE_ALLOWED_COLS
    if disallowed:
        raise ValueError(f"update_trade: disallowed column(s): {disallowed}")
    sets = ", ".join([f"{k} = %s" for k in data.keys()])
    vals = list(data.values()) + [trade_id]
    execute(f"UPDATE trades SET {sets} WHERE id = %s", vals)


def get_last_trade_for_rate_limit(telegram_id: int) -> Optional[dict]:
    return query_one(
        "SELECT created_at FROM trades WHERE user_id = %s AND status IN ('pending', 'filled') ORDER BY created_at DESC LIMIT 1",
        (telegram_id,),
    )


def get_trades_by_user(telegram_id: int, limit: int = 50) -> list:
    return query_all(
        "SELECT * FROM trades WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
        (telegram_id, limit),
    )


def insert_alert(data: dict) -> Optional[int]:
    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    vals = [data[c] for c in cols]
    row = execute_returning(
        f"INSERT INTO alerts ({col_names}) VALUES ({placeholders}) RETURNING id",
        vals,
    )
    return row["id"] if row else None


def get_alerts_by_user(telegram_id: int, active_only: bool = True) -> list:
    if active_only:
        return query_all(
            "SELECT * FROM alerts WHERE user_id = %s AND is_active = true ORDER BY created_at DESC",
            (telegram_id,),
        )
    return query_all(
        "SELECT * FROM alerts WHERE user_id = %s ORDER BY created_at DESC",
        (telegram_id,),
    )


def update_alert(alert_id: int, is_active: bool):
    execute(
        "UPDATE alerts SET is_active = %s WHERE id = %s",
        (is_active, alert_id),
    )


def get_alert_by_id_and_user(alert_id: int, telegram_id: int) -> Optional[dict]:
    return query_one(
        "SELECT * FROM alerts WHERE id = %s AND user_id = %s",
        (alert_id, telegram_id),
    )


def get_all_active_alerts() -> list:
    return query_all("SELECT * FROM alerts WHERE is_active = true")


def update_alert_triggered(alert_id: int):
    execute(
        "UPDATE alerts SET is_active = false, triggered_at = %s WHERE id = %s",
        (datetime.utcnow().isoformat(), alert_id),
    )


def insert_admin_log(data: dict):
    execute(
        "INSERT INTO admin_logs (admin_id, action, details) VALUES (%s, %s, %s)",
        (data.get("admin_id"), data.get("action"), data.get("details")),
    )


def get_trades_count() -> int:
    return query_count("SELECT COUNT(*) FROM trades")


def get_trades_count_filled() -> int:
    return query_count("SELECT COUNT(*) FROM trades WHERE status = 'filled'")


def get_trades_count_failed() -> int:
    return query_count("SELECT COUNT(*) FROM trades WHERE status = 'failed'")


def get_total_volume_filled() -> float:
    rows = query_all("SELECT size, price FROM trades WHERE status = 'filled'")
    total = 0.0
    for row in rows:
        size = float(row.get("size") or 0)
        price = float(row.get("price") or 0)
        total += size * price
    return total


def get_recent_trades(limit: int = 20) -> list:
    return query_all(
        "SELECT * FROM trades ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )


def get_recent_admin_logs(limit: int = 20) -> list:
    return query_all(
        "SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )


class UserRow:
    def __init__(self, data: dict):
        self._data = data or {}
        tid = self._data.get("telegram_id")
        self.telegram_id = int(tid) if tid is not None else None
        self.telegram_username = self._data.get("telegram_username")
        self.main_address = self._data.get("main_address")
        self.linked_signer_address = self._data.get("linked_signer_address")
        self.encrypted_linked_signer_pk = self._data.get("encrypted_linked_signer_pk")
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
