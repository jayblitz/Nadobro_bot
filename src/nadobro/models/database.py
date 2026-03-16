import enum
import json
from datetime import datetime
from typing import Any, Optional

from psycopg2 import sql as pgsql
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
    CLOSED = "closed"


class AlertCondition(enum.Enum):
    ABOVE = "above"
    BELOW = "below"
    FUNDING_ABOVE = "funding_above"
    FUNDING_BELOW = "funding_below"
    PNL_ABOVE = "pnl_above"
    PNL_BELOW = "pnl_below"


_VALID_NETWORKS = frozenset({"testnet", "mainnet"})


def _trades_table(network: str) -> str:
    if network not in _VALID_NETWORKS:
        raise ValueError(f"Invalid network: {network}")
    return f"trades_{network}"


def _alerts_table(network: str) -> str:
    if network not in _VALID_NETWORKS:
        raise ValueError(f"Invalid network: {network}")
    return f"alerts_{network}"


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


_TRADE_INSERT_ALLOWED_COLS = frozenset({
    "user_id", "product_id", "product_name", "order_type", "side",
    "size", "price", "leverage", "status", "order_digest", "pnl",
    "fees", "error_message", "created_at", "filled_at",
    "close_price", "closed_at",
})


def insert_trade(data: dict, network: str = "mainnet") -> Optional[int]:
    filtered = {k: v for k, v in data.items() if k != "network"}
    disallowed = set(filtered.keys()) - _TRADE_INSERT_ALLOWED_COLS
    if disallowed:
        raise ValueError(f"insert_trade: disallowed column(s): {disallowed}")
    table = _trades_table(network)
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    query = pgsql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
        pgsql.Identifier(table),
        pgsql.SQL(", ").join(pgsql.Identifier(c) for c in cols),
        pgsql.SQL(", ").join(pgsql.Placeholder() * len(cols)),
    )
    row = execute_returning(query, vals)
    return row["id"] if row else None


_TRADE_UPDATE_ALLOWED_COLS = frozenset({
    "status", "order_digest", "price", "filled_at", "error_message",
    "pnl", "fees", "close_price", "closed_at",
})


def update_trade(trade_id: int, data: dict, network: str = "mainnet"):
    disallowed = set(data.keys()) - _TRADE_UPDATE_ALLOWED_COLS
    if disallowed:
        raise ValueError(f"update_trade: disallowed column(s): {disallowed}")
    table = _trades_table(network)
    set_clause = pgsql.SQL(", ").join(
        pgsql.SQL("{} = %s").format(pgsql.Identifier(k)) for k in data.keys()
    )
    query = pgsql.SQL("UPDATE {} SET {} WHERE id = %s").format(
        pgsql.Identifier(table), set_clause
    )
    vals = list(data.values()) + [trade_id]
    execute(query, vals)


def get_last_trade_for_rate_limit(telegram_id: int, network: str = "mainnet") -> Optional[dict]:
    table = _trades_table(network)
    return query_one(
        f"SELECT created_at FROM {table} WHERE user_id = %s AND status = 'filled' ORDER BY created_at DESC LIMIT 1",
        (telegram_id,),
    )


def find_open_trade(telegram_id: int, product_id: int, network: str = "mainnet") -> Optional[dict]:
    table = _trades_table(network)
    return query_one(
        f"SELECT * FROM {table} WHERE user_id = %s AND product_id = %s AND status = 'filled' ORDER BY created_at DESC LIMIT 1",
        (telegram_id, product_id),
    )


def get_trades_by_user(telegram_id: int, limit: int = 50, network: str = "mainnet") -> list:
    table = _trades_table(network)
    return query_all(
        f"SELECT * FROM {table} WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
        (telegram_id, limit),
    )


_ALERT_INSERT_ALLOWED_COLS = frozenset({
    "user_id", "product_id", "product_name", "condition",
    "target_value", "is_active", "created_at",
})


def insert_alert(data: dict, network: str = "mainnet") -> Optional[int]:
    filtered = {k: v for k, v in data.items() if k != "network"}
    disallowed = set(filtered.keys()) - _ALERT_INSERT_ALLOWED_COLS
    if disallowed:
        raise ValueError(f"insert_alert: disallowed column(s): {disallowed}")
    table = _alerts_table(network)
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    query = pgsql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
        pgsql.Identifier(table),
        pgsql.SQL(", ").join(pgsql.Identifier(c) for c in cols),
        pgsql.SQL(", ").join(pgsql.Placeholder() * len(cols)),
    )
    row = execute_returning(query, vals)
    return row["id"] if row else None


def get_alerts_by_user(telegram_id: int, active_only: bool = True, network: str = "mainnet") -> list:
    table = _alerts_table(network)
    if active_only:
        return query_all(
            f"SELECT * FROM {table} WHERE user_id = %s AND is_active = true ORDER BY created_at DESC",
            (telegram_id,),
        )
    return query_all(
        f"SELECT * FROM {table} WHERE user_id = %s ORDER BY created_at DESC",
        (telegram_id,),
    )


def update_alert(alert_id: int, is_active: bool, network: str = "mainnet"):
    table = _alerts_table(network)
    execute(
        f"UPDATE {table} SET is_active = %s WHERE id = %s",
        (is_active, alert_id),
    )


def get_alert_by_id_and_user(alert_id: int, telegram_id: int, network: str = "mainnet") -> Optional[dict]:
    table = _alerts_table(network)
    return query_one(
        f"SELECT * FROM {table} WHERE id = %s AND user_id = %s",
        (alert_id, telegram_id),
    )


def get_all_active_alerts(network: str = None) -> list:
    if network:
        table = _alerts_table(network)
        return query_all(f"SELECT * FROM {table} WHERE is_active = true")
    testnet_alerts = query_all("SELECT *, 'testnet' AS network FROM alerts_testnet WHERE is_active = true")
    mainnet_alerts = query_all("SELECT *, 'mainnet' AS network FROM alerts_mainnet WHERE is_active = true")
    return testnet_alerts + mainnet_alerts


def update_alert_triggered(alert_id: int, network: str = "mainnet"):
    table = _alerts_table(network)
    execute(
        f"UPDATE {table} SET is_active = false, triggered_at = %s WHERE id = %s",
        (datetime.utcnow().isoformat(), alert_id),
    )


def insert_admin_log(data: dict):
    execute(
        "INSERT INTO admin_logs (admin_id, action, details) VALUES (%s, %s, %s)",
        (data.get("admin_id"), data.get("action"), data.get("details")),
    )


def get_trades_count(network: str = None) -> int:
    if network:
        return query_count(f"SELECT COUNT(*) FROM {_trades_table(network)}")
    t = query_count("SELECT COUNT(*) FROM trades_testnet")
    m = query_count("SELECT COUNT(*) FROM trades_mainnet")
    return t + m


def get_trades_count_filled(network: str = None) -> int:
    if network:
        return query_count(f"SELECT COUNT(*) FROM {_trades_table(network)} WHERE status = 'filled'")
    t = query_count("SELECT COUNT(*) FROM trades_testnet WHERE status = 'filled'")
    m = query_count("SELECT COUNT(*) FROM trades_mainnet WHERE status = 'filled'")
    return t + m


def get_trades_count_failed(network: str = None) -> int:
    if network:
        return query_count(f"SELECT COUNT(*) FROM {_trades_table(network)} WHERE status = 'failed'")
    t = query_count("SELECT COUNT(*) FROM trades_testnet WHERE status = 'failed'")
    m = query_count("SELECT COUNT(*) FROM trades_mainnet WHERE status = 'failed'")
    return t + m


def get_total_volume_filled(network: str = None) -> float:
    if network:
        table = _trades_table(network)
        row = query_one(f"SELECT COALESCE(SUM(size * price), 0) AS total FROM {table} WHERE status = 'filled'")
        return float(row["total"]) if row else 0.0
    t = query_one("SELECT COALESCE(SUM(size * price), 0) AS total FROM trades_testnet WHERE status = 'filled'")
    m = query_one("SELECT COALESCE(SUM(size * price), 0) AS total FROM trades_mainnet WHERE status = 'filled'")
    return float(t["total"] if t else 0) + float(m["total"] if m else 0)


def get_recent_trades(limit: int = 20, network: str = None) -> list:
    if network:
        table = _trades_table(network)
        return query_all(
            f"SELECT *, '{network}' AS network FROM {table} ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
    t = query_all(
        "SELECT *, 'testnet' AS network FROM trades_testnet ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    m = query_all(
        "SELECT *, 'mainnet' AS network FROM trades_mainnet ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    combined = t + m
    combined.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return combined[:limit]


def get_recent_admin_logs(limit: int = 20) -> list:
    return query_all(
        "SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )


def upsert_copy_trader(wallet_address: str, label: str = "", is_curated: bool = False) -> Optional[int]:
    wallet = wallet_address.strip()
    row = execute_returning(
        """INSERT INTO copy_traders (wallet_address, label, is_curated, active)
           VALUES (%s, %s, %s, true)
           ON CONFLICT (wallet_address) DO UPDATE
               SET label = EXCLUDED.label, is_curated = EXCLUDED.is_curated, active = true
           RETURNING id""",
        (wallet, label, is_curated),
    )
    return row["id"] if row else None


def get_copy_trader(trader_id: int) -> Optional[dict]:
    return query_one("SELECT * FROM copy_traders WHERE id = %s", (trader_id,))


def get_copy_trader_by_wallet(wallet: str) -> Optional[dict]:
    return query_one("SELECT * FROM copy_traders WHERE wallet_address = %s", (wallet.strip(),))


def get_active_copy_traders() -> list:
    return query_all("SELECT * FROM copy_traders WHERE active = true ORDER BY id")


def get_curated_copy_traders() -> list:
    return query_all("SELECT * FROM copy_traders WHERE active = true AND is_curated = true ORDER BY id")


def deactivate_copy_trader(trader_id: int):
    execute("UPDATE copy_traders SET active = false WHERE id = %s", (trader_id,))


def create_copy_mirror(user_id: int, trader_id: int, budget_usd: float = 100.0,
                        risk_factor: float = 1.0, max_leverage: float = 10.0) -> Optional[int]:
    row = execute_returning(
        """INSERT INTO copy_mirrors (user_id, trader_id, budget_usd, risk_factor, max_leverage, active)
           VALUES (%s, %s, %s, %s, %s, true)
           ON CONFLICT (user_id, trader_id) DO UPDATE
               SET budget_usd = EXCLUDED.budget_usd,
                   risk_factor = EXCLUDED.risk_factor,
                   max_leverage = EXCLUDED.max_leverage,
                   active = true,
                   stopped_at = NULL
           RETURNING id""",
        (user_id, trader_id, budget_usd, risk_factor, max_leverage),
    )
    return row["id"] if row else None


def get_copy_mirror(mirror_id: int) -> Optional[dict]:
    return query_one("SELECT * FROM copy_mirrors WHERE id = %s", (mirror_id,))


def get_user_active_mirrors(user_id: int) -> list:
    return query_all(
        """SELECT m.*, t.wallet_address, t.label
           FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
           WHERE m.user_id = %s AND m.active = true
           ORDER BY m.created_at""",
        (user_id,),
    )


def get_mirrors_for_trader(trader_id: int) -> list:
    return query_all(
        "SELECT * FROM copy_mirrors WHERE trader_id = %s AND active = true",
        (trader_id,),
    )


def get_all_active_mirrors() -> list:
    return query_all(
        """SELECT m.*, t.wallet_address, t.label
           FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
           WHERE m.active = true AND t.active = true"""
    )


def stop_copy_mirror(mirror_id: int):
    execute(
        "UPDATE copy_mirrors SET active = false, stopped_at = %s WHERE id = %s",
        (datetime.utcnow().isoformat(), mirror_id),
    )


def update_mirror_last_synced(mirror_id: int, fill_tid: int):
    execute(
        "UPDATE copy_mirrors SET last_synced_fill_tid = %s WHERE id = %s",
        (fill_tid, mirror_id),
    )


def count_user_active_mirrors(user_id: int) -> int:
    return query_count(
        "SELECT COUNT(*) FROM copy_mirrors WHERE user_id = %s AND active = true",
        (user_id,),
    )


def insert_copy_trade(data: dict) -> Optional[int]:
    cols = [
        "user_id", "mirror_id", "hl_fill_tid", "hl_coin",
        "nado_product_id", "side", "hl_size", "hl_price",
        "nado_size", "nado_price", "nado_trade_id", "status",
        "error_message", "created_at", "filled_at",
    ]
    filtered = {k: v for k, v in data.items() if k in cols and v is not None}
    if "created_at" not in filtered:
        filtered["created_at"] = datetime.utcnow().isoformat()
    col_names = list(filtered.keys())
    vals = [filtered[c] for c in col_names]
    placeholders = ", ".join(["%s"] * len(col_names))
    col_str = ", ".join(col_names)
    row = execute_returning(
        f"INSERT INTO copy_trades ({col_str}) VALUES ({placeholders}) RETURNING id",
        vals,
    )
    return row["id"] if row else None


def get_copy_trades_by_user(user_id: int, limit: int = 50) -> list:
    return query_all(
        "SELECT * FROM copy_trades WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
        (user_id, limit),
    )


def get_copy_trades_by_mirror(mirror_id: int, limit: int = 50) -> list:
    return query_all(
        "SELECT * FROM copy_trades WHERE mirror_id = %s ORDER BY created_at DESC LIMIT %s",
        (mirror_id, limit),
    )


def copy_trade_exists(hl_fill_tid: int, user_id: int) -> bool:
    return query_count(
        "SELECT COUNT(*) FROM copy_trades WHERE hl_fill_tid = %s AND user_id = %s",
        (hl_fill_tid, user_id),
    ) > 0


def get_active_trader_wallets() -> list[str]:
    rows = query_all(
        """SELECT DISTINCT t.wallet_address
           FROM copy_traders t JOIN copy_mirrors m ON t.id = m.trader_id
           WHERE t.active = true AND m.active = true"""
    )
    return [r["wallet_address"] for r in rows]


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
