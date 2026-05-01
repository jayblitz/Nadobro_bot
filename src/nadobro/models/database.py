import enum
import json
from datetime import datetime
from typing import Optional, TypeAlias

from psycopg2 import sql as pgsql
from src.nadobro.db import query_one, query_all, execute, execute_returning, query_count

# JSON-compatible values for bot_state (serialized with json.dumps unless value is str).
JsonSerializable: TypeAlias = (
    str | int | float | bool | None | list["JsonSerializable"] | dict[str, "JsonSerializable"]
)


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


def set_bot_state(key: str, value: JsonSerializable):
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
    "fill_price", "fill_size", "fill_fee", "builder_fee", "slippage_bps",
    "source", "strategy_session_id", "open_trade_id", "realized_pnl", "is_taker",
    "funding_paid",
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
    "fill_price", "fill_size", "fill_fee", "builder_fee", "slippage_bps",
    "source", "strategy_session_id", "open_trade_id", "realized_pnl", "is_taker",
    "funding_paid",
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
        f"""
        SELECT *
        FROM {table}
        WHERE user_id = %s
          AND product_id = %s
          AND status IN ('filled', 'partially_filled')
          AND COALESCE(open_trade_id, 0) = 0
          AND order_type NOT ILIKE '%%close%%'
        ORDER BY COALESCE(filled_at, created_at) DESC
        LIMIT 1
        """,
        (telegram_id, product_id),
    )


def get_trades_by_user(
    telegram_id: int,
    limit: int | None = 50,
    network: str = "mainnet",
    strategy_session_id: int | None = None,
) -> list:
    table = _trades_table(network)
    where = ["user_id = %s"]
    params: list = [telegram_id]
    if strategy_session_id is not None:
        where.append("strategy_session_id = %s")
        params.append(int(strategy_session_id))
    where_sql = " AND ".join(where)
    if limit is None or int(limit) <= 0:
        return query_all(
            f"SELECT * FROM {table} WHERE {where_sql} ORDER BY created_at DESC",
            tuple(params),
        )
    params.append(int(limit))
    return query_all(
        f"SELECT * FROM {table} WHERE {where_sql} ORDER BY created_at DESC LIMIT %s",
        tuple(params),
    )


def get_trade_by_id(trade_id: int, network: str = "mainnet") -> Optional[dict]:
    table = _trades_table(network)
    return query_one(f"SELECT * FROM {table} WHERE id = %s LIMIT 1", (trade_id,))


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


def pause_copy_mirror(mirror_id: int):
    execute(
        "UPDATE copy_mirrors SET paused = true WHERE id = %s AND active = true",
        (mirror_id,),
    )


def resume_copy_mirror(mirror_id: int):
    execute(
        "UPDATE copy_mirrors SET paused = false WHERE id = %s AND active = true",
        (mirror_id,),
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


def copy_trade_exists(hl_fill_tid: int, user_id: int, mirror_id: int) -> bool:
    return query_count(
        "SELECT COUNT(*) FROM copy_trades WHERE hl_fill_tid = %s AND user_id = %s AND mirror_id = %s",
        (hl_fill_tid, user_id, mirror_id),
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
        self.mainnet_volume_usd = float(self._data.get("mainnet_volume_usd") or 0)
        self.testnet_volume_usd = float(self._data.get("testnet_volume_usd") or 0)
        self.private_access_granted = bool(self._data.get("private_access_granted"))
        self.private_access_code_id = self._data.get("private_access_code_id")
        self.private_access_granted_at = self._data.get("private_access_granted_at")
        self.private_access_granted_by = self._data.get("private_access_granted_by")


# --- New copy trading ORM functions (Nado-native v2) ---

def create_copy_mirror_v2(user_id: int, trader_id: int, network: str,
                          margin_per_trade: float = 50.0, max_leverage: float = 10.0,
                          cumulative_stop_loss_pct: float = 50.0,
                          cumulative_take_profit_pct: float = 100.0,
                          total_allocated_usd: float = 500.0) -> Optional[int]:
    row = execute_returning(
        """INSERT INTO copy_mirrors 
           (user_id, trader_id, network, margin_per_trade, max_leverage, 
            cumulative_stop_loss_pct, cumulative_take_profit_pct, total_allocated_usd, active)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true)
           ON CONFLICT (user_id, trader_id, network) DO UPDATE
               SET margin_per_trade = EXCLUDED.margin_per_trade,
                   max_leverage = EXCLUDED.max_leverage,
                   cumulative_stop_loss_pct = EXCLUDED.cumulative_stop_loss_pct,
                   cumulative_take_profit_pct = EXCLUDED.cumulative_take_profit_pct,
                   total_allocated_usd = EXCLUDED.total_allocated_usd,
                   active = true,
                   paused = false,
                   auto_stopped_reason = NULL,
                   cumulative_pnl = 0.0,
                   stopped_at = NULL
           RETURNING id""",
        (user_id, trader_id, network, margin_per_trade, max_leverage,
         cumulative_stop_loss_pct, cumulative_take_profit_pct, total_allocated_usd),
    )
    return row["id"] if row else None


def get_user_active_mirrors_v2(user_id: int, network: str = None) -> list:
    if network:
        return query_all(
            """SELECT m.*, t.wallet_address, t.label
               FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
               WHERE m.user_id = %s AND m.active = true AND m.network = %s
               ORDER BY m.created_at""",
            (user_id, network),
        )
    return query_all(
        """SELECT m.*, t.wallet_address, t.label
           FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
           WHERE m.user_id = %s AND m.active = true
           ORDER BY m.created_at""",
        (user_id,),
    )


def get_all_active_mirrors_v2(network: str = None) -> list:
    if network:
        return query_all(
            """SELECT m.*, t.wallet_address, t.label
               FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
               WHERE m.active = true AND NOT m.paused AND t.active = true AND m.network = %s""",
            (network,),
        )
    return query_all(
        """SELECT m.*, t.wallet_address, t.label
           FROM copy_mirrors m JOIN copy_traders t ON m.trader_id = t.id
           WHERE m.active = true AND NOT m.paused AND t.active = true"""
    )


def update_mirror_cumulative_pnl(mirror_id: int, pnl_delta: float):
    execute(
        "UPDATE copy_mirrors SET cumulative_pnl = cumulative_pnl + %s WHERE id = %s",
        (pnl_delta, mirror_id),
    )


def auto_stop_mirror(mirror_id: int, reason: str):
    execute(
        "UPDATE copy_mirrors SET active = false, paused = false, auto_stopped_reason = %s, stopped_at = %s WHERE id = %s",
        (reason, datetime.utcnow().isoformat(), mirror_id),
    )


def insert_copy_position(data: dict) -> Optional[int]:
    cols = [
        "mirror_id", "user_id", "product_id", "product_name", "side",
        "entry_price", "size", "leverage", "tp_price", "sl_price",
        "leader_entry_price", "leader_size", "status", "opened_at",
    ]
    filtered = {k: v for k, v in data.items() if k in cols and v is not None}
    if "opened_at" not in filtered:
        filtered["opened_at"] = datetime.utcnow().isoformat()
    if "status" not in filtered:
        filtered["status"] = "open"
    col_names = list(filtered.keys())
    vals = [filtered[c] for c in col_names]
    placeholders = ", ".join(["%s"] * len(col_names))
    col_str = ", ".join(col_names)
    row = execute_returning(
        f"INSERT INTO copy_positions ({col_str}) VALUES ({placeholders}) RETURNING id",
        vals,
    )
    return row["id"] if row else None


def get_open_copy_positions(mirror_id: int) -> list:
    return query_all(
        "SELECT * FROM copy_positions WHERE mirror_id = %s AND status = 'open' ORDER BY opened_at",
        (mirror_id,),
    )


def get_open_copy_position_for_product(mirror_id: int, product_id: int) -> Optional[dict]:
    return query_one(
        "SELECT * FROM copy_positions WHERE mirror_id = %s AND product_id = %s AND status = 'open' ORDER BY opened_at DESC LIMIT 1",
        (mirror_id, product_id),
    )


def close_copy_position(position_id: int, pnl: float = 0.0, reason: str = "leader_closed"):
    execute(
        "UPDATE copy_positions SET status = 'closed', pnl = %s, closed_at = %s, close_reason = %s WHERE id = %s",
        (pnl, datetime.utcnow().isoformat(), reason, position_id),
    )


def save_copy_snapshot(trader_id: int, network: str, positions_json: str):
    execute(
        """INSERT INTO copy_snapshots (trader_id, network, positions_json)
           VALUES (%s, %s, %s)""",
        (trader_id, network, positions_json),
    )


def get_latest_copy_snapshot(trader_id: int, network: str) -> Optional[dict]:
    return query_one(
        "SELECT * FROM copy_snapshots WHERE trader_id = %s AND network = %s ORDER BY captured_at DESC LIMIT 1",
        (trader_id, network),
    )


# ---------------------------------------------------------------------------
# Strategy Sessions ORM
# ---------------------------------------------------------------------------

_STRATEGY_SESSION_INSERT_COLS = frozenset({
    "user_id", "strategy", "product_id", "product_name", "network",
    "started_at", "status", "config_snapshot",
})


def insert_strategy_session(data: dict) -> Optional[int]:
    filtered = {k: v for k, v in data.items() if k in _STRATEGY_SESSION_INSERT_COLS and v is not None}
    if "started_at" not in filtered:
        filtered["started_at"] = datetime.utcnow().isoformat()
    if "status" not in filtered:
        filtered["status"] = "running"
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    query = pgsql.SQL("INSERT INTO strategy_sessions ({}) VALUES ({}) RETURNING id").format(
        pgsql.SQL(", ").join(pgsql.Identifier(c) for c in cols),
        pgsql.SQL(", ").join(pgsql.Placeholder() * len(cols)),
    )
    row = execute_returning(
        query,
        vals,
    )
    return row["id"] if row else None


_STRATEGY_SESSION_UPDATE_COLS = frozenset({
    "stopped_at", "status", "total_cycles", "total_orders_placed",
    "total_orders_filled", "total_orders_cancelled", "realized_pnl",
    "total_fees_paid", "total_volume_usd", "total_funding_paid",
    "stop_reason", "error_message",
})


def update_strategy_session(session_id: int, data: dict):
    filtered = {k: v for k, v in data.items() if k in _STRATEGY_SESSION_UPDATE_COLS}
    if not filtered:
        return
    set_clause = pgsql.SQL(", ").join(
        pgsql.SQL("{} = %s").format(pgsql.Identifier(k)) for k in filtered.keys()
    )
    query = pgsql.SQL("UPDATE strategy_sessions SET {} WHERE id = %s").format(set_clause)
    vals = list(filtered.values()) + [session_id]
    execute(query, vals)


def increment_session_metrics(
    session_id: int,
    cycles: int = 0,
    orders_placed: int = 0,
    orders_filled: int = 0,
    orders_cancelled: int = 0,
    pnl: float = 0.0,
    fees: float = 0.0,
    volume: float = 0.0,
    funding: float = 0.0,
):
    execute(
        """UPDATE strategy_sessions SET
            total_cycles = total_cycles + %s,
            total_orders_placed = total_orders_placed + %s,
            total_orders_filled = total_orders_filled + %s,
            total_orders_cancelled = total_orders_cancelled + %s,
            realized_pnl = realized_pnl + %s,
            total_fees_paid = total_fees_paid + %s,
            total_volume_usd = total_volume_usd + %s,
            total_funding_paid = total_funding_paid + %s
        WHERE id = %s""",
        (cycles, orders_placed, orders_filled, orders_cancelled,
         pnl, fees, volume, funding, session_id),
    )


def get_strategy_sessions_by_user(
    user_id: int,
    strategy: str = None,
    network: str = None,
    limit: int = 50,
) -> list:
    conditions = ["user_id = %s"]
    params: list = [user_id]
    if strategy:
        conditions.append("strategy = %s")
        params.append(strategy)
    if network:
        conditions.append("network = %s")
        params.append(network)
    params.append(limit)
    where = " AND ".join(conditions)
    return query_all(
        f"SELECT * FROM strategy_sessions WHERE {where} ORDER BY started_at DESC LIMIT %s",
        params,
    )


def get_active_strategy_session(user_id: int, network: str) -> Optional[dict]:
    return query_one(
        "SELECT * FROM strategy_sessions WHERE user_id = %s AND network = %s AND status = 'running' ORDER BY started_at DESC LIMIT 1",
        (user_id, network),
    )


def get_running_strategy_sessions(user_id: int, network: str | None = None) -> list:
    """All sessions still marked running (used for status display and supersede on new start)."""
    conditions = ["user_id = %s", "status = 'running'"]
    params: list = [user_id]
    if network:
        conditions.append("network = %s")
        params.append(network)
    where = " AND ".join(conditions)
    return query_all(
        f"SELECT * FROM strategy_sessions WHERE {where} ORDER BY started_at DESC",
        tuple(params),
    )


# ---------------------------------------------------------------------------
# Time limits, Studio sessions, and conditional orders
# ---------------------------------------------------------------------------

_VALID_TIME_LIMIT_SOURCES = frozenset({"manual", "studio", "bro", "time_limit"})
_STUDIO_ACTIVE_STATES = ("EXTRACTING", "CLARIFYING", "CONFIRMING", "EXECUTING")


def _json_payload(value, default):
    if value is None:
        return json.dumps(default)
    if isinstance(value, str):
        return value
    return json.dumps(value)


def set_position_time_limit(position_id: int, ts, source: str = "manual"):
    source = source if source in _VALID_TIME_LIMIT_SOURCES else "manual"
    execute(
        """UPDATE positions
           SET time_limit = %s, time_limit_source = %s, time_limit_fired_at = NULL
           WHERE id = %s""",
        (ts, source, position_id),
    )


def set_order_time_limit(order_id: int, ts, source: str = "manual"):
    source = source if source in _VALID_TIME_LIMIT_SOURCES else "manual"
    execute(
        """UPDATE open_orders
           SET time_limit = %s, time_limit_source = %s, time_limit_fired_at = NULL, updated_at = now()
           WHERE id = %s""",
        (ts, source, order_id),
    )


def clear_position_time_limit(position_id: int):
    execute(
        "UPDATE positions SET time_limit = NULL, time_limit_source = NULL, time_limit_fired_at = NULL WHERE id = %s",
        (position_id,),
    )


def clear_order_time_limit(order_id: int):
    execute(
        """UPDATE open_orders
           SET time_limit = NULL, time_limit_source = NULL, time_limit_fired_at = NULL, updated_at = now()
           WHERE id = %s""",
        (order_id,),
    )


def fetch_due_time_limits(now_utc, network: str, limit: int = 50) -> dict:
    """Atomically claim due position/order time limits for one network."""
    if network not in _VALID_NETWORKS:
        raise ValueError(f"Invalid network: {network}")
    rows_positions = query_all(
        """
        WITH due AS (
            SELECT id
            FROM positions
            WHERE network = %s
              AND status = 'open'
              AND time_limit IS NOT NULL
              AND time_limit <= %s
              AND time_limit_fired_at IS NULL
            ORDER BY time_limit ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE positions p
        SET time_limit_fired_at = %s
        FROM due
        WHERE p.id = due.id
        RETURNING p.*
        """,
        (network, now_utc, limit, now_utc),
    )
    rows_orders = query_all(
        """
        WITH due AS (
            SELECT id
            FROM open_orders
            WHERE network = %s
              AND status IN ('open', 'pending', 'armed')
              AND time_limit IS NOT NULL
              AND time_limit <= %s
              AND time_limit_fired_at IS NULL
            ORDER BY time_limit ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE open_orders o
        SET time_limit_fired_at = %s, updated_at = now()
        FROM due
        WHERE o.id = due.id
        RETURNING o.*
        """,
        (network, now_utc, limit, now_utc),
    )
    return {"positions": rows_positions, "orders": rows_orders}


def insert_studio_session(data: dict) -> Optional[int]:
    allowed = {
        "telegram_id", "network", "state", "intent_json", "history_json", "strategy_session_id",
    }
    filtered = {k: v for k, v in data.items() if k in allowed and v is not None}
    filtered.setdefault("state", "EXTRACTING")
    filtered["intent_json"] = _json_payload(filtered.get("intent_json"), {})
    filtered["history_json"] = _json_payload(filtered.get("history_json"), [])
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    row = execute_returning(
        pgsql.SQL("INSERT INTO studio_sessions ({}) VALUES ({}) RETURNING id").format(
            pgsql.SQL(", ").join(pgsql.Identifier(c) for c in cols),
            pgsql.SQL(", ").join(pgsql.Placeholder() * len(cols)),
        ),
        vals,
    )
    return row["id"] if row else None


def update_studio_session(session_id: int, data: dict):
    allowed = {"state", "intent_json", "history_json", "strategy_session_id"}
    filtered = {k: v for k, v in data.items() if k in allowed}
    if not filtered:
        return
    if "intent_json" in filtered:
        filtered["intent_json"] = _json_payload(filtered["intent_json"], {})
    if "history_json" in filtered:
        filtered["history_json"] = _json_payload(filtered["history_json"], [])
    filtered["updated_at"] = datetime.utcnow().isoformat()
    set_clause = pgsql.SQL(", ").join(
        pgsql.SQL("{} = %s").format(pgsql.Identifier(k)) for k in filtered.keys()
    )
    execute(
        pgsql.SQL("UPDATE studio_sessions SET {} WHERE id = %s").format(set_clause),
        list(filtered.values()) + [session_id],
    )


def cancel_active_studio_sessions(telegram_id: int, network: str, exclude_id: int | None = None):
    params: list = [datetime.utcnow().isoformat(), telegram_id, network]
    extra = ""
    if exclude_id is not None:
        extra = " AND id <> %s"
        params.append(int(exclude_id))
    execute(
        f"""UPDATE studio_sessions
            SET state = 'CANCELLED', updated_at = %s
            WHERE telegram_id = %s AND network = %s
              AND state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING')
              {extra}""",
        tuple(params),
    )


def get_active_studio_session(telegram_id: int, network: str) -> Optional[dict]:
    return query_one(
        """SELECT * FROM studio_sessions
           WHERE telegram_id = %s AND network = %s
             AND state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING')
           ORDER BY updated_at DESC
           LIMIT 1""",
        (telegram_id, network),
    )


def get_studio_session(session_id: int) -> Optional[dict]:
    return query_one("SELECT * FROM studio_sessions WHERE id = %s", (session_id,))


def get_active_studio_sessions_for_user(telegram_id: int, network: str | None = None) -> list:
    params: list = [telegram_id]
    network_sql = ""
    if network:
        network_sql = " AND network = %s"
        params.append(network)
    return query_all(
        f"""SELECT * FROM studio_sessions
            WHERE telegram_id = %s{network_sql}
              AND state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING')
            ORDER BY updated_at DESC""",
        tuple(params),
    )


def insert_conditional_order(data: dict) -> Optional[int]:
    allowed = {
        "telegram_id", "network", "studio_session_id", "strategy_session_id", "symbol",
        "action", "order_type", "intent_json", "conditions_json", "status",
        "time_limit", "time_limit_source",
    }
    filtered = {k: v for k, v in data.items() if k in allowed and v is not None}
    filtered.setdefault("order_type", "conditional")
    filtered.setdefault("status", "armed")
    filtered["intent_json"] = _json_payload(filtered.get("intent_json"), {})
    filtered["conditions_json"] = _json_payload(filtered.get("conditions_json"), [])
    cols = list(filtered.keys())
    vals = [filtered[c] for c in cols]
    row = execute_returning(
        pgsql.SQL("INSERT INTO conditional_orders ({}) VALUES ({}) RETURNING id").format(
            pgsql.SQL(", ").join(pgsql.Identifier(c) for c in cols),
            pgsql.SQL(", ").join(pgsql.Placeholder() * len(cols)),
        ),
        vals,
    )
    return row["id"] if row else None


def get_armed_conditional_orders(network: str, limit: int = 100) -> list:
    return query_all(
        """SELECT * FROM conditional_orders
           WHERE network = %s AND status = 'armed'
           ORDER BY updated_at ASC
           LIMIT %s""",
        (network, limit),
    )


def claim_conditional_order(order_id: int) -> Optional[dict]:
    return execute_returning(
        """UPDATE conditional_orders
           SET status = 'firing', fired_at = now(), updated_at = now()
           WHERE id = %s AND status = 'armed'
           RETURNING *""",
        (order_id,),
    )


def update_conditional_order(order_id: int, data: dict):
    allowed = {
        "status", "last_evaluated_at", "last_evaluation", "error_message",
        "time_limit_fired_at", "fired_at",
    }
    filtered = {k: v for k, v in data.items() if k in allowed}
    if not filtered:
        return
    filtered["updated_at"] = datetime.utcnow().isoformat()
    set_clause = pgsql.SQL(", ").join(
        pgsql.SQL("{} = %s").format(pgsql.Identifier(k)) for k in filtered.keys()
    )
    execute(
        pgsql.SQL("UPDATE conditional_orders SET {} WHERE id = %s").format(set_clause),
        list(filtered.values()) + [order_id],
    )


def get_active_conditional_orders_for_user(telegram_id: int, network: str | None = None) -> list:
    params: list = [telegram_id]
    network_sql = ""
    if network:
        network_sql = " AND network = %s"
        params.append(network)
    return query_all(
        f"""SELECT * FROM conditional_orders
            WHERE telegram_id = %s{network_sql}
              AND status IN ('armed', 'firing')
            ORDER BY created_at DESC""",
        tuple(params),
    )


# ---------------------------------------------------------------------------
# Fill Sync Queue ORM
# ---------------------------------------------------------------------------

def insert_fill_sync(data: dict) -> Optional[int]:
    cols = ["trade_id", "network", "user_id", "subaccount_hex", "order_digest", "product_id", "placed_at_ts"]
    filtered = {k: v for k, v in data.items() if k in cols and v is not None}
    col_names = list(filtered.keys())
    vals = [filtered[c] for c in col_names]
    query = pgsql.SQL("INSERT INTO fill_sync_queue ({}) VALUES ({}) RETURNING id").format(
        pgsql.SQL(", ").join(pgsql.Identifier(c) for c in col_names),
        pgsql.SQL(", ").join(pgsql.Placeholder() * len(col_names)),
    )
    row = execute_returning(
        query,
        vals,
    )
    return row["id"] if row else None


def get_pending_fill_syncs(limit: int = 100) -> list:
    return query_all(
        "SELECT * FROM fill_sync_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT %s",
        (limit,),
    )


def resolve_fill_sync(sync_id: int):
    execute(
        "UPDATE fill_sync_queue SET status = 'resolved', resolved_at = %s WHERE id = %s",
        (datetime.utcnow().isoformat(), sync_id),
    )


def expire_fill_sync(sync_id: int):
    execute(
        "UPDATE fill_sync_queue SET status = 'expired', resolved_at = %s WHERE id = %s",
        (datetime.utcnow().isoformat(), sync_id),
    )


def increment_fill_sync_attempts(sync_id: int):
    execute(
        "UPDATE fill_sync_queue SET attempts = attempts + 1 WHERE id = %s",
        (sync_id,),
    )
