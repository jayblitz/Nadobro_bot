import logging
from datetime import datetime
from src.nadobro.models.database import (
    get_bot_state_raw, set_bot_state,
    insert_admin_log, get_trades_count, get_trades_count_filled, get_trades_count_failed,
    get_total_volume_filled, get_recent_trades,
    get_recent_admin_logs as db_get_recent_admin_logs,
)
from src.nadobro.config import ADMIN_USER_IDS
from src.nadobro.services.user_service import get_all_users_count, get_active_users_count

logger = logging.getLogger(__name__)


def is_admin(telegram_id: int) -> bool:
    return telegram_id in ADMIN_USER_IDS


def log_admin_action(admin_id: int, action: str, details: str = None):
    insert_admin_log({"admin_id": admin_id, "action": action, "details": details})


def get_bot_stats() -> dict:
    return {
        "total_users": get_all_users_count(),
        "active_users_7d": get_active_users_count(),
        "total_trades": get_trades_count(),
        "filled_trades": get_trades_count_filled(),
        "failed_trades": get_trades_count_failed(),
        "total_volume_usd": get_total_volume_filled(),
    }


def is_trading_paused() -> bool:
    raw = get_bot_state_raw("trading_paused")
    return raw == "true" if raw else False


def set_trading_paused(paused: bool, admin_id: int):
    set_bot_state("trading_paused", "true" if paused else "false")
    log_admin_action(admin_id, "pause_trading" if paused else "resume_trading")


def get_recent_admin_logs(limit: int = 20) -> list:
    logs = db_get_recent_admin_logs(limit)
    return [
        {
            "admin_id": l.get("admin_id"),
            "action": l.get("action"),
            "details": l.get("details"),
            "created_at": (l.get("created_at") or "")[:19] if l.get("created_at") else "",
        }
        for l in logs
    ]


def add_copy_trader(admin_id: int, wallet: str, label: str = "", is_curated: bool = False) -> tuple[bool, str]:
    if not is_admin(admin_id):
        return False, "Unauthorized."
    from src.nadobro.services.copy_service import add_trader
    ok, msg, trader_id = add_trader(wallet, label=label, is_curated=is_curated)
    if ok:
        log_admin_action(admin_id, "add_copy_trader", f"wallet={wallet[:16]}... label={label} curated={is_curated}")
    return ok, msg


def remove_copy_trader(admin_id: int, trader_id: int) -> tuple[bool, str]:
    if not is_admin(admin_id):
        return False, "Unauthorized."
    from src.nadobro.services.copy_service import remove_trader
    ok, msg = remove_trader(trader_id)
    if ok:
        log_admin_action(admin_id, "remove_copy_trader", f"trader_id={trader_id}")
    return ok, msg


def list_copy_traders() -> list[dict]:
    from src.nadobro.services.copy_service import get_available_traders
    return get_available_traders()


def get_copy_trading_stats() -> dict:
    from src.nadobro.models.database import get_active_copy_traders, get_all_active_mirrors
    from src.nadobro.db import query_count
    traders = get_active_copy_traders()
    mirrors = get_all_active_mirrors()
    total_copy_trades = query_count("SELECT COUNT(*) FROM copy_trades")
    filled_copy_trades = query_count("SELECT COUNT(*) FROM copy_trades WHERE status = 'filled'")
    return {
        "active_traders": len(traders),
        "active_mirrors": len(mirrors),
        "total_copy_trades": total_copy_trades,
        "filled_copy_trades": filled_copy_trades,
    }


def get_recent_trades_all(limit: int = 20) -> list:
    trades = get_recent_trades(limit)
    return [
        {
            "user_id": t.get("user_id"),
            "product": t.get("product_name"),
            "side": t.get("side"),
            "size": t.get("size"),
            "price": t.get("price"),
            "status": t.get("status"),
            "created_at": (t.get("created_at") or "")[:19] if t.get("created_at") else "",
        }
        for t in trades
    ]
