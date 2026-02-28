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
