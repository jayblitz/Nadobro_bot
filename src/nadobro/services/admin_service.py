import logging
from datetime import datetime
from src.nadobro.models.database import AdminLog, BotState, Trade, User, get_session
from src.nadobro.config import ADMIN_USER_IDS
from src.nadobro.services.user_service import get_all_users_count, get_active_users_count

logger = logging.getLogger(__name__)


def is_admin(telegram_id: int) -> bool:
    return telegram_id in ADMIN_USER_IDS


def log_admin_action(admin_id: int, action: str, details: str = None):
    with get_session() as session:
        log = AdminLog(admin_id=admin_id, action=action, details=details)
        session.add(log)
        session.commit()


def get_bot_stats() -> dict:
    with get_session() as session:
        total_users = get_all_users_count()
        active_users = get_active_users_count()
        total_trades = session.query(Trade).count()
        from src.nadobro.models.database import TradeStatus
        filled_trades = session.query(Trade).filter_by(status=TradeStatus.FILLED).count()
        failed_trades = session.query(Trade).filter_by(status=TradeStatus.FAILED).count()

        from sqlalchemy import func
        total_volume = session.query(func.sum(Trade.size * Trade.price)).filter(
            Trade.status == TradeStatus.FILLED,
            Trade.price.isnot(None)
        ).scalar() or 0

        return {
            "total_users": total_users,
            "active_users_7d": active_users,
            "total_trades": total_trades,
            "filled_trades": filled_trades,
            "failed_trades": failed_trades,
            "total_volume_usd": float(total_volume),
        }


def is_trading_paused() -> bool:
    with get_session() as session:
        state = session.query(BotState).filter_by(key="trading_paused").first()
        if state:
            return state.value == "true"
    return False


def set_trading_paused(paused: bool, admin_id: int):
    with get_session() as session:
        state = session.query(BotState).filter_by(key="trading_paused").first()
        if state:
            state.value = "true" if paused else "false"
            state.updated_at = datetime.utcnow()
        else:
            state = BotState(key="trading_paused", value="true" if paused else "false")
            session.add(state)
        session.commit()

    action = "pause_trading" if paused else "resume_trading"
    log_admin_action(admin_id, action)


def get_recent_admin_logs(limit: int = 20) -> list:
    with get_session() as session:
        logs = (
            session.query(AdminLog)
            .order_by(AdminLog.created_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "admin_id": l.admin_id,
                "action": l.action,
                "details": l.details,
                "created_at": l.created_at.isoformat(),
            }
            for l in logs
        ]


def get_recent_trades_all(limit: int = 20) -> list:
    with get_session() as session:
        trades = (
            session.query(Trade)
            .order_by(Trade.created_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "user_id": t.user_id,
                "product": t.product_name,
                "side": t.side.value,
                "size": t.size,
                "price": t.price,
                "status": t.status.value,
                "created_at": t.created_at.isoformat(),
            }
            for t in trades
        ]
