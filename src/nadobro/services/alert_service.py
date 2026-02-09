import logging
from datetime import datetime
from src.nadobro.models.database import Alert, AlertCondition, NetworkMode, get_session
from src.nadobro.config import get_product_id, get_product_name
from src.nadobro.services.user_service import get_user

logger = logging.getLogger(__name__)


def create_alert(telegram_id: int, product: str, condition: str, target_value: float) -> dict:
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    user = get_user(telegram_id)
    if not user:
        return {"success": False, "error": "User not found."}

    cond_map = {
        "above": AlertCondition.ABOVE,
        "below": AlertCondition.BELOW,
        "funding_above": AlertCondition.FUNDING_ABOVE,
        "funding_below": AlertCondition.FUNDING_BELOW,
        "pnl_above": AlertCondition.PNL_ABOVE,
        "pnl_below": AlertCondition.PNL_BELOW,
    }
    alert_cond = cond_map.get(condition)
    if not alert_cond:
        return {"success": False, "error": f"Unknown condition '{condition}'. Use: above, below, funding_above, funding_below"}

    with get_session() as session:
        alert = Alert(
            user_id=telegram_id,
            product_id=product_id,
            product_name=get_product_name(product_id),
            condition=alert_cond,
            target_value=target_value,
            network=user.network_mode,
        )
        session.add(alert)
        session.commit()
        alert_id = alert.id

    return {
        "success": True,
        "alert_id": alert_id,
        "product": get_product_name(product_id),
        "condition": condition,
        "target": target_value,
    }


def get_user_alerts(telegram_id: int) -> list:
    with get_session() as session:
        alerts = (
            session.query(Alert)
            .filter_by(user_id=telegram_id, is_active=True)
            .order_by(Alert.created_at.desc())
            .all()
        )
        return [
            {
                "id": a.id,
                "product": a.product_name,
                "condition": a.condition.value,
                "target": a.target_value,
                "network": a.network.value,
                "created_at": a.created_at.isoformat(),
            }
            for a in alerts
        ]


def delete_alert(telegram_id: int, alert_id: int) -> dict:
    with get_session() as session:
        alert = session.query(Alert).filter_by(id=alert_id, user_id=telegram_id).first()
        if not alert:
            return {"success": False, "error": "Alert not found."}
        alert.is_active = False
        session.commit()
        return {"success": True, "message": f"Alert #{alert_id} deleted."}


def get_triggered_alerts(prices: dict) -> list:
    triggered = []
    with get_session() as session:
        active_alerts = session.query(Alert).filter_by(is_active=True).all()
        for alert in active_alerts:
            product_name = alert.product_name.replace("-PERP", "")
            if product_name not in prices:
                continue

            current_price = prices[product_name].get("mid", 0)
            if current_price == 0:
                continue

            should_trigger = False
            if alert.condition == AlertCondition.ABOVE and current_price >= alert.target_value:
                should_trigger = True
            elif alert.condition == AlertCondition.BELOW and current_price <= alert.target_value:
                should_trigger = True

            if should_trigger:
                alert.is_active = False
                alert.triggered_at = datetime.utcnow()
                triggered.append({
                    "user_id": alert.user_id,
                    "product": alert.product_name,
                    "condition": alert.condition.value,
                    "target": alert.target_value,
                    "current_price": current_price,
                })

        session.commit()
    return triggered
