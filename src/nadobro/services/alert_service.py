import logging
from datetime import datetime
from src.nadobro.models.database import AlertCondition, insert_alert, get_alerts_by_user, get_alert_by_id_and_user, update_alert, get_all_active_alerts, update_alert_triggered
from src.nadobro.config import get_product_id, get_product_name
from src.nadobro.services.user_service import get_user
from src.nadobro.i18n import get_active_language, localize_text

logger = logging.getLogger(__name__)


def _loc(text):
    return localize_text(text, get_active_language())


def create_alert(telegram_id: int, product: str, condition: str, target_value: float) -> dict:
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": _loc("Unknown product '{product}'.").format(product=product)}

    user = get_user(telegram_id)
    if not user:
        return {"success": False, "error": _loc("User not found.")}

    cond_map = {"above": AlertCondition.ABOVE.value, "below": AlertCondition.BELOW.value}
    alert_cond = cond_map.get(condition)
    if not alert_cond:
        return {"success": False, "error": _loc("Unknown condition '{condition}'. Use: above, below").format(condition=condition)}

    alert_id = insert_alert({
        "user_id": telegram_id,
        "product_id": product_id,
        "product_name": get_product_name(product_id),
        "condition": alert_cond,
        "target_value": target_value,
        "network": user.network_mode.value,
    })
    if not alert_id:
        return {"success": False, "error": _loc("Failed to create alert.")}

    return {
        "success": True,
        "alert_id": alert_id,
        "product": get_product_name(product_id),
        "condition": condition,
        "target": target_value,
    }


def get_user_alerts(telegram_id: int) -> list:
    alerts = get_alerts_by_user(telegram_id, active_only=True)
    return [
        {
            "id": a.get("id"),
            "product": a.get("product_name"),
            "condition": a.get("condition"),
            "target": a.get("target_value"),
            "network": a.get("network"),
            "created_at": (a.get("created_at") or "")[:19] if a.get("created_at") else "",
        }
        for a in alerts
    ]


def delete_alert(telegram_id: int, alert_id: int) -> dict:
    alert = get_alert_by_id_and_user(alert_id, telegram_id)
    if not alert:
        return {"success": False, "error": _loc("Alert not found.")}
    update_alert(alert_id, is_active=False)
    return {"success": True, "message": _loc("Alert #{alert_id} deleted.").format(alert_id=alert_id)}


def get_triggered_alerts(prices: dict) -> list:
    triggered = []
    active_alerts = get_all_active_alerts()
    for alert in active_alerts:
        product_name = (alert.get("product_name") or "").replace("-PERP", "")
        if product_name not in prices:
            continue
        current_price = prices[product_name].get("mid", 0)
        if current_price == 0:
            continue
        cond = alert.get("condition")
        target = float(alert.get("target_value") or 0)
        should_trigger = False
        if cond == AlertCondition.ABOVE.value and current_price >= target:
            should_trigger = True
        elif cond == AlertCondition.BELOW.value and current_price <= target:
            should_trigger = True
        if should_trigger:
            update_alert_triggered(alert["id"])
            triggered.append({
                "user_id": alert.get("user_id"),
                "product": alert.get("product_name"),
                "condition": cond,
                "target": target,
                "current_price": current_price,
            })
    return triggered
