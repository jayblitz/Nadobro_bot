import logging
import os
import re
import secrets
import time
from typing import Optional

from src.nadobro.config import LOWIQPTS_BRIDGE_CHAT_ID
from src.nadobro.services.user_service import get_user

logger = logging.getLogger(__name__)

_POINTS_CACHE: dict[int, dict] = {}
_POINTS_CACHE_TTL_SECONDS = 3600
_POINTS_WINDOW_LABEL = "Last 7 Days"
_POINTS_REPLY_TIMEOUT_SECONDS = max(
    5,
    int(os.environ.get("LOWIQPTS_REPLY_TIMEOUT_SECONDS", "25") or "25"),
)

_PENDING_QUEUE_KEY = "lowiqpts_pending_queue"
_PENDING_BY_WALLET_KEY = "lowiqpts_pending_by_wallet"

_WALLET_RE = re.compile(r"0x[a-fA-F0-9]{40}")
_NO_POINTS_HINT_RE = re.compile(
    r"(no\s+points|no\s+data|no\s+trades|nothing\s+found|not\s+found|0\s*points?)",
    re.IGNORECASE,
)


def _bridge_chat_id() -> Optional[int]:
    raw = (LOWIQPTS_BRIDGE_CHAT_ID or "").strip() or os.environ.get("LOWIQPTS_BRIDGE_CHAT_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _pending_maps(bot_data: dict) -> tuple[list[dict], dict[str, list[dict]]]:
    queue = bot_data.setdefault(_PENDING_QUEUE_KEY, [])
    by_wallet = bot_data.setdefault(_PENDING_BY_WALLET_KEY, {})
    return queue, by_wallet


def _prune_stale_pending(bot_data: dict, ttl_seconds: int = 120) -> None:
    now = time.time()
    queue, by_wallet = _pending_maps(bot_data)
    fresh = [req for req in queue if now - float(req.get("ts", 0)) <= ttl_seconds]
    queue[:] = fresh
    wallet_map: dict[str, list[dict]] = {}
    for req in fresh:
        wallet = str(req.get("wallet", "")).lower()
        if wallet:
            wallet_map.setdefault(wallet, []).append(req)
    by_wallet.clear()
    by_wallet.update(wallet_map)


def _extract_wallet(text: str) -> Optional[str]:
    if not text:
        return None
    match = _WALLET_RE.search(text)
    return match.group(0).lower() if match else None


def _remove_pending_req(bot_data: dict, req: dict) -> None:
    queue, by_wallet = _pending_maps(bot_data)
    try:
        queue.remove(req)
    except ValueError:
        pass
    wallet = str(req.get("wallet", "")).lower()
    items = by_wallet.get(wallet) or []
    items = [x for x in items if x is not req]
    if items:
        by_wallet[wallet] = items
    else:
        by_wallet.pop(wallet, None)


def _pending_req_by_id(bot_data: dict, req_id: str) -> Optional[dict]:
    if not req_id:
        return None
    queue, _ = _pending_maps(bot_data)
    for req in queue:
        if str(req.get("req_id", "")) == str(req_id):
            return req
    return None


def claim_pending_request(bot_data: dict, response_text: str) -> Optional[dict]:
    _prune_stale_pending(bot_data)
    queue, by_wallet = _pending_maps(bot_data)
    wallet = _extract_wallet(response_text or "")
    if wallet and by_wallet.get(wallet):
        req = by_wallet[wallet].pop(0)
        if not by_wallet[wallet]:
            by_wallet.pop(wallet, None)
        _remove_pending_req(bot_data, req)
        return req
    if queue:
        req = queue[0]
        _remove_pending_req(bot_data, req)
        return req
    return None


def parse_lowiq_points_reply(text: str) -> Optional[dict]:
    if not text:
        return None
    points_match = re.search(r"points?\s*[:\-]\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    volume_match = re.search(r"volume\s*[:\-]\s*\$?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    cpp_match = re.search(r"(?:cost\s*/\s*point|cost\s+per\s+point)\s*[:\-]\s*\$?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    explicit_fields = bool(points_match or volume_match or cpp_match)
    if not explicit_fields and _NO_POINTS_HINT_RE.search(text):
        return {
            "points": 0.0,
            "volume_usd": 0.0,
            "cost_per_point": 0.0,
            "no_activity": True,
        }
    if not explicit_fields:
        return None

    def _to_float(match_obj) -> float:
        if not match_obj:
            return 0.0
        return float(match_obj.group(1).replace(",", ""))

    points = _to_float(points_match)
    volume = _to_float(volume_match)
    cost_per_point = _to_float(cpp_match)
    if points <= 0 and volume <= 0 and cost_per_point <= 0:
        return {
            "points": 0.0,
            "volume_usd": 0.0,
            "cost_per_point": 0.0,
            "no_activity": True,
        }
    return {
        "points": points,
        "volume_usd": volume,
        "cost_per_point": cost_per_point,
        "no_activity": False,
    }


def build_dashboard_payload(parsed: dict) -> dict:
    points = float(parsed.get("points") or 0.0)
    volume = float(parsed.get("volume_usd") or 0.0)
    cpp = float(parsed.get("cost_per_point") or 0.0)
    total_cost = cpp * points if points > 0 and cpp > 0 else 0.0
    ppm = ((points / volume) * 1_000_000.0) if volume > 0 else 0.0
    return {
        "ok": True,
        "scope": "week",
        "scope_mode": "lowiqpts_week",
        "window_label": _POINTS_WINDOW_LABEL,
        "points_source": "lowiqpts_bridge",
        "points_estimated": False,
        "volume_usd": volume,
        "points": points,
        "cost_per_point": cpp,
        "total_costs": total_cost,
        "fees_paid": total_cost,
        "ppm": ppm,
        "maker_count": 0,
        "taker_count": 0,
        "maker_pct": 0.0,
        "taker_pct": 0.0,
        "maker_bar": "░░░░░░░░░░",
        "positions": 0,
        "avg_hold_seconds": 0,
        "missing_fields": [],
        "no_activity": bool(parsed.get("no_activity")),
    }


def save_points_snapshot(telegram_id: int, payload: dict) -> None:
    _POINTS_CACHE[int(telegram_id)] = {"ts": time.time(), "data": payload}


def get_points_dashboard(telegram_id: int, scope: str = "week") -> dict:
    _ = scope  # week-only source by design
    cached = _POINTS_CACHE.get(int(telegram_id))
    if cached and (time.time() - float(cached.get("ts", 0)) < _POINTS_CACHE_TTL_SECONDS):
        return cached["data"]
    user = get_user(telegram_id)
    if not user or not user.main_address:
        return {"ok": False, "error": "Wallet not linked yet. Open 👛 Wallet first."}
    return {"ok": False, "error": "No points snapshot yet. Tap Refresh to fetch last-week data."}


async def request_points_refresh(context, telegram_id: int, chat_id: int) -> dict:
    user = get_user(telegram_id)
    if not user or not user.main_address:
        return {"ok": False, "error": "❌ Please connect your wallet first via the Wallet button."}
    bridge_id = _bridge_chat_id()
    if bridge_id is None:
        return {"ok": False, "error": "❌ LOWIQPTS bridge is not configured."}

    wallet = str(user.main_address).lower()
    bot_data = context.application.bot_data
    _prune_stale_pending(bot_data)
    queue, by_wallet = _pending_maps(bot_data)
    req_id = secrets.token_urlsafe(12)
    req = {
        "req_id": req_id,
        "telegram_id": int(telegram_id),
        "chat_id": int(chat_id),
        "wallet": wallet,
        "ts": time.time(),
    }
    queue.append(req)
    by_wallet.setdefault(wallet, []).append(req)

    try:
        await context.bot.send_message(chat_id=bridge_id, text=f"/nado {wallet}")
        job_queue = getattr(context.application, "job_queue", None)
        if job_queue:
            job_queue.run_once(
                _on_points_refresh_timeout,
                when=_POINTS_REPLY_TIMEOUT_SECONDS,
                data={"req_id": req_id},
                name=f"lowiqpts-timeout:{req_id}",
            )
        return {"ok": True, "timeout_seconds": _POINTS_REPLY_TIMEOUT_SECONDS}
    except Exception as e:
        _remove_pending_req(bot_data, req)
        logger.warning("Failed to send lowiq bridge query: %s", e)
        return {"ok": False, "error": "❌ Could not reach points bridge. Try again shortly."}


async def _on_points_refresh_timeout(context) -> None:
    data = getattr(context.job, "data", {}) or {}
    req_id = str(data.get("req_id", "")).strip()
    if not req_id:
        return

    bot_data = context.application.bot_data
    req = _pending_req_by_id(bot_data, req_id)
    if not req:
        # Already matched by bridge response.
        return

    _remove_pending_req(bot_data, req)
    chat_id = int(req.get("chat_id"))
    from src.nadobro.handlers.keyboards import points_scope_kb

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⏱ Points refresh is taking longer than expected.\n"
                "Please tap Refresh to retry."
            ),
            reply_markup=points_scope_kb("week"),
        )
    except Exception as e:
        logger.warning("Failed to deliver points timeout banner: %s", e)
