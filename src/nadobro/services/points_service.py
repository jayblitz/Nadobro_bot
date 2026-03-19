import logging
import os
import re
import secrets
import time
from typing import Optional

from telegram.constants import ParseMode

from src.nadobro.handlers.formatters import fmt_points_dashboard
from src.nadobro.handlers.keyboards import points_scope_kb
from src.nadobro.handlers.points_mascot import mascot_caption_for_cost, mascot_path_for_cost
from src.nadobro.services.lowiq_relay_client import (
    close_session as relay_close_session,
    poll_events as relay_poll_events,
    relay_is_configured,
    send_user_reply as relay_send_user_reply,
    start_session as relay_start_session,
)
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
_ACTIVE_BY_CHAT_KEY = "lowiqpts_active_by_chat"
_RELAY_CURSOR_KEY = "lowiqpts_relay_cursor"

_WALLET_RE = re.compile(r"0x[a-fA-F0-9]{40}")
_NO_POINTS_HINT_RE = re.compile(
    r"(no\s+points|no\s+data|no\s+trades|nothing\s+found|not\s+found|0\s*points?)",
    re.IGNORECASE,
)


def _pending_maps(bot_data: dict) -> tuple[list[dict], dict[str, list[dict]]]:
    queue = bot_data.setdefault(_PENDING_QUEUE_KEY, [])
    by_wallet = bot_data.setdefault(_PENDING_BY_WALLET_KEY, {})
    return queue, by_wallet


def _active_map(bot_data: dict) -> dict[int, str]:
    active = bot_data.setdefault(_ACTIVE_BY_CHAT_KEY, {})
    normalized: dict[int, str] = {}
    for chat_id, req_id in (active or {}).items():
        try:
            normalized[int(chat_id)] = str(req_id)
        except (TypeError, ValueError):
            continue
    active.clear()
    active.update(normalized)
    return active


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
    req_id = str(req.get("req_id", ""))
    active = _active_map(bot_data)
    for chat_id, active_req_id in list(active.items()):
        if active_req_id == req_id:
            active.pop(chat_id, None)


def _pending_req_by_id(bot_data: dict, req_id: str) -> Optional[dict]:
    if not req_id:
        return None
    queue, _ = _pending_maps(bot_data)
    for req in queue:
        if str(req.get("req_id", "")) == str(req_id):
            return req
    return None


def _set_active_req_for_chat(bot_data: dict, chat_id: int, req_id: str) -> None:
    _active_map(bot_data)[int(chat_id)] = str(req_id)


def _touch_pending_request(req: dict) -> None:
    req["ts"] = time.time()


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
    queue_req_ids = {str(req.get("req_id", "")) for req in fresh}
    active = _active_map(bot_data)
    for chat_id, req_id in list(active.items()):
        if req_id not in queue_req_ids:
            active.pop(chat_id, None)


def get_active_pending_request(bot_data: dict, chat_id: int) -> Optional[dict]:
    _prune_stale_pending(bot_data)
    req_id = str(_active_map(bot_data).get(int(chat_id), "")).strip()
    if not req_id:
        return None
    req = _pending_req_by_id(bot_data, req_id)
    if not req:
        _active_map(bot_data).pop(int(chat_id), None)
    return req


def complete_pending_request(bot_data: dict, req: dict) -> None:
    _remove_pending_req(bot_data, req)


def _schedule_timeout(application, req_id: str) -> None:
    job_queue = getattr(application, "job_queue", None)
    if not job_queue:
        return
    job_name = f"lowiqpts-timeout:{req_id}"
    for job in job_queue.get_jobs_by_name(job_name):
        try:
            job.schedule_removal()
        except Exception:
            pass
    job_queue.run_once(
        _on_points_refresh_timeout,
        when=_POINTS_REPLY_TIMEOUT_SECONDS,
        data={"req_id": req_id},
        name=job_name,
    )


def _extract_session_id(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    candidates = [
        payload.get("session_id"),
        (payload.get("data") or {}).get("session_id") if isinstance(payload.get("data"), dict) else None,
        (payload.get("session") or {}).get("id") if isinstance(payload.get("session"), dict) else None,
    ]
    for value in candidates:
        if value:
            return str(value)
    return ""


def _extract_event_text(event: dict) -> str:
    if not isinstance(event, dict):
        return ""
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    text = (
        event.get("text")
        or event.get("message")
        or event.get("caption")
        or payload.get("text")
    )
    return str(text or "").strip()


def _extract_event_session_id(event: dict) -> str:
    if not isinstance(event, dict):
        return ""
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    session = event.get("session") if isinstance(event.get("session"), dict) else {}
    candidates = [event.get("session_id"), payload.get("session_id"), session.get("id")]
    for value in candidates:
        if value:
            return str(value)
    return ""


def _extract_events_response(payload: dict) -> tuple[list[dict], Optional[str]]:
    if not isinstance(payload, dict):
        return [], None
    events_raw = payload.get("events")
    if events_raw is None and isinstance(payload.get("data"), dict):
        events_raw = payload["data"].get("events")
    events = events_raw if isinstance(events_raw, list) else []
    next_cursor = payload.get("next_cursor")
    if not next_cursor and isinstance(payload.get("data"), dict):
        next_cursor = payload["data"].get("next_cursor")
    if not next_cursor:
        next_cursor = payload.get("cursor")
    return [e for e in events if isinstance(e, dict)], (str(next_cursor) if next_cursor else None)


def _claim_pending_for_event(bot_data: dict, session_id: str, text: str) -> Optional[dict]:
    _prune_stale_pending(bot_data)
    queue, by_wallet = _pending_maps(bot_data)
    if session_id:
        for req in queue:
            if str(req.get("relay_session_id", "")) == session_id:
                _touch_pending_request(req)
                _set_active_req_for_chat(bot_data, int(req.get("chat_id")), str(req.get("req_id", "")))
                return req
    wallet = _extract_wallet(text)
    if wallet and by_wallet.get(wallet):
        req = by_wallet[wallet][0]
        _touch_pending_request(req)
        _set_active_req_for_chat(bot_data, int(req.get("chat_id")), str(req.get("req_id", "")))
        return req
    if queue:
        req = queue[0]
        _touch_pending_request(req)
        _set_active_req_for_chat(bot_data, int(req.get("chat_id")), str(req.get("req_id", "")))
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
        "points_source": "lowiqpts_relay",
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
    if not relay_is_configured():
        return {"ok": False, "error": "❌ LOWIQPTS relay is not configured."}

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
    _set_active_req_for_chat(bot_data, chat_id, req_id)

    try:
        relay_resp = await relay_start_session(
            telegram_user_id=int(telegram_id),
            chat_id=int(chat_id),
            wallet=wallet,
            request_id=req_id,
        )
        if not relay_resp.get("ok"):
            raise RuntimeError(relay_resp.get("error") or "relay_start_failed")
        session_id = _extract_session_id(relay_resp)
        if not session_id:
            raise RuntimeError("relay_session_missing")
        req["relay_session_id"] = session_id
        _schedule_timeout(context.application, req_id)
        return {"ok": True, "timeout_seconds": _POINTS_REPLY_TIMEOUT_SECONDS}
    except Exception as e:
        _remove_pending_req(bot_data, req)
        logger.warning("Failed to start lowiq relay session: %s", e)
        return {"ok": False, "error": "❌ Could not reach LOWIQPTS relay. Try again shortly."}


async def relay_user_reply_to_lowiqpts(context, chat_id: int, text: str) -> dict:
    req = get_active_pending_request(context.application.bot_data, chat_id)
    if not req:
        return {"ok": False, "handled": False}
    if not relay_is_configured():
        return {"ok": False, "handled": True, "error": "❌ LOWIQPTS relay is not configured."}

    cleaned = (text or "").strip()
    if not cleaned:
        return {"ok": True, "handled": True}

    if cleaned.lower() in {"cancel", "stop", "exit", "quit"}:
        session_id = str(req.get("relay_session_id", "")).strip()
        if session_id:
            await relay_close_session(session_id=session_id, reason="cancelled_by_user")
        complete_pending_request(context.application.bot_data, req)
        return {"ok": True, "handled": True, "cancelled": True}

    try:
        session_id = str(req.get("relay_session_id", "")).strip()
        if not session_id:
            return {"ok": False, "handled": True, "error": "❌ LOWIQPTS relay session is missing."}
        relay_resp = await relay_send_user_reply(session_id=session_id, text=cleaned)
        if not relay_resp.get("ok"):
            raise RuntimeError(relay_resp.get("error") or "relay_send_failed")
        _touch_pending_request(req)
        _schedule_timeout(context.application, str(req.get("req_id", "")))
        return {"ok": True, "handled": True}
    except Exception as e:
        logger.warning("Failed to relay user reply to lowiqpts relay: %s", e)
        return {"ok": False, "handled": True, "error": "❌ Could not relay your reply to LOWIQPTS right now."}


async def _process_relay_event(bot_app, bot_data: dict, event: dict) -> None:
    text = _extract_event_text(event)
    if not text:
        return
    session_id = _extract_event_session_id(event)
    req = _claim_pending_for_event(bot_data, session_id, text)
    if not req:
        return

    chat_id = int(req.get("chat_id"))
    telegram_id = int(req.get("telegram_id"))
    await bot_app.bot.send_message(chat_id=chat_id, text=text)
    parsed = parse_lowiq_points_reply(text)
    if not parsed:
        _schedule_timeout(bot_app, str(req.get("req_id", "")))
        return

    payload = build_dashboard_payload(parsed)
    save_points_snapshot(telegram_id, payload)
    complete_pending_request(bot_data, req)
    await bot_app.bot.send_message(
        chat_id=chat_id,
        text=fmt_points_dashboard(payload),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=points_scope_kb("week"),
    )

    session_id = str(req.get("relay_session_id", "")).strip()
    if session_id:
        await relay_close_session(session_id=session_id, reason="completed")

    if bool(payload.get("no_activity")):
        return

    mascot_path = mascot_path_for_cost(float(payload.get("cost_per_point", 0) or 0))
    if mascot_path:
        try:
            with open(mascot_path, "rb") as img:
                await bot_app.bot.send_photo(
                    chat_id=chat_id,
                    photo=img,
                    caption=mascot_caption_for_cost(float(payload.get("cost_per_point", 0) or 0)),
                )
        except Exception:
            logger.warning("Failed to send points mascot image", exc_info=True)


async def poll_lowiqpts_relay_events(bot_app) -> None:
    if not relay_is_configured():
        return
    bot_data = bot_app.bot_data
    cursor = bot_data.get(_RELAY_CURSOR_KEY)
    response = await relay_poll_events(cursor=str(cursor) if cursor else None)
    if not response.get("ok"):
        return

    events, next_cursor = _extract_events_response(response)
    if next_cursor:
        bot_data[_RELAY_CURSOR_KEY] = next_cursor
    for event in events:
        try:
            await _process_relay_event(bot_app, bot_data, event)
        except Exception:
            logger.warning("Failed to process lowiq relay event", exc_info=True)


async def _on_points_refresh_timeout(context) -> None:
    data = getattr(context.job, "data", {}) or {}
    req_id = str(data.get("req_id", "")).strip()
    if not req_id:
        return

    bot_data = context.application.bot_data
    req = _pending_req_by_id(bot_data, req_id)
    if not req:
        # Already matched by relay response.
        return

    _remove_pending_req(bot_data, req)
    chat_id = int(req.get("chat_id"))
    session_id = str(req.get("relay_session_id", "")).strip()
    if session_id:
        await relay_close_session(session_id=session_id, reason="timeout")

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
