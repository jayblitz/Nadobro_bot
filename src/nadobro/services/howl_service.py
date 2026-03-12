import logging
import json
import time
from datetime import datetime, timedelta

from src.nadobro.models.database import get_bot_state_raw, set_bot_state
from src.nadobro.services.settings_service import get_strategy_settings, update_user_settings
from src.nadobro.services.bro_llm import analyze_for_howl
from src.nadobro.db import query_all

logger = logging.getLogger(__name__)

HOWL_STATE_PREFIX = "howl:"
HOWL_PENDING_PREFIX = "howl_pending:"


def _howl_key(telegram_id: int, network: str) -> str:
    return f"{HOWL_STATE_PREFIX}{telegram_id}:{network}"


def _pending_key(telegram_id: int, network: str) -> str:
    return f"{HOWL_PENDING_PREFIX}{telegram_id}:{network}"


def get_recent_bro_trades(telegram_id: int, hours: int = 24) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    rows = query_all(
        "SELECT product_name, side, pnl, size, price, created_at, status "
        "FROM trades WHERE user_id = %s AND created_at > %s "
        "ORDER BY created_at DESC LIMIT 50",
        (telegram_id, cutoff),
    )
    return rows or []


def compute_performance_metrics(trades: list[dict], bro_state: dict) -> dict:
    if not trades:
        return {
            "total_trades": 0,
            "win_rate": 0,
            "avg_pnl": 0,
            "total_pnl": bro_state.get("total_pnl", 0),
            "max_win": 0,
            "max_loss": 0,
            "consecutive_holds": bro_state.get("consecutive_holds", 0),
        }

    pnls = [float(t.get("pnl", 0) or 0) for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    total = wins + losses

    return {
        "total_trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / total * 100) if total > 0 else 0,
        "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
        "total_pnl": sum(pnls),
        "cumulative_pnl": bro_state.get("total_pnl", 0),
        "max_win": max(pnls) if pnls else 0,
        "max_loss": min(pnls) if pnls else 0,
        "consecutive_holds": bro_state.get("consecutive_holds", 0),
        "total_decisions": len(bro_state.get("decisions_log", [])),
    }


def run_howl_analysis(telegram_id: int, network: str, bot_state: dict) -> dict | None:
    _, bro_settings = get_strategy_settings(telegram_id, "bro")
    bro_state = bot_state.get("bro_state", {})

    trades = get_recent_bro_trades(telegram_id)
    metrics = compute_performance_metrics(trades, bro_state)

    result = analyze_for_howl(
        trade_history=trades,
        current_settings=bro_settings,
        performance_metrics=metrics,
    )

    if not result:
        return None

    howl_record = {
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": metrics,
        "suggestions": result.get("suggestions", []),
        "overall_assessment": result.get("overall_assessment", ""),
        "confidence": result.get("confidence", 0),
        "status": "pending",
    }

    set_bot_state(_pending_key(telegram_id, network), howl_record)

    last_runs = []
    raw = get_bot_state_raw(_howl_key(telegram_id, network))
    if raw:
        try:
            last_runs = json.loads(raw)
            if not isinstance(last_runs, list):
                last_runs = [last_runs]
        except Exception:
            last_runs = []
    last_runs.append({
        "ts": datetime.utcnow().isoformat(),
        "trades_analyzed": len(trades),
        "suggestions_count": len(result.get("suggestions", [])),
    })
    set_bot_state(_howl_key(telegram_id, network), last_runs[-10:])

    return howl_record


def get_pending_howl(telegram_id: int, network: str) -> dict | None:
    raw = get_bot_state_raw(_pending_key(telegram_id, network))
    if not raw:
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if data.get("status") in ("pending", "partially_applied"):
            return data
        return None
    except Exception:
        return None


def approve_howl_suggestion(telegram_id: int, network: str, suggestion_index: int) -> tuple[bool, str]:
    pending = get_pending_howl(telegram_id, network)
    if not pending:
        return False, "No pending HOWL suggestions"

    suggestions = pending.get("suggestions", [])
    if suggestion_index < 0 or suggestion_index >= len(suggestions):
        return False, f"Invalid suggestion index ({suggestion_index})"

    suggestion = suggestions[suggestion_index]
    param = suggestion.get("parameter", "")
    new_value = suggestion.get("suggested_value")

    if not param or new_value is None:
        return False, "Invalid suggestion data"

    try:
        if isinstance(new_value, str):
            try:
                new_value = float(new_value)
                if new_value == int(new_value):
                    new_value = int(new_value)
            except ValueError:
                pass

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            bro = strategies.setdefault("bro", {})
            bro[param] = new_value

        update_user_settings(telegram_id, _mutate)

        suggestions[suggestion_index]["status"] = "approved"
        pending["status"] = "partially_applied" if any(
            s.get("status") == "pending" for s in suggestions
        ) else "applied"
        set_bot_state(_pending_key(telegram_id, network), pending)

        return True, f"Applied: {param} = {new_value}"
    except Exception as e:
        return False, f"Failed to apply: {str(e)[:100]}"


def reject_howl_suggestion(telegram_id: int, network: str, suggestion_index: int) -> tuple[bool, str]:
    pending = get_pending_howl(telegram_id, network)
    if not pending:
        return False, "No pending HOWL suggestions"

    suggestions = pending.get("suggestions", [])
    if suggestion_index < 0 or suggestion_index >= len(suggestions):
        return False, f"Invalid suggestion index"

    suggestions[suggestion_index]["status"] = "rejected"
    pending["status"] = "partially_applied" if any(
        s.get("status") not in ("approved", "rejected") for s in suggestions
    ) else "completed"
    set_bot_state(_pending_key(telegram_id, network), pending)

    return True, "Suggestion rejected"


def dismiss_all_howl(telegram_id: int, network: str) -> bool:
    pending = get_pending_howl(telegram_id, network)
    if not pending:
        return False
    pending["status"] = "dismissed"
    set_bot_state(_pending_key(telegram_id, network), pending)
    return True


def format_howl_message(howl_data: dict) -> str:
    lines = ["🐺 *HOWL — Nightly Optimization Report*\n"]

    assessment = howl_data.get("overall_assessment", "")
    if assessment:
        lines.append(f"_{assessment}_\n")

    metrics = howl_data.get("metrics", {})
    if metrics:
        total = metrics.get("total_trades", 0)
        wr = metrics.get("win_rate", 0)
        total_pnl = metrics.get("total_pnl", 0)
        lines.append(f"📊 24h: {total} trades | Win rate: {wr:.0f}% | PnL: ${total_pnl:+.2f}\n")

    suggestions = howl_data.get("suggestions", [])
    if suggestions:
        lines.append("*Suggested Changes:*")
        for i, s in enumerate(suggestions):
            param = s.get("parameter", "?")
            current = s.get("current_value", "?")
            new = s.get("suggested_value", "?")
            rationale = s.get("rationale", "")
            impact = s.get("expected_impact", "")
            status = s.get("status", "pending")

            if status != "pending":
                emoji = "✅" if status == "approved" else "❌"
                lines.append(f"\n{emoji} `{param}`: {current} → {new} [{status}]")
            else:
                lines.append(f"\n{i+1}. `{param}`: {current} → *{new}*")
                if rationale:
                    lines.append(f"   _{rationale}_")
                if impact:
                    lines.append(f"   Expected: {impact}")
    else:
        lines.append("No specific changes suggested — current parameters look good.")

    confidence = howl_data.get("confidence", 0)
    lines.append(f"\nConfidence: {confidence:.0%}")

    return "\n".join(lines)
