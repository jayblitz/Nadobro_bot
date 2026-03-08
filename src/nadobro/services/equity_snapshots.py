"""Record equity snapshots and compute 1d/7d changes for portfolio display."""
import json
import time
from typing import Optional

from src.nadobro.models.database import get_bot_state_raw, set_bot_state

EQUITY_HISTORY_KEY = "equity_history"
MAX_POINTS = 500
ONE_DAY_SEC = 86400
SEVEN_DAY_SEC = 7 * ONE_DAY_SEC


def _key(telegram_id: int) -> str:
    return f"{EQUITY_HISTORY_KEY}:{telegram_id}"


def record_snapshot(telegram_id: int, total_equity: float) -> None:
    """Append equity snapshot, cap history size."""
    key = _key(telegram_id)
    raw = get_bot_state_raw(key)
    history = []
    if raw:
        try:
            history = json.loads(raw)
        except Exception:
            history = []
    ts = time.time()
    history.append({"ts": ts, "equity": float(total_equity)})
    history = history[-MAX_POINTS:]
    set_bot_state(key, history)


def get_1d_7d_changes(telegram_id: int) -> tuple[Optional[float], Optional[float]]:
    """Return (1d_pct, 7d_pct) or (None, None) if insufficient data."""
    key = _key(telegram_id)
    raw = get_bot_state_raw(key)
    if not raw:
        return None, None
    try:
        history = json.loads(raw)
    except Exception:
        return None, None
    if not history:
        return None, None
    now = time.time()
    current = float(history[-1].get("equity", 0))
    if current <= 0:
        return None, None

    def _closest(target_age_sec: float) -> Optional[tuple[float, float]]:
        target_ts = now - target_age_sec
        best = None
        best_diff = float("inf")
        for h in history[:-1]:
            pts = float(h.get("ts", 0))
            eq = float(h.get("equity", 0))
            diff = abs(pts - target_ts)
            if diff < best_diff and eq > 0:
                best_diff = diff
                best = (pts, eq)
        return best

    p1d = None
    p7d = None
    closest_1d = _closest(ONE_DAY_SEC)
    if closest_1d:
        _, eq_1d = closest_1d
        p1d = ((current - eq_1d) / eq_1d) * 100.0
    closest_7d = _closest(SEVEN_DAY_SEC)
    if closest_7d:
        _, eq_7d = closest_7d
        p7d = ((current - eq_7d) / eq_7d) * 100.0
    return p1d, p7d


def get_history_for_csv(telegram_id: int, limit: int = 200) -> list[dict]:
    """Return recent history for CSV export."""
    key = _key(telegram_id)
    raw = get_bot_state_raw(key)
    if not raw:
        return []
    try:
        history = json.loads(raw)
    except Exception:
        return []
    return history[-limit:]
