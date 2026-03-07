import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.nadobro.services.user_service import get_user_readonly_client, get_user


_POINTS_CACHE: dict[str, dict] = {}
_POINTS_CACHE_TTL_SECONDS = 45
_ARCHIVE_PAGE_LIMIT = 500
_ARCHIVE_MAX_PAGES = 8
_SEASON1_START = datetime(2026, 1, 30, tzinfo=timezone.utc)
_OFFICIAL_POINTS_FIELDS = (
    "points",
    "total_points",
    "nado_points",
    "trading_points",
    "epoch_points",
    "points_total",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def _from_x18(raw) -> float:
    try:
        return float(raw) / 1e18
    except Exception:
        return 0.0


def _from_x18_dynamic(raw) -> float:
    try:
        fv = float(raw)
    except Exception:
        return 0.0
    # Archive payloads can be either x18 integers or plain numbers.
    if abs(fv) >= 1e9:
        return fv / 1e18
    return fv


def _epoch_start(now: datetime) -> datetime:
    if now <= _SEASON1_START:
        return _SEASON1_START
    seconds = int((now - _SEASON1_START).total_seconds())
    epoch_idx = seconds // (7 * 24 * 3600)
    return _SEASON1_START + timedelta(days=7 * epoch_idx)


def _scope_window(scope: str) -> tuple[Optional[datetime], datetime, str]:
    now = _now_utc()
    current_start = _epoch_start(now)
    if scope == "all":
        return None, now, "scope_all"
    if scope == "epoch":
        # Include small buffer before current epoch for smooth rollovers.
        return current_start - timedelta(days=2), now, "scope_epoch"
    return current_start, now, "current_mode"


def _epoch_number(now: datetime) -> int:
    if now < _SEASON1_START:
        return 1
    days = (now - _SEASON1_START).days
    return int(days // 7) + 1


def _maker_taker_bar(maker_pct: float) -> str:
    maker_blocks = max(0, min(10, int(round((maker_pct / 100.0) * 10))))
    return "█" * maker_blocks + "░" * (10 - maker_blocks)


def _try_extract_points(raw: dict) -> Optional[float]:
    if not isinstance(raw, dict):
        return None

    # Prefer explicit point fields from official gateway payloads.
    for key in _OFFICIAL_POINTS_FIELDS:
        if key in raw:
            val = _from_x18_dynamic(raw.get(key))
            if val > 0:
                return val

    for key in ("data", "result", "payload", "stats", "account", "subaccount"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            val = _try_extract_points(nested)
            if val is not None and val > 0:
                return val

    return None


def _query_official_points(client, subaccount: str, min_time: Optional[int], max_time: int) -> tuple[Optional[float], str]:
    if not hasattr(client, "_query_rest"):
        return None, "none"

    query_candidates = (
        ("subaccount_points", {"subaccount": subaccount, "start_time": min_time, "end_time": max_time}),
        ("points", {"subaccount": subaccount, "start_time": min_time, "end_time": max_time}),
        ("trading_points", {"subaccount": subaccount, "start_time": min_time, "end_time": max_time}),
        ("user_points", {"subaccount": subaccount, "start_time": min_time, "end_time": max_time}),
    )

    for query_type, params in query_candidates:
        safe_params = {k: v for k, v in params.items() if v is not None}
        try:
            response = client._query_rest(query_type, safe_params) or {}
        except Exception:
            continue
        points = _try_extract_points(response)
        if points is not None and points > 0:
            return points, f"gateway:{query_type}"

    return None, "none"


def _query_matches(client, subaccount: str, min_time: Optional[int], max_time: int) -> list[dict]:
    out: list[dict] = []
    cursor = max_time
    for _ in range(_ARCHIVE_MAX_PAGES):
        payload = {
            "matches": {
                "subaccounts": [subaccount],
                "max_time": cursor,
                "limit": _ARCHIVE_PAGE_LIMIT,
                "isolated": False,
            }
        }
        resp = client.query_archive(payload) or {}
        matches = resp.get("matches") or []
        if not matches:
            break
        tx_timestamp_by_submission_idx = {}
        for tx in (resp.get("txs") or []):
            try:
                tx_idx = str(tx.get("submission_idx", ""))
                tx_timestamp_by_submission_idx[tx_idx] = int(tx.get("timestamp"))
            except Exception:
                continue
        stop = False
        for m in matches:
            ts = tx_timestamp_by_submission_idx.get(str(m.get("submission_idx", "")))
            m["_timestamp"] = ts
            if min_time is not None and ts is not None and ts < min_time:
                stop = True
                continue
            out.append(m)
        if stop:
            break
        min_seen = None
        for m in matches:
            ts = m.get("_timestamp")
            if isinstance(ts, int):
                min_seen = ts if min_seen is None else min(min_seen, ts)
        if min_seen is None:
            break
        cursor = min_seen - 1
    return out


def _query_orders(client, subaccount: str, min_time: Optional[int], max_time: int) -> list[dict]:
    out: list[dict] = []
    cursor = max_time
    for _ in range(_ARCHIVE_MAX_PAGES):
        payload = {
            "orders": {
                "subaccounts": [subaccount],
                "max_time": cursor,
                "limit": _ARCHIVE_PAGE_LIMIT,
                "isolated": False,
            }
        }
        resp = client.query_archive(payload) or {}
        orders = resp.get("orders") or []
        if not orders:
            break
        stop = False
        for o in orders:
            try:
                ts = int(o.get("last_fill_timestamp") or o.get("first_fill_timestamp") or 0)
            except Exception:
                ts = 0
            if min_time is not None and ts and ts < min_time:
                stop = True
                continue
            out.append(o)
        if stop:
            break
        min_seen = None
        for o in orders:
            try:
                ts = int(o.get("last_fill_timestamp") or o.get("first_fill_timestamp") or 0)
            except Exception:
                ts = 0
            if ts:
                min_seen = ts if min_seen is None else min(min_seen, ts)
        if min_seen is None:
            break
        cursor = min_seen - 1
    return out


def _estimate_points(volume_usd: float, maker_ratio: float) -> float:
    # Estimate only: tuned to be volume-driven with maker preference.
    if volume_usd <= 0:
        return 0.0
    return max(0.0, (volume_usd / 1200.0) * (1.0 + maker_ratio * 0.35))


def get_points_dashboard(telegram_id: int, scope: str = "current") -> dict:
    scope = (scope or "current").lower()
    if scope not in ("current", "all", "epoch"):
        scope = "current"

    cache_key = f"{telegram_id}:{scope}"
    cached = _POINTS_CACHE.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0) < _POINTS_CACHE_TTL_SECONDS):
        return cached["data"]

    user = get_user(telegram_id)
    if not user or not user.main_address:
        return {"ok": False, "error": "Wallet not linked yet. Open 👛 Wallet first."}

    client = get_user_readonly_client(telegram_id)
    if not client or not getattr(client, "subaccount_hex", None):
        return {"ok": False, "error": "Could not initialize Nado client for this wallet."}

    min_dt, max_dt, scope_mode = _scope_window(scope)
    min_time = _to_unix(min_dt) if min_dt else None
    max_time = _to_unix(max_dt)

    official_points, points_source = _query_official_points(client, client.subaccount_hex, min_time, max_time)
    matches = _query_matches(client, client.subaccount_hex, min_time, max_time)
    orders = _query_orders(client, client.subaccount_hex, min_time, max_time)

    total_volume = 0.0
    total_fees = 0.0
    maker_count = 0
    taker_count = 0
    maker_volume = 0.0
    taker_volume = 0.0
    traded_products = set()
    archive_points_sum = 0.0
    archive_points_seen = False

    for m in matches:
        quote = abs(_from_x18(m.get("quote_filled", 0)))
        fee = _from_x18(m.get("fee", 0))
        is_taker = bool(m.get("is_taker"))
        try:
            product_id = int(m.get("product_id"))
            traded_products.add(product_id)
        except Exception:
            pass
        for field in _OFFICIAL_POINTS_FIELDS:
            raw_val = m.get(field)
            if raw_val is None:
                continue
            parsed = _from_x18_dynamic(raw_val)
            if parsed > 0:
                archive_points_sum += parsed
                archive_points_seen = True
                break
        total_volume += quote
        total_fees += fee
        if is_taker:
            taker_count += 1
            taker_volume += quote
        else:
            maker_count += 1
            maker_volume += quote

    realized_pnl = 0.0
    hold_samples = []
    for o in orders:
        realized_pnl += _from_x18(o.get("realized_pnl", 0))
        try:
            product_id = int(o.get("product_id"))
            traded_products.add(product_id)
        except Exception:
            pass
        try:
            first_ts = int(o.get("first_fill_timestamp") or 0)
            last_ts = int(o.get("last_fill_timestamp") or 0)
        except Exception:
            first_ts = 0
            last_ts = 0
        if first_ts and last_ts and last_ts >= first_ts:
            hold_samples.append(last_ts - first_ts)

    total_count = maker_count + taker_count
    maker_ratio = (maker_count / total_count) if total_count else 0.0
    maker_pct = maker_ratio * 100.0
    taker_pct = max(0.0, 100.0 - maker_pct)

    if official_points is None and archive_points_seen:
        official_points = archive_points_sum
        points_source = "archive:matches"

    estimated_points = _estimate_points(total_volume, maker_ratio)
    selected_points = official_points if (official_points is not None and official_points > 0) else estimated_points
    points_estimated = official_points is None or official_points <= 0
    cost_basis = total_fees + max(0.0, -realized_pnl)
    ppm = ((selected_points / total_volume) * 1_000_000.0) if total_volume > 0 else 0.0
    cost_per_point = (cost_basis / selected_points) if selected_points > 0 else 0.0
    avg_hold_seconds = int(sum(hold_samples) / len(hold_samples)) if hold_samples else 0
    missing_fields = []
    if points_estimated:
        missing_fields.append("official_points")
    if not matches:
        missing_fields.append("fills")
    if not orders:
        missing_fields.append("orders")

    data = {
        "ok": True,
        "scope": scope,
        "scope_mode": scope_mode,
        "epoch": _epoch_number(_now_utc()),
        "period_start": min_dt,
        "period_end": max_dt,
        "volume_usd": total_volume,
        "points": selected_points,
        "points_estimated": points_estimated,
        "points_source": points_source if not points_estimated else "estimated",
        "estimated_points": estimated_points,
        "pnl_realized": realized_pnl,
        "fees_paid": total_fees,
        "total_costs": cost_basis,
        "cost_per_point": cost_per_point,
        "ppm": ppm,
        "positions": len(traded_products),
        "avg_hold_seconds": avg_hold_seconds,
        "maker_count": maker_count,
        "taker_count": taker_count,
        "maker_volume": maker_volume,
        "taker_volume": taker_volume,
        "maker_pct": maker_pct,
        "taker_pct": taker_pct,
        "maker_bar": _maker_taker_bar(maker_pct),
        "missing_fields": missing_fields,
    }
    _POINTS_CACHE[cache_key] = {"ts": time.time(), "data": data}
    return data
