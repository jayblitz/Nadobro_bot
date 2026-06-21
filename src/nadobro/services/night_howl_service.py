"""Night HOWL — nightly per-user trade-pattern analysis + a backtest-backed
morning report, delivered at each user's local 8am and saved for later reference.

This builds on the existing HOWL auto-tuner (``howl_service``, the Alpha-Agent
param suggester) but is broader and read-only: it sweeps ALL of a user's trades
over the last 24h across every strategy, computes their unique trading pattern,
replays their live strategy config over the last-24h candles through the
backtester (net of fees/funding), and turns that into concrete, evidence-based
recommendations. The report is unique per user, sent at their local 8am, and
persisted (key-value store) so they can pull it up later via ``/howl``.

Layering:
* PURE (unit-tested, no DB/network): ``compute_user_pattern``,
  ``derive_recommendations``, ``compare_configs``, ``night_howl_due``,
  ``render_report_markdown``.
* PERSISTENCE: ``save_report`` / ``list_report_dates`` / ``get_report`` over the
  ``bot_state`` key-value table.
* ORCHESTRATION (DB/network): ``build_report`` ties it together for one user.
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

REPORT_PREFIX = "howl_report:"        # howl_report:<tid>:<network>:<YYYY-MM-DD>
INDEX_PREFIX = "howl_report_index:"   # howl_report_index:<tid>:<network> -> [dates]
LAST_SENT_PREFIX = "howl_last_sent:"  # howl_last_sent:<tid>:<network> -> "<YYYY-MM-DD>"
MAX_SAVED_REPORTS = 30
TARGET_LOCAL_HOUR = 8


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #

def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _trade_ts(trade: Dict[str, Any]) -> Optional[float]:
    """Epoch seconds for a trade's created_at (datetime / ISO string / epoch)."""
    raw = trade.get("created_at") or trade.get("filled_at")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(raw, (int, float)):
        v = float(raw)
        return v / 1000.0 if v > 1e11 else v
    try:
        txt = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except ValueError:
        return None


def _trade_fees(trade: Dict[str, Any]) -> float:
    return _f(trade.get("fees")) + _f(trade.get("fill_fee")) + _f(trade.get("builder_fee"))


def _trade_realized(trade: Dict[str, Any]) -> Optional[float]:
    for k in ("realized_pnl", "pnl"):
        if trade.get(k) is not None:
            return _f(trade.get(k))
    return None


# --------------------------------------------------------------------------- #
# PURE: pattern metrics                                                        #
# --------------------------------------------------------------------------- #

def compute_user_pattern(
    trades: List[Dict[str, Any]],
    *,
    now_ts: Optional[float] = None,
    window_hours: float = 24.0,
) -> Dict[str, Any]:
    """Summarize a user's last-``window_hours`` trading behaviour from raw trade
    rows. Pure — pass a list of trade dicts, get a metrics dict back."""
    now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    cutoff = now_ts - window_hours * 3600.0

    recent = []
    for t in trades:
        ts = _trade_ts(t)
        if ts is None or ts < cutoff:
            continue
        status = str(t.get("status") or "").lower()
        if status in ("failed", "cancelled", "rejected"):
            continue
        recent.append((ts, t))

    n = len(recent)
    if n == 0:
        return {
            "trades": 0, "volume_usd": 0.0, "fees_usd": 0.0, "funding_usd": 0.0,
            "realized_pnl_usd": 0.0, "net_pnl_usd": 0.0, "win_rate": None,
            "wins": 0, "losses": 0, "top_pairs": [], "by_source": {},
            "busiest_hour_utc": None, "avg_trade_usd": 0.0,
        }

    volume = fees = funding = realized = 0.0
    wins = losses = realized_count = 0
    pair_counter: Counter = Counter()
    pair_volume: Counter = Counter()
    source_counter: Counter = Counter()
    hour_counter: Counter = Counter()

    for ts, t in recent:
        notional = abs(_f(t.get("size")) * _f(t.get("price")))
        volume += notional
        fees += _trade_fees(t)
        funding += _f(t.get("funding_paid"))
        pair = str(t.get("product_name") or "?").upper()
        pair_counter[pair] += 1
        pair_volume[pair] += notional
        source_counter[str(t.get("source") or "manual").lower()] += 1
        hour_counter[datetime.fromtimestamp(ts, timezone.utc).hour] += 1
        r = _trade_realized(t)
        if r is not None and r != 0.0:
            realized += r
            realized_count += 1
            if r > 0:
                wins += 1
            else:
                losses += 1

    win_rate = (wins / realized_count) if realized_count else None
    busiest_hour = hour_counter.most_common(1)[0][0] if hour_counter else None
    top_pairs = [
        {"pair": p, "trades": c, "volume_usd": round(pair_volume[p], 2)}
        for p, c in pair_counter.most_common(3)
    ]

    return {
        "trades": n,
        "volume_usd": round(volume, 2),
        "fees_usd": round(fees, 4),
        "funding_usd": round(funding, 4),
        "realized_pnl_usd": round(realized, 4),
        "net_pnl_usd": round(realized - fees + funding, 4),
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "wins": wins,
        "losses": losses,
        "top_pairs": top_pairs,
        "by_source": dict(source_counter),
        "busiest_hour_utc": busiest_hour,
        "avg_trade_usd": round(volume / n, 2) if n else 0.0,
    }


# --------------------------------------------------------------------------- #
# PURE: backtest comparison + recommendations                                 #
# --------------------------------------------------------------------------- #

def compare_configs(
    strategy: str,
    base_cfg: Dict[str, Any],
    variants: Dict[str, Dict[str, Any]],
    candles: List[Any],
    costs: Any = None,
) -> List[Dict[str, Any]]:
    """Backtest the base config and each named variant over ``candles`` and
    return a list (best net first) of ``{name, net_pnl, gross_pnl, fees,
    funding, max_drawdown}``. ``variants`` maps a label -> config overlay."""
    from src.nadobro.engine.backtester import run_backtest

    out: List[Dict[str, Any]] = []
    runs = {"current": dict(base_cfg)}
    for name, overlay in (variants or {}).items():
        runs[name] = {**base_cfg, **overlay}
    for name, cfg in runs.items():
        try:
            rep = run_backtest(strategy, cfg, candles, costs=costs)
            out.append({
                "name": name,
                "net_pnl": float(rep.net_pnl),
                "gross_pnl": float(rep.gross_pnl),
                "fees": float(rep.fees),
                "funding": float(rep.funding),
                "max_drawdown": float(rep.max_drawdown),
            })
        except Exception as exc:  # noqa: BLE001 - a bad variant must not sink the report
            logger.debug("compare_configs variant %s failed: %s", name, exc)
    out.sort(key=lambda r: r["net_pnl"], reverse=True)
    return out


def derive_recommendations(
    pattern: Dict[str, Any],
    backtests: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """Turn metrics + backtest comparisons into concrete, plain-English advice.
    Pure and deterministic so the same data always yields the same guidance."""
    recs: List[str] = []
    n = int(pattern.get("trades") or 0)
    if n == 0:
        return ["No trades in the last 24h — nothing to tune. Consider a small "
                "grid or volume session on a liquid pair to start gathering data."]

    fees = _f(pattern.get("fees_usd"))
    realized = _f(pattern.get("realized_pnl_usd"))
    net = _f(pattern.get("net_pnl_usd"))
    volume = _f(pattern.get("volume_usd"))
    win_rate = pattern.get("win_rate")

    # Fee drag.
    if volume > 0 and fees / volume > 0.0015:
        recs.append(
            f"Fees ate {fees/volume*100:.2f}% of your ${volume:,.0f} volume. "
            "Lean on maker (resting) orders and a wider spread/step to cut taker fees."
        )
    if realized > 0 and net <= 0:
        recs.append(
            f"Your gross PnL was +${realized:,.2f} but fees/funding turned the net "
            f"to ${net:,.2f}. The edge is there — the costs are eating it; widen "
            "spreads or trade less often."
        )
    # Win rate.
    if isinstance(win_rate, (int, float)):
        if win_rate < 0.4:
            recs.append(
                f"Win rate was {win_rate*100:.0f}%. Tighten entries or raise SL/TP "
                "discipline — you're closing too many losers."
            )
        elif win_rate > 0.65 and net > 0:
            recs.append(
                f"Strong {win_rate*100:.0f}% win rate — consider sizing up modestly "
                "on your best pair while it holds."
            )
    # Concentration.
    top = pattern.get("top_pairs") or []
    if top and volume > 0 and _f(top[0].get("volume_usd")) / volume > 0.7:
        recs.append(
            f"{top[0]['pair']} was {_f(top[0]['volume_usd'])/volume*100:.0f}% of your "
            "volume — you're concentrated. A second uncorrelated pair would spread risk."
        )

    # Backtest-backed: did a variant beat the live config over the last 24h?
    if backtests:
        cur = next((b for b in backtests if b["name"] == "current"), None)
        best = backtests[0] if backtests else None
        if cur is not None and best is not None and best["name"] != "current" \
                and best["net_pnl"] > cur["net_pnl"] + 1e-9:
            recs.append(
                f"Backtest on the last 24h: your live config netted "
                f"${cur['net_pnl']:,.2f}; a '{best['name']}' setting would have netted "
                f"${best['net_pnl']:,.2f} after fees. Worth trying."
            )
        elif cur is not None:
            recs.append(
                f"Backtest on the last 24h: your live config was the best of the "
                f"variants tested (net ${cur['net_pnl']:,.2f} after fees) — hold course."
            )

    if not recs:
        recs.append("Solid, balanced session — no changes recommended. Keep doing "
                    "what's working and watch fees as volume grows.")
    return recs


# --------------------------------------------------------------------------- #
# PURE: scheduling + rendering                                                 #
# --------------------------------------------------------------------------- #

def night_howl_due(
    now_utc: datetime,
    tz_offset_hours: float,
    last_sent_date: Optional[str],
    *,
    target_local_hour: int = TARGET_LOCAL_HOUR,
) -> bool:
    """True when it's the target local hour for this user and today's report
    hasn't been sent yet. Drives an hourly scheduler tick for per-user local
    delivery without a per-user cron."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    local = now_utc + timedelta(hours=tz_offset_hours)
    if local.hour != int(target_local_hour):
        return False
    return last_sent_date != local.strftime("%Y-%m-%d")


def render_report_markdown(
    pattern: Dict[str, Any],
    recommendations: List[str],
    *,
    local_date: str,
    backtests: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Compose the per-user report as Telegram-friendly Markdown. Unique per user
    because every figure is theirs."""
    lines = [f"🌙 *Night HOWL — {local_date}*", ""]
    n = int(pattern.get("trades") or 0)
    if n == 0:
        lines.append("_No trades in the last 24h._")
        lines.append("")
    else:
        net = _f(pattern.get("net_pnl_usd"))
        net_emoji = "🟢" if net >= 0 else "🔴"
        wr = pattern.get("win_rate")
        wr_txt = f"{wr*100:.0f}%" if isinstance(wr, (int, float)) else "—"
        lines += [
            "*Your last 24h*",
            f"• Trades: {n}  ·  Volume: ${_f(pattern.get('volume_usd')):,.0f}",
            f"• {net_emoji} Net PnL (after fees/funding): ${net:,.2f}",
            f"• Gross realized: ${_f(pattern.get('realized_pnl_usd')):,.2f}  ·  "
            f"Fees: ${_f(pattern.get('fees_usd')):,.2f}  ·  "
            f"Funding: ${_f(pattern.get('funding_usd')):,.2f}",
            f"• Win rate: {wr_txt} ({int(pattern.get('wins') or 0)}W / {int(pattern.get('losses') or 0)}L)",
        ]
        top = pattern.get("top_pairs") or []
        if top:
            lines.append("• Top pairs: " + ", ".join(
                f"{p['pair']} ({p['trades']})" for p in top))
        bh = pattern.get("busiest_hour_utc")
        if bh is not None:
            lines.append(f"• Busiest hour: {int(bh):02d}:00 UTC")
        lines.append("")

    if backtests:
        lines.append("*24h backtest (net of fees)*")
        for b in backtests[:4]:
            tag = " ⭐" if b is backtests[0] else ""
            lines.append(f"• {b['name']}: ${b['net_pnl']:,.2f}{tag}")
        lines.append("")

    lines.append("*Recommendations*")
    for r in recommendations:
        lines.append(f"• {r}")
    lines.append("")
    lines.append("_Saved — pull it up anytime with /howl. Informational only, not "
                 "financial advice._")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# PERSISTENCE (bot_state key-value store)                                      #
# --------------------------------------------------------------------------- #

def _report_key(telegram_id: int, network: str, date: str) -> str:
    return f"{REPORT_PREFIX}{int(telegram_id)}:{network}:{date}"


def _index_key(telegram_id: int, network: str) -> str:
    return f"{INDEX_PREFIX}{int(telegram_id)}:{network}"


def save_report(telegram_id: int, network: str, report: Dict[str, Any]) -> None:
    """Persist a report under its date and prepend it to the user's index
    (capped at ``MAX_SAVED_REPORTS``, oldest evicted)."""
    from src.nadobro.models.database import get_bot_state, set_bot_state

    date = str(report.get("date"))
    set_bot_state(_report_key(telegram_id, network, date), report)
    idx = get_bot_state(_index_key(telegram_id, network)) or {}
    dates = [d for d in (idx.get("dates") or []) if d != date]
    dates.insert(0, date)
    evicted = dates[MAX_SAVED_REPORTS:]
    dates = dates[:MAX_SAVED_REPORTS]
    set_bot_state(_index_key(telegram_id, network), {"dates": dates})
    for old in evicted:
        try:
            set_bot_state(_report_key(telegram_id, network, old), None)
        except Exception:  # noqa: BLE001 - eviction is best-effort
            pass


def list_report_dates(telegram_id: int, network: str) -> List[str]:
    from src.nadobro.models.database import get_bot_state

    idx = get_bot_state(_index_key(telegram_id, network)) or {}
    return list(idx.get("dates") or [])


def get_report(telegram_id: int, network: str, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Latest report when ``date`` is None, else that specific date."""
    from src.nadobro.models.database import get_bot_state

    if date is None:
        dates = list_report_dates(telegram_id, network)
        if not dates:
            return None
        date = dates[0]
    return get_bot_state(_report_key(telegram_id, network, date))


# --------------------------------------------------------------------------- #
# ORCHESTRATION (DB + network) — build one user's report                       #
# --------------------------------------------------------------------------- #

# Per-strategy backtest variants tried against the user's last-24h candles. Each
# is a config overlay; the comparison reports which net-of-fees result wins.
def _variants_for(strategy: str, cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    s = (strategy or "").lower()
    if s in ("grid", "rgrid", "dgrid"):
        step = cfg.get("min_spread_between_orders") or cfg.get("step_pct")
        if step:
            d = Decimal(str(step))
            return {
                "wider step": {"min_spread_between_orders": d * 2, "step_pct": d * 2},
                "tighter step": {"min_spread_between_orders": d / 2, "step_pct": d / 2},
            }
    return {}


def _candles_for(client: Any, product_id: int) -> List[Any]:
    """Fetch the last 24x 1h candles and convert to backtester Candle objects."""
    from src.nadobro.engine.backtester import candles_from_ohlc

    try:
        rows = client.get_candlesticks(int(product_id), timeframe="1h", limit=24) or []
    except Exception as exc:  # noqa: BLE001
        logger.debug("night_howl candle fetch failed pid=%s: %s", product_id, exc)
        return []
    return candles_from_ohlc(rows, interval_s=3600.0)


def build_report(
    telegram_id: int,
    network: str,
    *,
    client: Any = None,
    now_utc: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Assemble (but do not send) one user's Night HOWL report. Returns the
    report dict (also persisted) or None if there's nothing to report. Best
    effort: never raises — a per-user failure must not break the nightly sweep."""
    from src.nadobro.models.database import get_trades_by_user
    from src.nadobro.services.bot_runtime import _load_state
    from src.nadobro.services.user_service import get_user_readonly_client

    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        tz_offset = _user_tz_offset(telegram_id)
        local_date = (now_utc + timedelta(hours=tz_offset)).strftime("%Y-%m-%d")

        cutoff = now_utc - timedelta(hours=24)
        trades = get_trades_by_user(
            telegram_id,
            limit=None,
            network=network,
            since_created_at=cutoff,
        ) or []
        pattern = compute_user_pattern(trades, now_ts=now_utc.timestamp())

        # Backtest the user's live strategy (if any) over its last-24h candles.
        backtests: List[Dict[str, Any]] = []
        state = _load_state(telegram_id, network) or {}
        strategy = str(state.get("strategy") or "")
        product = str(state.get("product") or "")
        if strategy and product:
            try:
                from src.nadobro.services.engine_runtime import (
                    ENGINE_MAPPED_STRATEGIES, map_strategy_config,
                )
                if strategy in ENGINE_MAPPED_STRATEGIES:
                    cli = client or get_user_readonly_client(telegram_id, network=network)
                    pid = state.get("product_id")
                    if cli is not None and pid is not None:
                        candles = _candles_for(cli, int(pid))
                        if len(candles) >= 4:
                            mid = candles[-1].close
                            settings = {k: v for k, v in state.items()
                                        if not isinstance(v, (dict, list))}
                            cfg = map_strategy_config(
                                strategy, settings, mid, product=product,
                                leverage=int(_f(settings.get("leverage"), 1)),
                            )
                            backtests = compare_configs(
                                strategy, cfg, _variants_for(strategy, cfg), candles,
                            )
            except Exception as exc:  # noqa: BLE001 - backtest is enrichment, not required
                logger.debug("night_howl backtest skipped user=%s: %s", telegram_id, exc)

        recommendations = derive_recommendations(pattern, backtests)
        markdown = render_report_markdown(
            pattern, recommendations, local_date=local_date, backtests=backtests,
        )
        report = {
            "date": local_date,
            "generated_at": now_utc.isoformat(),
            "network": network,
            "pattern": pattern,
            "backtests": backtests,
            "recommendations": recommendations,
            "markdown": markdown,
        }
        save_report(telegram_id, network, report)
        return report
    except Exception as exc:  # noqa: BLE001 - never break the sweep
        logger.warning("night_howl build_report failed user=%s: %s", telegram_id, exc, exc_info=True)
        return None


def night_howl_enabled(telegram_id: int) -> bool:
    """Per-user opt-out. Stored in settings as ``night_howl_enabled``; default
    True (on). Users can turn it off without losing saved reports."""
    try:
        from src.nadobro.services.settings_service import get_user_settings

        _, settings = get_user_settings(telegram_id)
        val = (settings or {}).get("night_howl_enabled", True)
        return bool(val) if val is not None else True
    except Exception:  # noqa: BLE001
        return True


def _user_tz_offset(telegram_id: int) -> float:
    """User's UTC offset in hours for local-8am delivery. Stored in settings as
    ``howl_tz_offset``; defaults to 0 (UTC) when unset."""
    try:
        from src.nadobro.services.settings_service import get_user_settings

        _, settings = get_user_settings(telegram_id)
        return _f((settings or {}).get("howl_tz_offset"), 0.0)
    except Exception:  # noqa: BLE001
        return 0.0


def mark_sent(telegram_id: int, network: str, local_date: str) -> None:
    from src.nadobro.models.database import set_bot_state

    set_bot_state(f"{LAST_SENT_PREFIX}{int(telegram_id)}:{network}", {"date": local_date})


def last_sent_date(telegram_id: int, network: str) -> Optional[str]:
    from src.nadobro.models.database import get_bot_state

    rec = get_bot_state(f"{LAST_SENT_PREFIX}{int(telegram_id)}:{network}") or {}
    return rec.get("date")

