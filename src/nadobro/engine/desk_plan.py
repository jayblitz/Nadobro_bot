"""Desk execution plan model — the structured object between "text" and "orders".

A Desk plan is what the text-to-trade parser produces and what the
DeskController executes: one entry leg (market / limit / TWAP), optionally
gated on an entry trigger, optionally chained into exit legs (TP/SL). Plans
are plain dicts in persistence, dataclasses in code.

This module lives in ``engine/`` (stdlib-only, pure) because both sides need
it: services (parser, store, preview card) and the engine controller that
runs plans. Service-side helpers that touch config/env live in
``services.desk_plans``, which re-exports everything here.

Routing semantics (product decision, 2026-06):
- "buy" / "sell"  -> SPOT  (stock tokens like QQQX/SPYX are always spot)
- "long" / "short" -> PERP
- explicit "perp"/"spot"/leverage wording overrides the default.

Validation here is DETERMINISTIC and runs after any parse (regex or LLM).
The LLM only ever extracts fields; it never decides whether a plan is valid,
and nothing executes without the user pressing Confirm on the preview card.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

# -- limits (product decisions, 2026-06) -----------------------------------
MAX_PLAN_DURATION_MINUTES = 7 * 24 * 60  # 7 days, entry leg
MAX_TRIGGER_WAIT_MINUTES = 7 * 24 * 60   # trigger must fire within 7 days
MIN_TWAP_DURATION_MINUTES = 2
MIN_TWAP_INTERVAL_SECONDS = 10
DEFAULT_TWAP_INTERVAL_SECONDS = 30
MAX_TWAP_SLICES = 2000


ALGOS = ("market", "limit", "twap")
MARKETS = ("spot", "perp")
SIDES = ("buy", "sell")
# price_cross is parse-time only: "once ETH hits 2500" carries no direction,
# so it resolves to price_above/price_below against the arrival mid at confirm.
TRIGGER_KINDS = ("price_above", "price_below", "price_cross", "pct_move", "time")
EXEC_MODES = ("taker", "maker")

# Plan lifecycle (persisted in execution_plans.status)
ST_DRAFT = "draft"
ST_AWAITING_TRIGGER = "awaiting_trigger"
ST_RUNNING = "running"
ST_COMPLETED = "completed"
ST_CANCELLED = "cancelled"
ST_FAILED = "failed"
ACTIVE_STATUSES = (ST_AWAITING_TRIGGER, ST_RUNNING)
TERMINAL_STATUSES = (ST_COMPLETED, ST_CANCELLED, ST_FAILED)


@dataclass
class EntryTrigger:
    """Start condition for the entry leg.

    ``pct_move`` is a parse-time representation only: it is resolved to an
    absolute ``price_above``/``price_below`` against the live mid at CONFIRM
    time (so "dumps 2%" anchors to the price the user saw on the preview
    card, and the runtime watcher stays a dumb absolute-price comparison
    that survives restarts).
    """
    kind: str
    price: Optional[float] = None        # price_above / price_below
    pct: Optional[float] = None          # pct_move: signed % from arrival mid
    delay_minutes: Optional[float] = None  # time: start N minutes from confirm
    fire_at_ts: Optional[float] = None   # time: resolved at confirm

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "price": self.price,
            "pct": self.pct,
            "delay_minutes": self.delay_minutes,
            "fire_at_ts": self.fire_at_ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "EntryTrigger":
        return cls(
            kind=str(d.get("kind") or ""),
            price=_opt_float(d.get("price")),
            pct=_opt_float(d.get("pct")),
            delay_minutes=_opt_float(d.get("delay_minutes")),
            fire_at_ts=_opt_float(d.get("fire_at_ts")),
        )


@dataclass
class ExitPlan:
    """Chained exit legs. Percentages are vs the ACTUAL average entry price
    (never the requested size/price — partial TWAPs exit what they hold).
    Perp exits run as a reduce-only TripleBarrier; spot exits run as the
    Desk sell-leg watcher."""
    tp_pct: Optional[float] = None
    sl_pct: Optional[float] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    trailing_pct: Optional[float] = None  # perp only (TripleBarrier trailing)

    def is_empty(self) -> bool:
        return all(
            v is None
            for v in (self.tp_pct, self.sl_pct, self.tp_price, self.sl_price, self.trailing_pct)
        )

    def to_dict(self) -> dict:
        return {
            "tp_pct": self.tp_pct,
            "sl_pct": self.sl_pct,
            "tp_price": self.tp_price,
            "sl_price": self.sl_price,
            "trailing_pct": self.trailing_pct,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ExitPlan":
        return cls(
            tp_pct=_opt_float(d.get("tp_pct")),
            sl_pct=_opt_float(d.get("sl_pct")),
            tp_price=_opt_float(d.get("tp_price")),
            sl_price=_opt_float(d.get("sl_price")),
            trailing_pct=_opt_float(d.get("trailing_pct")),
        )


@dataclass
class ExecutionPlan:
    algo: str = "market"            # market | limit | twap
    market: str = "spot"            # spot | perp
    product: Optional[str] = None   # catalog symbol, uppercase
    side: Optional[str] = None      # buy | sell (perp: buy=long, sell=short)
    size_base: Optional[float] = None
    size_quote: Optional[float] = None  # USD notional ("$500 of ETH")
    limit_price: Optional[float] = None
    leverage: Optional[int] = None      # perp only
    duration_minutes: Optional[float] = None      # twap
    interval_seconds: Optional[float] = None      # twap
    exec_mode: str = "taker"        # twap slice style; maker is opt-in
    entry_trigger: Optional[EntryTrigger] = None
    exits: Optional[ExitPlan] = None
    raw_text: str = ""
    plan_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    created_ts: float = field(default_factory=time.time)

    # -- derived ------------------------------------------------------------
    @property
    def is_long(self) -> bool:
        return self.side == "buy"

    def venue_pair(self) -> str:
        """Market-qualified trading-pair key for the engine adapter.

        The adapter resolves BOTH the product_id and the spot/perp margin
        routing from a single ``ProductMeta`` keyed by this string. A bare
        base ("ETH") resolves to the PERP on a dual-listed asset, so a spot
        plan MUST use the ``-SPOT`` alias or "buy 2 ETH" silently opens a
        perp. See build_product_meta_from_catalog.
        """
        suffix = "PERP" if self.market == "perp" else "SPOT"
        return f"{self.product}-{suffix}"

    def twap_slices(self) -> Optional[int]:
        if self.algo != "twap" or not self.duration_minutes or not self.interval_seconds:
            return None
        return max(1, int((self.duration_minutes * 60) // self.interval_seconds))

    def describe(self) -> str:
        """One-line summary for logs/alerts (not UI — the card formats its own)."""
        bits = [self.algo.upper(), (self.side or "?").upper(), self.product or "?",
                self.market.upper()]
        if self.size_base:
            bits.append(f"{self.size_base:g} base")
        elif self.size_quote:
            bits.append(f"${self.size_quote:g}")
        if self.duration_minutes:
            bits.append(f"over {self.duration_minutes:g}m")
        if self.entry_trigger:
            bits.append(f"trigger={self.entry_trigger.kind}")
        if self.exits and not self.exits.is_empty():
            bits.append("with exits")
        return " ".join(bits)

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "algo": self.algo,
            "market": self.market,
            "product": self.product,
            "side": self.side,
            "size_base": self.size_base,
            "size_quote": self.size_quote,
            "limit_price": self.limit_price,
            "leverage": self.leverage,
            "duration_minutes": self.duration_minutes,
            "interval_seconds": self.interval_seconds,
            "exec_mode": self.exec_mode,
            "entry_trigger": self.entry_trigger.to_dict() if self.entry_trigger else None,
            "exits": self.exits.to_dict() if self.exits else None,
            "raw_text": self.raw_text,
            "created_ts": self.created_ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ExecutionPlan":
        trigger = d.get("entry_trigger")
        exits = d.get("exits")
        return cls(
            plan_id=str(d.get("plan_id") or uuid.uuid4().hex[:16]),
            algo=str(d.get("algo") or "market").lower(),
            market=str(d.get("market") or "spot").lower(),
            product=(str(d["product"]).upper().strip() if d.get("product") else None),
            side=(str(d["side"]).lower().strip() if d.get("side") else None),
            size_base=_opt_float(d.get("size_base")),
            size_quote=_opt_float(d.get("size_quote")),
            limit_price=_opt_float(d.get("limit_price")),
            leverage=_opt_int(d.get("leverage")),
            duration_minutes=_opt_float(d.get("duration_minutes")),
            interval_seconds=_opt_float(d.get("interval_seconds")),
            exec_mode=str(d.get("exec_mode") or "taker").lower(),
            entry_trigger=EntryTrigger.from_dict(trigger) if isinstance(trigger, dict) else None,
            exits=ExitPlan.from_dict(exits) if isinstance(exits, dict) else None,
            raw_text=str(d.get("raw_text") or ""),
            created_ts=float(d.get("created_ts") or time.time()),
        )


def _opt_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def _opt_int(v) -> Optional[int]:
    f = _opt_float(v)
    return int(f) if f is not None else None


# ---------------------------------------------------------------------------
# Deterministic validation — the gate the LLM cannot cross
# ---------------------------------------------------------------------------

def validate_plan(
    plan: ExecutionPlan,
    *,
    perp_symbols: set[str],
    spot_symbols: set[str],
    max_leverage: Optional[int] = None,
    mid_price: Optional[float] = None,
    min_notional_usd: float = 10.0,
) -> list[str]:
    """Return human-readable problems; empty list == structurally valid.

    Catalog sets are injected so tests never hit the network and callers can
    cache. ``mid_price`` enables the notional check when available (the
    preview step always fetches it; parse-time validation may skip it).
    """
    problems: list[str] = []

    if plan.algo not in ALGOS:
        problems.append(f"Unsupported order style '{plan.algo}'.")
    if plan.market not in MARKETS:
        problems.append(f"Unknown market '{plan.market}'.")
    if plan.side not in SIDES:
        problems.append("Missing side (buy/sell or long/short).")
    if plan.exec_mode not in EXEC_MODES:
        problems.append(f"Unknown execution mode '{plan.exec_mode}'.")

    # -- product/market resolution
    if not plan.product:
        problems.append("Missing product.")
    elif plan.market == "perp" and plan.product not in perp_symbols:
        if plan.product in spot_symbols:
            problems.append(
                f"{plan.product} has no perp market on Nado — it trades spot only "
                f"(say 'buy'/'sell' instead of 'long'/'short')."
            )
        else:
            problems.append(f"Unknown perp product {plan.product}.")
    elif plan.market == "spot" and plan.product not in spot_symbols:
        if plan.product in perp_symbols:
            problems.append(
                f"{plan.product} has no spot market on Nado — say 'long'/'short' "
                f"to trade the perp."
            )
        else:
            problems.append(f"Unknown spot product {plan.product}.")

    # -- size
    if plan.size_base is None and plan.size_quote is None:
        problems.append("Missing size (e.g. '0.5 ETH' or '$500 of ETH').")
    else:
        if plan.size_base is not None and plan.size_base <= 0:
            problems.append("Size must be positive.")
        if plan.size_quote is not None and plan.size_quote <= 0:
            problems.append("USD size must be positive.")
        if plan.size_base is not None and plan.size_quote is not None:
            problems.append("Give size as base amount OR USD value, not both.")
    if mid_price and plan.size_base is not None:
        if plan.size_base * mid_price < min_notional_usd:
            problems.append(f"Order is below the ${min_notional_usd:.0f} minimum size.")
    if plan.size_quote is not None and plan.size_quote < min_notional_usd:
        problems.append(f"Order is below the ${min_notional_usd:.0f} minimum size.")

    # -- market-specific rules
    if plan.market == "spot":
        if plan.leverage and plan.leverage > 1:
            problems.append(
                "Spot has no leverage — drop the leverage or say 'long'/'short' for a perp."
            )
        if plan.exits and plan.exits.trailing_pct is not None:
            problems.append("Trailing stops are perp-only for now.")
    else:
        if plan.leverage is not None:
            if plan.leverage < 1:
                problems.append("Leverage must be at least 1x.")
            elif max_leverage and plan.leverage > max_leverage:
                problems.append(f"Max leverage for {plan.product} is {max_leverage}x.")

    # -- algo-specific rules
    if plan.algo == "limit":
        if plan.limit_price is None or plan.limit_price <= 0:
            problems.append("Limit order needs a positive limit price.")
    if plan.algo == "twap":
        dur = plan.duration_minutes or 0
        if dur < MIN_TWAP_DURATION_MINUTES:
            problems.append(f"TWAP duration must be at least {MIN_TWAP_DURATION_MINUTES} minutes.")
        elif dur > MAX_PLAN_DURATION_MINUTES:
            problems.append("TWAP duration can be at most 7 days.")
        interval = plan.interval_seconds or DEFAULT_TWAP_INTERVAL_SECONDS
        if interval < MIN_TWAP_INTERVAL_SECONDS:
            problems.append(f"TWAP interval must be at least {MIN_TWAP_INTERVAL_SECONDS}s.")
        elif dur and (dur * 60) / interval > MAX_TWAP_SLICES:
            problems.append(
                f"That schedule is {int((dur * 60) / interval)} slices — max is "
                f"{MAX_TWAP_SLICES}. Use a longer interval."
            )
        if plan.limit_price is not None:
            problems.append("TWAP and a fixed limit price don't combine — pick one.")

    # -- trigger
    trig = plan.entry_trigger
    if trig is not None:
        if trig.kind not in TRIGGER_KINDS:
            problems.append(f"Unsupported trigger '{trig.kind}'.")
        elif trig.kind in ("price_above", "price_below", "price_cross"):
            if trig.price is None or trig.price <= 0:
                problems.append("Trigger needs a positive price level.")
        elif trig.kind == "pct_move":
            if trig.pct is None or trig.pct == 0:
                problems.append("Percent trigger needs a non-zero percent move.")
            elif abs(trig.pct) > 50:
                problems.append("Percent trigger must be within ±50%.")
        elif trig.kind == "time":
            delay = trig.delay_minutes
            if (delay is None or delay <= 0) and not trig.fire_at_ts:
                problems.append("Time trigger needs a positive delay (e.g. 'in 2 hours').")
            elif delay is not None and delay > MAX_TRIGGER_WAIT_MINUTES:
                problems.append("Time trigger can be at most 7 days out.")

    # -- exits
    ex = plan.exits
    if ex is not None and not ex.is_empty():
        for label, pct in (("Take-profit", ex.tp_pct), ("Stop-loss", ex.sl_pct),
                           ("Trailing stop", ex.trailing_pct)):
            if pct is not None and not (0 < pct <= 100):
                problems.append(f"{label} percent must be between 0 and 100.")
        for label, px in (("Take-profit", ex.tp_price), ("Stop-loss", ex.sl_price)):
            if px is not None and px <= 0:
                problems.append(f"{label} price must be positive.")
        if ex.tp_pct is not None and ex.tp_price is not None:
            problems.append("Give take-profit as percent OR price, not both.")
        if ex.sl_pct is not None and ex.sl_price is not None:
            problems.append("Give stop-loss as percent OR price, not both.")
        if plan.market == "spot" and plan.side == "sell":
            problems.append("Exits only apply when buying — a spot sell IS the exit.")

    return problems


# ---------------------------------------------------------------------------
# Trigger resolution at confirm time
# ---------------------------------------------------------------------------

def resolve_trigger(trigger: EntryTrigger, *, arrival_mid: float, now: Optional[float] = None) -> EntryTrigger:
    """Pin a parse-time trigger to absolute terms at CONFIRM.

    pct_move -> price_above/price_below against the arrival mid the user saw;
    time delay -> absolute fire_at_ts. Already-absolute triggers pass through.
    """
    now = now if now is not None else time.time()
    if trigger.kind == "pct_move" and trigger.pct:
        target = arrival_mid * (1 + trigger.pct / 100.0)
        kind = "price_above" if trigger.pct > 0 else "price_below"
        return EntryTrigger(kind=kind, price=target, pct=trigger.pct)
    if trigger.kind == "price_cross" and trigger.price:
        kind = "price_above" if trigger.price >= arrival_mid else "price_below"
        return EntryTrigger(kind=kind, price=trigger.price)
    if trigger.kind == "time" and trigger.fire_at_ts is None and trigger.delay_minutes:
        return EntryTrigger(
            kind="time",
            delay_minutes=trigger.delay_minutes,
            fire_at_ts=now + trigger.delay_minutes * 60.0,
        )
    return trigger


def trigger_satisfied(trigger: Optional[EntryTrigger], *, mid: float, now: Optional[float] = None) -> bool:
    """Dumb, restart-safe runtime check against a RESOLVED trigger."""
    if trigger is None:
        return True
    now = now if now is not None else time.time()
    if trigger.kind == "price_above":
        return trigger.price is not None and mid >= trigger.price
    if trigger.kind == "price_below":
        return trigger.price is not None and 0 < mid <= trigger.price
    if trigger.kind == "time":
        return trigger.fire_at_ts is not None and now >= trigger.fire_at_ts
    # Unresolved pct_move (should not reach runtime) — never fire silently.
    return False


_HUMAN_TRIGGER = {
    "price_above": "price rises to ${price:,.4g}",
    "price_below": "price falls to ${price:,.4g}",
}


def describe_trigger(trigger: Optional[EntryTrigger]) -> str:
    if trigger is None:
        return "immediately"
    if trigger.kind in _HUMAN_TRIGGER and trigger.price is not None:
        base = _HUMAN_TRIGGER[trigger.kind].format(price=trigger.price)
        if trigger.pct:
            base += f" ({trigger.pct:+.2f}% from arrival)"
        return base
    if trigger.kind == "price_cross" and trigger.price is not None:
        return f"price reaches ${trigger.price:,.4g}"
    if trigger.kind == "pct_move" and trigger.pct is not None:
        verb = "pumps" if trigger.pct > 0 else "dumps"
        return f"price {verb} {abs(trigger.pct):g}% from arrival"
    if trigger.kind == "time":
        if trigger.delay_minutes:
            mins = trigger.delay_minutes
            if mins >= 60:
                return f"in {mins / 60:g}h"
            return f"in {mins:g}m"
        return "at scheduled time"
    return trigger.kind
