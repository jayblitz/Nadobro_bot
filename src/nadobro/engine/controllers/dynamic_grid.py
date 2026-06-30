"""Dynamic Grid controller — switches GRID <-> RGRID by volatility regime.

Each tick it classifies the market with the tunable variance-ratio routine
(``variance_regime``, driven by the user's ``dgrid_*`` settings) and runs the
matching executor — a long :class:`GridExecutor` in ranges / uptrends, a short
:class:`ReverseGridExecutor` in downtrends.

Mid-flight flip (2026-06 fix)
=============================
Previously the controller classified the regime ONLY when no executor was
active and then returned early ("no mid-flight swap"). A market-making grid
never terminates under normal operation, so the regime was frozen at first
spawn and a long grid rode straight into a downtrend — the loss this fixes.

Now, on a CONFIRMED phase change (debounced by ``dgrid_flip_confirm_ticks``),
the controller flips **directional**: it stops the live grid — which closes the
open position with a reduce-only market order (``GridExecutor._stop_out``,
``keep_position=False``) — and spawns the opposite-side grid against a fresh
mid. A ``reset_threshold_bp`` re-centers the grid in place once price has
travelled that far from the spawn anchor, even without a phase change.

``NO_ORDERS_AUDIT-FIX-R4``: before spawning, bounds are rebuilt against the
current mid and the chosen side via ``step_pct`` / ``levels_count``.
"""
from __future__ import annotations

import inspect
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional

from src.nadobro.engine.adapter.base import Fill
from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.controllers.grid_trading import build_grid_config
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.routines import variance_regime
from src.nadobro.engine.types import OrderType, TradeType, _dec

logger = logging.getLogger(__name__)

# Re-center geometry. A dynamic grid is supposed to FOLLOW price — re-quoting
# its free/completed slots around a fresh mid as price drifts — so that the
# ladder keeps working in range instead of resting stale once price walks away.
# The executor re-center only re-prices UNFILLED maker opens (it never flattens
# or pays taker fees — held inventory keeps its close legs), so following price
# closely is cheap; the real cost is venue request load, which the per-tick
# min-interval below bounds. Default re-center trigger = ~one band width of
# drift, floored so a tiny step can't churn every tick.
_DGRID_AUTO_RESET_FLOOR_BP = 12.0
# Don't re-center more than once per this many seconds, so a fast move can
# neither hammer the venue with cancel/replace bursts nor starve fill
# processing (the re-center path returns before ticking the executor).
_DGRID_RECENTER_MIN_INTERVAL_S = 5.0


def _parse_tp_tiers(raw: object) -> List[float]:
    """Profit-booking tiers (% of margin), ascending. Accepts a list or a
    comma string; defaults to 2/4/6%. 0/empty disables booking."""
    if raw is None:
        return [2.0, 4.0, 6.0]
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        items = [p for p in str(raw).replace(" ", "").split(",") if p]
    out: List[float] = []
    for it in items:
        try:
            v = float(it)
        except (TypeError, ValueError):
            continue
        if v > 0:
            out.append(v)
    return sorted(set(out))


class DynamicGridController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="dynamic_grid", **kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair"))
        # Regime knobs (threaded from user settings by map_strategy_config).
        self.short_window = int(self.cfg("dgrid_short_window", 4) or 4)
        self.long_window = int(self.cfg("dgrid_long_window", 12) or 12)
        self.trend_on_vr = float(self.cfg("dgrid_trend_on_vr", 1.25) or 1.25)
        self.range_on_vr = float(self.cfg("dgrid_range_on_vr", 1.15) or 1.15)
        # Sustained-drift trend filter (percent over the long window). Flips the
        # grid direction on a slow one-way grind the variance ratio misses (a
        # steady decline keeps VR<1 yet bleeds a long grid). 0 disables it.
        self.trend_drift_pct = float(self.cfg("dgrid_trend_drift_pct", 0.30) or 0.30)
        self.flip_confirm_ticks = int(self.cfg("dgrid_flip_confirm_ticks", 2) or 2)
        # Trend-capture (2026-06-21): once a run has gone in profit by
        # ``trail_arm_pct`` (favorable move from the spawn anchor), a reversal of
        # ``reversal_flip_pct`` from the run's price extreme FLIPS the side — the
        # winner is flattened (reduce-only, in profit) and the opposite grid arms.
        # This is the user-requested "close in profit on reversal and switch
        # long<->short" that the slow variance classifier alone misses.
        self.trail_arm_pct = float(self.cfg("dgrid_trail_arm_pct", 1.0) or 0.0)
        self.trail_giveback_pct = float(self.cfg("dgrid_trail_giveback_pct", 0.5) or 0.0)
        self.reversal_flip_pct = float(self.cfg("dgrid_reversal_flip_pct", 0.4) or 0.0)
        self._run_extreme: Optional[Decimal] = None  # favorable price extreme since spawn
        self._run_armed: bool = False                # peak favorable move cleared arm_pct
        self._reversal_streak: int = 0
        # Tiered profit-booking: as the run's unrealized PnL climbs past rising
        # tiers (% of margin), close a fraction of the live position reduce-only
        # to lock in gains and keep PnL near-positive when the move reverses.
        self.tp_tiers_pct = _parse_tp_tiers(self.cfg("dgrid_tp_tiers_pct"))
        self.tp_fraction = min(1.0, max(0.0, float(self.cfg("dgrid_tp_fraction", 0.33) or 0.33)))
        self._booked_tiers: set[int] = set()
        # Re-center is ON by default so the grid tracks price (the whole point of
        # a *dynamic* grid). The executor re-center only re-quotes unfilled maker
        # opens — no flatten, no realized loss — so it is safe to follow closely.
        # Threshold precedence: an explicit user value wins (with a small
        # geometry floor); otherwise auto = ~one band width of drift.
        step_bp = float(_dec(self.cfg("step_pct", 0) or 0) * Decimal(10000))
        band_bp = step_bp * float(max(int(self.cfg("levels_count", 0) or 0) - 1, 1))
        _reset = float(self.cfg("dgrid_reset_threshold_bp", 0.0) or 0.0)
        if _reset > 0:
            # Honor the user's threshold but keep a small floor so it can't churn
            # inside a single step (half a band is still well within the grid).
            _reset = max(_reset, _DGRID_AUTO_RESET_FLOOR_BP, band_bp * 0.5)
        else:
            # Auto-follow default: re-center once price has drifted ~one band.
            _reset = max(_DGRID_AUTO_RESET_FLOOR_BP, band_bp)
        self.reset_threshold_bp = _reset
        self._last_recenter_ts = 0.0
        # Live phase + telemetry (surfaced to /status via run_engine_cycle).
        self.current_phase: str = variance_regime.GRID
        self.last_regime: Optional[str] = None  # back-compat: "TRENDING_*"/"RANGING"
        self.variance_ratio: float = 0.0
        self.realized_move_bp: float = 0.0
        self.last_direction: str = variance_regime.FLAT
        self._phase_confirm_streak: int = 0
        self._grid_anchor_mid: Optional[Decimal] = None
        self._dgrid_event: Optional[Dict[str, str]] = None
        # Per-tick diagnostics surfaced to the services log so a "no orders"
        # run is pinpointable (candle feed vs gate pause vs spawn refusal).
        self._last_candle_count: int = 0
        self._last_mid: Optional[Decimal] = None

    async def on_start(self) -> None:
        return None

    async def _candles(self) -> List[dict]:
        provider = self.cfg("candle_provider")
        if provider is None:
            return []
        result = provider(self.trading_pair)  # type: ignore[operator]
        if inspect.isawaitable(result):
            result = await result
        return list(result or [])

    def _rebuild_bounds_for_side(self, side: TradeType, mid: Decimal) -> dict:
        """NO_ORDERS_AUDIT-FIX-R4: derive side-correct start/end + limit from
        the live mid + the step/levels knobs. Returns a shallow override dict
        layered onto ``self.configs`` for one ``build_grid_config`` call. Falls
        back to whatever ``self.configs`` already had when step/levels absent.
        """
        if mid <= 0:
            return {}
        step = _dec(self.cfg("step_pct", 0) or 0)
        levels = int(self.cfg("levels_count", 0) or 0)
        if step <= 0 and bool(self.cfg("auto_spread", False)) and self.gate_atr_pct > 0:
            # ATR auto-step (Phase 3): level spacing tracks k x ATR so the
            # captured edge scales with realized volatility; floored so the
            # round trip clears fees, capped to stay a market-making grid.
            k = _dec(self.cfg("auto_spread_k", "1.5"))
            floor = _dec(self.cfg("spread_floor_half_pct", "0.00015")) * 2
            cap = _dec(self.cfg("spread_cap_half_pct", "0.005")) * 2
            step = max(floor, min(_dec(str(self.gate_atr_pct)) * k, cap))
        if step <= 0 or levels < 1:
            return {}
        span = step * Decimal(max(levels - 1, 1))
        # POST-ONLY-CROSS fix: offset the near-mid boundary onto the maker side
        # by max(step/2, 1.5bp) so a post-only LIMIT_MAKER never sits AT mid and
        # crosses the book (venue error_code 2008 — the exact failure that made
        # every dgrid SELL flip refuse to arm).
        maker_offset = max(step / Decimal(2), Decimal("0.00015"))
        # GRID-DUAL-UNIT fix: don't rebuild a fill-blind, mid-referenced hard
        # stop from sl_pct (premature wick stop-outs on top of the margin-%
        # rail). SL is the avg-entry barrier + the fee-aware session rail; the
        # rebuild only adjusts the band bounds.
        if side is TradeType.SELL:
            return {
                "start_price": mid * (Decimal(1) + maker_offset),
                "end_price": mid * (Decimal(1) + maker_offset + span),
                "limit_price": Decimal(0),
            }
        # BUY (long grid)
        return {
            "start_price": mid * (Decimal(1) - maker_offset - span),
            "end_price": mid * (Decimal(1) - maker_offset),
            "limit_price": Decimal(0),
        }

    # -- regime classification -------------------------------------------
    async def _classify(self) -> str:
        """Refresh telemetry from the variance-ratio routine and return the
        desired phase (holds the current phase on insufficient history)."""
        candles = await self._candles()
        self._last_candle_count = len(candles)
        if not candles:
            # #1 reason a started dgrid "does nothing": no candle feed (cold
            # cache / gateway throttle / provider never injected). Make it loud.
            logger.warning(
                "dgrid no candles for pair=%s (controller=%s) — cannot classify "
                "regime; holding phase=%s, will retry next tick",
                self.trading_pair, self.id, self.current_phase,
            )
            return self.current_phase
        info = await variance_regime.run(
            self.trading_pair, candles,
            short_window=self.short_window, long_window=self.long_window,
            trend_on=self.trend_on_vr, range_on=self.range_on_vr,
            trend_drift_pct=self.trend_drift_pct,
            current_phase=self.current_phase,
        )
        self.variance_ratio = float(str(info.get("variance_ratio") or 0.0))
        self.last_direction = str(info.get("direction") or variance_regime.FLAT)
        # Back-compat telemetry string for /status — a trend is EITHER a VR at/
        # above the threshold OR a sustained directional drift (the slow-grind
        # case the VR misses), matching the phase decision below.
        is_trend = (self.variance_ratio >= self.trend_on_vr) or bool(info.get("trend_by_drift"))
        if is_trend and self.last_direction == variance_regime.DOWN:
            self.last_regime = "TRENDING_DOWN"
        elif is_trend and self.last_direction == variance_regime.UP:
            self.last_regime = "TRENDING_UP"
        else:
            self.last_regime = "RANGING"
        if info.get("insufficient_history"):
            return self.current_phase
        return str(info.get("phase") or self.current_phase)

    def _update_realized_move(self, mid: Optional[Decimal]) -> None:
        if mid and mid > 0 and self._grid_anchor_mid and self._grid_anchor_mid > 0:
            self.realized_move_bp = float(
                abs((mid - self._grid_anchor_mid) / self._grid_anchor_mid) * Decimal(10000)
            )

    def _inventory_net_base(self) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        try:
            return _dec(self.inventory.get(self.user_id, self.trading_pair, self.id).net_amount_base)
        except Exception:  # noqa: BLE001 - inventory read failures must not crash ticks
            logger.warning("dgrid inventory read failed pair=%s (controller=%s)",
                           self.trading_pair, self.id, exc_info=True)
            return Decimal(0)

    async def _mid(self) -> Optional[Decimal]:
        try:
            return _dec(await self.adapter.mid_price(self.trading_pair))
        except Exception:  # noqa: BLE001
            return None

    # -- trend capture (trailing reversal flip) --------------------------
    def _reset_run_tracking(self, mid: Optional[Decimal]) -> None:
        """Re-seed the favorable-extreme / arm state for a fresh run (called on
        every spawn and flip so each leg trails from its own anchor)."""
        self._run_extreme = _dec(mid) if (mid and mid > 0) else None
        self._run_armed = False
        self._reversal_streak = 0

    def _is_long_phase(self) -> bool:
        return self.current_phase != variance_regime.RGRID

    def _update_run_extremes(self, mid: Optional[Decimal]) -> None:
        """Track the most-favorable price reached this run and arm the trailing
        reversal once the favorable move from the spawn anchor clears
        ``trail_arm_pct`` (so we only ever 'lock & flip' a run that went green)."""
        if mid is None or mid <= 0 or not self._grid_anchor_mid or self._grid_anchor_mid <= 0:
            return
        long = self._is_long_phase()
        if self._run_extreme is None:
            self._run_extreme = mid
        elif long:
            self._run_extreme = max(self._run_extreme, mid)
        else:
            self._run_extreme = min(self._run_extreme, mid)
        anchor = self._grid_anchor_mid
        fav = ((self._run_extreme - anchor) / anchor) if long else ((anchor - self._run_extreme) / anchor)
        if self.trail_arm_pct > 0 and float(fav) * 100.0 >= self.trail_arm_pct:
            self._run_armed = True

    async def _maybe_reversal_flip(self, mid: Optional[Decimal]) -> bool:
        """Once armed (run went in profit), a reversal of ``reversal_flip_pct``
        from the favorable extreme flips the side — debounced by
        ``flip_confirm_ticks``. ``_flip_to`` flattens the held position
        reduce-only (in profit, since price only retraced the trail) and arms the
        opposite grid. Returns True when a flip fired."""
        if (self.reversal_flip_pct <= 0 or not self._run_armed or mid is None or mid <= 0
                or not self._run_extreme or self._run_extreme <= 0):
            return False
        long = self._is_long_phase()
        ext = self._run_extreme
        retrace = ((ext - mid) / ext) if long else ((mid - ext) / ext)
        if float(retrace) * 100.0 < self.reversal_flip_pct:
            self._reversal_streak = 0
            return False
        self._reversal_streak += 1
        if self._reversal_streak < max(1, self.flip_confirm_ticks):
            return False
        target = variance_regime.GRID if self.current_phase == variance_regime.RGRID else variance_regime.RGRID
        logger.info(
            "dgrid reversal flip armed pair=%s phase=%s extreme=%s mid=%s retrace=%.2f%% "
            "(controller=%s)",
            self.trading_pair, self.current_phase, ext, mid, float(retrace) * 100.0, self.id,
        )
        await self._flip_to(target, mid, reason="reversal")
        return True

    async def on_tick(self) -> None:
        pair = self.trading_pair
        # dgrid's variance-ratio selector chooses GRID vs RGRID for EVERY
        # regime — trend, range, AND breakout/expansion — so the gate must
        # never sit it out. Keep the gate call only for ATR/telemetry; both
        # pause flags off => dgrid always quotes.
        await self.evaluate_quote_gate(pair, pause_on_trend=False, pause_on_breakout=False)

        desired = await self._classify()
        mid = await self._mid()
        self._last_mid = mid
        self._update_realized_move(mid)

        active = self.my_executors(active_only=True)
        if active:
            # Trend-capture: track the run's favorable price extreme, then flip on
            # a confirmed reversal once the run is in profit (closes the winner in
            # profit and arms the opposite side). Runs BEFORE the slow variance
            # flip so a sharp turn is caught immediately.
            self._update_run_extremes(mid)
            if await self._maybe_reversal_flip(mid):
                return
            flip_needed = desired != self.current_phase
            if flip_needed:
                self._phase_confirm_streak += 1
                if self._phase_confirm_streak >= max(1, self.flip_confirm_ticks):
                    await self._flip_to(desired, mid, reason="flip")
                    return
            else:
                self._phase_confirm_streak = 0
                now = time.time()
                if (self.reset_threshold_bp > 0 and mid is not None
                        and self.realized_move_bp >= self.reset_threshold_bp
                        and (now - self._last_recenter_ts) >= _DGRID_RECENTER_MIN_INTERVAL_S):
                    # Same regime, but price has run away from the grid anchor:
                    # re-center the resting ladder IN PLACE (re-quote unfilled
                    # opens around the new mid) WITHOUT closing the held
                    # position — no flatten, no realized loss, no fee churn.
                    # Rate-limited so a fast move can't churn cancels every tick.
                    # Do NOT return here: now that re-center fires often (default
                    # ~one band width), the executor must still be ticked this
                    # cycle so close-leg fills, the SL/TP barriers, and profit
                    # booking keep running. A slow same-regime grind would
                    # otherwise re-center every tick and never process fills.
                    self._last_recenter_ts = now
                    await self._recenter(mid)
            # Manage the live grid: gate / inventory cap suppress NEW entries
            # only; fills, close legs and stops keep running.
            exposure = self.exposure_allowed_sides(pair, mid) if mid else {"buy": True, "sell": True}
            for ex in active:
                worsening_allowed = (
                    exposure["buy"] if ex.__class__ is GridExecutor else exposure["sell"]
                )
                ex.suppress_new_entries = self.gate_paused or not worsening_allowed
                await self.orchestrator.tick(ex.id)
            # Book partial profit as the run's uPnL climbs past rising tiers.
            await self._maybe_book_profit(mid)
            return

        # No live executor.
        self._phase_confirm_streak = 0
        # Sit out: breakout / expansion (price accepted nowhere) — do NOT arm.
        if self.gate_paused:
            return
        await self._spawn_phase(desired, mid)

    # -- spawn / flip ----------------------------------------------------
    async def _flip_to(self, new_phase: str, mid: Optional[Decimal], *, reason: str) -> None:
        old_phase = self.current_phase
        # Close the live position via the executor's reduce-only flatten
        # (GridExecutor._stop_out, keep_position=False), then re-arm the side.
        for ex in self.my_executors(active_only=True):
            try:
                await self.orchestrator.stop(ex.id)
            except Exception:  # noqa: BLE001 - a failed close must still let us re-arm next tick
                logger.warning("dgrid %s: stop of executor %s failed during %s",
                               self.id, ex.id, reason, exc_info=True)
        self._phase_confirm_streak = 0
        # Breakout / expansion: the position is now closed (protective), but do
        # NOT arm a fresh grid into the regime the gate says to sit out. The
        # no-executor branch re-arms once the gate clears.
        if self.gate_paused:
            logger.info(
                "dgrid %s closed on %s (%s) but gate paused (%s) — deferring re-arm "
                "until the range returns (controller=%s)",
                reason, old_phase, self.last_direction, self.gate_reason, self.id,
            )
            return
        if mid is None:
            mid = await self._mid()
        spawned = await self._spawn_phase(new_phase, mid)
        logger.info(
            "dgrid %s %s->%s vr=%.3f dir=%s mid=%s spawned=%s (controller=%s)",
            reason, old_phase, new_phase, self.variance_ratio, self.last_direction,
            mid, spawned, self.id,
        )
        # Surfaced once per flip so the runtime can notify the user — only when a
        # new side actually armed (a refused spawn must not claim a switch). Both
        # the variance flip and the trailing reversal flip notify.
        if reason in ("flip", "reversal") and old_phase != new_phase and spawned:
            self._dgrid_event = {
                "from": old_phase,
                "to": new_phase,
                "variance_ratio": f"{self.variance_ratio:.2f}",
                "direction": self.last_direction,
                "reason": reason,
            }

    async def _recenter(self, mid: Decimal) -> None:
        """Re-quote the live grid's resting ladder around a fresh mid without
        closing the position (delegates to GridExecutor.recenter). Re-anchors
        the realized-move counter so the next re-center measures from here."""
        side = TradeType.SELL if self.current_phase == variance_regime.RGRID else TradeType.BUY
        overlay = self._rebuild_bounds_for_side(side, _dec(mid))
        if not overlay:
            # No step/levels knobs -> cannot compute a side-correct band; skip.
            return
        start = _dec(overlay.get("start_price", 0))
        end = _dec(overlay.get("end_price", 0))
        if start <= 0 or end <= 0:
            return
        recentered = False
        for ex in self.my_executors(active_only=True):
            rc = getattr(ex, "recenter", None)
            if callable(rc):
                await rc(start, end)
                recentered = True
        if recentered:
            self._grid_anchor_mid = _dec(mid)
            self.realized_move_bp = 0.0
            logger.info(
                "dgrid recenter phase=%s mid=%s band=[%s, %s] move=%.1fbp (controller=%s)",
                self.current_phase, mid, start, end, self.reset_threshold_bp, self.id,
            )

    async def _maybe_book_profit(self, mid: Optional[Decimal]) -> None:
        """Scale out reduce-only as the run's unrealized PnL crosses rising tiers
        (% of margin), locking in gains so PnL stays near-positive on a reversal.
        Each tier books once per run; the ladder resets on a fresh spawn/flip."""
        if (not self.tp_tiers_pct or self.tp_fraction <= 0 or self.inventory is None
                or mid is None or mid <= 0):
            return
        margin = _dec(self.cfg("margin_quote") or 0)
        if margin <= 0:
            return
        hold = self.inventory.get(self.user_id, self.trading_pair, self.id)
        net = hold.net_amount_base
        if abs(net) <= 0:
            return
        upnl_pct = float(hold.unrealized_pnl(mid) / margin * Decimal(100))
        # Newly crossed, not-yet-booked tiers.
        to_book = [
            i for i, t in enumerate(self.tp_tiers_pct)
            if upnl_pct >= t and i not in self._booked_tiers
        ]
        if not to_book:
            return
        frac = min(1.0, self.tp_fraction * len(to_book))
        close_base = abs(net) * _dec(frac)
        try:
            lot = self.adapter.lot_size(self.trading_pair)
            if lot and lot > 0:
                close_base = (close_base // lot) * lot
        except Exception:  # noqa: BLE001  # policy: degrade-ok(unquantized close is bumped by the venue min-notional guard)
            pass
        if close_base <= 0:
            return  # below one lot — wait for a bigger position / higher tier
        close_side = TradeType.SELL if net > 0 else TradeType.BUY
        # DGRID-BOOK-RACE fix: route the reduction THROUGH the live grid
        # executor (reduce_position) instead of firing a naked reduce-only
        # MARKET at the adapter. The executor places the order, records the fill
        # in the shared inventory, and advances its own per-level close
        # accounting — so its resting close legs and the controller's net view
        # can't drift apart. Falls back to nothing if no live executor.
        booked = Decimal(0)
        try:
            active_executors = list(self.my_executors(active_only=True))
            for ex in active_executors:
                rp = getattr(ex, "reduce_position", None)
                if callable(rp):
                    booked += await rp(close_base - booked)
                if booked >= close_base:
                    break
            if booked <= 0 and not active_executors:
                # Fallback: no live executor exposed a reduce path, but inventory
                # is held — book directly with a reduce-only MARKET so the
                # position can still scale out (preserves prior behavior).
                lev = int(self.cfg("leverage", 1) or 1)
                order = await self.adapter.place_order(
                    self.trading_pair, close_side, OrderType.MARKET, close_base, None, lev, True,
                )
                # DGRID-BOOK-RECORD fix: only the amount that actually FILLED
                # reduces the position. A naked reduce-only MARKET can come back
                # unfilled (no liquidity / venue reject); booking close_base
                # regardless would desync inventory from the venue and mark the
                # tier booked when nothing closed. Record the real fill so the
                # controller's net view stays true and the next tier sizes off
                # the remaining position.
                filled = _dec(getattr(order, "filled_base", 0) or 0)
                if filled > 0:
                    self.inventory.apply_fill(
                        self.user_id, self.trading_pair, self.id, close_side,
                        filled, _dec(getattr(order, "filled_quote", 0) or 0),
                        _dec(getattr(order, "fee_quote", 0) or 0),
                    )
                booked = filled
        except Exception:  # noqa: BLE001 - booking is best-effort; retry next tick
            logger.warning("dgrid book_profit failed pair=%s (controller=%s)",
                           self.trading_pair, self.id, exc_info=True)
            return
        if booked <= 0:
            return  # nothing reduced
        for i in to_book:
            self._booked_tiers.add(i)
        logger.info(
            "dgrid book_profit pair=%s side=%s base=%s uPnL=%.2f%% tiers=%s (controller=%s)",
            self.trading_pair, close_side.name, booked, upnl_pct,
            [self.tp_tiers_pct[i] for i in to_book], self.id,
        )

    async def _spawn_phase(self, phase: str, mid: Optional[Decimal]) -> bool:
        net = self._inventory_net_base()
        if abs(net) > Decimal("1e-12"):
            logger.warning(
                "dgrid spawn deferred phase=%s pair=%s: controller inventory still non-flat "
                "net_base=%s (controller=%s)",
                phase, self.trading_pair, net, self.id,
            )
            return False
        side, cls = (
            (TradeType.SELL, ReverseGridExecutor) if phase == variance_regime.RGRID
            else (TradeType.BUY, GridExecutor)
        )
        if mid is None or mid <= 0:
            logger.warning("dgrid %s: no mid for spawn (phase=%s) — retry next tick",
                           self.id, phase)
            return False
        overlay = self._rebuild_bounds_for_side(side, _dec(mid))
        merged = {**self.configs, **overlay} if overlay else self.configs
        cfg = build_grid_config(merged, side)
        ex = cls(cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
                 inventory=self.inventory)
        logger.info(
            "dgrid spawning %s grid pair=%s phase=%s vr=%.3f mid=%s levels=%s "
            "notional=%s start=%s end=%s (controller=%s)",
            side.name, self.trading_pair, phase, self.variance_ratio, mid,
            cfg.max_open_orders, cfg.total_amount_quote, cfg.start_price, cfg.end_price, self.id,
        )
        spawned = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=cfg.total_amount_quote,
                                position_size_quote=cfg.total_amount_quote)
        )
        if spawned:
            self.current_phase = phase
            self._grid_anchor_mid = _dec(mid)
            self.realized_move_bp = 0.0
            # Fresh position -> reset the profit-booking ladder so the new run
            # can book from its first tier again.
            self._booked_tiers = set()
            # Fresh run -> re-seed the trailing-reversal extreme/arm from here.
            self._reset_run_tracking(mid)
        else:
            reason = self.orchestrator.last_spawn_reason(self.id) or "unknown"
            logger.warning(
                "dgrid spawn_executor refused for pair=%s (controller=%s) reason=%s "
                "— no grid placed (will retry next tick)",
                self.trading_pair, self.id, reason,
            )
        return spawned

    # -- telemetry -------------------------------------------------------
    def consume_dgrid_event(self) -> Optional[Dict[str, str]]:
        """Pop the pending GRID<->RGRID flip event (None if no flip)."""
        event = self._dgrid_event
        self._dgrid_event = None
        return event

    def dgrid_metrics(self) -> Dict[str, object]:
        """Live phase + variance + anchor/side telemetry for the /status card."""
        side = "SELL" if self.current_phase == variance_regime.RGRID else "BUY"
        return {
            "dgrid_phase": self.current_phase,
            "dgrid_variance_ratio": float(self.variance_ratio),
            "dgrid_realized_move_bp": float(self.realized_move_bp),
            "dgrid_reset_threshold_bp": float(self.reset_threshold_bp),
            # Shared grid telemetry block (Anchor / Side / Drift on the card).
            "grid_anchor_price": float(self._grid_anchor_mid) if self._grid_anchor_mid else 0.0,
            "grid_reset_side": side,
            "grid_drift_from_anchor_pct": float(self.realized_move_bp) / 100.0,
            "grid_reset_active": bool(self.reset_threshold_bp > 0),
        }
