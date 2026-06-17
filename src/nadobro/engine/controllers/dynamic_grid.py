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
from decimal import Decimal
from typing import Dict, List, Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.controllers.grid_trading import build_grid_config
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.routines import variance_regime
from src.nadobro.engine.types import TradeType, _dec

logger = logging.getLogger(__name__)

# Reset re-center guards: when opt-in reset is enabled, never let it fire inside
# the grid's own band (would flatten + rebuild on normal oscillation). Floor at
# 50bp and at this multiple of the band width, whichever is larger.
_DGRID_RESET_FLOOR_BP = 50.0
_DGRID_RESET_BAND_MULT = 3.0


class DynamicGridController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="dynamic_grid", **kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair"))
        # Regime knobs (threaded from user settings by map_strategy_config).
        self.short_window = int(self.cfg("dgrid_short_window", 4) or 4)
        self.long_window = int(self.cfg("dgrid_long_window", 12) or 12)
        self.trend_on_vr = float(self.cfg("dgrid_trend_on_vr", 1.25) or 1.25)
        self.range_on_vr = float(self.cfg("dgrid_range_on_vr", 1.15) or 1.15)
        self.flip_confirm_ticks = int(self.cfg("dgrid_flip_confirm_ticks", 2) or 2)
        # Reset re-center is OPT-IN (0 = OFF). When enabled it does a reduce-only
        # close + full ladder rebuild, so it must never fire inside the grid's
        # own band: floor it well above the band width (and a hard 50bp minimum)
        # so a tiny value can't churn fees every tick.
        _reset = float(self.cfg("dgrid_reset_threshold_bp", 0.0) or 0.0)
        if _reset > 0:
            step_bp = float(_dec(self.cfg("step_pct", 0) or 0) * Decimal(10000))
            band_bp = step_bp * float(max(int(self.cfg("levels_count", 0) or 0) - 1, 1))
            _reset = max(_reset, _DGRID_RESET_FLOOR_BP, band_bp * _DGRID_RESET_BAND_MULT)
        self.reset_threshold_bp = _reset
        # Live phase + telemetry (surfaced to /status via run_engine_cycle).
        self.current_phase: str = variance_regime.GRID
        self.last_regime: Optional[str] = None  # back-compat: "TRENDING_*"/"RANGING"
        self.variance_ratio: float = 0.0
        self.realized_move_bp: float = 0.0
        self.last_direction: str = variance_regime.FLAT
        self._phase_confirm_streak: int = 0
        self._grid_anchor_mid: Optional[Decimal] = None
        self._dgrid_event: Optional[Dict[str, str]] = None

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
        sl = _dec(self.cfg("sl_pct", 0) or 0)
        if side is TradeType.SELL:
            return {
                "start_price": mid,
                "end_price": mid * (Decimal(1) + span),
                "limit_price": (mid * (Decimal(1) + sl)) if sl > 0 else Decimal(0),
            }
        # BUY (long grid)
        return {
            "start_price": mid * (Decimal(1) - span),
            "end_price": mid,
            "limit_price": (mid * (Decimal(1) - sl)) if sl > 0 else Decimal(0),
        }

    # -- regime classification -------------------------------------------
    async def _classify(self) -> str:
        """Refresh telemetry from the variance-ratio routine and return the
        desired phase (holds the current phase on insufficient history)."""
        candles = await self._candles()
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
            current_phase=self.current_phase,
        )
        self.variance_ratio = float(str(info.get("variance_ratio") or 0.0))
        self.last_direction = str(info.get("direction") or variance_regime.FLAT)
        # Back-compat telemetry string for older /status readers — reflect the
        # ACTUAL variance-ratio regime, not just drift sign. Only a VR at/above
        # the trend threshold is a trend; otherwise it is ranging (even if price
        # is drifting).
        if self.variance_ratio >= self.trend_on_vr and self.last_direction == variance_regime.DOWN:
            self.last_regime = "TRENDING_DOWN"
        elif self.variance_ratio >= self.trend_on_vr and self.last_direction == variance_regime.UP:
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

    async def _mid(self) -> Optional[Decimal]:
        try:
            return _dec(await self.adapter.mid_price(self.trading_pair))
        except Exception:  # noqa: BLE001
            return None

    async def on_tick(self) -> None:
        pair = self.trading_pair
        # dgrid's regime selector TRADES trends (ReverseGrid on a downtrend) —
        # the gate must not pre-empt it. Only breakout / expansion (price
        # accepted nowhere) makes dgrid sit out.
        await self.evaluate_quote_gate(pair, pause_on_trend=False)

        desired = await self._classify()
        mid = await self._mid()
        self._update_realized_move(mid)

        active = self.my_executors(active_only=True)
        if active:
            flip_needed = desired != self.current_phase
            if flip_needed:
                self._phase_confirm_streak += 1
                if self._phase_confirm_streak >= max(1, self.flip_confirm_ticks):
                    await self._flip_to(desired, mid, reason="flip")
                    return
            else:
                self._phase_confirm_streak = 0
                if (self.reset_threshold_bp > 0 and mid is not None
                        and self.realized_move_bp >= self.reset_threshold_bp):
                    # Same regime, but price has run away from the grid anchor:
                    # re-center the resting ladder IN PLACE (re-quote unfilled
                    # opens around the new mid) WITHOUT closing the held
                    # position — no flatten, no realized loss, no fee churn.
                    await self._recenter(mid)
                    return
            # Manage the live grid: gate / inventory cap suppress NEW entries
            # only; fills, close legs and stops keep running.
            exposure = self.exposure_allowed_sides(pair, mid) if mid else {"buy": True, "sell": True}
            for ex in active:
                worsening_allowed = (
                    exposure["buy"] if ex.__class__ is GridExecutor else exposure["sell"]
                )
                ex.suppress_new_entries = self.gate_paused or not worsening_allowed
                await self.orchestrator.tick(ex.id)
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
        # new side actually armed (a refused spawn must not claim a switch).
        if reason == "flip" and old_phase != new_phase and spawned:
            self._dgrid_event = {
                "from": old_phase,
                "to": new_phase,
                "variance_ratio": f"{self.variance_ratio:.2f}",
                "direction": self.last_direction,
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

    async def _spawn_phase(self, phase: str, mid: Optional[Decimal]) -> bool:
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
