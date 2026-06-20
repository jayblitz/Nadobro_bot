"""Grid Trading controller — thin wrapper that spawns a single GridExecutor
across the configured price range and ticks it. Long-bias.

Implemented in Phase 4.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Dict, Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.grid_executor import GridExecutor, GridExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import TradeType, TripleBarrierConfig, _dec

logger = logging.getLogger(__name__)

# Re-center guards (shared semantics with dgrid): an enabled reset must never
# fire inside the grid's own band — floor it at 50bp and at 3x the band width.
_GRID_RESET_FLOOR_BP = 50.0
_GRID_RESET_BAND_MULT = 3.0


def build_grid_config(configs: dict, side: TradeType) -> GridExecutorConfig:
    return GridExecutorConfig(
        trading_pair=str(configs["trading_pair"]),
        side=side,
        start_price=_dec(configs["start_price"]),
        end_price=_dec(configs["end_price"]),
        limit_price=_dec(configs.get("limit_price", 0)),
        total_amount_quote=_dec(configs["total_amount_quote"]),
        min_spread_between_orders=_dec(configs.get("min_spread_between_orders", "0.005")),
        max_open_orders=int(configs.get("max_open_orders", 10)),
        max_orders_per_batch=int(configs.get("max_orders_per_batch", 10)),
        activation_bounds=(_dec(configs["activation_bounds"]) if configs.get("activation_bounds") is not None else None),
        triple_barrier_config=configs.get("triple_barrier_config"),
        leverage=int(configs.get("leverage", 1)),
        keep_position=bool(configs.get("keep_position", False)),
    )


class GridController(Controller):
    SIDE = TradeType.BUY
    EXECUTOR_CLS = GridExecutor
    # When set, the gate pauses ONLY on this trend direction (the favorable
    # trend — the one this directional grid exists to trade — is allowed).
    # ``None`` keeps the conservative default: pause on either trend.
    GATE_ADVERSE_TREND: Optional[str] = None

    def __init__(self, **kwargs: object) -> None:
        super().__init__(name=kwargs.pop("name", "grid_trading"), **kwargs)  # type: ignore[arg-type]
        self._executor_id: Optional[str] = None
        self.trading_pair = str(self.cfg("trading_pair"))
        # In-place re-center ("reset and continue"): re-quote the resting ladder
        # around a fresh mid as price drifts, WITHOUT closing the position
        # (GridExecutor.recenter). Opt-in via reset_threshold_bp; floored so it
        # never fires on normal in-band oscillation.
        _reset = float(self.cfg("reset_threshold_bp", 0.0) or 0.0)
        if _reset > 0:
            step_bp = float(_dec(self.cfg("step_pct", 0) or 0) * Decimal(10000))
            band_bp = step_bp * float(max(int(self.cfg("levels_count", 0) or 0) - 1, 1))
            _reset = max(_reset, _GRID_RESET_FLOOR_BP, band_bp * _GRID_RESET_BAND_MULT)
        self.reset_threshold_bp = _reset
        self._anchor_mid: Optional[Decimal] = None
        self.realized_move_bp: float = 0.0
        self._reset_active: bool = False

    async def on_start(self) -> None:
        # Regime gate: never ARM a grid into a trending/breakout market —
        # the post-mortem failure was "reset re-armed just in time for the
        # next leg down". A paused start defers the spawn to a later tick.
        await self.evaluate_quote_gate(
            str(self.configs.get("trading_pair")), adverse_trend=self.GATE_ADVERSE_TREND
        )
        if self.gate_paused:
            return
        await self._spawn()

    async def _spawn(self) -> None:
        cfg = build_grid_config(self.configs, self.SIDE)
        ex = self.EXECUTOR_CLS(
            cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=cfg.total_amount_quote,
                                position_size_quote=cfg.total_amount_quote)
        )
        if ok:
            self._executor_id = ex.id
            try:
                self._anchor_mid = _dec(await self.adapter.mid_price(self.trading_pair))
            except Exception:  # noqa: BLE001
                self._anchor_mid = None
            self.realized_move_bp = 0.0

    def _rebuild_bounds_for_side(self, mid: Decimal) -> dict:
        """Side-correct start/end/limit from the live mid + step/levels knobs
        (same mapping as the engine config). Empty when knobs are absent."""
        if mid <= 0:
            return {}
        step = _dec(self.cfg("step_pct", 0) or 0)
        levels = int(self.cfg("levels_count", 0) or 0)
        if step <= 0 or levels < 1:
            return {}
        span = step * Decimal(max(levels - 1, 1))
        # GRID-DUAL-UNIT fix: don't rebuild a fill-blind, mid-referenced hard
        # stop from sl_pct (it caused premature wick stop-outs on top of the
        # margin-% rail). SL is the avg-entry barrier + the fee-aware session
        # rail; the rebuild only adjusts the band bounds.
        if self.SIDE is TradeType.SELL:
            return {
                "start_price": mid, "end_price": mid * (Decimal(1) + span),
                "limit_price": Decimal(0),
            }
        return {
            "start_price": mid * (Decimal(1) - span), "end_price": mid,
            "limit_price": Decimal(0),
        }

    async def _maybe_recenter(self, mid: Optional[Decimal]) -> None:
        self._reset_active = False
        if self.reset_threshold_bp <= 0 or mid is None or mid <= 0:
            return
        if self._anchor_mid and self._anchor_mid > 0:
            self.realized_move_bp = float(
                abs((mid - self._anchor_mid) / self._anchor_mid) * Decimal(10000)
            )
        if self.realized_move_bp < self.reset_threshold_bp:
            return
        overlay = self._rebuild_bounds_for_side(_dec(mid))
        if not overlay:
            return
        start = _dec(overlay["start_price"])
        end = _dec(overlay["end_price"])
        recentered = False
        for ex in self.my_executors(active_only=True):
            rc = getattr(ex, "recenter", None)
            if callable(rc):
                await rc(start, end)
                recentered = True
        if recentered:
            self._anchor_mid = _dec(mid)
            self.realized_move_bp = 0.0
            self._reset_active = True
            logger.info("grid %s recenter side=%s mid=%s band=[%s, %s]",
                        self.id, self.SIDE.name, mid, start, end)

    async def on_tick(self) -> None:
        pair = str(self.configs.get("trading_pair"))
        await self.evaluate_quote_gate(pair, adverse_trend=self.GATE_ADVERSE_TREND)
        active = self.my_executors()
        if not active and self._executor_id is None and not self.gate_paused:
            # Spawn was gate-deferred at on_start; the regime is now ranging.
            await self._spawn()
            active = self.my_executors()
        # Inventory cap: a long grid's only worsening side is its entries.
        try:
            mid = await self.adapter.mid_price(pair)
        except Exception:  # noqa: BLE001 - cap check degrades to gate-only
            mid = None
        # Reset & continue: re-center the ladder around the new mid as price
        # drifts (no position close).
        await self._maybe_recenter(_dec(mid) if mid is not None else None)
        exposure = self.exposure_allowed_sides(pair, mid) if mid else {"buy": True, "sell": True}
        entry_side_allowed = exposure["buy"] if self.SIDE is TradeType.BUY else exposure["sell"]
        for ex in active:
            # PAUSE / cap blocks NEW entry levels only; fills, close legs and
            # stops keep managing through the executor tick below.
            ex.suppress_new_entries = self.gate_paused or not entry_side_allowed
            await self.orchestrator.tick(ex.id)

    def grid_metrics(self) -> Dict[str, object]:
        """Anchor / side / drift / reset telemetry for the /status card."""
        return {
            "grid_anchor_price": float(self._anchor_mid) if self._anchor_mid else 0.0,
            "grid_reset_side": self.SIDE.name,
            "grid_drift_from_anchor_pct": self.realized_move_bp / 100.0,
            # "ON" when the re-center feature is enabled (threshold set), so the
            # /status "Soft Reset" reflects configuration, not a one-tick blip.
            "grid_reset_active": bool(self.reset_threshold_bp > 0),
            "grid_reset_threshold_bp": float(self.reset_threshold_bp),
        }
