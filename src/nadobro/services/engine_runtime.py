"""Engine v2 runtime connector (production hardening A).

Wires the engine into a runnable whole: constructs the ExecutorOrchestrator
(with a real RiskEngine + DB-backed kill switch), the NadoAdapter, the
DB-backed inventory, and the strategy Controllers — then drives them and
persists executor lifecycle rows.

This is the integration point bot_runtime's async loop calls (via
``services/strategy_runtime``): start a controller for a running strategy,
tick it each cycle, persist state. The engine library stays DB/venue-agnostic;
all the real wiring lives here.
"""
from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.controllers.controller_base import Controller, ControllerState
from src.nadobro.engine.controllers.copy_trading import CopyController
from src.nadobro.engine.controllers.delta_neutral import DeltaNeutralController
from src.nadobro.engine.controllers.desk import DeskController
from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.risk import RiskEngine
from src.nadobro.engine.types import RiskLimits, RiskState, TradeType, TripleBarrierConfig, _dec

logger = logging.getLogger(__name__)

# Strategy id (bot_runtime's keys) -> engine controller class.
CONTROLLER_REGISTRY: Dict[str, type] = {
    "grid": GridController,
    # rgrid = directional recycling ladder: long ladder in uptrends, short ladder
    # in downtrends, multi-level, booking profit per level (user spec). That IS
    # the DynamicGridController (same engine as D-Grid). The classic one-sided
    # ReverseGridController is no longer the default; fill_anchored=1 opts into
    # the trend-following taker momentum instead.
    "rgrid": DynamicGridController,
    "dgrid": DynamicGridController,
    "mid": MarketMakingController,
    "dn": DeltaNeutralController,
    "vol": VolumeBotController,
    "copy": CopyController,
    # Desk text-to-trade plans. NOT in ENGINE_MAPPED_STRATEGIES — desk
    # sessions are driven by services/desk_runtime's scheduler job, not by
    # bot_runtime strategy cycles.
    "desk": DeskController,
}


# --------------------------------------------------------------------------
# construction
# --------------------------------------------------------------------------
def build_adapter(
    client: object, products: Dict[str, object], on_place: Optional[Any] = None
) -> NadoAdapterBase:
    """Construct the live Nado adapter from a NadoClient + product-metadata map.
    ``products`` maps trading_pair -> ProductMeta (see adapter/nado.py).
    ``on_place`` (optional) is called with each placed digest for session linking."""
    from src.nadobro.engine.adapter.nado import NadoAdapter

    return NadoAdapter(client, products, on_place=on_place)  # type: ignore[arg-type]


def build_risk_engine(limits: Optional[RiskLimits] = None) -> RiskEngine:
    from src.nadobro.services.engine_persistence import DbKillSwitchStore

    return RiskEngine(limits or RiskLimits(), kill_switch=DbKillSwitchStore())


def build_orchestrator(
    *,
    limits: Optional[RiskLimits] = None,
    risk_state_provider: Optional[Any] = None,
    trade_recorder: Optional[object] = None,
) -> ExecutorOrchestrator:
    return ExecutorOrchestrator(
        risk_engine=build_risk_engine(limits),
        risk_state_provider=risk_state_provider or (lambda _cid: RiskState()),
        trade_recorder=trade_recorder,
    )


def deterministic_controller_id(strategy: str, user_id: int, network: str) -> str:
    """Stable, cross-process controller id. BUG-ER-2 fix: with this, a second
    worker that tries to start the same strategy hits the engine_executors
    table's existing rows and we can detect the duplicate via
    ``_remote_active(...)``.
    """
    return f"{strategy}:{int(user_id)}:{str(network)}"


def build_controller(
    strategy: str,
    *,
    user_id: int,
    configs: Dict[str, object],
    orchestrator: ExecutorOrchestrator,
    adapter: NadoAdapterBase,
    inventory: object,
    limits: Optional[RiskLimits] = None,
    controller_id: Optional[str] = None,
) -> Controller:
    cls = CONTROLLER_REGISTRY.get(strategy)
    # Phase 4 opt-in: TreadFi-style fill-anchored quoting replaces the
    # classic ladder for grid/rgrid when the user enables ``fill_anchored``.
    if configs.get("controller_override") == "fill_anchored":
        cls = FillAnchoredQuotingController
    if cls is None:
        raise ValueError(f"no engine controller for strategy '{strategy}'")
    return cls(
        user_id=user_id, orchestrator=orchestrator, adapter=adapter, inventory=inventory,
        configs=configs, limits=limits, controller_id=controller_id,
    )


# --------------------------------------------------------------------------
# runtime manager
# --------------------------------------------------------------------------
class EngineRuntime:
    """Owns live controllers keyed by (user_id, network, strategy). Driven from
    bot_runtime's async loop: ``start`` once, ``tick`` each cycle, ``stop`` on
    teardown. Persists executor lifecycle rows after each tick."""

    def __init__(
        self,
        *,
        executor_store: Optional[object] = None,
        trade_recorder: Optional[object] = None,
    ) -> None:
        self._controllers: Dict[tuple, Controller] = {}
        self._orchestrators: Dict[tuple, ExecutorOrchestrator] = {}
        self._executor_store = executor_store
        self._trade_recorder = trade_recorder

    def _key(self, user_id: int, network: str, strategy: str) -> tuple:
        return (user_id, network, strategy)

    def is_running(self, user_id: int, network: str, strategy: str) -> bool:
        c = self._controllers.get(self._key(user_id, network, strategy))
        if c is not None and c.is_active:
            return True
        # BUG-ER-2 fix: cross-process visibility. Another worker process may
        # have started this strategy; check engine_executors under the
        # deterministic controller id. SCOPED to the CURRENT run's session id
        # (NO-ORDERS fix): controller_id is stable across runs, so a prior run's
        # non-terminated row would otherwise make this return True forever and
        # the build/tick gate would skip building — strategy never places an
        # order. Scoping to the active session means only THIS run's executors
        # count, and stale rows from dead runs are ignored.
        from src.nadobro.services.engine_persistence import resolve_running_session_id
        session_id = resolve_running_session_id(strategy, user_id, network)
        return _remote_active(strategy, user_id, network, session_id)

    def has_local_active(self, user_id: int, network: str, strategy: str) -> bool:
        """True only when THIS process holds a live (active) controller for the
        strategy — the precondition for ``tick`` to do anything. Distinct from
        ``is_running`` (which also trusts cross-process engine_executors rows)."""
        c = self._controllers.get(self._key(user_id, network, strategy))
        return c is not None and c.is_active

    def needs_recovery(self, user_id: int, network: str, strategy: str) -> bool:
        """BUG-TICK-1 recovery: True when a locally-registered controller has
        entered the terminal FAILED state. Such a controller will never tick
        again (``is_active`` is permanently False), so the engine cycle must
        tear it down and rebuild a fresh one instead of silently no-op'ing
        forever — the exact silent-stall this audit chased."""
        c = self._controllers.get(self._key(user_id, network, strategy))
        return c is not None and c.state is ControllerState.FAILED

    async def start(
        self,
        user_id: int,
        network: str,
        strategy: str,
        configs: Dict[str, object],
        adapter: NadoAdapterBase,
        inventory: object,
        *,
        limits: Optional[RiskLimits] = None,
        risk_state_provider: Optional[Any] = None,
    ) -> Controller:
        # BUG-ER-1 fix: if a previous instance is still registered (e.g. the
        # controller failed mid-tick or the user restarted the strategy
        # without an explicit stop), tear it down BEFORE replacing it with a
        # fresh orchestrator/controller. Otherwise the old orchestrator's
        # active executors keep ticking against the venue with no owner.
        key = self._key(user_id, network, strategy)
        if key in self._controllers or key in self._orchestrators:
            try:
                await self.stop(user_id, network, strategy)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "stale engine runtime cleanup failed for %s; replacing anyway",
                    key, exc_info=True,
                )

        # Fresh run = clean inventory. controller_id is stable across runs, so a
        # new session must NOT inherit the prior run's engine_position_hold (it
        # would skew exposure caps / sizing). PnL is already session-scoped via
        # trades_<network>; this keeps the engine's own position view per-run.
        cid = deterministic_controller_id(strategy, user_id, network)
        clear_inv = getattr(inventory, "clear_for_controller", None)
        if callable(clear_inv):
            try:
                clear_inv(cid)
            except Exception:  # noqa: BLE001 - best-effort; never block start
                logger.debug("inventory clear_for_controller failed for %s", cid, exc_info=True)

        orch = build_orchestrator(
            limits=limits,
            risk_state_provider=risk_state_provider,
            trade_recorder=self._trade_recorder,
        )
        controller = build_controller(
            strategy, user_id=user_id, configs=configs, orchestrator=orch,
            adapter=adapter, inventory=inventory, limits=limits,
            controller_id=deterministic_controller_id(strategy, user_id, network),
        )
        self._orchestrators[key] = orch
        self._controllers[key] = controller
        try:
            spawned = await orch.spawn_controller(controller)
        except Exception:
            # If spawn_controller raised, roll back our bookkeeping so a
            # retry can start clean rather than crashing on the duplicate
            # registration check above.
            self._orchestrators.pop(key, None)
            self._controllers.pop(key, None)
            raise
        if not spawned:
            # Controller refused to start (kill switch / risk gate). Roll
            # back so the dispatch result clearly says "not running".
            self._orchestrators.pop(key, None)
            self._controllers.pop(key, None)
        return controller

    async def tick(self, user_id: int, network: str, strategy: str) -> None:
        key = self._key(user_id, network, strategy)
        controller = self._controllers.get(key)
        orch = self._orchestrators.get(key)
        if controller is None or orch is None:
            return
        await orch.tick_controller(controller.id)
        self._persist_executors(orch)

    async def stop(self, user_id: int, network: str, strategy: str) -> None:
        key = self._key(user_id, network, strategy)
        orch = self._orchestrators.get(key)
        controller = self._controllers.get(key)
        cid = deterministic_controller_id(strategy, user_id, network)
        if orch is not None and controller is not None:
            await orch.stop_controller(controller.id)
            self._persist_executors(orch)
        else:
            # Cross-process stop: this process doesn't own the orchestrator, so
            # mark the controller's non-terminated engine_executors rows
            # TERMINATED in the DB. Without this a stop handled outside the owner
            # process leaves stale ACTIVE rows that _remote_active would treat as
            # "still running" — blocking the next run from ever building.
            try:
                from src.nadobro.services.engine_persistence import terminate_engine_executors
                terminate_engine_executors(cid)
            except Exception:  # noqa: BLE001
                logger.debug("cross-process executor terminate sweep failed for %s", cid, exc_info=True)
        self._controllers.pop(key, None)
        self._orchestrators.pop(key, None)
        # Clear the live-progress row so a stopped strategy doesn't leave stale
        # cycles/funding behind for the next start. Best-effort.
        try:
            from src.nadobro.services.engine_persistence import clear_controller_progress

            clear_controller_progress(deterministic_controller_id(strategy, user_id, network))
        except Exception:  # noqa: BLE001
            logger.debug("clear dn progress failed", exc_info=True)

    def _persist_executors(self, orch: ExecutorOrchestrator) -> None:
        if self._executor_store is None:
            return
        for ex in orch.list():
            try:
                self._executor_store.save(ex)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - persistence must not break a tick
                logger.warning("executor persistence failed for %s", ex.id, exc_info=True)


def _should_build_controller(
    *, needs_recovery: bool, has_local_active: bool, worker_mode: bool, is_running: bool
) -> bool:
    """Decide whether THIS process must (re)build the controller before ticking.

    - A locally-FAILED controller always rebuilds (recovery).
    - With a live LOCAL controller, never rebuild (just tick).
    - With NO local controller: the cycle-running worker ADOPTS (builds) so a
      crashed/recycled worker doesn't no-op forever against a stale remote row;
      a non-worker (e.g. the scheduler's local fallback) only builds when no
      live owner exists (``not is_running``), so it never double-builds.
    """
    if needs_recovery:
        return True
    if has_local_active:
        return False
    return worker_mode or not is_running


def _fingerprint_value(value: object) -> object:
    """Comparable representation for config/risk objects.

    Decimal/dataclass/Enum instances do not always compare predictably once
    nested inside generic dicts, and callables such as candle providers must not
    participate in live-config equality.
    """
    if callable(value):
        return "<callable>"
    if isinstance(value, Decimal):
        return ("Decimal", str(value))
    if isinstance(value, Enum):
        return ("Enum", value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__name__,
            tuple(
                (field.name, _fingerprint_value(getattr(value, field.name)))
                for field in fields(value)
            ),
        )
    if isinstance(value, dict):
        return tuple(
            (str(k), _fingerprint_value(v))
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
            if not callable(v)
        )
    if isinstance(value, (list, tuple)):
        return tuple(_fingerprint_value(v) for v in value)
    return value


def _live_config_signature(
    configs: Dict[str, object], limits: Optional[RiskLimits] = None
) -> tuple:
    """Stable signature for parameters that should change a live controller.

    Excludes mid-derived price anchors so ordinary market movement does not
    churn a controller. Includes risk limits because leverage/notional changes
    must also update the runtime risk engine before the next order is spawned.
    """
    cfg_sig = tuple(
        (str(k), _fingerprint_value(v))
        for k, v in sorted(configs.items(), key=lambda item: str(item[0]))
        if k not in _LIVE_CONFIG_SIGNATURE_EXCLUDE and not callable(v)
    )
    return (cfg_sig, _fingerprint_value(limits) if limits is not None else None)


def _grid_bounds_for_side(configs: Dict[str, object], side: TradeType, mid: object) -> Dict[str, object]:
    """Rebuild side-correct grid bounds from stable step/level knobs.

    This mirrors the mapping/recenter math while keeping the helper local to the
    runtime reconfig path, where the live executor side is known.
    """
    mid_dec = _dec(mid)
    if mid_dec <= 0:
        return {}
    step = _dec(configs.get("step_pct") or configs.get("min_spread_between_orders") or 0)
    levels = int(configs.get("levels_count") or configs.get("max_open_orders") or 0)
    if step <= 0 or levels < 1:
        return {}
    span = step * Decimal(max(levels - 1, 1))
    maker_offset = max(step / Decimal(2), Decimal("0.00015"))
    if side is TradeType.SELL:
        return {
            "start_price": mid_dec * (Decimal(1) + maker_offset),
            "end_price": mid_dec * (Decimal(1) + maker_offset + span),
            "limit_price": Decimal(0),
        }
    return {
        "start_price": mid_dec * (Decimal(1) - maker_offset - span),
        "end_price": mid_dec * (Decimal(1) - maker_offset),
        "limit_price": Decimal(0),
    }


def _apply_mid_controller_config(controller: Controller, configs: Dict[str, object]) -> None:
    """Refresh MarketMakingController attrs read only at __init__."""
    controller.trading_pair = str(configs["trading_pair"])  # type: ignore[attr-defined]
    controller.spread_bid_pct = max(  # type: ignore[attr-defined]
        _dec(configs.get("spread_bid_pct", "0.001")),
        _dec(configs.get("spread_floor_half_pct", "0.00015")),
    )
    controller.spread_ask_pct = max(  # type: ignore[attr-defined]
        _dec(configs.get("spread_ask_pct", "0.001")),
        _dec(configs.get("spread_floor_half_pct", "0.00015")),
    )
    controller.order_amount_quote = _dec(configs.get("order_amount_quote", "10"))  # type: ignore[attr-defined]
    controller.price_distance_tolerance = _dec(  # type: ignore[attr-defined]
        configs.get("price_distance_tolerance", "0.0005")
    )
    max_base = configs.get("max_base_quote")
    min_base = configs.get("min_base_quote")
    controller.max_base_quote = _dec(max_base) if max_base is not None else None  # type: ignore[attr-defined]
    controller.min_base_quote = _dec(min_base) if min_base is not None else None  # type: ignore[attr-defined]
    controller.profit_protection = bool(configs.get("profit_protection", False))  # type: ignore[attr-defined]
    controller.auto_spread = bool(configs.get("auto_spread", False))  # type: ignore[attr-defined]
    controller.auto_spread_k = _dec(configs.get("auto_spread_k", "1.5"))  # type: ignore[attr-defined]
    controller.spread_floor_half_pct = _dec(configs.get("spread_floor_half_pct", "0.00015"))  # type: ignore[attr-defined]
    controller.spread_cap_half_pct = _dec(configs.get("spread_cap_half_pct", "0.005"))  # type: ignore[attr-defined]
    # Live edits to Mid Mode directional bias take effect on the next tick.
    from src.nadobro.engine.controllers.market_making import _safe_bias
    controller.directional_bias = _safe_bias(configs.get("directional_bias", "0"))  # type: ignore[attr-defined]


def _apply_orchestrator_risk_limits(orch: ExecutorOrchestrator, limits: RiskLimits) -> None:
    """Refresh the live RiskEngine limits on the orchestrator.

    ExecutorOrchestrator exposes the engine as ``risk`` in production. Keep the
    legacy/test ``risk_engine`` spelling as a fallback so lightweight fakes still
    exercise the same path.
    """
    for attr in ("risk", "risk_engine"):
        risk_engine = getattr(orch, attr, None)
        if risk_engine is not None:
            try:
                risk_engine.limits = limits
            except Exception:  # noqa: BLE001 - bad fakes must not break a tick
                logger.debug("risk limit refresh failed via orchestrator.%s", attr, exc_info=True)


async def _reset_mm_quotes(controller: Controller, orch: ExecutorOrchestrator) -> None:
    """Forget/stop current MM-style quotes so the next tick uses new sizing."""
    for attr_id, attr_price in (("_bid_id", "_bid_price"), ("_ask_id", "_ask_price")):
        ex_id = getattr(controller, attr_id, None)
        if ex_id is not None:
            await orch.stop(ex_id)
            setattr(controller, attr_id, None)
            setattr(controller, attr_price, None)


def _apply_fill_anchored_controller_config(controller: Controller, configs: Dict[str, object]) -> None:
    """Refresh FillAnchoredQuotingController attrs read only at __init__."""
    _apply_mid_controller_config(controller, configs)
    controller.mode = str(configs.get("anchor_mode", getattr(controller, "mode", "grid"))).lower()  # type: ignore[attr-defined]
    controller.reset_threshold_pct = _dec(  # type: ignore[attr-defined]
        configs.get(
            "reset_threshold_pct",
            "0.0025" if getattr(controller, "mode", "grid") == "grid" else "0.00125",
        )
    )
    controller.momentum = bool(configs.get("momentum", False))  # type: ignore[attr-defined]
    controller.vwap_volume_fraction = _dec(configs.get("vwap_volume_fraction", "0") or "0")  # type: ignore[attr-defined]
    controller.concession_enabled = bool(configs.get("concession_enabled", False))  # type: ignore[attr-defined]
    controller.concession_escalation_ticks = max(1, int(configs.get("concession_escalation_ticks", 5) or 5))  # type: ignore[attr-defined]
    _cfrac = _dec(configs.get("concession_fraction", "0.5") or "0.5")  # type: ignore[attr-defined]
    controller.concession_fraction = max(Decimal("0.05"), min(_cfrac, Decimal(1)))  # type: ignore[attr-defined]


async def _apply_grid_live_config(
    strategy: str,
    controller: Controller,
    orch: ExecutorOrchestrator,
    configs: Dict[str, object],
    mid: object,
) -> None:
    """Apply sizing/spread/risk edits to active grid executors in place.

    Recenter cancels only free/open entry orders and preserves held inventory +
    reduce-only close legs, avoiding the flattening side effect of a full
    controller stop.
    """
    from src.nadobro.engine.controllers.grid_trading import build_grid_config

    controller.trading_pair = str(configs.get("trading_pair") or getattr(controller, "trading_pair", ""))  # type: ignore[attr-defined]
    for ex in orch.list(controller.id, active_only=True):
        recenter = getattr(ex, "recenter", None)
        ex_config = getattr(ex, "config", None)
        side = getattr(ex, "open_side", None)
        if not callable(recenter) or ex_config is None or not isinstance(side, TradeType):
            continue
        if str(getattr(ex_config, "trading_pair", "")) != str(configs.get("trading_pair")):
            logger.warning(
                "live %s reconfig skipped executor %s: trading_pair changed %s -> %s",
                strategy, getattr(ex, "id", "<unknown>"),
                getattr(ex_config, "trading_pair", None), configs.get("trading_pair"),
            )
            continue
        overlay = _grid_bounds_for_side(configs, side, mid)
        next_cfg = build_grid_config({**configs, **overlay}, side)
        for attr in (
            "start_price",
            "end_price",
            "limit_price",
            "total_amount_quote",
            "min_spread_between_orders",
            "max_open_orders",
            "max_orders_per_batch",
            "activation_bounds",
            "triple_barrier_config",
            "leverage",
            "keep_position",
        ):
            setattr(ex_config, attr, getattr(next_cfg, attr))
        await recenter(next_cfg.start_price, next_cfg.end_price)


async def _apply_live_controller_update(
    strategy: str,
    controller: Controller,
    orch: ExecutorOrchestrator,
    configs: Dict[str, object],
    limits: RiskLimits,
    mid: object,
) -> None:
    """Apply live UI settings to a locally owned controller before ticking."""
    controller.configs = dict(configs)
    controller.limits = limits
    _apply_orchestrator_risk_limits(orch, limits)

    is_fill_anchored = (
        configs.get("controller_override") == "fill_anchored"
        or isinstance(controller, FillAnchoredQuotingController)
    )
    if is_fill_anchored:
        _apply_fill_anchored_controller_config(controller, configs)
        await _reset_mm_quotes(controller, orch)
        return

    if strategy == "mid":
        _apply_mid_controller_config(controller, configs)
        await _reset_mm_quotes(controller, orch)
        return

    if strategy in ("grid", "rgrid", "dgrid"):
        await _apply_grid_live_config(strategy, controller, orch, configs, mid)


def _remote_active(
    strategy: str, user_id: int, network: str, session_id: Optional[int] = None
) -> bool:
    """Check engine_executors for non-terminated rows under the deterministic
    controller id, optionally SCOPED to a run's ``session_id``. Used by
    ``EngineRuntime.is_running`` to detect strategies started by *another worker
    process* (BUG-ER-2) for the CURRENT run only — a stale row from a prior run
    (same stable controller id) must not be treated as "already running".
    Defensive: returns False on any DB failure so a transient error does not
    block strategy startup entirely.
    """
    try:
        from src.nadobro.db import query_count
    except Exception:  # noqa: BLE001
        return False
    cid = deterministic_controller_id(strategy, user_id, network)
    try:
        if session_id is not None:
            return bool(query_count(
                "SELECT 1 FROM engine_executors "
                "WHERE controller_id = %s AND state <> 'TERMINATED' "
                "AND strategy_session_id = %s",
                (cid, int(session_id)),
            ))
        return bool(query_count(
            "SELECT 1 FROM engine_executors "
            "WHERE controller_id = %s AND state <> 'TERMINATED'",
            (cid,),
        ))
    except Exception:  # noqa: BLE001
        return False


def _default_runtime() -> EngineRuntime:
    from src.nadobro.services.engine_persistence import (
        DbExecutorStore,
        DbTradeRecorder,
    )

    return EngineRuntime(
        executor_store=DbExecutorStore(),
        trade_recorder=DbTradeRecorder(),
    )


# Process-wide runtime. MUST be driven from bot_runtime's single async event
# loop (its async ``_run_cycle``), NOT the sync ``_dispatch_strategy``: the
# orchestrator's asyncio primitives are loop-bound.
#
# Integration contract (the remaining bot_runtime hookup — validated on
# testnet):
#   on strategy start -> await RUNTIME.start(user_id, network, strategy,
#                            configs, build_adapter(client, product_meta),
#                            DbInventoryRepository(), limits=...)
#   each async cycle  -> await RUNTIME.tick(user_id, network, strategy)
#   on strategy stop  -> await RUNTIME.stop(user_id, network, strategy)
# where ``configs`` maps the user's saved strategy settings to the controller's
# config keys and ``product_meta`` comes from the live product catalog.
RUNTIME = _default_runtime()


# --------------------------------------------------------------------------
# bot_runtime hookup (feature-gated): settings -> controller config mapping,
# product metadata, and the per-cycle driver.
# --------------------------------------------------------------------------
# Strategies the engine can drive today. NO_ORDERS_AUDIT-FIX-R1: ``dn`` is now
# included. Previously it was excluded "for a follow-up" but ``dn`` was ALSO
# in ``strategy_runtime.LEGACY_STRATEGY_KEYS`` which silently no-op'd every
# DN cycle. The DeltaNeutralController has been live since Phase 4, so wire
# it up here and emit its config keys in ``map_strategy_config``.
ENGINE_MAPPED_STRATEGIES = ("grid", "rgrid", "dgrid", "mid", "vol", "dn")

# How many of THIS session's most-recent recorded fills to seed the fill-anchored
# exposure VWAP from on (re)build (matches FillAnchoredQuotingController history).
_FILL_HISTORY_SEED = 200

# These controllers derive live order sizing from config values that can be
# edited from the UI while the bot remains LIVE. A price move alone changes
# start/end anchors every cycle, so those mid-derived keys are deliberately
# excluded from the signature below.
_LIVE_RECONFIGURABLE_STRATEGIES = ("grid", "rgrid", "dgrid", "mid")
_LIVE_CONFIG_SIGNATURE_EXCLUDE = frozenset({
    "start_price",
    "end_price",
    "limit_price",
    "candle_provider",
    # DN-only restore fields; excluded defensively if a future strategy shares
    # this helper.
    "restore_cycles_completed",
    "restore_funding_usd",
})


def engine_v2_enabled() -> bool:
    """Master switch for routing live strategy execution through the engine.

    BUG-SR-1 / BR-1 fix: default is now ON. The legacy ``run_cycle`` strategy
    dispatch was removed during the engine-v2 cutover, so leaving this OFF
    causes the bot to *silently no-op* every cycle (cycles report "success"
    yet place no orders). Operators that need to roll back to the legacy
    path no longer have one — set ``NADO_ENGINE_V2_RUNTIME=false`` only as
    an emergency kill switch (the bot will accept it but log a critical
    warning so it's never a silent footgun).
    """
    raw = os.environ.get("NADO_ENGINE_V2_RUNTIME", "").strip().lower()
    if raw == "":
        return True
    if raw in ("0", "false", "no", "off"):
        logger.critical(
            "engine v2 runtime explicitly DISABLED via NADO_ENGINE_V2_RUNTIME=%s "
            "— engine-mapped strategies will silently no-op until this is unset",
            raw,
        )
        return False
    return raw in ("1", "true", "yes", "on")


def _f(settings: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(settings.get(key, default))
    except (TypeError, ValueError):
        return default


def _quote_defense_defaults(settings, notional, *, auto_spread: bool) -> dict:
    """Regime gate / inventory cap / ATR-spread knobs for grid family + MM.

    Defaults per the 2026-06 grid post-mortem plan: gate ON (pause new quotes
    in trends/breakouts, notify once per flip), net-exposure cap at 30% of
    allocated margin with resume at 70% of the cap, ATR(14, 1m) x 1.5 spread
    with a fee floor (1.5 bp/side) when the user didn't pin a spread.
    """
    # Per-side spread bounds from the user's min/max spread (bps → fraction).
    # Previously hardcoded (1.5 bp floor / 50 bp cap) so min_spread_bp /
    # max_spread_bp were dead inputs; now they drive the manual floor AND the
    # ATR auto-spread clamp. Defaults match the old constants, so an unset config
    # behaves exactly as before. floor clamped ≥0 (mid may concede to 0) and the
    # cap is held ≥ floor so a stray min>max can't invert the band.
    floor_half = Decimal(str(max(0.0, _f(settings, "min_spread_bp", 1.5)))) / Decimal(10000)
    cap_half = Decimal(str(_f(settings, "max_spread_bp", 50.0))) / Decimal(10000)
    if cap_half < floor_half:
        cap_half = floor_half
    return {
        "regime_gate_enabled": bool(_f(settings, "regime_gate_enabled", 1.0)),
        "max_net_exposure_pct": _f(settings, "max_net_exposure_pct", 30.0),
        "exposure_resume_frac": 0.7,
        "margin_quote": Decimal(str(notional)),
        "auto_spread": auto_spread,
        "auto_spread_k": Decimal(str(_f(settings, "auto_spread_k", 1.5))),
        "spread_floor_half_pct": floor_half,
        "spread_cap_half_pct": cap_half,
        "candle_provider": None,  # injected in run_engine_cycle (client there)
    }


def _effective_leverage(settings: Dict[str, Any], fallback: float = 1.0) -> float:
    """Resolve the effective leverage that turns the user's *margin* (collateral)
    into deployed position notional for the grid/MM family.

    Precedence: an explicit ``mm_leverage_override`` (set by the Tiny Budget
    preset / leverage selector) wins; otherwise the session ``leverage``; else
    ``fallback``. Floored at 1x. The venue cap is NOT applied here — the start
    guard (``_run_mm_start_guard``) already rejects leverage above the pair max
    and the adapter re-validates at placement, so this stays a pure function of
    settings (no catalog/network dependency in the hot mapping path).
    """
    raw = _f(settings, "mm_leverage_override", 0.0)
    if raw <= 0:
        raw = _f(settings, "leverage", 0.0)
    if raw <= 0:
        raw = float(fallback or 1.0)
    return max(1.0, raw)


def map_strategy_config(
    strategy: str, settings: Dict[str, Any], mid: Decimal, *, product: str,
    leverage: int = 1, network: str = "mainnet",
) -> Dict[str, object]:
    """Derive an engine controller config from a user's saved strategy settings
    + current mid. Documented, testnet-tunable mappings (not 1:1 with legacy).
    """
    mid = _dec(mid)
    # ``notional`` here is the user's allocated MARGIN (collateral). The grid/MM
    # family deploys ``margin x effective_leverage`` of position notional, so a
    # $100 margin at 5x quotes ~$500 across the ladder (and unlocks more levels).
    # DN/Volume size their own legs and do not use ``deployed`` below.
    notional = _f(settings, "cycle_notional_usd", _f(settings, "notional_usd", 100.0))
    eff_lev = _effective_leverage(settings, float(leverage or 1.0))
    deployed = notional * eff_lev
    # Participation chunk (Phase 1b): when a participation preset is active,
    # bot_runtime resolves the per-cycle order notional once at start (chunk =
    # preset multiplier × 24h volume, floored at min-notional, capped at
    # deployed). Use it as the per-order size in place of the deployed-budget
    # sizing. 0/unset → keep the deployed-based size (opt-out, unchanged).
    _mm_chunk = _f(settings, "mm_cycle_notional_usd", 0.0)
    _chunk_dec = Decimal(str(_mm_chunk)) if _mm_chunk > 0 else None
    # The quoting spread is USER-SET, not one hardcoded value for everyone. Read
    # the strategy's own spread field (the frontend writes rgrid_spread_bp /
    # dgrid_spread_bp), falling back to the generic spread_bp. Previously this
    # only read spread_bp, so rgrid/dgrid users' spread settings were ignored
    # and the grid quoted at the default step regardless.
    if strategy == "rgrid":
        _spread_bp = _f(settings, "rgrid_spread_bp", _f(settings, "spread_bp", 10.0))
    elif strategy == "dgrid":
        _spread_bp = _f(settings, "dgrid_spread_bp", _f(settings, "spread_bp", 8.0))
    else:
        _spread_bp = _f(settings, "spread_bp", 5.0)
    spread_frac = Decimal(str(_spread_bp)) / Decimal(10000)
    levels = max(1, int(_f(settings, "levels", 2)))
    # SL/TP honor the per-strategy fields (rgrid/dgrid store them under
    # rgrid_stop_loss_pct / rgrid_take_profit_pct), so a user's custom value
    # drives the barrier instead of the sl_pct/tp_pct default.
    from src.nadobro.services.strategy_registry import effective_sl_tp_pct
    _sl_pct, _tp_pct = effective_sl_tp_pct(strategy, settings)
    if _tp_pct <= 0:
        _tp_pct = _f(settings, "tp_pct", 0.6)
    if _sl_pct <= 0:
        _sl_pct = _f(settings, "sl_pct", 0.5)
    tp = Decimal(str(_tp_pct)) / Decimal(100)
    sl = Decimal(str(_sl_pct)) / Decimal(100)

    if strategy == "mid":
        _mid_bias = max(-1.0, min(_f(settings, "directional_bias", 0.0), 1.0))
        # Directional bias intentionally builds one-sided inventory, which would
        # otherwise be choked by the symmetric net-exposure cap. Per the docs,
        # extreme bias (±1) is allowed up to 20% more directional exposure (and
        # therefore needs ~20% more margin headroom); scale the cap linearly with
        # |bias| so the lean is effective but still bounded.
        _bias_exposure_mult = 1.0 + 0.20 * abs(_mid_bias)
        return {
            "trading_pair": product,
            "spread_bid_pct": spread_frac,
            "spread_ask_pct": spread_frac,
            # Mid Mode directional bias in [-1, +1]: the controller skews the
            # per-side spreads (documented ±0.2 alpha-tilt) to lean the book
            # long/short. _f coerces the legacy text default ("neutral") to 0.0.
            "directional_bias": _mid_bias,
            # Mid is a single bid + single ask (NOT a ladder), so the full
            # deployed notional goes into each side — ``levels`` does not subdivide
            # the quote here (it would just silently shrink the size). With a
            # participation preset, the per-cycle chunk replaces the full size.
            "order_amount_quote": _chunk_dec or Decimal(str(deployed)),
            "max_base_quote": Decimal(str(_f(settings, "inventory_soft_limit_usd", deployed))),
            "price_distance_tolerance": (spread_frac / Decimal(2)) or Decimal("0.0005"),
            "leverage": int(eff_lev),
            # Regime gate + inventory cap + ATR auto-spread (2026-06 upgrade).
            # auto_spread engages when the user left spread unset/zero. margin_quote
            # tracks the DEPLOYED size so the net-exposure cap scales with leverage.
            **_quote_defense_defaults(settings, deployed, auto_spread=spread_frac <= 0),
            # Bias-scaled net-exposure cap (overrides the _quote_defense_defaults
            # value): +20% at |bias|=1 so a directional lean isn't suppressed.
            "max_net_exposure_pct": _f(settings, "max_net_exposure_pct", 30.0) * _bias_exposure_mult,
        }
    if strategy == "dn":
        # NO_ORDERS_AUDIT-FIX-R1: DN config keys for DeltaNeutralController.
        # The controller expects trading_pair_long/short, hedge_ratio,
        # leg_amount_quote, max_drift_pct, and barriers. ``product`` here is
        # the base symbol (e.g. "QQQ"); the long leg is the SPOT pair and the
        # short leg is the PERP. run_engine_cycle resolves the real per-leg
        # product_ids + isolated flag via get_dn_pair (see _materialize_dn_leg_meta).
        from src.nadobro.engine.types import TripleBarrierConfig as _TBC

        base = str(product or "").upper().split("-", 1)[0]
        # Nado spot is quoted/collateralized in USDT0 (see config SPOT ids).
        # The real spot product_id is resolved from the catalog in
        # _materialize_dn_leg_meta — this string is the display/inventory key.
        long_pair = f"{base}-USDT0"        # spot leg
        short_pair = f"{base}-PERP"         # perp leg
        hedge_ratio = Decimal(str(_f(settings, "dn_hedge_ratio", 1.0)))
        leg_quote = Decimal(str(_f(settings, "fixed_margin_usd", notional)))
        max_drift = Decimal(str(_f(settings, "dn_max_drift_pct", 5.0))) / Decimal(100)
        # Hold duration is now the MINIMUM hold: default 1h, clamp [60s, 24h].
        # After it elapses the controller keeps the hedge open while funding
        # stays favorable and closes BOTH legs on a funding flip (or at the
        # max-hold safety cap). funding_exit_enabled=False restores a fixed hold.
        hold_seconds = int(max(60.0, min(_f(settings, "dn_hold_seconds", 3600.0), 86400.0)))
        # Safety cap on the total hold; 0 disables it (hold while favorable).
        # Clamp to <= 7d so a stray setting can't strand a hedge indefinitely.
        max_hold_seconds = int(max(0.0, min(_f(settings, "dn_max_hold_seconds", 86400.0), 604800.0)))
        funding_exit_enabled = _f(settings, "dn_funding_exit", 1.0) >= 0.5
        funding_flip_confirmations = int(max(1.0, _f(settings, "dn_funding_flip_confirmations", 2.0)))
        funding_poll_seconds = int(max(0.0, _f(settings, "dn_funding_poll_seconds", 60.0)))
        # Volume-farming: repeat open->hold->close N times with a gap between.
        cycles = int(max(1.0, _f(settings, "dn_cycles", 1.0)))
        cycle_gap_seconds = int(max(0.0, _f(settings, "dn_cycle_gap_seconds", 30.0)))
        # Per-leg TP/SL are OFF by default for DN: a one-sided TP would close a
        # single leg early and break the hedge. Only honor them if the operator
        # explicitly opts in via dn_leg_tp_pct / dn_leg_sl_pct.
        leg_tp = Decimal(str(_f(settings, "dn_leg_tp_pct", 0.0))) / Decimal(100)
        leg_sl = Decimal(str(_f(settings, "dn_leg_sl_pct", 0.0))) / Decimal(100)
        return {
            "trading_pair": long_pair,  # for parent base-class .trading_pair if read
            "trading_pair_long": long_pair,
            "trading_pair_short": short_pair,
            "hedge_ratio": hedge_ratio,
            "leg_amount_quote": leg_quote,
            "max_drift_pct": max_drift,
            "hold_seconds": hold_seconds,            # minimum hold
            "max_hold_seconds": max_hold_seconds,
            "funding_exit_enabled": funding_exit_enabled,
            "funding_flip_confirmations": funding_flip_confirmations,
            "funding_poll_seconds": funding_poll_seconds,
            "cycles": cycles,
            "cycle_gap_seconds": cycle_gap_seconds,
            "barriers": _TBC(take_profit=leg_tp or None, stop_loss=leg_sl or None),
            # Strictly 1x short by design (margin = full notional). Surfaced so
            # the adapter sizes isolated margin correctly for the perp leg.
            "leverage": 1,
        }
    if strategy == "vol":
        interval = max(1.0, _f(settings, "interval_seconds", 60))
        # VOL-MARGIN fix: the vol card collects the run size under
        # ``session_margin_usd`` (strategy_handler), but the generic ``notional``
        # above only reads cycle_notional_usd/notional_usd — so a user who set
        # "$500" still traded the $100 default. Prefer the user's session margin,
        # then fall back to the legacy keys.
        vol_notional = _f(
            settings, "session_margin_usd",
            _f(settings, "cycle_notional_usd", _f(settings, "notional_usd", 100.0)),
        )
        # Normalize the trading pair so the VolumeBotController validation
        # sees a canonical base (e.g. ``KBTC``) regardless of whether
        # ``state.product`` was stored as ``KBTC`` (current UI) or as a
        # dashed pair like ``KBTC-USDC0`` (legacy/tests).
        try:
            from src.nadobro.config import normalize_volume_spot_symbol

            vol_pair = normalize_volume_spot_symbol(str(product or "")) or str(product or "")
        except Exception:
            vol_pair = str(product or "")
        try:
            from src.nadobro.services.product_catalog import get_spot_maker_fee_rate

            spot_maker_fee_rate = get_spot_maker_fee_rate(vol_pair, network=network)
        except Exception:
            spot_maker_fee_rate = None
        if spot_maker_fee_rate is None:
            if settings.get("vol_maker_fee_rate") is not None:
                spot_maker_fee_rate = _f(settings, "vol_maker_fee_rate", 0.0)
            else:
                spot_maker_fee_rate = _f(settings, "vol_maker_fee_bp", 0.0) / 10000.0
        return {
            "trading_pair": vol_pair,
            "total_amount_quote": Decimal(str(vol_notional)),
            # Compatibility only; the Volume controller now places one order per
            # leg and waits for full fill instead of TWAP slicing over time.
            "total_duration": interval * 4,
            "order_interval": interval,
            "market": "spot",
            "leverage": 1,
            "spot_maker_fee_rate": Decimal(str(spot_maker_fee_rate or 0.0)),
            # VOL-LOOP / VOL-NO-CAP: cumulative volume target (0 = single
            # round-trip) and a hard cycle ceiling so the loop can't run away.
            "target_volume_usd": Decimal(str(_f(settings, "target_volume_usd", 0.0))),
            "max_cycles": int(max(1, _f(settings, "vol_max_cycles", 100))),
            # Buy rests at/inside the book; sell uses this as the minimum edge
            # above entry after positive maker-fee coverage.
            "vol_maker_offset_bp": _f(settings, "vol_maker_offset_bp", 5.0),
            "vol_min_edge_bp": _f(settings, "vol_min_edge_bp", _f(settings, "vol_maker_offset_bp", 5.0)),
        }
    # grid / rgrid / dgrid family.
    #
    # Phase 4 opt-in: fill-anchored quoting (TreadFi Grid/RGrid semantics).
    # One bid + one ask around a fill-anchored reference instead of a static
    # ladder; reset_threshold_pct uses TreadFi's defaults (0.25% grid /
    # 0.125% rgrid) unless overridden.
    # grid and rgrid both default to the MULTI-LEVEL recycling ladder (user
    # choice, 2026-06). grid -> classic long ladder (GridController); rgrid ->
    # dynamic directional ladder (DynamicGridController, via CONTROLLER_REGISTRY).
    # The fill-anchored quoting (grid: single-pair maker with no-cross +
    # soft-reset-to-mid / rgrid: trend-following taker momentum) is the opt-in
    # via fill_anchored=1.
    _fa_default = 0.0
    if strategy in ("grid", "rgrid") and bool(_f(settings, "fill_anchored", _fa_default)):
        default_reset = 0.25 if strategy == "grid" else 0.125
        # The UI writes the reset threshold under per-strategy keys
        # (grid_reset_threshold_pct / rgrid_reset_threshold_pct), but the
        # fill-anchored controller reads ``reset_threshold_pct``. Read the UI
        # keys here so the "Reset Threshold" button actually drives the soft
        # reset — previously a silent no-op: the controller always fell back to
        # the hardcoded default because nothing translated the key. Percent →
        # fraction. (Also fixes live edits: _apply_fill_anchored_controller_config
        # refreshes from this same mapped value each cycle.)
        if strategy == "rgrid":
            _reset_pct = _f(settings, "rgrid_reset_threshold_pct",
                            _f(settings, "grid_reset_threshold_pct",
                               _f(settings, "reset_threshold_pct", default_reset)))
        else:
            _reset_pct = _f(settings, "grid_reset_threshold_pct",
                            _f(settings, "reset_threshold_pct", default_reset))
        return {
            "trading_pair": product,
            "controller_override": "fill_anchored",
            "anchor_mode": strategy,
            "reset_threshold_pct": Decimal(str(_reset_pct)) / Decimal(100),
            "spread_bid_pct": spread_frac if spread_frac > 0 else Decimal("0.001"),
            "spread_ask_pct": spread_frac if spread_frac > 0 else Decimal("0.001"),
            # Fill-anchored places ONE bid + ONE ask per cycle; a participation
            # chunk sizes that order directly, else the deployed budget / levels.
            "order_amount_quote": _chunk_dec or (Decimal(str(deployed)) / Decimal(levels)),
            "price_distance_tolerance": (spread_frac / Decimal(2)) or Decimal("0.0005"),
            "leverage": int(eff_lev),
            # rgrid → taker-momentum (buy the break up / sell the break down).
            # grid stays maker (no-cross spread capture).
            "momentum": strategy == "rgrid",
            # Exposure-price VWAP window from the user's discretion knob (now
            # live; previously a dead input). Only meaningful for rgrid.
            "vwap_volume_fraction": (
                _f(settings, "rgrid_discretion", 0.0) if strategy == "rgrid" else 0.0
            ),
            # Grid two-step stall escalation: after the soft-reset maker leg
            # stalls for N ticks, a bounded reduce-only taker concession flattens
            # part of the one-sided exposure before the SL rail. Grid only
            # (rgrid is momentum-driven, no soft reset).
            "concession_enabled": strategy == "grid",
            "concession_escalation_ticks": int(max(1.0, _f(settings, "grid_concession_ticks", 5.0))),
            "concession_fraction": _f(settings, "grid_concession_fraction", 0.5),
            **_quote_defense_defaults(settings, deployed, auto_spread=spread_frac <= 0),
            # Keep the regime gate OFF: rgrid is a trend strategy (the gate would
            # pause momentum exactly when it must act), and grid is the
            # GRID-IN-TRENDS default (quote in every regime; inventory cap +
            # soft-reset + session SL/TP rail are the backstops). Both honor an
            # explicit user override (regime_gate_enabled=1 re-arms it).
            **(
                {"regime_gate_enabled": 0.0}
                if (strategy == "rgrid" or (strategy == "grid" and "regime_gate_enabled" not in settings))
                else {}
            ),
        }
    #
    # NO_ORDERS_AUDIT-FIX-R4: spread_bp is now interpreted as the per-level
    # STEP (distance between adjacent grid levels), not the total band. With
    # `levels` levels stepping by `spread_frac` each:
    #
    #   * grid  (BUY, long):  N levels stepping DOWN from mid.
    #                         span = (levels - 1) * spread_frac
    #                         start = mid * (1 - span)  (lo)
    #                         end   = mid               (hi)  — all buys ≤ mid
    #
    #   * rgrid (SELL, short): N levels stepping UP from mid.
    #                         start = mid                    (lo)
    #                         end   = mid * (1 + span)       (hi)  — all sells ≥ mid
    #
    #   * dgrid: side is chosen dynamically per tick. We DON'T fix the band
    #     here; instead we pass ``step_pct`` and ``levels_count`` so the
    #     DynamicGridController can rebuild the side-correct band against
    #     a fresh mid before spawning the executor.
    #
    # Why the old code was wrong: it used a symmetric band (mid ± band) where
    # band = step × levels. A BUY grid then had levels ABOVE mid, which a
    # post-only LIMIT_MAKER buy gets rejected for (would cross the book).
    # Those levels never placed — silent partial failure.

    # Floor the per-level step so a near-zero spread_bp doesn't collapse the
    # grid to one price level (which then divides by zero in
    # generate_grid_levels).
    if spread_frac <= 0:
        spread_frac = Decimal("0.0005")  # 5 bp fallback
    span = spread_frac * Decimal(max(levels - 1, 1))

    # GRID-DUAL-UNIT fix: do NOT derive a hard ``limit_price`` stop from the
    # user's sl_pct. That stop is referenced to the run/rebuild mid and ignores
    # how much of the grid has actually filled, so it fires on a brief wick to
    # mid*(1-sl) even when little is at risk — a premature stop-out on top of the
    # session margin-% rail. SL is now governed consistently by (a) the executor
    # avg-entry barrier (triple_barrier_config.stop_loss, fill-aware) and (b) the
    # fee-aware session rail. limit_price stays available as an explicit
    # catastrophic stop but is no longer auto-set from sl.
    # POST-ONLY-CROSS fix: the boundary level nearest mid must be a strict maker,
    # or a post-only LIMIT_MAKER placed AT mid crosses the book and the venue
    # rejects it (error_code 2008 — seen on every rgrid SELL and, once grids
    # ladder past one level, on the top BUY too). Offset the near boundary by at
    # least half a grid step (floored at 1.5 bp) onto the maker side: buys
    # strictly below mid, sells strictly above.
    maker_offset = max(spread_frac / Decimal(2), Decimal("0.00015"))
    if strategy == "rgrid":
        start_price = mid * (Decimal(1) + maker_offset)
        end_price = mid * (Decimal(1) + maker_offset + span)
        limit_price = Decimal(0)
    else:  # grid OR dgrid-as-long-default; dgrid recomputes at on_tick
        start_price = mid * (Decimal(1) - maker_offset - span)
        end_price = mid * (Decimal(1) - maker_offset)
        limit_price = Decimal(0)

    cfg: Dict[str, object] = {
        "trading_pair": product,
        "start_price": start_price,
        "end_price": end_price,
        "limit_price": limit_price,
        # Deployed position notional = margin x effective leverage (see top).
        # A participation chunk caps the per-cycle ladder notional when active.
        "total_amount_quote": _chunk_dec or Decimal(str(deployed)),
        "min_spread_between_orders": spread_frac,
        "max_open_orders": levels,
        "leverage": int(eff_lev),
        # Continuous laddering for the whole GridExecutor family (classic grid &
        # rgrid when fill_anchored=0, and D-Grid): re-arm round-tripped levels so
        # the ladder keeps working its band instead of draining to COMPLETE and
        # terminating. Bounded by the net-exposure cap + session rails. (Default
        # grid/rgrid run the fill-anchored controller, which already re-quotes
        # every tick; Mid is the market-making controller, likewise continuous —
        # this flag only affects the multi-level ladder executor.)
        "recycle_levels": True,
        "triple_barrier_config": TripleBarrierConfig(
            take_profit=tp or None, stop_loss=sl or None
        ),
        # NO_ORDERS_AUDIT-FIX-R4: extra knobs consumed by DynamicGridController
        # so it can rebuild the side-correct band on the fly. Ignored by
        # GridController / ReverseGridController.
        "step_pct": spread_frac,
        "levels_count": levels,
        "tp_pct": tp,
        "sl_pct": sl,
        # Regime gate + inventory cap + ATR auto-step (2026-06 upgrade).
        # margin_quote = DEPLOYED notional so the net-exposure cap scales with
        # leverage instead of choking a leveraged ladder at 30% of collateral.
        **_quote_defense_defaults(settings, deployed, auto_spread=spread_frac <= 0),
    }
    # NO_ORDERS_AUDIT-FIX-R2: DynamicGridController requires a candle_provider
    # callable to classify the volatility regime. Without one, _candles()
    # returns [] and on_tick exits early — no executor ever spawned, no orders
    # placed. We bind the provider in ``run_engine_cycle`` because it needs
    # access to the live ``client`` and ``product_id`` to call
    # ``client.get_candlesticks(...)``; setting ``"candle_provider": None``
    # here makes the contract explicit and lets the cycle driver inject the
    # real provider on first start.
    if strategy in ("dgrid", "rgrid"):
        # Both run the SAME dynamic directional-ladder engine (DynamicGridController:
        # long ladder in uptrends, short in downtrends), but with DIFFERENT default
        # tuning so the two products stay distinct:
        #   * rgrid = PURE trend-direction ladder — reacts sooner and flips faster
        #     (lower VR/drift thresholds, shorter windows, 1-tick flip confirm,
        #     earlier trailing-reversal), so it spends less time in the neutral
        #     mean-reversion grid and tracks the trend direction aggressively.
        #   * dgrid = VOLATILITY-BALANCED switcher — steadier thresholds and slower
        #     flips, so it mean-reverts (long grid) in ranges and only goes
        #     directional on a clearer volatility-regime signal.
        # Any explicit user setting still overrides these per-strategy defaults.
        _rg = strategy == "rgrid"
        cfg["candle_provider"] = None
        # (recycle_levels is set for the whole GridExecutor family above.)
        cfg["dgrid_short_window"] = int(max(2, _f(settings, "dgrid_short_window_points", 3 if _rg else 4)))
        cfg["dgrid_long_window"] = int(max(4, _f(settings, "dgrid_long_window_points", 8 if _rg else 12)))
        cfg["dgrid_trend_on_vr"] = _f(settings, "dgrid_trend_on_variance_ratio", 1.10 if _rg else 1.25)
        cfg["dgrid_range_on_vr"] = _f(settings, "dgrid_range_on_variance_ratio", 1.05 if _rg else 1.15)
        # Sustained-drift trend filter: flip the grid direction on a slow one-way
        # grind the variance ratio misses (a steady decline keeps VR<1 yet bleeds
        # a long grid). Percent over the long window; 0 disables. rgrid catches it
        # earlier (0.15%) so it turns with the trend sooner.
        cfg["dgrid_trend_drift_pct"] = _f(settings, "dgrid_trend_drift_pct", 0.15 if _rg else 0.30)
        # Tiered profit-booking: scale out reduce-only as the run's uPnL climbs
        # past these tiers (% of margin), closing dgrid_tp_fraction each time.
        cfg["dgrid_tp_tiers_pct"] = settings.get("dgrid_tp_tiers_pct") or [2.0, 4.0, 6.0]
        cfg["dgrid_tp_fraction"] = _f(settings, "dgrid_tp_fraction", 0.33)
        # Confirm-ticks debounce a flip. rgrid flips on the first confirmed change
        # (trend follower); dgrid waits an extra tick to avoid whipsaw.
        cfg["dgrid_flip_confirm_ticks"] = int(max(1, _f(settings, "dgrid_flip_confirm_ticks", 1 if _rg else 2)))
        # Trend-capture redesign (2026-06): as a run goes in profit, ratchet a
        # trailing take-profit so a reversal still closes green, and flip
        # long<->short on a confirmed price reversal from the run's extreme
        # (faster than waiting for the variance classifier to cross). Percents.
        # rgrid arms sooner and flips on a smaller reversal (it chases the trend).
        cfg["dgrid_trail_arm_pct"] = _f(settings, "dgrid_trail_arm_pct", 0.5 if _rg else 1.0)
        cfg["dgrid_trail_giveback_pct"] = _f(settings, "dgrid_trail_giveback_pct", 0.5)
        cfg["dgrid_reversal_flip_pct"] = _f(settings, "dgrid_reversal_flip_pct", 0.3 if _rg else 0.4)
        # Reset re-center drives an IN-PLACE re-quote of the resting ladder
        # (GridExecutor.recenter) — no flatten, no realized loss — so it can
        # follow price closely without bleeding fees. Pass the user's explicit
        # threshold through when set; otherwise pass 0 so the controller picks
        # its geometry default (~one band width of drift) and the ladder FOLLOWS
        # price as it trends instead of going stale ("placed a few orders and
        # stopped"). 50bp was too coarse — BTC rarely drifts that far inside one
        # 30-60s tick, so it almost never re-centered.
        _reset_key = "rgrid_reset_threshold_pct" if strategy == "rgrid" else "grid_reset_threshold_pct"
        cfg["dgrid_reset_threshold_bp"] = _f(
            settings, "dgrid_reset_threshold_bp",
            _f(settings, _reset_key, 0.0) * 100.0,
        )
        # Dynamic Grid's own min/max per-side spread bounds drive the auto-spread
        # clamp (previously dead — _quote_defense_defaults hardcoded the band).
        _dg_floor = Decimal(str(max(0.0, _f(settings, "dgrid_min_spread_bp", 2.0)))) / Decimal(10000)
        _dg_cap = Decimal(str(_f(settings, "dgrid_max_spread_bp", 50.0))) / Decimal(10000)
        cfg["spread_floor_half_pct"] = _dg_floor
        cfg["spread_cap_half_pct"] = max(_dg_cap, _dg_floor)

    # GRID in-place re-center: honor the user's reset threshold so the classic
    # long ladder follows price ("reset and continue") instead of going stale.
    # Drives GridExecutor.recenter (no flatten); the controller floors it above
    # the band. (rgrid now runs DynamicGridController — its re-center is wired
    # via dgrid_reset_threshold_bp in the block above.)
    if strategy == "grid":
        # Re-center ON by default: pass the user's explicit reset through, else 0
        # so the controller uses its band-width auto-follow (50bp was too coarse —
        # the ladder rarely re-centered and went stale below mid).
        cfg["reset_threshold_bp"] = _f(settings, "grid_reset_threshold_pct", 0.0) * 100.0
        # GRID-IN-TRENDS (user directive 2026-06-21): plain Grid was gated OUT of
        # trends/expansions by default and silently never quoted (the "always
        # market paused" report). Default the regime gate OFF for grid so it
        # quotes in every regime; the inventory net-exposure cap, the in-place
        # recenter, and the live-session SL/TP rails remain the backstops. Still
        # user-overridable: an explicit regime_gate_enabled=1 re-arms the gate.
        if "regime_gate_enabled" not in settings:
            cfg["regime_gate_enabled"] = 0.0
    return cfg


def map_risk_limits(
    settings: Dict[str, Any], strategy: Optional[str] = None, *, leverage: float = 1.0,
) -> RiskLimits:
    # Delta Neutral sizes each leg from ``fixed_margin_usd`` (NOT ``notional_usd``),
    # so its risk caps must follow the leg size or the Risk Engine rejects every
    # leg with ``max_single_order_quote`` and the controller never places an order
    # (the "LIVE but 0 orders" bug). Cap = per-leg notional × headroom; the
    # base-matched short can be slightly larger than the long (mid gap / hedge
    # ratio) and the venue may bump a leg up to its min-notional, hence the 2×.
    if str(strategy or "").lower() == "dn":
        leg = _f(settings, "fixed_margin_usd", _f(settings, "notional_usd", 100.0))
        hedge = max(1.0, _f(settings, "dn_hedge_ratio", 1.0))
        per_order_cap = max(1.0, leg * hedge) * 2.0
        return RiskLimits(
            # 2 legs + headroom for the close/trim and next-cycle overlap.
            max_open_executors=6,
            max_single_order_quote=Decimal(str(per_order_cap)),
            max_position_size_quote=Decimal(str(per_order_cap)),
        )
    # Grid/MM family: caps must follow the DEPLOYED notional (margin x leverage),
    # or the Risk Engine rejects every leveraged order — the same "LIVE but 0
    # orders" failure the DN branch above guards against. With leverage=1 this is
    # identical to the legacy margin-sized behavior (tests rely on that).
    margin = _f(settings, "cycle_notional_usd", _f(settings, "notional_usd", 100.0))
    eff_lev = _effective_leverage(settings, float(leverage or 1.0))
    deployed = margin * eff_lev
    levels = max(1, int(_f(settings, "levels", 2)))
    cap = _f(settings, "session_notional_cap_usd", 0.0) or (deployed * levels)
    return RiskLimits(
        max_open_executors=levels + 2,
        # A single bumped level can be up to the whole deployed size (when the
        # min-notional cap collapses the ladder to one level).
        max_single_order_quote=Decimal(str(deployed)),
        max_position_size_quote=Decimal(str(max(cap, deployed * 1.2))),
    )


def _persist_dn_progress(telegram_id: int, network: str, strategy: str) -> None:
    """Write the live DN controller progress (cycles-completed + funding-earned
    + phase) to engine_controller_state so the main process can surface it in
    /status. Cross-process via the deterministic controller id. Best-effort —
    never breaks a cycle."""
    if strategy != "dn":
        return
    try:
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        if controller is None:
            return
        from src.nadobro.services.engine_persistence import upsert_controller_progress

        upsert_controller_progress(
            controller.id,
            telegram_id,
            strategy=strategy,
            network=network,
            cycles_completed=int(getattr(controller, "cycles_completed", 0) or 0),
            funding_earned_usd=getattr(controller, "cumulative_funding", 0) or 0,
            phase=str(getattr(getattr(controller, "phase", None), "value", "") or ""),
        )
    except Exception:  # noqa: BLE001 - persistence must not break a cycle
        logger.debug("dn progress persist failed", exc_info=True)


def _friendly_start_error(reason: str) -> str:
    """Map an internal spawn-rejection reason to a user-facing message."""
    low = str(reason or "").lower()
    if "max_single_order_quote" in low or "max_position_size_quote" in low:
        return ("Order size exceeds this strategy's risk limit. Lower the Size or "
                "raise the cap, then start again.")
    if "max_open_executors" in low:
        return "Too many open legs for this strategy's risk limit."
    if "kill_switch" in low:
        return "Trading is paused (kill switch active). Try again shortly."
    if "insufficient" in low or "margin" in low or "health" in low:
        return "Not enough margin to open both legs. Add margin or lower the Size."
    return f"Strategy failed to start: {reason}" if reason else "Strategy failed to start."


def _opt_int(value: object) -> Optional[int]:
    """``int(value)`` or ``None`` if missing/unparseable (typed so mypy is happy
    with ``Any | None`` catalog fields)."""
    if value is None:
        return None
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return None


def _x18_dec(value: object, default: str) -> Decimal:
    """Convert an x18-scaled catalog field (stored as an int-ish string, value
    × 1e18) to a real Decimal. Falls back to ``default`` when missing/unparseable
    — but a legitimate 0 is preserved."""
    if value is None:
        return _dec(default)
    try:
        return _dec(str(int(str(value)))) / _dec(10 ** 18)
    except (TypeError, ValueError):
        try:
            return _dec(value)
        except Exception:  # noqa: BLE001
            return _dec(default)


def build_product_meta_from_catalog(client: object) -> Dict[str, object]:
    """{trading_pair -> ProductMeta} from the live product catalog, with each
    product's real tick_size / lot_size / min_notional plus is_perp /
    isolated_only.

    The previous implementation expected ``client.get_all_products_info()`` to
    return a ``"products"`` list of richly-described dicts, but that method
    actually returns ``{"perp": [{"id": ...}], "spot": [{"id": ...}]}`` — so it
    produced an EMPTY dict and every engine strategy silently fell back to the
    permissive ``ProductMeta(pid, 0.01, 0.001, 1)`` with wrong increments. We now
    read the resolved perp + spot catalogs (which carry the x18-scaled
    increments and the isolated_only flag) and key each product under its base,
    canonical symbol, and — for perps — the ``BASE-PERP`` alias, so whichever
    string a strategy uses for ``trading_pair`` resolves.
    """
    from src.nadobro.engine.adapter.nado import ProductMeta
    from src.nadobro.services import product_catalog as pc

    out: Dict[str, object] = {}
    network = str(getattr(client, "network", None) or "mainnet")

    def _register(meta: object, keys: Iterable[str], *, overwrite: bool) -> None:
        for key in keys:
            k = str(key or "").strip()
            if k and (overwrite or k not in out):
                out[k] = meta

    # Perps: id + price/size increments + min notional + isolated flag.
    try:
        perps = (pc.get_catalog(network=network, client=client) or {}).get("perps") or {}
    except Exception:  # noqa: BLE001
        logger.warning("perp catalog unavailable", exc_info=True)
        perps = {}
    for base, row in (perps.items() if isinstance(perps, dict) else []):
        if not isinstance(row, dict):
            continue
        pid = _opt_int(row.get("id"))
        if pid is None:
            continue
        meta = ProductMeta(
            product_id=pid,
            tick_size=_x18_dec(row.get("price_increment_x18"), "0.01"),
            lot_size=_x18_dec(row.get("size_increment_x18"), "0.001"),
            min_notional=_x18_dec(row.get("min_size_x18"), "1"),
            is_perp=True,
            isolated_only=bool(row.get("isolated_only")),
        )
        symbol = str(row.get("symbol") or f"{base}-PERP")
        # Market-qualified aliases (``BASE-PERP``) let a caller that handles
        # BOTH markets for the same base — the Desk controller — address the
        # perp unambiguously even when a spot listing shares the base.
        _register(meta, (base, symbol, f"{base}-PERP", f"{symbol}-PERP"), overwrite=True)

    # Spots: id + (best-effort) increments; never isolated. Don't clobber a perp
    # alias on a base collision (perps registered first).
    try:
        spots = (pc.get_spot_catalog(network=network) or {}).get("spots") or {}
    except Exception:  # noqa: BLE001
        logger.warning("spot catalog unavailable", exc_info=True)
        spots = {}
    for base, row in (spots.items() if isinstance(spots, dict) else []):
        if not isinstance(row, dict):
            continue
        pid = _opt_int(row.get("id"))
        if pid is None:
            continue
        meta = ProductMeta(
            product_id=pid,
            tick_size=_x18_dec(row.get("price_increment_x18"), "0.01"),
            lot_size=_x18_dec(row.get("size_increment_x18"), "0.001"),
            min_notional=_x18_dec(row.get("min_size_x18"), "1"),
            is_perp=False,
            isolated_only=False,
        )
        symbol = str(row.get("symbol") or base)
        # Base/symbol keys must NOT clobber a perp on a dual-listed base (a
        # grid/MM strategy keyed by bare base expects its perp). But the
        # ``BASE-SPOT`` alias is spot-only and collision-free — it's how the
        # Desk controller routes a spot plan on a dual-listed asset (e.g.
        # "buy 2 ETH" -> ETH-SPOT) to the SPOT product, not the perp.
        _register(meta, (base, symbol), overwrite=False)
        _register(meta, (f"{base}-SPOT", f"{symbol}-SPOT"), overwrite=True)

    return out


def _materialize_dn_leg_meta(
    meta: Dict[str, object],
    configs: Dict[str, Any],
    client: object,
    network: str,
    product: str,
) -> None:
    """Register ProductMeta for the DN spot (long) and perp (short) legs, keyed
    by the exact ``trading_pair_long`` / ``trading_pair_short`` strings the
    controller uses, with each leg's REAL product_id and the perp's
    isolated-only flag resolved from the DN pair catalog.

    Safety: we never fall back to a shared product_id for the two legs (that
    would trade one product twice). If a leg's id can't be resolved we leave it
    unregistered so the adapter raises a clear "Unknown trading pair" and the
    controller fails to start cleanly instead of mis-trading.
    """
    from src.nadobro.engine.adapter.nado import ProductMeta
    from src.nadobro.services.product_catalog import (
        get_dn_pair,
        is_product_isolated_only,
    )

    base = str(product or "").upper().split("-", 1)[0]
    long_sym = str(configs.get("trading_pair_long") or "")
    short_sym = str(configs.get("trading_pair_short") or "")

    dn: Dict[str, Any] = {}
    try:
        dn = dict(get_dn_pair(base, network=network, client=client) or {})
    except Exception:  # noqa: BLE001 - catalog is best-effort
        logger.warning("dn: get_dn_pair failed for %s", base, exc_info=True)
    try:
        iso = bool(is_product_isolated_only(base, network=network, client=client))
    except Exception:  # noqa: BLE001
        iso = False

    spot_pid = dn.get("spot_product_id")
    perp_pid = dn.get("perp_product_id")

    if long_sym:
        if spot_pid is not None:
            meta[long_sym] = ProductMeta(
                int(spot_pid), _dec("0.01"), _dec("0.001"), _dec("1"),
                is_perp=False, isolated_only=False,
            )
        else:
            logger.error(
                "dn: could not resolve SPOT product_id for %s (long leg %s); "
                "leaving unregistered so the controller fails cleanly",
                base, long_sym,
            )

    if short_sym:
        if perp_pid is not None:
            meta[short_sym] = ProductMeta(
                int(perp_pid), _dec("0.01"), _dec("0.001"), _dec("1"),
                is_perp=True, isolated_only=iso,
            )
        else:
            logger.error(
                "dn: could not resolve PERP product_id for %s (short leg %s); "
                "leaving unregistered so the controller fails cleanly",
                base, short_sym,
            )


async def _maybe_apply_overlay(
    telegram_id: int,
    network: str,
    strategy: str,
    product: str,
    product_id: object,
    configs: Dict[str, object],
    state: Dict[str, Any],
    *,
    client: object,
    mid: float,
) -> None:
    """Compute the multi-timeframe signal and apply BOUNDED overrides to the
    mapped ``configs`` in place. Best-effort: any failure leaves the base config
    untouched. Candle fetch + persistence run off the event loop."""
    try:
        from src.nadobro.services.overlay_actuator import (
            apply_overrides_to_configs,
            compute_overrides,
            overlay_applies,
        )

        if not overlay_applies(strategy):
            return
        pid = _opt_int(product_id)
        if pid is None or client is None or not hasattr(client, "get_candlesticks"):
            return

        from src.nadobro.services.async_utils import run_blocking, run_blocking_sdk
        from src.nadobro.services import market_features as _mf
        from src.nadobro.services.signal_engine import build_signal
        from src.nadobro.services.strategy_registry import effective_sl_tp_pct

        def _gather() -> Dict[str, Dict[str, object]]:
            def _fetch(p, tf, lim):
                return client.get_candlesticks(int(p), tf, int(lim))  # type: ignore[attr-defined]

            return _mf.multi_tf_features(_fetch, network, int(pid))

        features = await run_blocking_sdk(_gather)
        if not features:
            return

        # Live funding + current position side so the signal can flag when the
        # carry is hostile to what the strategy is actually holding. Funding is
        # one batched indexer read (off-loop, best-effort); position side is read
        # from the controller's in-memory inventory — no extra SDK call.
        funding_rate: Optional[float] = None
        if hasattr(client, "get_perp_funding_rates"):
            try:
                def _funding():
                    return client.get_perp_funding_rates([int(pid)])  # type: ignore[attr-defined]

                rates = await run_blocking_sdk(_funding) or {}
                entry = rates.get(int(pid)) or rates.get(str(pid)) or {}
                raw = entry.get("funding_rate") if isinstance(entry, dict) else None
                funding_rate = float(raw) if raw is not None else None
            except Exception:  # noqa: BLE001 - funding context is optional
                funding_rate = None

        position_side: Optional[str] = None
        try:
            controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
            inv = getattr(controller, "inventory", None)
            if controller is not None and inv is not None:
                hold = inv.get(telegram_id, controller.trading_pair, controller.id)
                net_base = float(getattr(hold, "net_amount_base", 0) or 0)
                if net_base > 0:
                    position_side = "long"
                elif net_base < 0:
                    position_side = "short"
        except Exception:  # noqa: BLE001 - position side is an optional enrichment
            position_side = None

        base_sl, base_tp = effective_sl_tp_pct(strategy, state)
        signal = build_signal(
            features,
            funding_rate=funding_rate,
            position_side=position_side,
            base_sl_pct=(base_sl if base_sl > 0 else 0.5),
            base_tp_pct=(base_tp if base_tp > 0 else 1.0),
        )
        overrides = compute_overrides(strategy, signal)
        changed = apply_overrides_to_configs(strategy, configs, overrides)

        # Surface the regime-adjusted barriers to the session SL/TP rail (which
        # reads state, not configs). Persisted with state at end of cycle, so
        # the rail uses them from the next cycle. Cleared to fall back to the
        # user's config when the signal produced no barrier.
        if signal.sl_pct is not None:
            state["overlay_sl_pct"] = float(signal.sl_pct)
        else:
            state.pop("overlay_sl_pct", None)
        if signal.tp_pct is not None:
            state["overlay_tp_pct"] = float(signal.tp_pct)
        else:
            state.pop("overlay_tp_pct", None)

        # Persist the signal + applied action for Night HOWL / audit (off loop).
        try:
            from src.nadobro.models.database import insert_overlay_signal

            row = {
                "user_id": int(telegram_id),
                "network": network,
                "strategy": strategy,
                "product_id": int(pid),
                "product_name": str(product or ""),
                "strategy_session_id": _opt_int(state.get("strategy_session_id")),
                "bias": signal.bias,
                "regime": signal.regime,
                "confidence": signal.confidence,
                "entry_ok": bool(signal.entry_ok),
                "scale": signal.scale,
                "spread_mult": signal.spread_mult,
                "sl_pct": signal.sl_pct,
                "tp_pct": signal.tp_pct,
                "action_json": changed,
                "reasons_json": list(signal.reasons),
                "risks_json": list(signal.risks),
            }
            await run_blocking(insert_overlay_signal, row)
        except Exception:  # noqa: BLE001 - persistence must never break a tick
            logger.debug("overlay signal persist failed", exc_info=True)

        if changed:
            logger.info(
                "overlay applied user=%s network=%s strategy=%s regime=%s bias=%.2f conf=%.2f changed=%s",
                telegram_id, network, strategy, signal.regime, signal.bias,
                signal.confidence, list(changed.keys()),
            )
    except Exception:  # noqa: BLE001 - overlay is advisory; never break the cycle
        logger.warning("overlay apply failed user=%s strategy=%s", telegram_id, strategy, exc_info=True)


async def run_engine_cycle(
    telegram_id: int,
    network: str,
    state: Dict[str, Any],
    client: object,
    mid: float,
    product: str,
    product_id: int,
) -> dict:
    """Gated per-cycle driver called from bot_runtime's async loop. Starts the
    controller on first cycle, ticks it thereafter. Returns a dispatch-style
    result dict. Live execution validated on testnet."""
    from src.nadobro.services.engine_persistence import DbInventoryRepository

    strategy = str(state.get("strategy") or "")
    if strategy not in ENGINE_MAPPED_STRATEGIES:
        return {"success": False, "error": f"strategy '{strategy}' not engine-mapped"}

    settings = {k: v for k, v in state.items() if not isinstance(v, (dict, list))}
    _start_lev = _f(settings, "leverage", 1)
    configs = map_strategy_config(strategy, settings, _dec(mid), product=product,
                                  leverage=int(_start_lev), network=network)
    limits = map_risk_limits(settings, strategy, leverage=_start_lev)

    # Financial overlay (background, no user config): compute a multi-timeframe
    # signal and apply BOUNDED overrides to the mapped configs before the
    # live-reconfigure path pushes them to the controller. Best-effort — any
    # failure leaves the base config untouched so a tick never breaks. The
    # overlay's own drawdown kill-switch lives in bot_runtime's session rail.
    await _maybe_apply_overlay(
        telegram_id, network, strategy, product, product_id, configs, state,
        client=client, mid=mid,
    )

    # NO_ORDERS_AUDIT-FIX-R2: inject the dgrid candle_provider HERE, because
    # this is where ``client`` and ``product_id`` are available. The provider
    # closure is recreated on every cycle, which is fine — the controller only
    # caches a reference at on_start.
    if strategy in ("grid", "rgrid", "dgrid", "mid") and configs.get("candle_provider") is None:
        _cli = client
        _pid = int(product_id)

        async def _candle_provider(_pair: str) -> list:
            # ASYNC: get_candlesticks is a sync SDK/REST call; with the gate
            # this provider now runs on EVERY tick of four strategies, so it
            # must go through the SDK thread pool instead of blocking the
            # event loop (the exact starvation class the blocking lint guards
            # — invisible here because the call is one level indirect).
            try:
                from src.nadobro.services.async_utils import run_blocking_sdk

                # 1m candles, last 200 — enough for ema_slow_period=50 + atr_window=14.
                return await run_blocking_sdk(
                    _cli.get_candlesticks, _pid, timeframe="1m", limit=200  # type: ignore[attr-defined]
                ) or []
            except Exception:  # noqa: BLE001 - never let candle fetch break a tick
                logger.warning(
                    "candle_provider failed for pair=%s product_id=%s",
                    _pair, _pid, exc_info=True,
                )
                return []

        configs["candle_provider"] = _candle_provider

    # BUG-TICK-1 recovery: if the local controller went terminal-FAILED (a
    # genuinely fatal error, or an exhausted transient-error streak), force a
    # rebuild even though a stale executor row may still make is_running()
    # report True via _remote_active. start() tears the FAILED controller down
    # before replacing it, so this cleanly resurrects a stalled strategy.
    needs_recovery = RUNTIME.needs_recovery(telegram_id, network, strategy)
    if needs_recovery:
        logger.warning(
            "engine controller FAILED for user=%s network=%s strategy=%s — "
            "rebuilding (BUG-TICK-1 recovery)",
            telegram_id, network, strategy,
        )
    # Build decision. RUNTIME.tick only works on a LOCAL controller, so the
    # process running this cycle must own one. A worker that runs cycles ADOPTS
    # (builds) when it has no local active controller — even if a dead process
    # left a non-terminated executor row making is_running() True — otherwise a
    # crashed/recycled worker would no-op forever. The main/non-worker fallback
    # still defers to a live owner (is_running True) so it never double-builds.
    from src.nadobro.services.bot_runtime import is_process_worker_mode
    _has_local = RUNTIME.has_local_active(telegram_id, network, strategy)
    _worker_mode = is_process_worker_mode()
    _should_build = _should_build_controller(
        needs_recovery=needs_recovery,
        has_local_active=_has_local,
        worker_mode=_worker_mode,
        # is_running is only needed for the non-worker defer case; skip the DB
        # round-trip when the worker will adopt anyway.
        is_running=(False if (_worker_mode and not _has_local)
                    else RUNTIME.is_running(telegram_id, network, strategy)),
    )
    if _should_build:
        meta = build_product_meta_from_catalog(client)
        # ensure the traded pair has metadata (fallback to a permissive default)
        if product not in meta:
            from src.nadobro.engine.adapter.nado import ProductMeta

            meta[product] = ProductMeta(int(product_id), _dec("0.01"), _dec("0.001"), _dec("1"))
        # GRID-MIN-NOTIONAL-INFLATE fix: each grid level is sized
        # total_amount_quote / levels, but the venue bumps any sub-min-notional
        # order UP to its minimum — so a small/many-level grid silently deploys
        # MORE than the configured (and risk-approved) size. Cap the level count
        # so each level is at least the venue minimum. Conservative: only ever
        # REDUCES the level count, never raises it.
        if strategy in ("grid", "rgrid", "dgrid"):
            try:
                _mn = float(getattr(meta.get(product), "min_notional", 0) or 0)
                _tot = float(configs.get("total_amount_quote") or 0)
                if _mn > 0 and _tot > 0:
                    _max_levels = max(1, int(_tot // _mn))
                    _cur = int(configs.get("max_open_orders") or 1)
                    if _cur > _max_levels:
                        configs["max_open_orders"] = _max_levels
                        configs["levels_count"] = _max_levels
                        logger.info(
                            "grid level cap (min-notional): %s -> %s levels "
                            "(total=%.2f min_notional=%.2f) user=%s strategy=%s",
                            _cur, _max_levels, _tot, _mn, telegram_id, strategy,
                        )
            except Exception:  # noqa: BLE001 - cap is best-effort, never block start
                logger.debug("grid min-notional level cap skipped", exc_info=True)
        # NO_ORDERS_AUDIT-FIX-R1: DN needs metadata for BOTH legs (spot long +
        # perp short), each with its OWN product_id. The old fallback keyed both
        # legs to the SAME ``product_id`` — which would have traded the perp
        # twice instead of spot+perp. Resolve real per-leg ids (and the perp's
        # isolated-only flag) from the DN pair catalog.
        if strategy == "dn":
            _materialize_dn_leg_meta(meta, configs, client, network, product)
            # DN-CYCLES fix: a rebuild (restart / worker handoff / recovery) used
            # to reset the controller's cycle counter to 0 and re-run the whole
            # configured cycle count. Restore the persisted progress so the
            # rebuilt controller resumes the count instead of restarting it.
            # GUARD: only restore for a run that has already ticked at least once
            # (state["runs"] > 0). A fresh user start has runs == 0, so a stale
            # progress row left by a prior crashed run (the controller_id is
            # stable across runs) can never bleed into a new run.
            if int(_f(state, "runs", 0)) > 0:
                try:
                    from src.nadobro.services.engine_persistence import (
                        get_controller_progress,
                    )

                    _cid = deterministic_controller_id(strategy, telegram_id, network)
                    _prog = get_controller_progress(_cid) or {}
                    if _prog:
                        configs["restore_cycles_completed"] = int(
                            _prog.get("cycles_completed") or 0
                        )
                        configs["restore_funding_usd"] = _prog.get("funding_earned_usd") or 0
                        # OPTION-1 anti-doubling reconcile: a rebuild (worker
                        # handoff / process restart) loses the in-memory
                        # controller, and RUNTIME.start clears the engine's
                        # inventory view — so on_start would open a SECOND hedge
                        # on top of the prior instance's still-open venue legs,
                        # orphaning the originals (no executor handle manages
                        # them). True "resume" needs executor rehydration; until
                        # then, if the persisted phase shows a cycle was in
                        # progress, FLATTEN any existing DN legs on the venue
                        # before re-opening so the new cycle starts from flat —
                        # never 2x exposure, never an unmanaged orphan. Closing
                        # nothing (already flat) is a safe no-op.
                        _phase = str(_prog.get("phase") or "").upper()
                        if _phase in {"OPENING", "HOLDING", "CLOSING"}:
                            try:
                                from src.nadobro.services.async_utils import run_blocking_sdk
                                from src.nadobro.services.trade_service import (
                                    close_delta_neutral_legs,
                                )
                                logger.warning(
                                    "dn rebuild mid-cycle (phase=%s) — flattening any "
                                    "existing legs before re-open user=%s product=%s",
                                    _phase, telegram_id, product,
                                )
                                await run_blocking_sdk(
                                    close_delta_neutral_legs,
                                    telegram_id, product, network,
                                    source="dn_rebuild_reconcile",
                                )
                            except Exception:  # noqa: BLE001 - best-effort safety net
                                logger.warning(
                                    "dn rebuild reconcile flatten failed user=%s product=%s",
                                    telegram_id, product, exc_info=True,
                                )
                except Exception:  # noqa: BLE001 - restore is best-effort, never block start
                    logger.debug("dn progress restore skipped", exc_info=True)
        # Fill-anchored (grid/rgrid) exposure VWAP must be peculiar to THIS
        # session + user. Seed it from the run's OWN recorded fills
        # (get_session_recent_fills is scoped by strategy_session_id + user_id) so
        # the anchor is provably session-scoped and survives a rebuild — never the
        # whole platform or another run/strategy. Empty for a fresh session.
        if configs.get("controller_override") == "fill_anchored":
            try:
                from src.nadobro.services.engine_persistence import resolve_running_session_id
                from src.nadobro.models.database import get_session_recent_fills

                _sid = resolve_running_session_id(strategy, telegram_id, network)
                if _sid:
                    configs["seed_fills"] = get_session_recent_fills(
                        int(_sid), network, limit=_FILL_HISTORY_SEED, user_id=telegram_id
                    )
            except Exception:  # noqa: BLE001 - seeding is best-effort, never block start
                logger.debug("fill-anchored seed_fills skipped", exc_info=True)
        # Wire placement-time digest→session linking so every venue fill is
        # attributed to this run (source='strategy') even when the executor's own
        # fill detection misses it — the root fix for session-volume undercount.
        _placement_recorder = getattr(RUNTIME, "_trade_recorder", None)
        _placement_cid = deterministic_controller_id(strategy, telegram_id, network)

        def _link_placed_digest(digest: str) -> None:
            if _placement_recorder is not None and hasattr(_placement_recorder, "link_placement"):
                _placement_recorder.link_placement(_placement_cid, digest)

        adapter = build_adapter(client, meta, on_place=_link_placed_digest)
        started_controller = await RUNTIME.start(
            telegram_id, network, strategy, configs, adapter, DbInventoryRepository(),
            limits=limits,
        )
        # Fail loudly when the controller could not start. If on_start raised
        # (e.g. a leg rejected by the risk gate or the venue), spawn_controller
        # marks the controller FAILED and EngineRuntime.start unregisters it.
        # Returning success here is what produced the silent "LIVE but 0 orders"
        # state — instead, surface the reason and let the caller tear down.
        registered = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        orch = RUNTIME._orchestrators.get((telegram_id, network, strategy))      # noqa: SLF001
        start_failed = registered is None or (
            started_controller is not None
            and started_controller.state is ControllerState.FAILED
        )
        if start_failed:
            reason = (
                getattr(started_controller, "_start_error", None)
                or "controller failed to start"
            )
            logger.error(
                "engine start FAILED user=%s network=%s strategy=%s reason=%s",
                telegram_id, network, strategy, reason,
            )
            try:
                await RUNTIME.stop(telegram_id, network, strategy)
            except Exception:  # noqa: BLE001
                logger.warning("engine start cleanup failed after spawn failure", exc_info=True)
            return {
                "success": False,
                "error": _friendly_start_error(str(reason)),
                "action": "engine_start_failed",
                "strategy": strategy,
            }
        active_n = len(orch.list(registered.id, active_only=True)) if (registered and orch) else 0
        logger.info(
            "engine_started user=%s network=%s strategy=%s active_executors=%s",
            telegram_id, network, strategy, active_n,
        )
        # Seed progress immediately so /status shows it right after Start
        # (DN spawns on_start, so there's no follow-up tick to wait for).
        _persist_dn_progress(telegram_id, network, strategy)
        action = "engine_recovered" if needs_recovery else "engine_started"
        if needs_recovery:
            state["last_recovery_ts"] = time.time()
        start_result: Dict[str, Any] = {"success": True, "action": action, "strategy": strategy}
        try:
            counts_fn = getattr(registered, "order_counts", None)
            if callable(counts_fn):
                _counts = counts_fn() or {}
                if _counts:
                    start_result["order_counts"] = _counts
            if strategy == "vol":
                vol_metrics_fn = getattr(registered, "volume_metrics", None)
                if callable(vol_metrics_fn):
                    _vol_metrics = vol_metrics_fn() or {}
                    start_result.update(_vol_metrics)
                    for key, value in _vol_metrics.items():
                        state[key] = value
        except Exception:  # noqa: BLE001 - start telemetry is best-effort
            logger.debug("engine start telemetry skipped", exc_info=True)
        return start_result

    if strategy in _LIVE_RECONFIGURABLE_STRATEGIES and _has_local:
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        orch = RUNTIME._orchestrators.get((telegram_id, network, strategy))      # noqa: SLF001
        if controller is not None and orch is not None:
            old_sig = _live_config_signature(controller.configs, controller.limits)
            new_sig = _live_config_signature(configs, limits)
            if old_sig != new_sig:
                await _apply_live_controller_update(strategy, controller, orch, configs, limits, mid)
                logger.info(
                    "engine live config updated user=%s network=%s strategy=%s",
                    telegram_id, network, strategy,
                )

    await RUNTIME.tick(telegram_id, network, strategy)
    # Regime-gate transition: surfaced exactly once per QUOTE<->PAUSE flip so
    # bot_runtime can notify the user ("paused — trending; resumes on range").
    gate_event = None
    dn_events: list = []
    dgrid_event = None
    dgrid_metrics: Dict[str, Any] = {}
    grid_metrics: Dict[str, Any] = {}
    order_counts: Dict[str, Any] = {}
    vol_metrics: Dict[str, Any] = {}
    try:
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        if controller is not None:
            gate_event = controller.consume_gate_event()
            consume_dn = getattr(controller, "consume_dn_events", None)
            if callable(consume_dn):
                dn_events = consume_dn() or []
            # Dynamic Grid: surface the GRID<->RGRID flip (notify once) and the
            # live phase/variance telemetry (powers the /status dashboard,
            # which previously always read GRID / 0.00 / 0.0bp).
            consume_dgrid = getattr(controller, "consume_dgrid_event", None)
            if callable(consume_dgrid):
                dgrid_event = consume_dgrid()
            metrics_fn = getattr(controller, "dgrid_metrics", None)
            if callable(metrics_fn):
                dgrid_metrics = metrics_fn() or {}
            # Grid / Reverse Grid anchor/side/reset telemetry for /status.
            grid_metrics_fn = getattr(controller, "grid_metrics", None)
            if callable(grid_metrics_fn):
                grid_metrics = grid_metrics_fn() or {}
            # Real venue-order activity (placed/filled/cancelled) — the cycle
            # result otherwise carries no count, leaving the per-cycle log and
            # /status stuck at 0 for engine strategies.
            counts_fn = getattr(controller, "order_counts", None)
            if callable(counts_fn):
                order_counts = counts_fn() or {}
            if strategy == "vol":
                vol_metrics_fn = getattr(controller, "volume_metrics", None)
                if callable(vol_metrics_fn):
                    vol_metrics = vol_metrics_fn() or {}
                    for key, value in vol_metrics.items():
                        state[key] = value
            # DN: surface the live funding rate + unfavorable-debounce to
            # /status. These were referenced by the status card but NEVER
            # written anywhere, so the dashboard showed "Rate 0.000000"
            # permanently. The controller refreshes last_funding_rate each
            # (throttled) HOLDING poll; persist it into runtime state here.
            if strategy == "dn":
                _fr = getattr(controller, "last_funding_rate", None)
                if _fr is not None:
                    try:
                        state["dn_last_funding_rate"] = float(_fr)
                    except (TypeError, ValueError):
                        pass
                state["dn_unfavorable_count"] = int(
                    getattr(controller, "funding_unfavorable_count", 0) or 0
                )
    except Exception:  # policy: degrade-ok(gate notify is best-effort)
        gate_event = None
        vol_metrics = {}
    # Persist DN live progress so /status (main process) can read it from the
    # worker that runs the cycle.
    _persist_dn_progress(telegram_id, network, strategy)
    # NO_ORDERS_AUDIT-FIX-DIAG: capture the decisive "why no orders" facts for
    # the grid family (gate verdict/reason, candle count, mid, active executors,
    # last spawn-refusal reason) so bot_runtime can log them in the SERVICES
    # stream — the controllers log these but under engine.controllers, which is
    # easy to miss. Surfaced as result["engine_diag"]; cheap, best-effort.
    engine_diag: Dict[str, Any] = {}
    try:
        orch = RUNTIME._orchestrators.get((telegram_id, network, strategy))  # noqa: SLF001
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        if controller is not None:
            active_n = len(orch.list(controller.id, active_only=True)) if orch is not None else None
            spawn_reason = orch.last_spawn_reason(controller.id) if orch is not None else None
            engine_diag = {
                "active_executors": active_n,
                "gate_verdict": getattr(controller, "gate_verdict", None),
                "gate_reason": getattr(controller, "gate_reason", None) or "",
                "gate_paused": bool(getattr(controller, "gate_paused", False)),
                "candle_count": int(getattr(controller, "_last_candle_count", 0) or 0),
                "mid": str(getattr(controller, "_last_mid", None)),
                "phase": getattr(controller, "current_phase", None),
                "variance_ratio": float(getattr(controller, "variance_ratio", 0.0) or 0.0),
                "spawn_refused": spawn_reason,
            }
            logger.debug(
                "engine_ticked user=%s strategy=%s active_executors=%s",
                telegram_id, strategy, active_n,
            )
    except Exception:  # noqa: BLE001  # policy: degrade-ok(diagnostics-only block)
        engine_diag = {}
    result: Dict[str, Any] = {"success": True, "action": "engine_ticked", "strategy": strategy}
    if vol_metrics:
        result.update(vol_metrics)
    # VOL-LOOP completion: a controller that has finished its work (e.g. Volume
    # reached its target volume / cap) signals it via ``completed`` so bot_runtime
    # can finalize the session instead of leaving the strategy idling "running".
    try:
        _ctrl_done = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        if _ctrl_done is not None and getattr(_ctrl_done, "completed", False):
            result["done"] = True
            result["action"] = "engine_completed"
            result["stop_reason"] = str(getattr(_ctrl_done, "stop_reason", "") or "completed")
            _vol_done = getattr(_ctrl_done, "session_volume_usd", None)
            if _vol_done is not None:
                result["session_volume_usd"] = float(_vol_done)
    except Exception:  # noqa: BLE001 - completion surfacing is best-effort
        logger.debug("completion surface failed", exc_info=True)
    if engine_diag:
        result["engine_diag"] = engine_diag
    if gate_event:
        result["gate_event"] = gate_event
    if dn_events:
        result["dn_events"] = dn_events
    if dgrid_metrics:
        result["dgrid_metrics"] = dgrid_metrics
    if grid_metrics:
        result["grid_metrics"] = grid_metrics
    if order_counts:
        result["order_counts"] = order_counts
    if dgrid_event:
        result["dgrid_event"] = dgrid_event
    return result
