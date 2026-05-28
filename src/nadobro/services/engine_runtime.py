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

import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.controllers.controller_base import Controller, ControllerState
from src.nadobro.engine.controllers.copy_trading import CopyController
from src.nadobro.engine.controllers.delta_neutral import DeltaNeutralController
from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.controllers.reverse_grid import ReverseGridController
from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.risk import RiskEngine
from src.nadobro.engine.types import RiskLimits, RiskState, TripleBarrierConfig, _dec

logger = logging.getLogger(__name__)

# Strategy id (bot_runtime's keys) -> engine controller class.
CONTROLLER_REGISTRY: Dict[str, type] = {
    "grid": GridController,
    "rgrid": ReverseGridController,
    "dgrid": DynamicGridController,
    "mid": MarketMakingController,
    "dn": DeltaNeutralController,
    "vol": VolumeBotController,
    "copy": CopyController,
}


# --------------------------------------------------------------------------
# construction
# --------------------------------------------------------------------------
def build_adapter(client: object, products: Dict[str, object]) -> NadoAdapterBase:
    """Construct the live Nado adapter from a NadoClient + product-metadata map.
    ``products`` maps trading_pair -> ProductMeta (see adapter/nado.py)."""
    from src.nadobro.engine.adapter.nado import NadoAdapter

    return NadoAdapter(client, products)  # type: ignore[arg-type]


def build_risk_engine(limits: Optional[RiskLimits] = None) -> RiskEngine:
    from src.nadobro.services.engine_persistence import DbKillSwitchStore

    return RiskEngine(limits or RiskLimits(), kill_switch=DbKillSwitchStore())


def build_orchestrator(
    *,
    limits: Optional[RiskLimits] = None,
    risk_state_provider: Optional[Any] = None,
) -> ExecutorOrchestrator:
    return ExecutorOrchestrator(
        risk_engine=build_risk_engine(limits),
        risk_state_provider=risk_state_provider or (lambda _cid: RiskState()),
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

    def __init__(self, *, executor_store: Optional[object] = None) -> None:
        self._controllers: Dict[tuple, Controller] = {}
        self._orchestrators: Dict[tuple, ExecutorOrchestrator] = {}
        self._executor_store = executor_store

    def _key(self, user_id: int, network: str, strategy: str) -> tuple:
        return (user_id, network, strategy)

    def is_running(self, user_id: int, network: str, strategy: str) -> bool:
        c = self._controllers.get(self._key(user_id, network, strategy))
        if c is not None and c.is_active:
            return True
        # BUG-ER-2 fix: cross-process visibility. Another worker process
        # may have started this strategy; check the engine_executors table
        # for any non-terminated rows under the deterministic controller id.
        return _remote_active(strategy, user_id, network)

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

        orch = build_orchestrator(limits=limits, risk_state_provider=risk_state_provider)
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
        if orch is not None and controller is not None:
            await orch.stop_controller(controller.id)
            self._persist_executors(orch)
        self._controllers.pop(key, None)
        self._orchestrators.pop(key, None)

    def _persist_executors(self, orch: ExecutorOrchestrator) -> None:
        if self._executor_store is None:
            return
        for ex in orch.list():
            try:
                self._executor_store.save(ex)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - persistence must not break a tick
                logger.warning("executor persistence failed for %s", ex.id, exc_info=True)


def _remote_active(strategy: str, user_id: int, network: str) -> bool:
    """Check the engine_executors table for non-terminated rows under the
    deterministic controller id. Used by ``EngineRuntime.is_running`` to
    detect strategies started by *another worker process* (BUG-ER-2).
    Defensive: returns False on any DB failure so a transient error does
    not block strategy startup entirely.
    """
    try:
        from src.nadobro.db import query_count
    except Exception:  # noqa: BLE001
        return False
    cid = deterministic_controller_id(strategy, user_id, network)
    try:
        return bool(query_count(
            "SELECT 1 FROM engine_executors "
            "WHERE controller_id = %s AND state <> 'TERMINATED'",
            (cid,),
        ))
    except Exception:  # noqa: BLE001
        return False


def _default_runtime() -> EngineRuntime:
    from src.nadobro.services.engine_persistence import DbExecutorStore

    return EngineRuntime(executor_store=DbExecutorStore())


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


def map_strategy_config(
    strategy: str, settings: Dict[str, Any], mid: Decimal, *, product: str, leverage: int = 1
) -> Dict[str, object]:
    """Derive an engine controller config from a user's saved strategy settings
    + current mid. Documented, testnet-tunable mappings (not 1:1 with legacy).
    """
    mid = _dec(mid)
    notional = _f(settings, "cycle_notional_usd", _f(settings, "notional_usd", 100.0))
    spread_frac = Decimal(str(_f(settings, "spread_bp", 5.0))) / Decimal(10000)
    levels = max(1, int(_f(settings, "levels", 2)))
    tp = Decimal(str(_f(settings, "tp_pct", 0.6))) / Decimal(100)
    sl = Decimal(str(_f(settings, "sl_pct", 0.5))) / Decimal(100)

    if strategy == "mid":
        return {
            "trading_pair": product,
            "spread_bid_pct": spread_frac,
            "spread_ask_pct": spread_frac,
            "order_amount_quote": Decimal(str(notional)) / Decimal(levels),
            "max_base_quote": Decimal(str(_f(settings, "inventory_soft_limit_usd", notional))),
            "price_distance_tolerance": (spread_frac / Decimal(2)) or Decimal("0.0005"),
            "leverage": leverage,
        }
    if strategy == "dn":
        # NO_ORDERS_AUDIT-FIX-R1: DN config keys for DeltaNeutralController.
        # The controller expects trading_pair_long/short, hedge_ratio,
        # leg_amount_quote, max_drift_pct, and barriers. ``product`` here is
        # the base symbol (e.g. "BTC"); the long leg is the SPOT pair and the
        # short leg is the PERP. The adapter resolves the trading_pair string
        # against the product catalog via build_product_meta_from_catalog.
        from src.nadobro.engine.types import TripleBarrierConfig as _TBC

        base = str(product or "").upper().split("-", 1)[0]
        long_pair = f"{base}-USDC0"        # spot leg
        short_pair = f"{base}-PERP"         # perp leg
        hedge_ratio = Decimal(str(_f(settings, "dn_hedge_ratio", 1.0)))
        leg_quote = Decimal(str(_f(settings, "fixed_margin_usd", notional)))
        max_drift = Decimal(str(_f(settings, "dn_max_drift_pct", 5.0))) / Decimal(100)
        return {
            "trading_pair": long_pair,  # for parent base-class .trading_pair if read
            "trading_pair_long": long_pair,
            "trading_pair_short": short_pair,
            "hedge_ratio": hedge_ratio,
            "leg_amount_quote": leg_quote,
            "max_drift_pct": max_drift,
            "barriers": _TBC(take_profit=tp or None, stop_loss=sl or None),
            "leverage": leverage,
        }
    if strategy == "vol":
        interval = max(1.0, _f(settings, "interval_seconds", 60))
        # Normalize the trading pair so the VolumeBotController validation
        # sees a canonical base (e.g. ``KBTC``) regardless of whether
        # ``state.product`` was stored as ``KBTC`` (current UI) or as a
        # dashed pair like ``KBTC-USDC0`` (legacy/tests).
        try:
            from src.nadobro.config import normalize_volume_spot_symbol

            vol_pair = normalize_volume_spot_symbol(str(product or "")) or str(product or "")
        except Exception:
            vol_pair = str(product or "")
        return {
            "trading_pair": vol_pair,
            "total_amount_quote": Decimal(str(notional)),
            "total_duration": interval * 4,
            "order_interval": interval,
            "market": "spot",
            "leverage": 1,
        }
    # grid / rgrid / dgrid family.
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

    if strategy == "rgrid":
        start_price = mid
        end_price = mid * (Decimal(1) + span)
        limit_price = mid * (Decimal(1) + sl) if sl > 0 else Decimal(0)
    else:  # grid OR dgrid-as-long-default; dgrid recomputes at on_tick
        start_price = mid * (Decimal(1) - span)
        end_price = mid
        limit_price = mid * (Decimal(1) - sl) if sl > 0 else Decimal(0)

    cfg: Dict[str, object] = {
        "trading_pair": product,
        "start_price": start_price,
        "end_price": end_price,
        "limit_price": limit_price,
        "total_amount_quote": Decimal(str(notional)),
        "min_spread_between_orders": spread_frac,
        "max_open_orders": levels,
        "leverage": leverage,
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
    }
    # NO_ORDERS_AUDIT-FIX-R2: DynamicGridController requires a candle_provider
    # callable to classify the volatility regime. Without one, _candles()
    # returns [] and on_tick exits early — no executor ever spawned, no orders
    # placed. We bind the provider in ``run_engine_cycle`` because it needs
    # access to the live ``client`` and ``product_id`` to call
    # ``client.get_candlesticks(...)``; setting ``"candle_provider": None``
    # here makes the contract explicit and lets the cycle driver inject the
    # real provider on first start.
    if strategy == "dgrid":
        cfg["candle_provider"] = None
    return cfg


def map_risk_limits(settings: Dict[str, Any]) -> RiskLimits:
    notional = _f(settings, "notional_usd", 100.0)
    levels = max(1, int(_f(settings, "levels", 2)))
    cap = _f(settings, "session_notional_cap_usd", 0.0) or (notional * levels)
    return RiskLimits(
        max_open_executors=levels + 2,
        max_single_order_quote=Decimal(str(notional)),
        max_position_size_quote=Decimal(str(cap)),
    )


def build_product_meta_from_catalog(client: object) -> Dict[str, object]:
    """Best-effort {trading_pair -> ProductMeta} from the live product catalog.
    Field names confirmed via the live catalog on testnet; defensive here."""
    from src.nadobro.engine.adapter.nado import ProductMeta

    out: Dict[str, object] = {}
    try:
        catalog = client.get_all_products_info()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        logger.warning("product catalog unavailable", exc_info=True)
        return out
    items = catalog if isinstance(catalog, list) else (catalog or {}).get("products", [])
    for p in items or []:
        if not isinstance(p, dict):
            continue
        pair = str(p.get("symbol") or p.get("product_name") or p.get("name") or "")
        pid = p.get("product_id") or p.get("id")
        if not pair or pid is None:
            continue
        out[pair] = ProductMeta(
            product_id=int(pid),
            tick_size=_dec(p.get("tick_size", "0.01")),
            lot_size=_dec(p.get("lot_size", p.get("min_size", "0.001"))),
            min_notional=_dec(p.get("min_notional", "1")),
        )
    return out


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
    configs = map_strategy_config(strategy, settings, _dec(mid), product=product,
                                  leverage=int(_f(settings, "leverage", 1)))
    limits = map_risk_limits(settings)

    # NO_ORDERS_AUDIT-FIX-R2: inject the dgrid candle_provider HERE, because
    # this is where ``client`` and ``product_id`` are available. The provider
    # closure is recreated on every cycle, which is fine — the controller only
    # caches a reference at on_start.
    if strategy == "dgrid" and configs.get("candle_provider") is None:
        _cli = client
        _pid = int(product_id)

        def _candle_provider(_pair: str) -> list:
            try:
                # 1m candles, last 200 — enough for ema_slow_period=50 + atr_window=14.
                return _cli.get_candlesticks(_pid, timeframe="1m", limit=200) or []  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - never let candle fetch break a tick
                logger.warning(
                    "dgrid candle_provider failed for pair=%s product_id=%s",
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
    if needs_recovery or not RUNTIME.is_running(telegram_id, network, strategy):
        meta = build_product_meta_from_catalog(client)
        # ensure the traded pair has metadata (fallback to a permissive default)
        if product not in meta:
            from src.nadobro.engine.adapter.nado import ProductMeta

            meta[product] = ProductMeta(int(product_id), _dec("0.01"), _dec("0.001"), _dec("1"))
        # NO_ORDERS_AUDIT-FIX-R1: DN needs metadata for BOTH legs (spot long +
        # perp short). If either is absent the adapter raises "Unknown trading
        # pair" inside the controller's first call, the exception is swallowed
        # by ``_guard``, and we never see the cause. Materialize both keys
        # from the configs so DN can spawn legs.
        if strategy == "dn":
            from src.nadobro.engine.adapter.nado import ProductMeta

            for key in ("trading_pair_long", "trading_pair_short"):
                pair = str(configs.get(key) or "")
                if pair and pair not in meta:
                    # Permissive defaults — real values come from the live
                    # catalog when it's available.
                    meta[pair] = ProductMeta(int(product_id), _dec("0.01"),
                                              _dec("0.001"), _dec("1"))
                    logger.warning(
                        "dn: no catalog metadata for %s; using permissive defaults",
                        pair,
                    )
        adapter = build_adapter(client, meta)
        await RUNTIME.start(
            telegram_id, network, strategy, configs, adapter, DbInventoryRepository(),
            limits=limits,
        )
        # NO_ORDERS_AUDIT-FIX-DIAG: log post-start executor count so an
        # operator can immediately see "controller started but spawned 0
        # executors" — the exact symptom this audit chased.
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        orch = RUNTIME._orchestrators.get((telegram_id, network, strategy))      # noqa: SLF001
        active_n = len(orch.list(controller.id, active_only=True)) if (controller and orch) else 0
        if controller is None:
            logger.error(
                "engine_started but controller is None user=%s network=%s strategy=%s "
                "— spawn_controller likely refused (risk/kill switch)",
                telegram_id, network, strategy,
            )
        elif active_n == 0 and strategy in ("grid", "rgrid", "vol", "dn"):
            # These strategies spawn executors in on_start; zero here means
            # on_start did nothing. dgrid/mid spawn on first tick, so allow.
            logger.warning(
                "engine_started but 0 executors for user=%s network=%s strategy=%s "
                "— controller on_start did not spawn any executors; check configs",
                telegram_id, network, strategy,
            )
        else:
            logger.info(
                "engine_started user=%s network=%s strategy=%s active_executors=%s",
                telegram_id, network, strategy, active_n,
            )
        action = "engine_recovered" if needs_recovery else "engine_started"
        if needs_recovery:
            state["last_recovery_ts"] = time.time()
        return {"success": True, "action": action, "strategy": strategy}

    await RUNTIME.tick(telegram_id, network, strategy)
    # NO_ORDERS_AUDIT-FIX-DIAG: surface executor count after each tick too,
    # bucketed at INFO every ~10 ticks. Comment out if too chatty.
    try:
        orch = RUNTIME._orchestrators.get((telegram_id, network, strategy))  # noqa: SLF001
        controller = RUNTIME._controllers.get((telegram_id, network, strategy))  # noqa: SLF001
        if orch is not None and controller is not None:
            active_n = len(orch.list(controller.id, active_only=True))
            logger.debug(
                "engine_ticked user=%s strategy=%s active_executors=%s",
                telegram_id, strategy, active_n,
            )
    except Exception:  # noqa: BLE001
        pass
    return {"success": True, "action": "engine_ticked", "strategy": strategy}
