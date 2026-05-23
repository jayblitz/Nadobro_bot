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
from decimal import Decimal
from typing import Any, Dict, Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.controllers.controller_base import Controller
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
        return c is not None and c.is_active

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
        key = self._key(user_id, network, strategy)
        orch = build_orchestrator(limits=limits, risk_state_provider=risk_state_provider)
        controller = build_controller(
            strategy, user_id=user_id, configs=configs, orchestrator=orch,
            adapter=adapter, inventory=inventory, limits=limits,
        )
        self._orchestrators[key] = orch
        self._controllers[key] = controller
        await orch.spawn_controller(controller)
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
# Strategies the engine can drive today (dn/copy have their own subsystems and
# are mapped in a follow-up).
ENGINE_MAPPED_STRATEGIES = ("grid", "rgrid", "dgrid", "mid", "vol")


def engine_v2_enabled() -> bool:
    """Master switch for routing live strategy execution through the engine.
    Default OFF — flip on per testnet validation (NADO_ENGINE_V2_RUNTIME)."""
    return os.environ.get("NADO_ENGINE_V2_RUNTIME", "false").strip().lower() in (
        "1", "true", "yes", "on",
    )


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
    if strategy == "vol":
        interval = max(1.0, _f(settings, "interval_seconds", 60))
        return {
            "trading_pair": product,
            "total_amount_quote": Decimal(str(notional)),
            "total_duration": interval * 4,
            "order_interval": interval,
            "market": "spot",
            "leverage": 1,
        }
    # grid / rgrid / dgrid family: center a band on mid
    band = mid * spread_frac * Decimal(levels)
    return {
        "trading_pair": product,
        "start_price": mid - band,
        "end_price": mid + band,
        # hard stop: below for long grids, above for the short (reverse) grid
        "limit_price": (mid * (Decimal(1) + sl)) if strategy == "rgrid" else (mid * (Decimal(1) - sl)),
        "total_amount_quote": Decimal(str(notional)),
        "min_spread_between_orders": spread_frac,
        "max_open_orders": levels,
        "leverage": leverage,
        "triple_barrier_config": TripleBarrierConfig(
            take_profit=tp or None, stop_loss=sl or None
        ),
    }


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

    if not RUNTIME.is_running(telegram_id, network, strategy):
        meta = build_product_meta_from_catalog(client)
        # ensure the traded pair has metadata (fallback to a permissive default)
        if product not in meta:
            from src.nadobro.engine.adapter.nado import ProductMeta

            meta[product] = ProductMeta(int(product_id), _dec("0.01"), _dec("0.001"), _dec("1"))
        adapter = build_adapter(client, meta)
        await RUNTIME.start(
            telegram_id, network, strategy, configs, adapter, DbInventoryRepository(),
            limits=limits,
        )
        return {"success": True, "action": "engine_started", "strategy": strategy}

    await RUNTIME.tick(telegram_id, network, strategy)
    return {"success": True, "action": "engine_ticked", "strategy": strategy}
