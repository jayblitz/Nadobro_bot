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
from src.nadobro.engine.types import RiskLimits, RiskState

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
