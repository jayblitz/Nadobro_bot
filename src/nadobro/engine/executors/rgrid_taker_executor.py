"""R-Grid taker executor — the only way Reverse Grid touches the venue.

Reverse Grid is defined by crossing the spread: "buy orders are placed at a price
equal to or above the average of your buy and sell exposure prices … they only
fill when the market price rises above this buy limit price". A resting limit at
that price would be a *maker* order on the wrong side of the book (a buy above
the market crosses immediately, and post-only gets it rejected), so the strategy
acts with a MARKET order the moment the trigger breaks. Passive-only is off, by
design and by the user's explicit rule.

This executor exists to make that structural rather than conventional:

* ``ExecutionStrategy.MARKET`` is enforced in the constructor. A future edit that
  quietly hands R-Grid a LIMIT/LIMIT_MAKER config raises instead of silently
  turning the strategy into a maker book that never fills on a break.
* ``intent`` records WHY the taker fired (an entry break, or the trailing soft
  reset banking a run) so fills stay attributable in the logs and the exposure
  window can exclude exits (the anchor must only reflect risk taken ON).

Everything else — placement, fill accumulation, inventory, barriers — is
:class:`OrderExecutor`'s, unchanged.
"""
from __future__ import annotations

from typing import Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import ExecutionStrategy, PositionAction, TradeType, _dec

# Why a taker fired. Kept as plain strings so it survives persistence/telemetry.
INTENT_ENTRY = "entry"          # a break of the exposure band — takes risk ON
INTENT_TRAIL_EXIT = "trail"     # the trailing soft reset banking a run
INTENT_REVERSAL = "reversal"    # the break went AGAINST the book — flatten, re-anchor
INTENT_REDUCE = "reduce"        # any other reduce-only flatten


class RGridTakerExecutor(OrderExecutor):
    """One market order for Reverse Grid. Taker-only, by construction."""

    def __init__(
        self,
        config: OrderExecutorConfig,
        *,
        user_id: int,
        controller_id: str,
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        executor_id: Optional[str] = None,
        intent: str = INTENT_ENTRY,
    ) -> None:
        if config.execution_strategy is not ExecutionStrategy.MARKET:
            raise ValueError(
                "RGridTakerExecutor is taker-only: Reverse Grid must cross the "
                "spread on a break, so execution_strategy must be MARKET "
                f"(got {config.execution_strategy.value})"
            )
        self.intent = str(intent or INTENT_ENTRY)
        super().__init__(
            config,
            user_id=user_id,
            controller_id=controller_id,
            adapter=adapter,
            inventory=inventory,
            executor_id=executor_id,
        )

    @property
    def is_exit(self) -> bool:
        """True when this taker REMOVED risk. The exposure anchor must skip these:
        an exit prints at whatever the market offered, and folding it back into the
        average drags the anchor to the exit price — which then re-triggers an
        entry within a tick of the close (two taker legs, no edge)."""
        return (
            self.config.position_action is PositionAction.CLOSE
            or self.intent in (INTENT_TRAIL_EXIT, INTENT_REVERSAL, INTENT_REDUCE)
        )


def build_entry_taker(
    trading_pair: str,
    side: TradeType,
    amount_base: object,
    *,
    leverage: int = 1,
) -> OrderExecutorConfig:
    """Config for a break entry: market, OPEN, taker."""
    return OrderExecutorConfig(
        trading_pair, side, _dec(amount_base), ExecutionStrategy.MARKET,
        leverage=int(leverage or 1),
        position_action=PositionAction.OPEN,
    )


def build_exit_taker(
    trading_pair: str,
    side: TradeType,
    amount_base: object,
    *,
    leverage: int = 1,
) -> OrderExecutorConfig:
    """Config for banking a run: market, CLOSE (reduce-only — never opens or
    flips), taker."""
    return OrderExecutorConfig(
        trading_pair, side, _dec(amount_base), ExecutionStrategy.MARKET,
        leverage=int(leverage or 1),
        position_action=PositionAction.CLOSE,
    )
