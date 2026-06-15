"""DB-backed persistence for the Engine v2 runtime (production wiring).

Real Postgres implementations (via ``db.py``) of the engine's persistence
seams, so position holds, executor lifecycle, and the risk kill switch survive
restarts and are visible cross-process:

- ``DbInventoryRepository`` — atomic ``apply_fill`` upsert into
  ``engine_position_hold``; satisfies the same interface as the in-memory
  ``InventoryRepository`` (and the Portfolio ``HoldsSource``).
- ``DbExecutorStore`` — upserts executor lifecycle rows into
  ``engine_executors``.
- ``DbKillSwitchStore`` — persists the risk kill switch in
  ``engine_kill_switch`` (migration 0009).

The engine library stays DB-agnostic; these live in the services layer and are
injected by the runtime owner. In-memory variants remain for unit tests.
"""
from __future__ import annotations

import dataclasses
import json
from decimal import Decimal
from typing import List, Optional

from src.nadobro.engine.inventory import PositionHold
from src.nadobro.engine.risk import KillSwitchStore
from src.nadobro.engine.types import TradeType, _dec

_STRATEGY_TYPES = {
    "OrderExecutor": "order",
    "PositionExecutor": "position",
    "GridExecutor": "grid",
    "ReverseGridExecutor": "reverse_grid",
    "DCAExecutor": "dca",
    "TWAPExecutor": "twap",
}


# --------------------------------------------------------------------------
# Inventory (engine_position_hold)
# --------------------------------------------------------------------------
def _row_to_hold(row: dict) -> PositionHold:
    return PositionHold(
        user_id=int(row["user_id"]),
        trading_pair=row["trading_pair"],
        controller_id=row["controller_id"],
        buy_amount_base=_dec(row["buy_amount_base"]),
        buy_amount_quote=_dec(row["buy_amount_quote"]),
        sell_amount_base=_dec(row["sell_amount_base"]),
        sell_amount_quote=_dec(row["sell_amount_quote"]),
        cum_fees_quote=_dec(row["cum_fees_quote"]),
    )


class DbInventoryRepository:
    """engine_position_hold-backed inventory. Same surface as the in-memory
    ``InventoryRepository`` so it is a drop-in for executors and Portfolio."""

    def apply_fill(
        self,
        user_id: int,
        trading_pair: str,
        controller_id: str,
        side: TradeType,
        base_qty: object,
        quote_qty: object,
        fee_quote: object = Decimal(0),
        timestamp: Optional[float] = None,
    ) -> PositionHold:
        from src.nadobro.db import execute_returning

        base, quote, fee = _dec(base_qty), _dec(quote_qty), _dec(fee_quote)
        if side is TradeType.BUY:
            bb, bq, sb, sq = base, quote, Decimal(0), Decimal(0)
        else:
            bb, bq, sb, sq = Decimal(0), Decimal(0), base, quote
        row = execute_returning(
            """
            INSERT INTO engine_position_hold
              (user_id, trading_pair, controller_id, buy_amount_base, buy_amount_quote,
               sell_amount_base, sell_amount_quote, cum_fees_quote, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (user_id, trading_pair, controller_id) DO UPDATE SET
              buy_amount_base   = engine_position_hold.buy_amount_base   + EXCLUDED.buy_amount_base,
              buy_amount_quote  = engine_position_hold.buy_amount_quote  + EXCLUDED.buy_amount_quote,
              sell_amount_base  = engine_position_hold.sell_amount_base  + EXCLUDED.sell_amount_base,
              sell_amount_quote = engine_position_hold.sell_amount_quote + EXCLUDED.sell_amount_quote,
              cum_fees_quote    = engine_position_hold.cum_fees_quote    + EXCLUDED.cum_fees_quote,
              updated_at = NOW()
            RETURNING *
            """,
            (user_id, trading_pair, controller_id, bb, bq, sb, sq, fee),
        )
        assert row is not None
        return _row_to_hold(row)

    def get(self, user_id: int, trading_pair: str, controller_id: str) -> PositionHold:
        from src.nadobro.db import query_one

        row = query_one(
            "SELECT * FROM engine_position_hold "
            "WHERE user_id=%s AND trading_pair=%s AND controller_id=%s",
            (user_id, trading_pair, controller_id),
        )
        if row is None:
            return PositionHold(user_id=user_id, trading_pair=trading_pair, controller_id=controller_id)
        return _row_to_hold(row)

    def list_for_user(self, user_id: int) -> List[PositionHold]:
        from src.nadobro.db import query_all

        return [_row_to_hold(r) for r in query_all(
            "SELECT * FROM engine_position_hold WHERE user_id=%s", (user_id,))]

    def list_for_controller(self, user_id: int, controller_id: str) -> List[PositionHold]:
        from src.nadobro.db import query_all

        return [_row_to_hold(r) for r in query_all(
            "SELECT * FROM engine_position_hold WHERE user_id=%s AND controller_id=%s",
            (user_id, controller_id))]


# --------------------------------------------------------------------------
# Executors (engine_executors)
# --------------------------------------------------------------------------
def _strategy_type(executor: object) -> str:
    name = type(executor).__name__
    return _STRATEGY_TYPES.get(name, name.lower())


def _side(executor: object) -> str:
    cfg = getattr(executor, "config", None)
    side = getattr(cfg, "side", None)
    if side is None:
        side = getattr(getattr(cfg, "order_config", None), "side", None)
    return str(getattr(side, "value", "BUY"))


def _config_json(executor: object) -> str:
    cfg = getattr(executor, "config", None)
    try:
        if cfg is not None and dataclasses.is_dataclass(cfg) and not isinstance(cfg, type):
            return json.dumps(dataclasses.asdict(cfg), default=str)
    except Exception:  # noqa: BLE001
        pass
    return "{}"


class DbExecutorStore:
    def save(self, executor: object) -> None:
        from src.nadobro.db import execute

        m = executor.metrics()  # type: ignore[attr-defined]
        close_type = getattr(executor, "close_type", None)
        close_type_val: Optional[str] = None
        if close_type is not None:
            close_type_val = close_type.value
        execute(
            """
            INSERT INTO engine_executors
              (id, user_id, controller_id, strategy_type, trading_pair, side, config_json,
               state, close_type, net_pnl_quote, fees_paid_quote, volume_quote,
               duration_seconds, keep_position, created_at, terminated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
                    to_timestamp(%s), to_timestamp(%s))
            ON CONFLICT (id) DO UPDATE SET
              state = EXCLUDED.state,
              close_type = EXCLUDED.close_type,
              net_pnl_quote = EXCLUDED.net_pnl_quote,
              fees_paid_quote = EXCLUDED.fees_paid_quote,
              volume_quote = EXCLUDED.volume_quote,
              duration_seconds = EXCLUDED.duration_seconds,
              terminated_at = EXCLUDED.terminated_at
            """,
            (
                executor.id, executor.user_id, executor.controller_id,  # type: ignore[attr-defined]
                _strategy_type(executor), executor.trading_pair, _side(executor),  # type: ignore[attr-defined]
                _config_json(executor), executor.state.value,  # type: ignore[attr-defined]
                close_type_val,
                m["net_pnl_quote"], m["fees_paid_quote"], m["volume_quote"],
                int(m["duration_seconds"]), executor.keep_position,  # type: ignore[attr-defined]
                executor.created_at, executor.terminated_at,  # type: ignore[attr-defined]
            ),
        )

    def get(self, executor_id: str) -> Optional[dict]:
        from src.nadobro.db import query_one

        return query_one("SELECT * FROM engine_executors WHERE id=%s", (executor_id,))


# --------------------------------------------------------------------------
# Kill switch (engine_kill_switch)
# --------------------------------------------------------------------------
class DbKillSwitchStore(KillSwitchStore):
    def __init__(self, scope: str = "global") -> None:
        self.scope = scope

    def is_engaged(self) -> bool:
        from src.nadobro.db import query_one

        row = query_one("SELECT engaged FROM engine_kill_switch WHERE scope=%s", (self.scope,))
        return bool(row and row["engaged"])

    def engage(self, reason: str) -> None:
        from src.nadobro.db import execute

        execute(
            "INSERT INTO engine_kill_switch (scope, engaged, reason, updated_at) "
            "VALUES (%s, TRUE, %s, NOW()) "
            "ON CONFLICT (scope) DO UPDATE SET engaged=TRUE, reason=EXCLUDED.reason, updated_at=NOW()",
            (self.scope, reason),
        )

    def disengage(self) -> None:
        from src.nadobro.db import execute

        execute(
            "INSERT INTO engine_kill_switch (scope, engaged, reason, updated_at) "
            "VALUES (%s, FALSE, NULL, NOW()) "
            "ON CONFLICT (scope) DO UPDATE SET engaged=FALSE, reason=NULL, updated_at=NOW()",
            (self.scope,),
        )

    def reason(self) -> Optional[str]:
        from src.nadobro.db import query_one

        row = query_one(
            "SELECT reason FROM engine_kill_switch WHERE scope=%s AND engaged=TRUE", (self.scope,)
        )
        return row["reason"] if row else None


# --------------------------------------------------------------------------
# Live controller progress (engine_controller_state)
# --------------------------------------------------------------------------
def upsert_controller_progress(
    controller_id: str,
    user_id: int,
    *,
    strategy: Optional[str] = None,
    network: Optional[str] = None,
    cycles_completed: int = 0,
    funding_earned_usd: object = Decimal(0),
    phase: Optional[str] = None,
) -> None:
    """Persist a controller's live progress so the main process (/status) can
    read it cross-process. Written each tick by the worker; best-effort —
    persistence must never break a cycle."""
    from src.nadobro.db import execute

    execute(
        """
        INSERT INTO engine_controller_state
          (controller_id, user_id, strategy, network, cycles_completed,
           funding_earned_usd, phase, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (controller_id) DO UPDATE SET
          cycles_completed   = EXCLUDED.cycles_completed,
          funding_earned_usd = EXCLUDED.funding_earned_usd,
          phase              = EXCLUDED.phase,
          updated_at         = NOW()
        """,
        (
            controller_id, int(user_id), strategy, network,
            int(cycles_completed), _dec(funding_earned_usd), phase,
        ),
    )


def get_controller_progress(controller_id: str) -> Optional[dict]:
    """Most recent persisted progress for ``controller_id``, or None."""
    from src.nadobro.db import query_one

    return query_one(
        "SELECT cycles_completed, funding_earned_usd, phase, updated_at "
        "FROM engine_controller_state WHERE controller_id=%s",
        (controller_id,),
    )


def clear_controller_progress(controller_id: str) -> None:
    """Remove a controller's progress row (called on stop). Best-effort."""
    from src.nadobro.db import execute

    execute("DELETE FROM engine_controller_state WHERE controller_id=%s", (controller_id,))


def count_engine_orders(controller_id: str) -> dict:
    """Per-controller order/position counts from engine_executors, so /status
    reflects real activity. The engine cycle result carries no counts, which is
    why the legacy order_observability stayed stuck at 0 for engine strategies.
    Each executor opens an entry order (placed) and, if it closed, a close order.
    """
    from src.nadobro.db import query_one

    row = query_one(
        """
        SELECT
          COUNT(*)                                              AS executors,
          COUNT(*) FILTER (WHERE terminated_at IS NOT NULL)     AS closed,
          COUNT(*) FILTER (WHERE volume_quote > 0)              AS filled,
          COUNT(*) FILTER (WHERE close_type = 'FAILED')         AS failed
        FROM engine_executors WHERE controller_id = %s
        """,
        (controller_id,),
    )
    if not row:
        return {"orders_placed": 0, "orders_filled": 0, "orders_cancelled": 0}
    executors = int(row.get("executors") or 0)
    closed = int(row.get("closed") or 0)
    # placed = one entry order per executor + one close order per closed executor.
    return {
        "orders_placed": executors + closed,
        "orders_filled": int(row.get("filled") or 0),
        "orders_cancelled": int(row.get("failed") or 0),
    }
