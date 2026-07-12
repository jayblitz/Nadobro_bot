"""Persistence for Desk execution plans (``desk_plans_{network}``).

Lifecycle: draft -> awaiting_trigger -> running -> completed/cancelled/failed.
Every transition is a guarded UPDATE (``... WHERE status = <expected>``), so
restarts and concurrent workers cannot double-fire a trigger or resurrect a
cancelled plan — whoever wins the UPDATE owns the transition. Plans do NOT
survive a redeploy: trading is strictly user-initiated, so the runner stands
every still-active plan down on boot (see ``desk_runtime._stand_down_on_boot``)
instead of re-attaching, unless ``NADO_DESK_RESUME_ON_RESTART=1``.

All functions are synchronous DB calls — call via ``run_blocking`` from
handlers/coroutines (the blocking-calls lint enforces this).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from src.nadobro.db import execute, execute_returning, query_all, query_one
from src.nadobro.trading.desk_plans import (
    ACTIVE_STATUSES,
    ST_AWAITING_TRIGGER,
    ST_CANCELLED,
    ST_DRAFT,
    ST_FAILED,
    ST_RUNNING,
    ExecutionPlan,
)

logger = logging.getLogger(__name__)

_VALID_NETWORKS = ("mainnet", "testnet")


def _table(network: str) -> str:
    net = str(network or "mainnet").lower()
    if net not in _VALID_NETWORKS:
        raise ValueError(f"unknown network {network!r}")
    return f"desk_plans_{net}"


def _utc_midnight() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def row_to_record(row: dict) -> dict[str, Any]:
    """DB row -> {plan: ExecutionPlan, status, state, ...} bundle."""
    plan_d = {}
    try:
        plan_d = json.loads(row.get("plan_json") or "{}")
    except Exception:
        logger.exception("desk_store: corrupt plan_json for plan_id=%s", row.get("plan_id"))
    state = {}
    try:
        state = json.loads(row.get("state_json") or "{}") or {}
    except Exception:
        state = {}
    return {
        "row_id": row.get("id"),
        "user_id": row.get("user_id"),
        "plan_id": row.get("plan_id"),
        "status": row.get("status"),
        "plan": ExecutionPlan.from_dict(plan_d),
        "state": state,
        "error": row.get("error"),
        "created_at": row.get("created_at"),
        "confirmed_at": row.get("confirmed_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
    }


# -- writes -----------------------------------------------------------------

def insert_draft(telegram_id: int, plan: ExecutionPlan, network: str) -> Optional[int]:
    row = execute_returning(
        f"""INSERT INTO {_table(network)} (user_id, plan_id, status, plan_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (plan_id) DO NOTHING
            RETURNING id""",
        (int(telegram_id), plan.plan_id, ST_DRAFT, json.dumps(plan.to_dict())),
    )
    return row["id"] if row else None


def confirm_plan(plan_id: str, telegram_id: int, network: str, plan: ExecutionPlan) -> bool:
    """draft -> awaiting_trigger; re-persists the plan (triggers are resolved
    to absolute terms at confirm time, so the stored JSON must be updated)."""
    row = execute_returning(
        f"""UPDATE {_table(network)}
            SET status = %s, plan_json = %s, confirmed_at = now()
            WHERE plan_id = %s AND user_id = %s AND status = %s
            RETURNING id""",
        (ST_AWAITING_TRIGGER, json.dumps(plan.to_dict()), plan_id, int(telegram_id), ST_DRAFT),
    )
    return bool(row)


def claim_trigger_fire(plan_id: str, network: str) -> bool:
    """awaiting_trigger -> running. Guarded: exactly one caller wins, so a
    restart-then-double-tick can never start the entry leg twice."""
    row = execute_returning(
        f"""UPDATE {_table(network)}
            SET status = %s, started_at = now()
            WHERE plan_id = %s AND status = %s
            RETURNING id""",
        (ST_RUNNING, plan_id, ST_AWAITING_TRIGGER),
    )
    return bool(row)


def finish_plan(plan_id: str, network: str, status: str, *, error: Optional[str] = None) -> bool:
    """running/awaiting_trigger -> terminal status."""
    row = execute_returning(
        f"""UPDATE {_table(network)}
            SET status = %s, error = %s, finished_at = now()
            WHERE plan_id = %s AND status = ANY(%s)
            RETURNING id""",
        (status, error, plan_id, list(ACTIVE_STATUSES)),
    )
    return bool(row)


def cancel_plan(plan_id: str, telegram_id: int, network: str) -> bool:
    """User cancel: active -> cancelled. Resting orders are the runner's job
    to clean up (it observes the status flip on its next tick); fills are kept."""
    row = execute_returning(
        f"""UPDATE {_table(network)}
            SET status = %s, finished_at = now()
            WHERE plan_id = %s AND user_id = %s AND status = ANY(%s)
            RETURNING id""",
        (ST_CANCELLED, plan_id, int(telegram_id), list(ACTIVE_STATUSES)),
    )
    return bool(row)


def discard_draft(plan_id: str, telegram_id: int, network: str) -> bool:
    """Delete an unconfirmed draft (preview Cancel). Never touches active rows."""
    row = execute_returning(
        f"""DELETE FROM {_table(network)}
            WHERE plan_id = %s AND user_id = %s AND status = %s
            RETURNING id""",
        (plan_id, int(telegram_id), ST_DRAFT),
    )
    return bool(row)


def fail_plan(plan_id: str, network: str, error: str) -> bool:
    return finish_plan(plan_id, network, ST_FAILED, error=str(error)[:500])


def update_state(plan_id: str, network: str, state: dict) -> None:
    """Runner checkpoint (executor stage, filled totals, exit-leg state)."""
    execute(
        f"UPDATE {_table(network)} SET state_json = %s WHERE plan_id = %s",
        (json.dumps(state), plan_id),
    )


# -- reads --------------------------------------------------------------------

def get_plan(plan_id: str, network: str) -> Optional[dict]:
    row = query_one(f"SELECT * FROM {_table(network)} WHERE plan_id = %s", (plan_id,))
    return row_to_record(row) if row else None


def list_active_plans(telegram_id: int, network: str) -> list[dict]:
    rows = query_all(
        f"""SELECT * FROM {_table(network)}
            WHERE user_id = %s AND status = ANY(%s)
            ORDER BY created_at""",
        (int(telegram_id), list(ACTIVE_STATUSES)),
    )
    return [row_to_record(r) for r in rows or []]


def list_recent_plans(telegram_id: int, network: str, limit: int = 10) -> list[dict]:
    rows = query_all(
        f"""SELECT * FROM {_table(network)}
            WHERE user_id = %s AND status <> %s
            ORDER BY created_at DESC LIMIT %s""",
        (int(telegram_id), ST_DRAFT, int(limit)),
    )
    return [row_to_record(r) for r in rows or []]


def list_users_with_active_plans(network: str) -> list[int]:
    """Restore path: which users need a Desk session re-attached after restart."""
    rows = query_all(
        f"SELECT DISTINCT user_id FROM {_table(network)} WHERE status = ANY(%s)",
        (list(ACTIVE_STATUSES),),
    )
    return [int(r["user_id"]) for r in rows or []]


def count_confirmed_today(telegram_id: int, network: str) -> int:
    """Plans confirmed since UTC midnight — the daily-cap denominator.
    Drafts don't count; cancelled/failed DO (the cap throttles starts, and a
    cancel-respawn loop should not bypass it)."""
    row = query_one(
        f"""SELECT COUNT(*) AS n FROM {_table(network)}
            WHERE user_id = %s AND confirmed_at >= %s""",
        (int(telegram_id), _utc_midnight()),
    )
    return int(row["n"]) if row else 0
