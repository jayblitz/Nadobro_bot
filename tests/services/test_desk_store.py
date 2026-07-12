"""Desk store contract: guarded status transitions + restart-safe semantics.

The DB layer is faked (the CI Postgres job exercises the real DDL via
init_db); what's pinned HERE is the part that prevents money bugs:
- every transition UPDATE carries its expected-status precondition, so a
  restart or a second worker can never double-fire a trigger, resurrect a
  cancelled plan, or finish a plan twice;
- the daily cap counts from UTC midnight and includes cancelled plans
  (cancel-respawn must not bypass the cap);
- table-name interpolation only accepts known networks.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.nadobro.trading import desk_store
from src.nadobro.trading.desk_plans import (
    ST_AWAITING_TRIGGER,
    ST_CANCELLED,
    ST_COMPLETED,
    ST_DRAFT,
    ST_RUNNING,
    ExecutionPlan,
)


class Captured:
    def __init__(self, ret=None):
        self.calls: list[tuple[str, tuple]] = []
        self.ret = ret

    def __call__(self, sql, params=None):
        self.calls.append((str(sql), tuple(params or ())))
        return self.ret

    @property
    def last_sql(self) -> str:
        return self.calls[-1][0]

    @property
    def last_params(self) -> tuple:
        return self.calls[-1][1]


def make_plan(**kw) -> ExecutionPlan:
    base = dict(algo="twap", market="spot", product="ETH", side="buy",
                size_base=5.0, duration_minutes=120, interval_seconds=30)
    base.update(kw)
    return ExecutionPlan(**base)


def test_table_name_rejects_unknown_network():
    with pytest.raises(ValueError):
        desk_store._table("mainnet; DROP TABLE users")
    assert desk_store._table("mainnet") == "desk_plans_mainnet"
    assert desk_store._table("TESTNET") == "desk_plans_testnet"


def test_insert_draft_is_idempotent_on_plan_id():
    cap = Captured(ret={"id": 7})
    with patch.object(desk_store, "execute_returning", cap):
        rid = desk_store.insert_draft(42, make_plan(), "mainnet")
    assert rid == 7
    assert "ON CONFLICT (plan_id) DO NOTHING" in cap.last_sql
    assert cap.last_params[0] == 42
    assert cap.last_params[2] == ST_DRAFT


def test_confirm_requires_draft_status_and_owner():
    cap = Captured(ret={"id": 1})
    plan = make_plan()
    with patch.object(desk_store, "execute_returning", cap):
        ok = desk_store.confirm_plan(plan.plan_id, 42, "mainnet", plan)
    assert ok
    sql = cap.last_sql
    assert "WHERE plan_id = %s AND user_id = %s AND status = %s" in sql
    assert cap.last_params[-1] == ST_DRAFT  # only drafts confirm
    assert cap.last_params[0] == ST_AWAITING_TRIGGER


def test_claim_trigger_fire_is_single_winner():
    cap = Captured(ret=None)  # someone else already claimed it
    with patch.object(desk_store, "execute_returning", cap):
        assert not desk_store.claim_trigger_fire("abc", "mainnet")
    assert "AND status = %s" in cap.last_sql
    assert cap.last_params == (ST_RUNNING, "abc", ST_AWAITING_TRIGGER)


def test_finish_only_from_active_statuses():
    cap = Captured(ret={"id": 1})
    with patch.object(desk_store, "execute_returning", cap):
        desk_store.finish_plan("abc", "mainnet", ST_COMPLETED)
    assert "status = ANY(%s)" in cap.last_sql
    active = cap.last_params[-1]
    assert set(active) == {ST_AWAITING_TRIGGER, ST_RUNNING}


def test_cancel_requires_owner_and_active():
    cap = Captured(ret=None)
    with patch.object(desk_store, "execute_returning", cap):
        assert not desk_store.cancel_plan("abc", 42, "mainnet")
    assert "user_id = %s" in cap.last_sql
    assert cap.last_params[0] == ST_CANCELLED


def test_count_confirmed_today_uses_utc_midnight():
    cap = Captured(ret={"n": 3})
    with patch.object(desk_store, "query_one", cap):
        n = desk_store.count_confirmed_today(42, "mainnet")
    assert n == 3
    cutoff = cap.last_params[-1]
    assert isinstance(cutoff, datetime)
    assert cutoff.tzinfo is timezone.utc
    assert (cutoff.hour, cutoff.minute, cutoff.second) == (0, 0, 0)
    # counts ALL confirmed rows regardless of terminal status
    assert "status" not in cap.last_sql.split("WHERE", 1)[1]


def test_row_to_record_round_trips_plan():
    plan = make_plan()
    rec = desk_store.row_to_record({
        "id": 1, "user_id": 42, "plan_id": plan.plan_id,
        "status": ST_RUNNING,
        "plan_json": __import__("json").dumps(plan.to_dict()),
        "state_json": '{"filled_base": 1.5}',
    })
    assert rec["plan"].to_dict() == plan.to_dict()
    assert rec["state"] == {"filled_base": 1.5}


def test_row_to_record_survives_corrupt_json():
    rec = desk_store.row_to_record({
        "id": 1, "user_id": 42, "plan_id": "x", "status": ST_RUNNING,
        "plan_json": "{not json", "state_json": "also not",
    })
    assert rec["plan"] is not None  # degraded but never raises
    assert rec["state"] == {}


def test_startup_ddl_includes_desk_plans():
    import inspect
    from src.nadobro import db
    src = inspect.getsource(db.init_db)
    assert "desk_plans_{net}" in src
