"""DB layer resilience (2026-06-11 Supabase "No route to host" incident).

Pins the disconnect-hygiene contract of src/nadobro/db.py:
- reads retry ONCE on a fresh connection after a disconnect-class error;
- writes are NEVER auto-retried (possible double-apply);
- a connection that raised a disconnect error is CLOSED, not returned to
  the pool (pool poisoning);
- rollback failures on an errored connection never mask the original error;
- new connections carry connect_timeout + TCP keepalives so a dead route
  cannot wedge a scheduler tick for the OS TCP timeout.
"""
from __future__ import annotations

from unittest.mock import patch

import psycopg2
import pytest

from src.nadobro import db


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append(sql)
        if self.conn.fail_with is not None:
            raise self.conn.fail_with

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self, fail_with=None, rollback_raises=False):
        self.fail_with = fail_with
        self.rollback_raises = rollback_raises
        self.executed: list[str] = []
        self.committed = False
        self.rolled_back = False

    def cursor(self, cursor_factory=None):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        if self.rollback_raises:
            raise psycopg2.InterfaceError("connection already closed")
        self.rolled_back = True

    def close(self):
        pass


class FakePool:
    def __init__(self, conns):
        self.conns = list(conns)
        self.returned: list[tuple[FakeConn, bool]] = []

    def getconn(self):
        return self.conns.pop(0)

    def putconn(self, conn, key=None, close=False):
        self.returned.append((conn, close))


def _with_pool(pool):
    return patch.object(db, "get_pool", return_value=pool)


def test_read_retries_once_on_disconnect_and_discards_dead_conn():
    dead = FakeConn(fail_with=psycopg2.OperationalError("no route to host"))
    fresh = FakeConn()
    pool = FakePool([dead, fresh])
    with _with_pool(pool):
        assert db.query_all("SELECT 1") == []
    # Dead conn closed (poison removed), fresh conn returned healthy.
    assert pool.returned == [(dead, True), (fresh, False)]
    assert fresh.committed


def test_read_gives_up_after_one_retry():
    conns = [
        FakeConn(fail_with=psycopg2.OperationalError("down")),
        FakeConn(fail_with=psycopg2.OperationalError("still down")),
    ]
    pool = FakePool(conns)
    with _with_pool(pool), pytest.raises(psycopg2.OperationalError):
        db.query_one("SELECT 1")
    assert [close for _, close in pool.returned] == [True, True]


def test_write_is_never_retried_on_disconnect():
    dead = FakeConn(fail_with=psycopg2.OperationalError("link lost mid-send"))
    spare = FakeConn()
    pool = FakePool([dead, spare])
    with _with_pool(pool), pytest.raises(psycopg2.OperationalError):
        db.execute("INSERT INTO trades VALUES (1)")
    # Exactly one attempt; the dead conn was closed; spare untouched.
    assert pool.returned == [(dead, True)]
    assert spare.executed == []


def test_rollback_failure_does_not_mask_original_error():
    conn = FakeConn(fail_with=ValueError("application bug"), rollback_raises=True)
    pool = FakePool([conn])
    with _with_pool(pool), pytest.raises(ValueError, match="application bug"):
        db.query_count("SELECT count(*) FROM x")
    # rollback blew up -> connection treated as broken and closed.
    assert pool.returned == [(conn, True)]


def test_non_disconnect_errors_keep_connection_in_pool():
    conn = FakeConn(fail_with=ValueError("bad params"))
    pool = FakePool([conn])
    with _with_pool(pool), pytest.raises(ValueError):
        db.query_all("SELECT 1")
    assert pool.returned == [(conn, False)]
    assert conn.rolled_back


def test_pool_connect_kwargs_bound_the_connect_and_enable_keepalives():
    assert db._DB_CONNECT_KWARGS["connect_timeout"] >= 1
    assert db._DB_CONNECT_KWARGS["keepalives"] == 1
    for key in ("keepalives_idle", "keepalives_interval", "keepalives_count"):
        assert db._DB_CONNECT_KWARGS[key] >= 1
