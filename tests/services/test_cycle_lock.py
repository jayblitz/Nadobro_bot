"""CYCLE-LOCK — engine_runtime.run_engine_cycle serialization (audit round 3).

71ca009 wrapped run_engine_cycle in a per-(user, network, strategy) asyncio.Lock
so the eager kickoff in bot_runtime cannot race the scheduled cycle and open a
Delta Neutral hedge TWICE (prod session 167 left ~$99 of unhedged spot). Nobody
audited it, so these tests pin what it does and — just as important — what it
CANNOT do.

Verified on production (Fly, app nadobro-bot): NADO_RUNTIME_MODE=SINGLE,
NADO_USE_MULTIPROCESS_STRATEGIES=FALSE, and _runtime_loop is latched by
set_bot_app at startup — so every cycle runs on one loop and the lock is
effective in the shipped configuration.
"""
from __future__ import annotations

import asyncio

from src.nadobro.strategy import engine_runtime as er


def test_same_loop_returns_the_same_lock():
    async def body():
        a = er._cycle_lock(1, "mainnet", "dn")
        b = er._cycle_lock(1, "mainnet", "dn")
        assert a is b, "a second caller on the same loop must share the lock"
    asyncio.run(body())
    er.release_cycle_lock(1, "mainnet", "dn")


def test_it_actually_serializes_two_concurrent_cycles():
    """The property the DN double-open fix depends on.

    CHARACTERISATION, not a regression test: the pre-hardening lock serialized
    same-loop callers too. It is here because nothing else in the suite proves the
    guarantee the DN fix rests on, and prod is single-loop (verified on Fly).
    """
    async def body():
        order = []

        async def worker(tag, hold):
            async with er._cycle_lock(7, "mainnet", "dn"):
                order.append(f"enter:{tag}")
                await asyncio.sleep(hold)
                order.append(f"exit:{tag}")

        await asyncio.gather(worker("kickoff", 0.02), worker("scheduled", 0.0))
        # Whoever wins, the two critical sections must not interleave.
        assert order[0].startswith("enter:") and order[1].startswith("exit:"), order
        assert order[1].split(":")[1] == order[0].split(":")[1], order
    asyncio.run(body())
    er.release_cycle_lock(7, "mainnet", "dn")


def test_different_keys_do_not_block_each_other():
    async def body():
        assert er._cycle_lock(1, "mainnet", "dn") is not er._cycle_lock(2, "mainnet", "dn")
        assert er._cycle_lock(1, "mainnet", "dn") is not er._cycle_lock(1, "testnet", "dn")
        assert er._cycle_lock(1, "mainnet", "dn") is not er._cycle_lock(1, "mainnet", "vol")
    asyncio.run(body())
    for u, n, s in ((1, "mainnet", "dn"), (2, "mainnet", "dn"),
                    (1, "testnet", "dn"), (1, "mainnet", "vol")):
        er.release_cycle_lock(u, n, s)


def test_a_lock_cached_on_a_closed_loop_is_rebound():
    """Loop-awareness, stated honestly.

    I hypothesised that caching a lock from the ``asyncio.run()`` fallback in
    ``_run_engine_start_sync`` would poison the key ("got Future attached to a
    different loop"). PROBED AND REFUTED: since 3.10 asyncio.Lock binds lazily at
    acquire, so an UNCONTENDED acquire on a new loop is fine. The loop-awareness is
    therefore defensive depth, not a bug fix — it only matters for a CONTENDED
    cross-loop acquire, where a waiter Future would be created on the wrong loop.

    What is observable, and what this pins, is the rebinding itself: a different
    loop must not silently reuse a lock whose waiters would live elsewhere.
    """
    async def first():
        return er._cycle_lock(99, "mainnet", "grid")
    first_lock = asyncio.run(first())          # that loop is now CLOSED

    async def second():
        lock = er._cycle_lock(99, "mainnet", "grid")
        async with lock:                       # works either way
            return lock
    second_lock = asyncio.run(second())

    assert second_lock is not first_lock, (
        "a lock cached against a closed loop was reused on a new loop"
    )
    er.release_cycle_lock(99, "mainnet", "grid")


def test_release_prevents_unbounded_growth():
    async def body():
        er._cycle_lock(1234, "mainnet", "vol")
        assert "1234:mainnet:vol" in er._CYCLE_LOCKS
        er.release_cycle_lock(1234, "mainnet", "vol")
        assert "1234:mainnet:vol" not in er._CYCLE_LOCKS
        er.release_cycle_lock(1234, "mainnet", "vol")   # idempotent
    asyncio.run(body())


def test_multiprocess_mode_is_loudly_flagged_as_unprotected(monkeypatch, caplog):
    """The lock cannot cross processes. If the runtime is ever switched to
    multiprocess the double-open returns — that must never be silent."""
    import src.nadobro.runtime.runtime_supervisor as rs

    monkeypatch.setattr(rs, "_MODE", "multiprocess", raising=False)
    monkeypatch.setattr(er, "_warned_mp_cycle_lock", False, raising=False)

    async def body():
        with caplog.at_level("ERROR"):
            er._cycle_lock(555, "mainnet", "dn")
        assert any("MULTIPROCESS" in r.message or "MULTIPROCESS" in r.getMessage()
                   for r in caplog.records), "switching to multiprocess was silent"
    asyncio.run(body())
    er.release_cycle_lock(555, "mainnet", "dn")
