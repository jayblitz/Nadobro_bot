#!/usr/bin/env python3
"""Nightly trade-attribution / PnL drift check (read-only).

Parameterless companion to scripts/audit_trade_attribution.sql: scans the last
N hours globally (all users/sessions) and reports attribution drift — the bug
class that corrupts History and volume (the venue reports no per-fill realized
PnL, so all PnL rests on our own fill attribution).

Designed for a scheduled loop: deterministic, quiet when clean, and the exit
code is the signal. Escalate to a Claude triage session only on exit 1 — see
docs/self_review/SCHEDULED_LOOPS.md.

Usage:
    NADO_AUDIT_DATABASE_URL=postgresql://... \
        .venv/bin/python scripts/reconcile_attribution.py [--network mainnet|testnet|both]
                                                          [--hours 24] [--samples 5] [--strict]

The DSN is read ONLY from NADO_AUDIT_DATABASE_URL (never DATABASE_URL) so the
script can't silently run against whatever the shell happens to carry; point it
at a read-only role/replica when available. The session is forced read-only.

Checks (derived from audit_trade_attribution.sql):
  hard — post-fix invariants, any row is a regression; they fail the run:
    swallowed_manual_fills   session-tagged rows still labeled source=manual
    unlinked_manual_closes   manual close rows without open_trade_id linkage
    untagged_close_digests   close fills whose digest has no close:* intent row
    session_rollup_mismatch  stored session volume/fees != recomputed from fills
  soft — candidates that can be legitimate manual activity; reported as WARN
  and only fail the run with --strict:
    session_window_manual_orphans  venue-confirmed manual/no-session fills
                                   inside an active session window (phantom-
                                   History candidates)
    post_stop_flatten_candidates   untagged/manual fills within 10min after a
                                   session stop (leaked flatten fills)

Exit codes: 0 clean, 1 drift, 2 config/connection/schema error.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import psycopg2
import psycopg2.extras

NETWORKS = ("mainnet", "testnet")


@dataclass(frozen=True)
class Check:
    name: str
    hard: bool
    sql: str  # {trades} is substituted with trades_<network>


CHECKS: tuple[Check, ...] = (
    Check(
        name="swallowed_manual_fills",
        hard=True,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.side, t.strategy_session_id,
                   t.order_digest, t.filled_at
            FROM {trades} t
            WHERE t.filled_at >= now() - %(window)s::interval
              AND t.strategy_session_id IS NOT NULL
              AND COALESCE(t.source, 'manual') = 'manual'
              AND t.submission_idx IS NOT NULL
            ORDER BY t.filled_at DESC
        """,
    ),
    Check(
        name="unlinked_manual_closes",
        hard=True,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.side, t.close_price,
                   t.realized_pnl, t.order_digest, t.closed_at
            FROM {trades} t
            WHERE COALESCE(t.closed_at, t.filled_at, t.created_at) >= now() - %(window)s::interval
              AND t.order_type ILIKE '%%close%%'
              AND COALESCE(t.source, 'manual') = 'manual'
              AND t.open_trade_id IS NULL
            ORDER BY COALESCE(t.closed_at, t.filled_at, t.created_at) DESC
        """,
    ),
    Check(
        name="untagged_close_digests",
        hard=True,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.side, t.source,
                   t.strategy_session_id, t.order_digest,
                   COALESCE(t.closed_at, t.filled_at) AS ts
            FROM {trades} t
            WHERE COALESCE(t.closed_at, t.filled_at, t.created_at) >= now() - %(window)s::interval
              AND t.order_type ILIKE '%%close%%'
              AND t.order_digest IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM order_intents oi
                  WHERE oi.order_digest = t.order_digest
                    AND oi.intent_id LIKE 'close:%%')
            ORDER BY ts DESC
        """,
    ),
    Check(
        # A session-tagged engine fill must never acquire close stamps: that is
        # the manual-close mis-pairing corruption (repaired on prod 2026-07-09;
        # find_open_trade now excludes session fills — this guards the fix).
        name="stamped_strategy_fills",
        hard=True,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.side,
                   t.strategy_session_id, t.close_price, t.pnl,
                   COALESCE(t.closed_at, t.filled_at) AS ts
            FROM {trades} t
            WHERE COALESCE(t.closed_at, t.filled_at, t.created_at) >= now() - %(window)s::interval
              AND t.strategy_session_id IS NOT NULL
              AND t.order_type = 'match'
              AND t.close_price IS NOT NULL
            ORDER BY ts DESC
        """,
    ),
    Check(
        name="session_rollup_mismatch",
        hard=True,
        sql="""
            SELECT s.id AS session_id, s.user_id, s.strategy, s.product_name,
                   s.stopped_at,
                   round(COALESCE(s.total_volume_usd, 0)::numeric, 2) AS stored_volume,
                   round(agg.volume::numeric, 2) AS recomputed_volume,
                   round(COALESCE(s.total_fees_paid, 0)::numeric, 4) AS stored_fees,
                   round(agg.fees::numeric, 4) AS recomputed_fees
            FROM strategy_sessions s
            LEFT JOIN LATERAL (
                SELECT COALESCE(SUM(COALESCE(ABS(NULLIF(t.quote_filled_x18, 0)) / 1e18,
                                 ABS(COALESCE(t.fill_size, t.size, 0))
                                   * COALESCE(NULLIF(t.fill_price, 0), t.price, 0))), 0) AS volume,
                       COALESCE(SUM(COALESCE(NULLIF(t.fee_x18, 0) / 1e18,
                                 COALESCE(t.fill_fee, t.fees, 0)
                                   + COALESCE(t.builder_fee, 0))), 0) AS fees
                FROM {trades} t
                WHERE t.strategy_session_id = s.id
                  AND COALESCE(t.source, '') <> 'manual'
            ) agg ON TRUE
            WHERE s.network = %(network)s
              AND s.stopped_at IS NOT NULL
              AND s.stopped_at >= now() - %(window)s::interval
              AND (ABS(COALESCE(s.total_volume_usd, 0) - agg.volume)
                     > GREATEST(1.0, 0.01 * GREATEST(ABS(COALESCE(s.total_volume_usd, 0)), agg.volume))
                OR ABS(COALESCE(s.total_fees_paid, 0) - agg.fees)
                     > GREATEST(0.25, 0.02 * GREATEST(ABS(COALESCE(s.total_fees_paid, 0)), agg.fees)))
            ORDER BY s.stopped_at DESC
        """,
    ),
    Check(
        name="session_window_manual_orphans",
        hard=False,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.side,
                   COALESCE(t.fill_size, t.size) AS size,
                   COALESCE(t.fill_price, t.price) AS price,
                   t.order_digest, t.filled_at
            FROM {trades} t
            WHERE t.filled_at >= now() - %(window)s::interval
              AND t.submission_idx IS NOT NULL
              AND COALESCE(t.source, 'manual') = 'manual'
              AND t.strategy_session_id IS NULL
              AND EXISTS (
                  SELECT 1 FROM strategy_sessions s
                  WHERE s.user_id = t.user_id
                    AND s.network = %(network)s
                    AND s.product_id = t.product_id
                    AND s.started_at <= t.filled_at
                    AND t.filled_at <= COALESCE(s.stopped_at, now()) + interval '10 minutes')
            ORDER BY t.filled_at DESC
        """,
    ),
    Check(
        name="post_stop_flatten_candidates",
        hard=False,
        sql="""
            SELECT t.id, t.user_id, t.product_name, t.source, t.strategy_session_id,
                   t.order_digest, t.filled_at, s.id AS stopped_session_id
            FROM strategy_sessions s
            JOIN {trades} t
              ON t.user_id = s.user_id
             AND t.product_id = s.product_id
             AND t.filled_at >= s.stopped_at
             AND t.filled_at < s.stopped_at + interval '10 minutes'
            WHERE s.network = %(network)s
              AND s.stopped_at IS NOT NULL
              AND s.stopped_at >= now() - %(window)s::interval
              AND t.submission_idx IS NOT NULL
              AND (t.strategy_session_id IS NULL OR COALESCE(t.source, 'manual') = 'manual')
            ORDER BY t.filled_at DESC
        """,
    ),
)


def run_network(cur, network: str, window: str, samples: int) -> tuple[int, int]:
    """Run all checks for one network. Returns (hard_hits, soft_hits)."""
    trades = f"trades_{network}"
    hard_hits = soft_hits = 0
    for check in CHECKS:
        cur.execute(
            check.sql.format(trades=trades),
            {"window": window, "network": network},
        )
        rows = cur.fetchall()
        n = len(rows)
        if n == 0:
            print(f"  OK    {check.name}: 0")
            continue
        level = "DRIFT" if check.hard else "WARN "
        print(f"  {level} {check.name}: {n}")
        for row in rows[:samples]:
            print(f"        {dict(row)}")
        if n > samples:
            print(f"        ... and {n - samples} more")
        if check.hard:
            hard_hits += n
        else:
            soft_hits += n
    return hard_hits, soft_hits


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--network", choices=(*NETWORKS, "both"), default="mainnet")
    ap.add_argument("--hours", type=float, default=24.0, help="lookback window (default 24)")
    ap.add_argument("--samples", type=int, default=5, help="sample rows per finding")
    ap.add_argument("--strict", action="store_true", help="soft (WARN) findings also fail the run")
    args = ap.parse_args()

    dsn = (os.environ.get("NADO_AUDIT_DATABASE_URL") or "").strip()
    if not dsn:
        print("reconcile_attribution: set NADO_AUDIT_DATABASE_URL (read-only DSN).", file=sys.stderr)
        return 2

    window = f"{args.hours} hours"
    networks = NETWORKS if args.network == "both" else (args.network,)

    try:
        conn = psycopg2.connect(
            dsn,
            options="-c default_transaction_read_only=on -c statement_timeout=60000",
        )
    except Exception as exc:  # noqa: BLE001 - connection problem is a config error
        print(f"reconcile_attribution: cannot connect: {exc}", file=sys.stderr)
        return 2

    hard_total = soft_total = 0
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for network in networks:
                print(f"[{network}] attribution drift check, last {window}")
                try:
                    hard, soft = run_network(cur, network, window, args.samples)
                except psycopg2.Error as exc:
                    print(f"reconcile_attribution: query failed on {network}: {exc}", file=sys.stderr)
                    return 2
                hard_total += hard
                soft_total += soft
    finally:
        conn.close()

    drift = hard_total > 0 or (args.strict and soft_total > 0)
    verdict = "DRIFT" if drift else "CLEAN"
    print(f"{verdict}: {hard_total} hard finding(s), {soft_total} warn finding(s).")
    if drift:
        print("Escalate: see docs/self_review/SCHEDULED_LOOPS.md (triage prompt).")
    return 1 if drift else 0


if __name__ == "__main__":
    sys.exit(main())
