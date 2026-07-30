"""PHANTOM FILL PRICE — against a REAL Postgres row.

This is the test the mock-based ones could not be. ``_write_matches``'s enrich is
a single UPDATE whose behaviour lives in the ``CASE ... ELSE col END`` branches:

    fill_price = CASE WHEN %s > 0 THEN %s ELSE fill_price END          <- fixed
    fill_price = CASE WHEN COALESCE(fill_price,0) = 0 AND %s > 0 ...    <- buggy

The PARAMS are byte-identical in both versions — only the condition differs — so a
test that asserts on the params tuple cannot tell them apart. Audit round 4 proved
that: reverting the four CASE expressions left 4 of the 5 mock-based
``PhantomFillPriceTests`` GREEN. The only way to observe an ELSE branch is to run
the statement against a row that already holds a value.

The row reproduced here is production session 164's (BTC ~63.6k):

    recorder wrote   fill_size 0.00265   fill_price 94,408   <- the REQUESTED size
    venue filled     base      0.00395   quote     250.18
    truth            250.18 / 0.00395 = 63,336.7             <- 1.49x lower

That phantom price made a grid session book a huge fake profit and the session
take-profit rail stopped the bot at a REAL $0.68 with TP set to 50%.

Run with:
    docker compose -f compose.postgres-test.yaml up -d
    NADO_TEST_DATABASE_URL=postgresql://nadobro:nadobro@127.0.0.1:5433/nadobro_test \
        .venv/bin/python -m pytest tests/services/test_nado_sync_enrich_db.py -q
"""
from __future__ import annotations

import os
from decimal import Decimal

import pytest


def _db_reachable() -> bool:
    if not (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")):
        return False
    try:
        import psycopg2

        url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ["DATABASE_URL"]
        psycopg2.connect(url).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(), reason="no reachable Postgres (DATABASE_URL)"
)

_USER = 990001
_PID = 2
_DIGEST = "0xphantom164"


@pytest.fixture(autouse=True)
def _clean_trades():
    from src.nadobro.db import execute, init_db

    init_db()
    execute("DELETE FROM trades_mainnet WHERE user_id = %s", (_USER,))
    yield
    execute("DELETE FROM trades_mainnet WHERE user_id = %s", (_USER,))


def _insert_recorder_row(*, fill_size, fill_price, fill_fee="0.5"):
    """A submit-time recorder row: it already carries a size/price, and its
    submission_idx is NULL so the enrich will claim it."""
    from src.nadobro.db import execute_returning

    row = execute_returning(
        """
        INSERT INTO trades_mainnet
            (user_id, product_id, product_name, order_type, side, size,
             fill_size, price, fill_price, fill_fee, status, order_digest,
             submission_idx, created_at)
        VALUES (%s, %s, 'BTC-PERP', 'market', 'short', %s,
                %s, %s, %s, %s, 'filled', %s, NULL, now())
        RETURNING id
        """,
        (_USER, _PID, fill_size, fill_size, fill_price, fill_price, fill_fee, _DIGEST),
    )
    return int(row["id"] if isinstance(row, dict) else row[0])


def _run_enrich(*, base, quote, fee):
    """Drive the real _write_matches with a venue match for the same digest."""
    from src.nadobro.utils.x18 import to_x18
    from src.nadobro.venue import nado_sync

    return nado_sync._write_matches(_USER, "mainnet", [{
        "submission_idx": "77",
        "digest": _DIGEST,
        "product_id": _PID,
        "product_name": "BTC-PERP",
        "base_filled": str(to_x18(base)),
        "quote_filled": str(to_x18(quote)),
        "fee": str(to_x18(fee)),
    }])


def _read(row_id):
    from src.nadobro.db import query_one

    return query_one(
        "SELECT fill_size, fill_price, price, fill_fee, submission_idx, "
        "base_filled_x18, quote_filled_x18 FROM trades_mainnet WHERE id = %s",
        (row_id,),
    )


def test_the_phantom_price_is_overwritten_on_a_real_row():
    """Session 164 exactly: a recorder row holding 0.00265 / 94,408 must come out
    of the enrich holding the venue's 0.00395 / 63,336.7."""
    row_id = _insert_recorder_row(fill_size="0.00265", fill_price="94408")
    _run_enrich(base="0.00395", quote="-250.18", fee="0.0271")
    got = _read(row_id)

    assert got is not None, "the enrich deleted or missed the recorder row"
    assert int(got["submission_idx"]) == 77, "the row was not claimed by the enrich"

    truth = Decimal("250.18") / Decimal("0.00395")          # 63,336.70...
    assert abs(Decimal(str(got["fill_price"])) - truth) < Decimal("0.01"), (
        f"fill_price is {got['fill_price']}, not the venue's {truth:.2f}. The "
        f"phantom 94,408 survived — this is the $0.68 take-profit stop."
    )
    assert abs(Decimal(str(got["fill_size"])) - Decimal("0.00395")) < Decimal("1e-9")
    assert abs(Decimal(str(got["price"])) - truth) < Decimal("0.01")


def test_the_fee_follows_the_venue_on_a_real_row():
    """fee_x18 is overwritten unconditionally, so fill_fee must be too — fees are
    netted into the session SL/TP rail, so a stale estimate stops a run early."""
    row_id = _insert_recorder_row(fill_size="0.00265", fill_price="94408",
                                  fill_fee="0.9999")
    _run_enrich(base="0.00395", quote="-250.18", fee="0.0271")
    got = _read(row_id)
    assert abs(Decimal(str(got["fill_fee"])) - Decimal("0.0271")) < Decimal("1e-9"), (
        f"fill_fee is {got['fill_fee']}, not the venue's 0.0271 — it contradicts "
        f"the fee_x18 written in the same statement"
    )


def test_the_human_columns_end_up_consistent_with_the_x18_columns():
    """The invariant behind the whole fix: a row must never disagree with itself."""
    from src.nadobro.utils.x18 import from_x18

    row_id = _insert_recorder_row(fill_size="0.00265", fill_price="94408")
    _run_enrich(base="0.00395", quote="-250.18", fee="0.0271")
    got = _read(row_id)

    x18_base = abs(from_x18(int(got["base_filled_x18"])))
    x18_quote = abs(from_x18(int(got["quote_filled_x18"])))
    assert abs(Decimal(str(got["fill_size"])) - x18_base) < Decimal("1e-9"), (
        "fill_size disagrees with base_filled_x18 in the same row"
    )
    derived = x18_quote / x18_base
    assert abs(Decimal(str(got["fill_price"])) - derived) < Decimal("0.01"), (
        "fill_price is not quote_filled_x18 / base_filled_x18 — the row is "
        "internally inconsistent, which is exactly the defect"
    )


def test_a_zero_venue_quote_leaves_a_good_row_alone():
    """The CASE guard: no venue base/quote must not wipe real values."""
    row_id = _insert_recorder_row(fill_size="0.00395", fill_price="63336.71")
    _run_enrich(base="0", quote="0", fee="0")
    got = _read(row_id)
    assert abs(Decimal(str(got["fill_size"])) - Decimal("0.00395")) < Decimal("1e-9")
    assert abs(Decimal(str(got["fill_price"])) - Decimal("63336.71")) < Decimal("0.01")


def test_a_matching_row_is_not_disturbed():
    row_id = _insert_recorder_row(fill_size="0.00395", fill_price="63364.0")
    _run_enrich(base="0.00395", quote="-250.29", fee="0.05")
    got = _read(row_id)
    truth = Decimal("250.29") / Decimal("0.00395")
    assert abs(Decimal(str(got["fill_price"])) - truth) < Decimal("0.01")
    assert abs(Decimal(str(got["fill_size"])) - Decimal("0.00395")) < Decimal("1e-9")
