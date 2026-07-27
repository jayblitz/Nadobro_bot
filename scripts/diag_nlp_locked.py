#!/usr/bin/env python3
"""Read-only diagnostic: NLP locked vs unlocked balances + burn round-trip.

Settles why ``burn_nlp`` returns venue error 2096 ("Do not have enough unlocked
NLP") while the vault card shows a healthy balance and "Lockup: Unlocked".

Checks, in order:
  1. What the venue actually reports: balance_locked / balance_unlocked and the
     per-lock unlock timestamps.
  2. What the bot derives: get_nlp_position() -> lp_balance (the number the
     Withdraw 100% preset burns) and the inferred lockup timer.
  3. Whether the float round-trip the bot performs
     (x18 int -> float -> int(round(f*1e18))) overshoots the exact unlocked wei.

READ-ONLY: only indexer/gateway queries. No writes, no signing, no burns.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/diag_nlp_locked.py <address> [mainnet|testnet]
"""
from __future__ import annotations

import sys


def main(address: str, network: str) -> None:
    from src.nadobro.venue.nado_client import NadoClient

    client = NadoClient.from_address(address, network)
    client.initialize()
    print(f"address={address} network={network}")
    print(f"subaccount={client.subaccount_hex}\n")

    pid = client.resolve_nlp_product_id()
    print(f"nlp_product_id={pid}")

    # 1. Raw venue view.
    locked = client.get_nlp_locked_balances()
    print("\n--- venue: nlp_locked_balances ---")
    print(f"  balance_locked   = {locked['balance_locked']!r}")
    print(f"  balance_unlocked = {locked['balance_unlocked']!r}")
    print(f"  locked_entries   = {locked['locked_entries']!r}")

    # Raw payload too — so we can see if the query silently returned nothing.
    raw = client._query_rest("nlp_locked_balances", {"subaccount": client.subaccount_hex})
    print(f"  RAW payload      = {str(raw)[:600]}")

    # 2. What the bot derives (this is what Withdraw 100% burns).
    pos = client.get_nlp_position() or {}
    print("\n--- bot: get_nlp_position() ---")
    for k in ("exists", "lp_balance", "lp_value_usdt0", "last_mint_ts_ms", "nav_usdt0"):
        print(f"  {k} = {pos.get(k)!r}")

    spot = client._spot_balance_amount(pid) if pid is not None else None
    print(f"  _spot_balance_amount(nlp) = {spot!r}   (fallback when locked query is empty)")

    from src.nadobro.vault.nlp_vault_service import lockup_remaining_seconds
    rem = lockup_remaining_seconds(pos.get("last_mint_ts_ms"))
    print(f"  lockup_seconds_remaining = {rem}  -> card shows "
          f"{'Unlocked' if rem <= 0 else 'LOCKED'}")

    # 3. Would a 100% burn exceed the unlocked balance?
    lp_balance = float(pos.get("lp_balance") or 0.0)
    unlocked = float(locked["balance_unlocked"])
    burn_req_x18 = int(round(lp_balance * 1e18))          # exactly what burn_nlp sends
    print("\n--- 100% burn simulation (NOT executed) ---")
    print(f"  lp_balance (locked+unlocked, or spot fallback) = {lp_balance!r}")
    print(f"  venue unlocked                                  = {unlocked!r}")
    print(f"  burn request x18 = {burn_req_x18}")
    if unlocked > 0:
        unlocked_x18_via_float = int(round(unlocked * 1e18))
        print(f"  unlocked  x18 (via float) = {unlocked_x18_via_float}")
        delta = burn_req_x18 - unlocked_x18_via_float
        print(f"  request - unlocked = {delta} wei "
              f"({'OVERSHOOT -> venue 2096' if delta > 0 else 'ok'})")
    if lp_balance > unlocked + 1e-12:
        print(f"  *** lp_balance EXCEEDS unlocked by {lp_balance - unlocked:.18f} NLP")
        print("      -> a 100% burn asks for locked tokens: venue rejects with 2096")

    # Float round-trip fidelity on the raw unlocked integer, if available.
    print("\n--- float round-trip fidelity ---")
    data = (raw or {}).get("data") or raw or {}
    try:
        u_raw = ((data.get("balance_unlocked") or {}).get("balance") or {}).get("amount")
        if u_raw is not None:
            U = int(u_raw)
            f = float(U) / 1e18
            back = int(round(f * 1e18))
            print(f"  exact unlocked wei U = {U}")
            print(f"  float(U)/1e18        = {f!r}")
            print(f"  int(round(f*1e18))   = {back}")
            print(f"  round-trip delta     = {back - U} wei "
                  f"({'OVERSHOOT -> 2096 on max burn' if back > U else 'safe'})")
        else:
            print("  (no raw unlocked amount in payload)")
    except Exception as exc:  # noqa: BLE001 - diagnostic only
        print(f"  round-trip check failed: {exc}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "mainnet")
