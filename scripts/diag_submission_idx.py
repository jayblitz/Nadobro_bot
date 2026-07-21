#!/usr/bin/env python3
"""Read-only diagnostic: does one venue ``submission_idx`` map to MULTIPLE
indexer matches (partial fills of one order)?

This settles whether ``nado_sync._write_matches`` undercounts volume. It keeps
ONE row per ``submission_idx`` (unique index) and stores that match's
*incremental* ``base_filled``. If a single order fills across several matches
(all sharing one ``submission_idx``, with a rising ``cumulative_base_filled``),
the recorder records only the first increment and drops the rest → volume is
understated.

READ-ONLY: only issues indexer ``get_matches`` queries. No writes, no signing.

Usage:
    .venv/bin/python scripts/diag_submission_idx.py <address> [mainnet|testnet] [pages]
"""
from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from decimal import Decimal


def _x18(v) -> Decimal:
    try:
        return abs(Decimal(str(v))) / Decimal(10 ** 18)
    except Exception:
        return Decimal(0)


async def main(address: str, network: str, pages: int) -> None:
    from src.nadobro.venue.nado_client import NadoClient

    client = NadoClient.from_address(address, network)
    print(f"address={address} network={network} subaccount={client.subaccount_hex}")

    # Page backwards through the match feed via the idx cursor.
    all_matches: list[dict] = []
    cursor = None
    for _ in range(max(1, pages)):
        batch = await client.get_matches(limit=500, idx=cursor)
        if not batch:
            break
        all_matches.extend(batch)
        idxs = [m.get("submission_idx") for m in batch if m.get("submission_idx") is not None]
        if not idxs:
            break
        cursor = str(min(int(i) for i in idxs) - 1)  # older than the oldest seen

    print(f"fetched {len(all_matches)} matches")
    if not all_matches:
        print("no matches — pick an address with fill history")
        return

    # (1) Is submission_idx unique per MATCH, or shared across an order's matches?
    by_sub: dict[str, list[dict]] = defaultdict(list)
    for m in all_matches:
        s = m.get("submission_idx")
        if s is not None:
            by_sub[str(s)].append(m)
    multi_sub = {s: ms for s, ms in by_sub.items() if len(ms) > 1}
    print(f"distinct submission_idx = {len(by_sub)}")
    print(f"submission_idx with >1 match = {len(multi_sub)}")
    if not multi_sub:
        print("  -> submission_idx is UNIQUE PER MATCH (one row per fill). The recorder's"
              "\n     dedup-by-submission_idx does NOT collapse an order's partial fills,"
              "\n     so summing the incremental fills is CORRECT. No dedup undercount.")

    # (2) The order (by digest) is what fills across matches. Verify the recorder's
    # per-match incremental fills SUM to the order's final cumulative — i.e. the
    # incremental columns reconcile and there is no lost volume.
    by_digest: dict[str, list[dict]] = defaultdict(list)
    for m in all_matches:
        d = str(m.get("digest") or (m.get("order") or {}).get("digest") or "")
        if d:
            by_digest[d].append(m)
    multi_orders = {d: ms for d, ms in by_digest.items() if len(ms) > 1}
    print(f"\ndistinct orders (by digest) = {len(by_digest)}")
    print(f"orders filled across >1 match = {len(multi_orders)}")

    incr_total = sum(_x18(m.get("base_filled")) for m in all_matches)         # what the recorder sums
    order_total = sum(max(_x18(m.get("cumulative_base_filled")) for m in ms)  # order-final cumulatives
                      for ms in by_digest.values())
    print("\nbase filled — reconciliation:")
    print(f"  sum of per-match incrementals (recorder path): {incr_total:,.6f}")
    print(f"  sum of per-order final cumulatives:            {order_total:,.6f}")
    diff = incr_total - order_total
    print(f"  difference: {diff:,.6f}  "
          f"({'MATCH — recorder sums correctly, no undercount' if abs(diff) < Decimal('1e-6') else 'MISMATCH — investigate'})")

    for d, ms in list(multi_orders.items())[:3]:
        incr = [str(_x18(m.get("base_filled"))) for m in ms]
        cum = [str(_x18(m.get("cumulative_base_filled"))) for m in ms]
        subs = [str(m.get("submission_idx")) for m in ms]
        print(f"\n  order {d[:14]}…: {len(ms)} matches, submission_idx={subs}")
        print(f"    incremental base_filled: {incr}  (sum={sum(_x18(m.get('base_filled')) for m in ms)})")
        print(f"    cumulative_base_filled : {cum}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    addr = sys.argv[1]
    net = sys.argv[2] if len(sys.argv) > 2 else "mainnet"
    n_pages = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    asyncio.run(main(addr, net, n_pages))
