"""Capture real NadoClient response shapes (testnet) so the Engine v2 adapter
can be mapped to exact field names instead of guesses.

RUN ON TESTNET with a linked 1CT signer. It uses read-only calls, then places
ONE tiny limit order far from mid (won't fill), captures the open-order +
place + cancel shapes, and cancels it. Paste the full output back.

Usage (pick how you build the client for your bot):
    # via the bot helper (preferred — handles the linked signer):
    TELEGRAM_ID=<your_id> python scripts/capture_nado_shapes.py --product-id 2
    # or direct:
    NADO_ADDRESS=0x... NADO_NETWORK=testnet python scripts/capture_nado_shapes.py --product-id 2

Nothing here is destructive beyond a single tiny, immediately-cancelled limit
order priced far from the market so it cannot fill.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os


def _client():
    tid = os.environ.get("TELEGRAM_ID")
    if tid:
        from src.nadobro.services.bot_runtime import get_user_nado_client

        c = get_user_nado_client(int(tid))
        if c is None:
            raise SystemExit("get_user_nado_client returned None — is the wallet linked on testnet?")
        return c
    from src.nadobro.venue.nado_client import NadoClient

    addr = os.environ["NADO_ADDRESS"]
    c = NadoClient.from_address(addr, network=os.environ.get("NADO_NETWORK", "testnet"))
    c.initialize()
    return c


def dump(label, value):
    print(f"\n===== {label} =====")
    try:
        print(json.dumps(value, indent=2, default=str)[:4000])
    except Exception as e:  # noqa: BLE001
        print(f"<repr> {value!r}  (json failed: {e})")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--product-id", type=int, required=True, help="a liquid testnet product_id")
    ap.add_argument("--size", type=float, default=0.001, help="tiny order size")
    args = ap.parse_args()
    c = _client()
    pid = args.product_id

    # --- read-only shapes ---
    try:
        dump("get_market_price(pid)", c.get_market_price(pid))
    except Exception as e:  # noqa: BLE001
        dump("get_market_price ERROR", str(e))
    try:
        dump("get_open_orders(pid) [before]", c.get_open_orders(pid, refresh=True))
    except Exception as e:  # noqa: BLE001
        dump("get_open_orders ERROR", str(e))
    try:
        dump("get_matches(product_ids=[pid])", await c.get_matches(product_ids=[pid], limit=5))
    except Exception as e:  # noqa: BLE001
        dump("get_matches ERROR", str(e))
    try:
        dump("get_all_positions()", c.get_all_positions())
    except Exception as e:  # noqa: BLE001
        dump("get_all_positions ERROR", str(e))

    # --- place a tiny far-from-mid limit buy (should rest, not fill) ---
    mp = {}
    try:
        mp = c.get_market_price(pid) or {}
    except Exception:  # noqa: BLE001
        pass
    bid = float(mp.get("bid") or mp.get("mid") or mp.get("price") or 0) or 1.0
    far_price = round(bid * 0.5, 6)  # 50% below market -> won't fill
    digest = None
    try:
        resp = c.place_limit_order(pid, args.size, far_price, is_buy=True, post_only=True)
        dump("place_limit_order RESPONSE", resp)
        if isinstance(resp, dict):
            digest = resp.get("digest") or resp.get("order_id") or resp.get("id")
        dump("derived digest", digest)
    except Exception as e:  # noqa: BLE001
        dump("place_limit_order ERROR", str(e))

    try:
        dump("get_open_orders(pid) [after place]", c.get_open_orders(pid, refresh=True))
    except Exception as e:  # noqa: BLE001
        dump("get_open_orders[after] ERROR", str(e))

    # --- cancel it ---
    if digest:
        try:
            dump("cancel_orders RESPONSE", await c.cancel_orders(product_id=pid, digests=[str(digest)]))
        except Exception as e:  # noqa: BLE001
            dump("cancel_orders ERROR", str(e))
        try:
            dump("get_open_orders(pid) [after cancel]", c.get_open_orders(pid, refresh=True))
        except Exception as e:  # noqa: BLE001
            dump("get_open_orders[after cancel] ERROR", str(e))


if __name__ == "__main__":
    asyncio.run(main())
