"""Phase 5 readiness verifier — Tread Fi parity end-to-end smoke test.

Run this immediately before kicking off the testnet or mainnet soak. It
confirms that every Phase 0–4 wiring point is alive against a real Nado
gateway / archive (no mocks, no stubs):

  1. Catalog: BTC-PERP exposes min_size, size_increment, price_increment,
     maker_fee_rate, taker_fee_rate.
  2. Tiny Budget Preset math: $50 collateral × ceil(min_size / 50) leverage
     reaches the venue floor on the selected pair.
  3. POV engine: `archive.../market_snapshots` returns ≥ 2 hourly buckets
     for BTC-PERP and `compute_pov_duration("normal", ...)` produces
     finite, positive duration / cycle / interval values.
  4. Pre-trade card builder: `build_pretrade_breakdown` returns required
     margin per quote with the documented Tread perp formula and the locked
     1 bps builder fee.
  5. Maker rate: signed fraction parsed from the `symbols` payload (negative
     on most majors).

Usage::

    python scripts/phase5_readiness_check.py --network testnet --product BTC

Exit code 0 if every check passes, 1 otherwise. The exit text doubles as a
soak-log appendix — paste verbatim into ``docs/soak/phase5-<network>-<date>.md``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Any

# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 5 Tread Fi parity readiness check")
    p.add_argument("--network", choices=("mainnet", "testnet"), default="testnet")
    p.add_argument("--product", default="BTC", help="Perp base symbol (default: BTC)")
    p.add_argument("--collateral", type=float, default=50.0, help="Collateral USD (default: 50)")
    p.add_argument("--preset", choices=("aggressive", "normal", "passive"), default="normal")
    p.add_argument("--bias", type=float, default=0.0, help="Mid Mode bias in [-1, 1] (default: 0)")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of human text")
    return p.parse_args()


# --------------------------------------------------------------------------- #
# Check runner                                                                #
# --------------------------------------------------------------------------- #


class CheckResult:
    __slots__ = ("name", "ok", "detail", "value")

    def __init__(self, name: str, ok: bool, detail: str, value: Any = None) -> None:
        self.name = name
        self.ok = bool(ok)
        self.detail = str(detail)
        self.value = value

    def to_dict(self) -> dict:
        out = {"name": self.name, "ok": self.ok, "detail": self.detail}
        if self.value is not None:
            out["value"] = self.value
        return out


def _check(name: str, fn) -> CheckResult:
    try:
        ok, detail, value = fn()
        return CheckResult(name, ok, detail, value)
    except Exception as exc:
        return CheckResult(name, False, f"raised: {exc}")


# --------------------------------------------------------------------------- #
# Individual checks                                                           #
# --------------------------------------------------------------------------- #


def check_catalog_min_size(network: str, product: str):
    from src.nadobro.venue.product_catalog import get_product_min_quote_notional_usd

    def run():
        value = get_product_min_quote_notional_usd(product, network=network)
        ok = value is not None and value > 0
        detail = f"min_size = ${value:.2f} USDT0" if ok else f"got {value!r}"
        return ok, detail, value

    return _check("catalog.min_size", run)


def check_catalog_increments(network: str, product: str):
    from src.nadobro.venue.product_catalog import (
        get_product_size_increment,
        get_product_price_increment,
    )

    def run():
        size_inc = get_product_size_increment(product, network=network)
        price_inc = get_product_price_increment(product, network=network)
        ok = size_inc is not None and price_inc is not None and size_inc > 0 and price_inc > 0
        return (
            ok,
            f"size_increment={size_inc} price_increment={price_inc}",
            {"size_increment": size_inc, "price_increment": price_inc},
        )

    return _check("catalog.increments", run)


def check_catalog_max_leverage(network: str, product: str):
    from src.nadobro.config import get_product_max_leverage

    def run():
        value = get_product_max_leverage(product, network=network)
        ok = isinstance(value, int) and value >= 1
        return ok, f"max_leverage = {value}x", value

    return _check("catalog.max_leverage", run)


def check_maker_taker_fees(network: str, product: str):
    from src.nadobro.venue.product_catalog import (
        get_product_maker_fee_rate,
        get_product_taker_fee_rate,
    )

    def run():
        maker = get_product_maker_fee_rate(product, network=network)
        taker = get_product_taker_fee_rate(product, network=network)
        ok = maker is not None and taker is not None
        detail = f"maker={None if maker is None else f'{maker * 10000:+.2f} bps'} / taker={None if taker is None else f'{taker * 10000:+.2f} bps'}"
        return ok, detail, {"maker_rate": maker, "taker_rate": taker}

    return _check("catalog.fees", run)


def check_tiny_budget_math(network: str, product: str, collateral: float):
    from src.nadobro.config import get_product_max_leverage
    from src.nadobro.venue.product_catalog import get_product_min_quote_notional_usd

    def run():
        min_size = get_product_min_quote_notional_usd(product, network=network)
        cap = get_product_max_leverage(product, network=network)
        if not min_size or not cap:
            return False, "catalog returned no min_size or max_leverage", None
        required_lev = max(1, math.ceil(min_size / max(1.0, collateral)))
        target_lev = max(required_lev, math.ceil(required_lev * 1.1))
        target_lev = min(int(target_lev), int(cap))
        notional_after = collateral * target_lev
        ok = notional_after >= min_size
        detail = (
            f"${collateral:.0f} × {target_lev}x = ${notional_after:.0f} "
            f"vs min_size ${min_size:.0f} (cap {cap}x) → {'OK' if ok else 'INSUFFICIENT'}"
        )
        return ok, detail, {
            "collateral": collateral,
            "required_leverage": required_lev,
            "target_leverage": target_lev,
            "lev_cap": cap,
            "notional_after": notional_after,
            "min_size": min_size,
        }

    return _check("tiny_budget.math", run)


def check_pov_engine(network: str, product: str, collateral: float, preset: str):
    from src.nadobro.config import get_product_id
    from src.nadobro.quant import pov_engine
    from src.nadobro.venue.nado_archive import get_pair_24h_volume_usd

    def run():
        pid = get_product_id(product, network=network)
        if pid is None:
            return False, "get_product_id returned None", None
        volume = get_pair_24h_volume_usd(network=network, product_id=int(pid))
        if not volume or volume <= 0:
            return False, f"archive returned volume={volume!r}", None
        meta = pov_engine.compute_pov_duration(
            notional_usd=collateral, preset=preset, pair_24h_volume_usd=volume
        )
        ok = (
            meta["duration_minutes"] > 0
            and meta["interval_seconds"] > 0
            and meta["cycle_notional_usd"] > 0
            and math.isfinite(meta["duration_minutes"])
        )
        detail = (
            f"24h vol ${volume:,.0f}; preset={meta['preset']} "
            f"dur={meta['duration_minutes']:.1f} min "
            f"interval={meta['interval_seconds']}s "
            f"cycle=${meta['cycle_notional_usd']:.2f}"
        )
        return ok, detail, meta

    return _check("pov_engine.compute", run)


def check_pretrade_breakdown(network: str, product: str, collateral: float, preset: str, bias: float):
    from src.nadobro.config import get_product_max_leverage
    from src.nadobro.services import mm_dashboard
    from src.nadobro.config import NADO_BUILDER_FEE_RATE_1_BPS

    def run():
        leverage = get_product_max_leverage(product, network=network)
        conf = {
            "notional_usd": collateral,
            "cycle_notional_usd": collateral,
            "spread_bp": 5.0,
            "sl_pct": 0.5,
            "directional_bias": bias,
            "participation_preset": preset,
            "max_open_orders": 6,
        }
        breakdown = mm_dashboard.build_pretrade_breakdown(
            strategy_id="dgrid",
            conf=conf,
            network=network,
            product=product,
            leverage=float(leverage),
        )
        margin = breakdown["margin"]["required_margin_per_quote_usd"]
        builder_bps = breakdown["fees"]["builder_fee_bps"]
        ok = (
            margin > 0
            and abs(builder_bps - float(NADO_BUILDER_FEE_RATE_1_BPS) / 10.0) < 1e-9
            and breakdown["margin"]["participation_preset"] == preset
        )
        detail = (
            f"required_margin_per_quote=${margin:.2f} builder={builder_bps:.2f} bps "
            f"max_loss=${breakdown['max_loss_usd']:.2f}"
        )
        return ok, detail, {
            "required_margin_per_quote_usd": margin,
            "builder_fee_bps": builder_bps,
            "max_loss_usd": breakdown["max_loss_usd"],
            "preset": breakdown["margin"]["participation_preset"],
        }

    return _check("mm_dashboard.build_pretrade_breakdown", run)


def check_builder_fee_locked():
    from src.nadobro.config import (
        NADO_BUILDER_FEE_RATE_1_BPS,
        get_nado_builder_routing_config,
    )

    def run():
        builder_id, fee_rate = get_nado_builder_routing_config()
        ok = fee_rate == NADO_BUILDER_FEE_RATE_1_BPS == 10
        return (
            ok,
            f"builder_id={builder_id} fee_rate={fee_rate} (expected 10)",
            {"builder_id": builder_id, "fee_rate": fee_rate},
        )

    return _check("config.builder_fee_locked_1bps", run)


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #


def main() -> int:
    args = _parse_args()

    checks = [
        check_builder_fee_locked(),
        check_catalog_max_leverage(args.network, args.product),
        check_catalog_min_size(args.network, args.product),
        check_catalog_increments(args.network, args.product),
        check_maker_taker_fees(args.network, args.product),
        check_tiny_budget_math(args.network, args.product, args.collateral),
        check_pov_engine(args.network, args.product, args.collateral, args.preset),
        check_pretrade_breakdown(
            args.network, args.product, args.collateral, args.preset, args.bias
        ),
    ]

    all_ok = all(c.ok for c in checks)
    if args.json:
        print(json.dumps(
            {
                "ok": all_ok,
                "network": args.network,
                "product": args.product,
                "collateral": args.collateral,
                "preset": args.preset,
                "bias": args.bias,
                "checks": [c.to_dict() for c in checks],
            },
            indent=2,
        ))
    else:
        title = f"Phase 5 readiness — network={args.network} product={args.product} collateral=${args.collateral:.0f} preset={args.preset} bias={args.bias:+.2f}"
        print(title)
        print("=" * len(title))
        for c in checks:
            mark = "✅" if c.ok else "❌"
            print(f"{mark} {c.name}: {c.detail}")
        print()
        print(("ALL CHECKS PASSED — proceed with soak." if all_ok
               else "FAILURES DETECTED — do NOT start the soak until each ❌ is resolved."))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
