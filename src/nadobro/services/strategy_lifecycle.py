"""Strategy lifecycle helpers shared by runtime stop paths."""

from __future__ import annotations

from typing import Any

from src.nadobro.config import get_spot_metadata, get_spot_product_id, normalize_volume_spot_symbol
from src.nadobro.services.strategy_registry import normalize_strategy_id
from src.nadobro.services.trade_service import close_all_positions, close_delta_neutral_legs, stop_volume_spot_cleanup


def cleanup_strategy_positions(telegram_id: int, network: str, state: dict[str, Any]) -> dict[str, Any]:
    """Close or clean up positions according to the active strategy shape."""
    strategy = normalize_strategy_id(str(state.get("strategy") or ""))
    slippage_pct = float(state.get("slippage_pct") or 1.0)
    session_id = state.get("strategy_session_id")

    if strategy == "dn":
        return close_delta_neutral_legs(
            telegram_id,
            str(state.get("product") or ""),
            network=network,
            slippage_pct=slippage_pct,
            strategy_session_id=session_id,
        )

    if strategy == "vol" and str(state.get("vol_market") or "perp") == "spot":
        prod = normalize_volume_spot_symbol(str(state.get("product") or ""))
        spot_pid = get_spot_product_id(prod, network=network)
        if spot_pid is None:
            return {"success": True, "skipped": True}
        sym = str((get_spot_metadata(prod, network=network) or {}).get("symbol") or prod).upper()
        return stop_volume_spot_cleanup(
            telegram_id,
            int(spot_pid),
            sym,
            network=network,
            slippage_pct=slippage_pct,
            strategy_session_id=session_id,
        )

    return close_all_positions(telegram_id, network=network)
