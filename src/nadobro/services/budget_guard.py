import logging
from typing import Optional

from src.nadobro.services.user_service import get_user, get_user_readonly_client
from src.nadobro.services.settings_service import get_user_settings

logger = logging.getLogger(__name__)

RISK_PROFILES = {
    "conservative": {
        "max_budget_pct": 0.30,
        "max_leverage": 3,
        "max_positions": 2,
        "max_single_position_pct": 0.20,
        "min_margin_buffer_pct": 0.40,
    },
    "balanced": {
        "max_budget_pct": 0.60,
        "max_leverage": 5,
        "max_positions": 3,
        "max_single_position_pct": 0.35,
        "min_margin_buffer_pct": 0.25,
    },
    "aggressive": {
        "max_budget_pct": 1.0,
        "max_leverage": 10,
        "max_positions": 4,
        "max_single_position_pct": 0.50,
        "min_margin_buffer_pct": 0.10,
    },
}


def get_risk_limits(risk_profile: str) -> dict:
    return RISK_PROFILES.get(risk_profile, RISK_PROFILES["balanced"]).copy()


def get_account_snapshot(telegram_id: int) -> Optional[dict]:
    try:
        client = get_user_readonly_client(telegram_id)
        if not client:
            return None

        balance_data = client.get_balance()
        if not balance_data or not balance_data.get("exists"):
            return None

        balances = balance_data.get("balances", {}) or {}
        usdt_balance = float(balances.get(0, balances.get("0", 0)))

        positions = []
        try:
            raw_positions = client.get_all_positions()
            if isinstance(raw_positions, list):
                for pos in raw_positions:
                    signed_amt = float(pos.get("signed_amount", 0) or pos.get("size", 0) or 0)
                    size = abs(signed_amt)
                    if size > 0:
                        entry = float(pos.get("entry_price", 0) or pos.get("avg_entry_price", 0) or 0)
                        product_name = pos.get("product_name", "") or pos.get("product", "")
                        notional = float(pos.get("notional_usd", 0) or 0) or size * entry
                        pnl = float(pos.get("unrealized_pnl", 0) or pos.get("pnl", 0) or 0)
                        leverage = float(pos.get("leverage", 0) or pos.get("effective_leverage", 0) or 0)
                        if leverage <= 0 and entry > 0 and notional > 0:
                            margin_used = float(pos.get("margin", 0) or pos.get("initial_margin", 0) or 0)
                            leverage = notional / margin_used if margin_used > 0 else 5
                        leverage = max(1, leverage)
                        positions.append({
                            "product_id": pos.get("product_id"),
                            "product": product_name,
                            "side": "long" if signed_amt > 0 else "short",
                            "size": size,
                            "notional_usd": notional,
                            "entry_price": entry,
                            "unrealized_pnl": pnl,
                            "leverage": leverage,
                        })
        except Exception as e:
            logger.warning("Failed to fetch positions for budget guard: %s", e)

        total_exposure = sum(p["notional_usd"] for p in positions)

        return {
            "usdt_balance": usdt_balance,
            "positions": positions,
            "position_count": len(positions),
            "total_exposure_usd": total_exposure,
            "available_margin": max(0, usdt_balance - sum(
                p["notional_usd"] / max(1, float(p.get("leverage", 5))) for p in positions
            )),
        }
    except Exception as e:
        logger.error("Account snapshot failed for user %s: %s", telegram_id, e)
        return None


def check_can_open_position(
    telegram_id: int,
    proposed_notional_usd: float,
    proposed_leverage: float,
    bro_settings: dict,
) -> tuple[bool, str]:
    risk_profile = bro_settings.get("risk_level", "balanced")
    limits = get_risk_limits(risk_profile)
    budget = float(bro_settings.get("budget_usd", 500))

    if proposed_leverage > limits["max_leverage"]:
        return False, (
            f"Leverage {proposed_leverage}x exceeds {risk_profile} limit of {limits['max_leverage']}x"
        )

    snapshot = get_account_snapshot(telegram_id)
    if not snapshot:
        return False, "Could not fetch account data"

    usdt_balance = snapshot["usdt_balance"]
    current_exposure = snapshot["total_exposure_usd"]
    current_positions = snapshot["position_count"]

    effective_budget = min(budget, usdt_balance * limits["max_budget_pct"])

    if current_positions >= limits["max_positions"]:
        return False, (
            f"Max positions ({limits['max_positions']}) reached for {risk_profile} profile"
        )

    max_single = effective_budget * limits["max_single_position_pct"]
    if proposed_notional_usd > max_single:
        return False, (
            f"Position ${proposed_notional_usd:.0f} exceeds single-position limit "
            f"${max_single:.0f} ({risk_profile})"
        )

    new_total_exposure = current_exposure + proposed_notional_usd
    if new_total_exposure > effective_budget:
        remaining = max(0, effective_budget - current_exposure)
        return False, (
            f"Would exceed budget. Current exposure: ${current_exposure:.0f}, "
            f"budget: ${effective_budget:.0f}, remaining: ${remaining:.0f}"
        )

    required_margin = proposed_notional_usd / proposed_leverage
    min_buffer = usdt_balance * limits["min_margin_buffer_pct"]
    available_for_trade = usdt_balance - min_buffer
    if required_margin > available_for_trade:
        return False, (
            f"Insufficient margin. Need ${required_margin:.0f}, "
            f"available (after buffer): ${available_for_trade:.0f}"
        )

    return True, "OK"


def compute_position_size(
    budget_usd: float,
    leverage: float,
    risk_profile: str,
    current_exposure_usd: float,
    account_balance: float,
) -> float:
    limits = get_risk_limits(risk_profile)
    effective_budget = min(budget_usd, account_balance * limits["max_budget_pct"])
    remaining_budget = max(0, effective_budget - current_exposure_usd)
    max_single = effective_budget * limits["max_single_position_pct"]
    notional = min(remaining_budget, max_single)
    return max(0, notional)


def should_emergency_flatten(
    telegram_id: int,
    bro_settings: dict,
) -> tuple[bool, str]:
    snapshot = get_account_snapshot(telegram_id)
    if not snapshot:
        return False, "Cannot check — snapshot unavailable"

    budget = float(bro_settings.get("budget_usd", 500))
    max_loss_pct = float(bro_settings.get("max_loss_pct", 15))

    total_unrealized_pnl = sum(
        float(p.get("unrealized_pnl", 0) or 0) for p in snapshot.get("positions", [])
    )

    denominator = max(budget, snapshot["usdt_balance"]) if snapshot["usdt_balance"] > 0 else budget
    loss_pct = abs(total_unrealized_pnl) / denominator * 100 if denominator > 0 and total_unrealized_pnl < 0 else 0

    if loss_pct >= max_loss_pct:
        return True, (
            f"Unrealized loss {loss_pct:.1f}% exceeds max {max_loss_pct}% of budget"
        )

    balance = snapshot["usdt_balance"]
    if balance < budget * 0.05:
        return True, f"Account balance ${balance:.0f} critically low"

    return False, "OK"


def get_budget_status(telegram_id: int, bro_settings: dict) -> dict:
    snapshot = get_account_snapshot(telegram_id)
    if not snapshot:
        return {"error": "Cannot fetch account data"}

    budget = float(bro_settings.get("budget_usd", 500))
    risk_profile = bro_settings.get("risk_level", "balanced")
    limits = get_risk_limits(risk_profile)

    effective_budget = min(budget, snapshot["usdt_balance"] * limits["max_budget_pct"])
    remaining = max(0, effective_budget - snapshot["total_exposure_usd"])

    return {
        "account_balance": snapshot["usdt_balance"],
        "budget_usd": budget,
        "effective_budget": effective_budget,
        "current_exposure": snapshot["total_exposure_usd"],
        "remaining_budget": remaining,
        "utilization_pct": (snapshot["total_exposure_usd"] / effective_budget * 100) if effective_budget > 0 else 0,
        "position_count": snapshot["position_count"],
        "max_positions": limits["max_positions"],
        "risk_profile": risk_profile,
        "positions": snapshot["positions"],
    }
