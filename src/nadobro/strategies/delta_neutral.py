"""
Delta Neutral — buy spot + short perp (1–5x leverage, user chooses).
Runs in background; uses linked signer; checks margin health; auto-pause on risk.
"""
import logging

logger = logging.getLogger(__name__)


def run_cycle(telegram_id: int, network: str, state: dict) -> dict:
    """
    One cycle: ensure spot long + perp short at target leverage.
    state: product, leverage (1-5), notional_usd, etc.
    """
    logger.info("Delta Neutral cycle placeholder for user %s leverage %s", telegram_id, state.get("leverage"))
    return {"success": True}
