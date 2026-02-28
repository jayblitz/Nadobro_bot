"""
Volume Bot — $100 flips until target volume; shows fee/PnL preview first.
Runs in background; uses linked signer; checks margin health; auto-pause on risk.
"""
import logging

logger = logging.getLogger(__name__)


def get_fee_pnl_preview(telegram_id: int, product: str, target_volume_usd: float) -> dict:
    """Return estimated fees and PnL range for target volume (for user confirmation)."""
    return {"target_volume_usd": target_volume_usd, "estimated_fees": 0, "preview": "Placeholder"}


def run_cycle(telegram_id: int, network: str, state: dict) -> dict:
    """
    One cycle: place $100 flip (long/short) toward target volume.
    state: product, target_volume_usd, current_volume_usd, etc.
    """
    logger.info("Volume bot cycle placeholder for user %s target %s", telegram_id, state.get("target_volume_usd"))
    return {"success": True, "volume_done": 0}
