import json
import logging
import time
from datetime import datetime
from typing import Optional

from src.nadobro.config import get_product_id, PRODUCTS
from src.nadobro.models.database import BotState, get_session
from src.nadobro.services.user_service import get_user_nado_client, get_user

logger = logging.getLogger(__name__)

STATE_PREFIX = "whale_strategy:"

SPOT_PRODUCT_IDS = {
    "BTC": 1,
}

PERP_PRODUCT_IDS = {
    "BTC": 2,
    "ETH": 4,
    "SOL": 8,
}


def _state_key(telegram_id: int) -> str:
    return f"{STATE_PREFIX}{telegram_id}"


def _default_state() -> dict:
    return {
        "active": False,
        "mode": "neutral",
        "product": "BTC",
        "target_size_usd": 1000.0,
        "started_at": None,
        "last_signal_at": None,
        "last_signal_price": 0.0,
        "signals_received": 0,
        "pnl_usd": 0.0,
    }


def _load_state(telegram_id: int) -> dict:
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id)).first()
        if not row or not row.value:
            return _default_state()
        try:
            loaded = json.loads(row.value)
            state = _default_state()
            state.update(loaded if isinstance(loaded, dict) else {})
            return state
        except Exception:
            logger.warning("Invalid whale strategy state JSON for user %s", telegram_id)
            return _default_state()


def _save_state(telegram_id: int, state: dict):
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id)).first()
        payload = json.dumps(state)
        if row:
            row.value = payload
            row.updated_at = datetime.utcnow()
        else:
            row = BotState(key=_state_key(telegram_id), value=payload)
            session.add(row)
        session.commit()


def generate_human_update(action: str, price: float, reason: str) -> str:
    action_upper = action.upper()
    price_str = f"${price:,.2f}"

    if action_upper == "LONG":
        emoji = "🐂"
        title = "BULLISH MODE ACTIVATED"
        explanation = (
            f"{emoji} {title}\n\n"
            f"Price: {price_str}\n"
            f"Reason: {reason}\n\n"
            "What this means: A whale (big investor) just bought a LOT of crypto. "
            "When whales buy, prices usually go up. We're riding the wave!\n\n"
            "What the bot did: Closed any short (betting-on-drop) positions and "
            "is now holding spot (the actual coin) to profit from the price going up."
        )
    elif action_upper == "SHORT":
        emoji = "🐻"
        title = "BEARISH MODE ACTIVATED"
        explanation = (
            f"{emoji} {title}\n\n"
            f"Price: {price_str}\n"
            f"Reason: {reason}\n\n"
            "What this means: A whale just dumped a huge amount of crypto. "
            "When whales sell, prices usually drop. We're profiting from the fall!\n\n"
            "What the bot did: Closed any long positions and opened a short "
            "(betting price goes down) to make money as the price drops."
        )
    elif action_upper == "NEUTRAL":
        emoji = "🛡"
        title = "DEFENSIVE MODE ACTIVATED"
        explanation = (
            f"{emoji} {title}\n\n"
            f"Price: {price_str}\n"
            f"Reason: {reason}\n\n"
            "What this means: No clear whale signal, so we're playing it safe. "
            "The bot holds equal long spot + short perp positions, which cancel each other out.\n\n"
            "Why? This 'delta-neutral' setup earns ~15% APR from funding fees with near-zero price risk. "
            "Think of it like a savings account, but for crypto!"
        )
    else:
        emoji = "ℹ️"
        explanation = (
            f"{emoji} STRATEGY UPDATE\n\n"
            f"Price: {price_str}\n"
            f"Action: {action_upper}\n"
            f"Reason: {reason}"
        )

    return explanation


class WhaleStrategy:
    def __init__(self, telegram_id: int, nado_client):
        self.telegram_id = telegram_id
        self.client = nado_client
        self.state = _load_state(telegram_id)

    def _save(self):
        _save_state(self.telegram_id, self.state)

    def _reload(self):
        self.state = _load_state(self.telegram_id)

    def _get_perp_product_id(self) -> int:
        product = self.state.get("product", "BTC")
        return PERP_PRODUCT_IDS.get(product, get_product_id(product) or 2)

    def _get_spot_product_id(self) -> int:
        product = self.state.get("product", "BTC")
        return SPOT_PRODUCT_IDS.get(product, 1)

    def start(self, product: str = "BTC", target_size_usd: float = 1000.0) -> str:
        product = product.upper()
        perp_id = PERP_PRODUCT_IDS.get(product)
        if perp_id is None:
            perp_id = get_product_id(product)
        if perp_id is None:
            return f"Unknown product '{product}'. Supported: {', '.join(PERP_PRODUCT_IDS.keys())}"

        self.state.update({
            "active": True,
            "mode": "neutral",
            "product": product,
            "target_size_usd": float(target_size_usd),
            "started_at": datetime.utcnow().isoformat(),
            "last_signal_at": None,
            "last_signal_price": 0.0,
            "signals_received": 0,
            "pnl_usd": 0.0,
        })
        self._save()
        logger.info(
            "Whale strategy started for user %s on %s with target $%.2f",
            self.telegram_id, product, target_size_usd,
        )
        return (
            f"Whale Strategy activated on {product}!\n"
            f"Target size: ${target_size_usd:,.2f}\n"
            f"Mode: NEUTRAL (delta-neutral farming)\n\n"
            "The bot will now respond to whale signals and automatically "
            "switch between long, short, and neutral positions."
        )

    def stop(self) -> str:
        if not self.state.get("active"):
            return "Whale Strategy is not currently active."

        product = self.state.get("product", "BTC")
        perp_id = self._get_perp_product_id()
        pnl = self.state.get("pnl_usd", 0.0)
        signals = self.state.get("signals_received", 0)

        try:
            logger.info("Stopping whale strategy for user %s, cancelling orders on perp %d", self.telegram_id, perp_id)
            self.client.cancel_all_orders(perp_id)
        except Exception as e:
            logger.error("Error cancelling orders during whale strategy stop for user %s: %s", self.telegram_id, e)

        self.state["active"] = False
        self.state["mode"] = "neutral"
        self._save()

        logger.info("Whale strategy stopped for user %s, PnL: $%.2f", self.telegram_id, pnl)

        pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
        return (
            f"Whale Strategy stopped on {product}.\n"
            f"Total signals processed: {signals}\n"
            f"Estimated PnL: {pnl_str}\n\n"
            "All perp orders have been cancelled. "
            "Spot balances remain in your wallet."
        )

    def process_signal(self, action: str, price: float) -> str:
        action = action.lower().strip()
        if action not in ("long", "short", "neutral"):
            return f"Unknown signal '{action}'. Use 'long', 'short', or 'neutral'."

        if not self.state.get("active"):
            return "Whale Strategy is not active. Use start() first."

        old_mode = self.state.get("mode", "neutral")
        logger.info(
            "Whale strategy signal for user %s: %s -> %s at price %.2f",
            self.telegram_id, old_mode, action, price,
        )

        if action == old_mode:
            self.state["last_signal_at"] = datetime.utcnow().isoformat()
            self.state["last_signal_price"] = price
            self.state["signals_received"] = self.state.get("signals_received", 0) + 1
            self._save()
            return (
                f"Already in {action.upper()} mode. No position changes needed.\n"
                f"Signal acknowledged at ${price:,.2f}."
            )

        if action == "long":
            result = self._execute_long(price)
            reason = "Whale buying detected - riding the pump"
        elif action == "short":
            result = self._execute_short(price)
            reason = "Whale dump detected - profiting from the drop"
        else:
            result = self._execute_neutral(price)
            reason = "No clear signal - farming funding fees in defensive mode"

        self.state["mode"] = action
        self.state["last_signal_at"] = datetime.utcnow().isoformat()
        self.state["last_signal_price"] = price
        self.state["signals_received"] = self.state.get("signals_received", 0) + 1
        self._save()

        human_msg = generate_human_update(action, price, reason)
        full_msg = f"{human_msg}\n\n--- Trade Details ---\n{result}"

        return full_msg

    def _execute_long(self, price: float) -> str:
        product = self.state.get("product", "BTC")
        perp_id = self._get_perp_product_id()
        messages = []

        try:
            logger.info("Executing LONG for user %s: cancelling perp orders on product %d", self.telegram_id, perp_id)
            cancel_result = self.client.cancel_all_orders(perp_id)
            cancelled = cancel_result.get("cancelled", 0)
            if cancelled > 0:
                messages.append(f"Cancelled {cancelled} existing perp order(s).")
        except Exception as e:
            logger.error("Error cancelling orders in _execute_long for user %s: %s", self.telegram_id, e)
            messages.append(f"Warning: Could not cancel existing orders: {e}")

        perp_exposure = self._get_perp_exposure()
        if perp_exposure < 0:
            close_size = abs(perp_exposure)
            logger.info(
                "Closing short perp position for user %s: buying %.6f on product %d",
                self.telegram_id, close_size, perp_id,
            )
            try:
                result = self.client.place_market_order(perp_id, close_size, is_buy=True)
                if result.get("success"):
                    messages.append(f"Closed short perp position: bought {close_size:.6f} {product}-PERP.")
                else:
                    messages.append(f"Failed to close short position: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error closing short in _execute_long for user %s: %s", self.telegram_id, e)
                messages.append(f"Error closing short position: {e}")

        messages.append(f"Now holding spot {product} only (riding the pump).")

        return "\n".join(messages) if messages else "Long mode activated, holding spot exposure."

    def _execute_short(self, price: float) -> str:
        product = self.state.get("product", "BTC")
        perp_id = self._get_perp_product_id()
        target_size_usd = float(self.state.get("target_size_usd", 1000.0))
        target_size = target_size_usd / price if price > 0 else 0
        messages = []

        try:
            logger.info("Executing SHORT for user %s: cancelling perp orders on product %d", self.telegram_id, perp_id)
            cancel_result = self.client.cancel_all_orders(perp_id)
            cancelled = cancel_result.get("cancelled", 0)
            if cancelled > 0:
                messages.append(f"Cancelled {cancelled} existing perp order(s).")
        except Exception as e:
            logger.error("Error cancelling orders in _execute_short for user %s: %s", self.telegram_id, e)
            messages.append(f"Warning: Could not cancel existing orders: {e}")

        perp_exposure = self._get_perp_exposure()
        if perp_exposure > 0:
            logger.info(
                "Closing long perp position for user %s: selling %.6f on product %d",
                self.telegram_id, perp_exposure, perp_id,
            )
            try:
                result = self.client.place_market_order(perp_id, perp_exposure, is_buy=False)
                if result.get("success"):
                    messages.append(f"Closed long perp position: sold {perp_exposure:.6f} {product}-PERP.")
                else:
                    messages.append(f"Failed to close long position: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error closing long in _execute_short for user %s: %s", self.telegram_id, e)
                messages.append(f"Error closing long position: {e}")

        if target_size > 0:
            logger.info(
                "Opening short perp for user %s: selling %.6f on product %d (target $%.2f)",
                self.telegram_id, target_size, perp_id, target_size_usd,
            )
            try:
                result = self.client.place_market_order(perp_id, target_size, is_buy=False)
                if result.get("success"):
                    messages.append(
                        f"Opened short position: {target_size:.6f} {product}-PERP "
                        f"(~${target_size_usd:,.2f} notional)."
                    )
                else:
                    messages.append(f"Failed to open short: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error opening short in _execute_short for user %s: %s", self.telegram_id, e)
                messages.append(f"Error opening short position: {e}")

        return "\n".join(messages) if messages else "Short mode activated."

    def _execute_neutral(self, price: float) -> str:
        product = self.state.get("product", "BTC")
        perp_id = self._get_perp_product_id()
        target_size_usd = float(self.state.get("target_size_usd", 1000.0))
        target_size = target_size_usd / price if price > 0 else 0
        messages = []

        try:
            logger.info("Executing NEUTRAL for user %s: cancelling perp orders on product %d", self.telegram_id, perp_id)
            cancel_result = self.client.cancel_all_orders(perp_id)
            cancelled = cancel_result.get("cancelled", 0)
            if cancelled > 0:
                messages.append(f"Cancelled {cancelled} existing perp order(s).")
        except Exception as e:
            logger.error("Error cancelling orders in _execute_neutral for user %s: %s", self.telegram_id, e)
            messages.append(f"Warning: Could not cancel existing orders: {e}")

        perp_exposure = self._get_perp_exposure()
        spot_balance_usd = self._get_spot_balance_usd(price)
        spot_size = spot_balance_usd / price if price > 0 else 0

        hedge_size = min(spot_size, target_size)

        if perp_exposure > 0:
            logger.info("Closing long perp in neutral mode for user %s: %.6f", self.telegram_id, perp_exposure)
            try:
                result = self.client.place_market_order(perp_id, perp_exposure, is_buy=False)
                if result.get("success"):
                    messages.append(f"Closed long perp: sold {perp_exposure:.6f} {product}-PERP.")
                else:
                    messages.append(f"Failed to close long perp: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error closing long perp in neutral for user %s: %s", self.telegram_id, e)
                messages.append(f"Error closing long perp: {e}")
            perp_exposure = 0.0

        current_short = abs(perp_exposure) if perp_exposure < 0 else 0.0
        needed_short = hedge_size - current_short

        if needed_short > 0.0001:
            logger.info(
                "Opening short perp hedge for user %s: %.6f on product %d",
                self.telegram_id, needed_short, perp_id,
            )
            try:
                result = self.client.place_market_order(perp_id, needed_short, is_buy=False)
                if result.get("success"):
                    messages.append(
                        f"Opened short hedge: {needed_short:.6f} {product}-PERP "
                        f"to match spot exposure."
                    )
                else:
                    messages.append(f"Failed to open hedge: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error opening hedge in neutral for user %s: %s", self.telegram_id, e)
                messages.append(f"Error opening hedge: {e}")
        elif needed_short < -0.0001:
            reduce_size = abs(needed_short)
            logger.info(
                "Reducing short perp hedge for user %s: buying %.6f on product %d",
                self.telegram_id, reduce_size, perp_id,
            )
            try:
                result = self.client.place_market_order(perp_id, reduce_size, is_buy=True)
                if result.get("success"):
                    messages.append(f"Reduced short hedge by {reduce_size:.6f} {product}-PERP to match spot.")
                else:
                    messages.append(f"Failed to reduce hedge: {result.get('error', 'unknown error')}")
            except Exception as e:
                logger.error("Error reducing hedge in neutral for user %s: %s", self.telegram_id, e)
                messages.append(f"Error reducing hedge: {e}")
        else:
            messages.append("Hedge is already balanced with spot exposure.")

        messages.append(
            f"Delta-neutral position active: ~${spot_balance_usd:,.2f} spot + "
            f"matching short perp hedge. Farming funding fees!"
        )

        return "\n".join(messages) if messages else "Neutral mode activated, delta-neutral farming."

    def _get_perp_exposure(self) -> float:
        perp_id = self._get_perp_product_id()
        try:
            orders = self.client.get_open_orders(perp_id)
            total = 0.0
            for order in orders:
                total += float(order.get("amount", 0))
            return total
        except Exception as e:
            logger.error("Error getting perp exposure for user %s: %s", self.telegram_id, e)
            return 0.0

    def _get_spot_balance_usd(self, price: float) -> float:
        spot_id = self._get_spot_product_id()
        try:
            balance = self.client.get_balance()
            balances = balance.get("balances", {})
            spot_amount = float(balances.get(spot_id, 0))
            return spot_amount * price if price > 0 else 0.0
        except Exception as e:
            logger.error("Error getting spot balance for user %s: %s", self.telegram_id, e)
            return 0.0

    def get_status(self) -> dict:
        self._reload()
        return {
            "active": bool(self.state.get("active")),
            "mode": self.state.get("mode", "neutral"),
            "product": self.state.get("product", "BTC"),
            "target_size_usd": self.state.get("target_size_usd", 1000.0),
            "started_at": self.state.get("started_at"),
            "last_signal_at": self.state.get("last_signal_at"),
            "last_signal_price": self.state.get("last_signal_price", 0.0),
            "signals_received": self.state.get("signals_received", 0),
            "pnl_usd": self.state.get("pnl_usd", 0.0),
        }


def get_whale_strategy(telegram_id: int) -> Optional[WhaleStrategy]:
    client = get_user_nado_client(telegram_id)
    if not client:
        logger.warning("Could not get nado client for user %s in whale strategy", telegram_id)
        return None
    return WhaleStrategy(telegram_id, client)
