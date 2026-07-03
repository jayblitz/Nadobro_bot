from telegram.ext import CallbackContext

from src.nadobro.services.strategy_pending_input import clear_strategy_pending_input
from src.nadobro.services.text_trade_pending import (
    clear_text_close_all_pending,
    clear_text_trade_pending,
)
from src.nadobro.services.wallet_pending_flow import clear_wallet_pending_flow

# Any in-progress conversational/multi-step state that should be discarded
# when users intentionally navigate back home.
_TRANSIENT_USER_DATA_KEYS = (
    "pending_trade",
    "pending_question",
    "pending_alert",
    "pending_strategy_input",
    "pending_bro_input",
    "pending_copy_wallet",
    "pending_admin_copy_wallet",
    "pending_referral_claim",
    "wallet_flow",
    "wallet_linked_signer_pk",
    "wallet_main_address",
    "wallet_linked_signer_address",
    "pending_text_trade",
    "pending_text_close_all",
    "trade_flow",
    "trade_flow_custom_size",
    "trade_flow_tp_input",
    "trade_flow_sl_input",
    "trade_flow_limit_price_input",
    "copy_setup",
    "active_setup",
    "vault_pending_amount",
)


def clear_pending_user_state(context: CallbackContext | None, telegram_user_id: int | None = None) -> None:
    if context is None:
        return
    for key in _TRANSIENT_USER_DATA_KEYS:
        context.user_data.pop(key, None)
    if telegram_user_id is not None:
        uid = int(telegram_user_id)
        for clearer in (
            clear_strategy_pending_input,
            clear_text_trade_pending,
            clear_text_close_all_pending,
            clear_wallet_pending_flow,
        ):
            try:
                clearer(uid)
            except Exception:
                pass
