"""Wallet callback handlers (wallet:* callback data). Rendering helpers live in wallet_view.py.

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_wallet shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.handlers.formatters import escape_md, fmt_wallet_balance_card, fmt_wallet_balance_error, fmt_wallet_connect_card, fmt_wallet_info, fmt_wallet_revoke_steps_card
from src.nadobro.handlers.keyboards import wallet_kb, wallet_kb_not_linked, wallet_revoke_confirm_kb
from src.nadobro.handlers.wallet_view import build_wallet_view_payload
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.user_service import get_user_readonly_client, get_user_wallet_info, switch_network, get_user, remove_user_private_key
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_wallet(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        show_connect = False
        try:
            info = get_user_wallet_info(telegram_id, verify_signer=True)
            is_linked = bool(info and info.get("is_linked"))
            if not is_linked and context is not None:
                from src.nadobro.handlers.wallet_view import _ensure_pending_wallet_signer

                pk_hex, _ = _ensure_pending_wallet_signer(context, telegram_id)
                msg, kb = fmt_wallet_connect_card(pk_hex), wallet_kb_not_linked()
                show_connect = True
            else:
                msg, kb = fmt_wallet_info(info), (wallet_kb() if is_linked else wallet_kb_not_linked())
        except Exception:
            msg, kb = build_wallet_view_payload(telegram_id, context=context, verify_signer=True)
        await _edit_loc(query,
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )
        # SECURITY: this message renders the 1CT signer private key. Remember its
        # coordinates so we can delete it from chat history the moment the wallet
        # is linked (the key is no longer needed once it's on Nado + encrypted).
        if show_connect and context is not None and getattr(query, "message", None) is not None:
            context.user_data["wallet_connect_msg"] = {
                "chat_id": query.message.chat_id,
                "message_id": query.message.message_id,
            }
    elif action == "balance":
        client = get_user_readonly_client(telegram_id)
        if not client:
            await _edit_loc(query, 
                fmt_wallet_balance_error(),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )
            return
        try:
            bal = client.get_balance()
            usdt = (bal.get("balances") or {}).get(0, 0) or (bal.get("balances") or {}).get("0", 0)
            msg = fmt_wallet_balance_card(float(usdt or 0))
        except Exception:
            msg = fmt_wallet_balance_error()
        await _edit_loc(query, msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=wallet_kb())
    elif action == "revoke_steps":
        await _edit_loc(query, fmt_wallet_revoke_steps_card(), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=wallet_revoke_confirm_kb())
    elif action == "revoke_confirm":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        success_msg = "✅ Key reset! Your stored signer has been cleared. Tap 👛 Wallet to link a new 1CT key."
        fail_msg = "❌ {msg}"
        if ok:
            await _edit_loc(query, success_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb_not_linked())
        else:
            await _edit_loc(query, fail_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb(), msg=escape_md(msg))
    elif action == "remove_active":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        prefix = "✅" if ok else "❌"
        await _edit_loc(query, 
            "{prefix} {msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
            prefix=prefix,
            msg=escape_md(msg),
        )
    elif action == "network" and len(parts) >= 3:
        net = parts[2]
        if net not in ("testnet", "mainnet"):
            return

        success, result_msg = await run_blocking(switch_network, telegram_id, net)

        if success:
            info = get_user_wallet_info(telegram_id)
            msg = fmt_wallet_info(info)
            await _edit_loc(query, 
                "{switch_msg}\n\n{wallet_info}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
                switch_msg=escape_md(result_msg),
                wallet_info=msg,
            )
        else:
            await _edit_loc(query, 
                "❌ {msg}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
                msg=escape_md(result_msg),
            )
