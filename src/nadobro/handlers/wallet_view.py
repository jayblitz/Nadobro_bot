from eth_account import Account

from src.nadobro.handlers.formatters import fmt_wallet_connect_card, fmt_wallet_info
from src.nadobro.handlers.keyboards import wallet_kb, wallet_kb_not_linked
from src.nadobro.services.user_service import get_user_wallet_info
from src.nadobro.services.wallet_pending_flow import (
    load_wallet_pending_flow,
    persist_wallet_pending_flow,
)


def _seed_wallet_flow_context(context, *, flow: str, pk_hex: str, linked_addr: str) -> None:
    context.user_data["wallet_flow"] = flow
    context.user_data["wallet_linked_signer_pk"] = pk_hex
    context.user_data["wallet_linked_signer_address"] = linked_addr


def hydrate_wallet_flow_context(context, telegram_id: int) -> bool:
    if context.user_data.get("wallet_flow"):
        return True
    pending = load_wallet_pending_flow(int(telegram_id))
    if not pending:
        return False
    _seed_wallet_flow_context(
        context,
        flow=pending["flow"],
        pk_hex=pending["pk_hex"],
        linked_addr=pending["linked_signer_address"],
    )
    return True


def _ensure_pending_wallet_signer(context, telegram_id: int) -> tuple[str, str]:
    pk_hex = str(context.user_data.get("wallet_linked_signer_pk") or "").strip()
    linked_addr = str(context.user_data.get("wallet_linked_signer_address") or "").strip()
    if pk_hex and linked_addr:
        persist_wallet_pending_flow(
            telegram_id,
            flow=str(context.user_data.get("wallet_flow") or "awaiting_main_address"),
            pk_hex=pk_hex,
            linked_signer_address=linked_addr,
        )
        return pk_hex, linked_addr
    pending = load_wallet_pending_flow(int(telegram_id))
    if pending:
        _seed_wallet_flow_context(
            context,
            flow=pending["flow"],
            pk_hex=pending["pk_hex"],
            linked_addr=pending["linked_signer_address"],
        )
        return pending["pk_hex"], pending["linked_signer_address"]
    account = Account.create()
    pk_hex = account.key.hex()
    if not pk_hex.startswith("0x"):
        pk_hex = "0x" + pk_hex
    linked_addr = account.address
    _seed_wallet_flow_context(
        context,
        flow="awaiting_main_address",
        pk_hex=pk_hex,
        linked_addr=linked_addr,
    )
    persist_wallet_pending_flow(
        telegram_id,
        flow="awaiting_main_address",
        pk_hex=pk_hex,
        linked_signer_address=linked_addr,
    )
    return pk_hex, linked_addr


def build_wallet_view_payload(telegram_id: int, context=None, verify_signer: bool = True):
    info = get_user_wallet_info(telegram_id, verify_signer=verify_signer)
    is_linked = bool(info and info.get("is_linked"))
    if not is_linked and context is not None:
        pk_hex, _ = _ensure_pending_wallet_signer(context, telegram_id)
        return fmt_wallet_connect_card(pk_hex), wallet_kb_not_linked()
    return fmt_wallet_info(info), (wallet_kb() if is_linked else wallet_kb_not_linked())
