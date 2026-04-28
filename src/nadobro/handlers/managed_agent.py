from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode

from src.nadobro.handlers.formatters import (
    fmt_managed_agent_disabled,
    fmt_managed_agent_enabled,
    fmt_managed_agent_globally_disabled,
    fmt_managed_agent_status,
)
from src.nadobro.handlers.keyboards import persistent_menu_kb
from src.nadobro.handlers.commands import PRIVATE_ACCESS_MSG
from src.nadobro.handlers.keyboards import private_access_kb
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.invite_service import has_private_access
from src.nadobro.services.perf import increment_counter
from src.nadobro.services.managed_agent_state import (
    get_managed_agent_state,
    is_managed_agent_globally_enabled,
    set_managed_agent_enabled,
)


async def _ensure_private_access(update: Update, telegram_id: int) -> bool:
    if await run_blocking(has_private_access, telegram_id):
        return True
    await update.message.reply_text(PRIVATE_ACCESS_MSG, reply_markup=private_access_kb())
    return False


async def cmd_agent_on(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    increment_counter("managed_agent.command.agent_on")
    if not is_managed_agent_globally_enabled():
        await update.message.reply_text(
            fmt_managed_agent_globally_disabled(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return
    set_managed_agent_enabled(telegram_id, True)
    await update.message.reply_text(
        fmt_managed_agent_enabled(),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def cmd_agent_off(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    increment_counter("managed_agent.command.agent_off")
    set_managed_agent_enabled(telegram_id, False)
    await update.message.reply_text(
        fmt_managed_agent_disabled(),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def cmd_agent_status(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    increment_counter("managed_agent.command.agent_status")
    state = get_managed_agent_state(telegram_id)
    enabled = bool(state.get("enabled"))
    updated_at = state.get("updated_at") or "n/a"
    global_enabled = is_managed_agent_globally_enabled()
    effective_enabled = enabled and global_enabled
    await update.message.reply_text(
        fmt_managed_agent_status(effective_enabled, global_enabled, str(updated_at)),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )
