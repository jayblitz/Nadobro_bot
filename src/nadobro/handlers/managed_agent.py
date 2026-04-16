from telegram import Update
from telegram.ext import CallbackContext

from src.nadobro.handlers.keyboards import persistent_menu_kb
from src.nadobro.services.perf import increment_counter
from src.nadobro.services.managed_agent_state import (
    get_managed_agent_state,
    is_managed_agent_globally_enabled,
    set_managed_agent_enabled,
)


async def cmd_agent_on(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    increment_counter("managed_agent.command.agent_on")
    if not is_managed_agent_globally_enabled():
        await update.message.reply_text(
            "Managed AI mode is currently disabled globally by ops. Try again later, boss.",
            reply_markup=persistent_menu_kb(),
        )
        return
    set_managed_agent_enabled(telegram_id, True)
    await update.message.reply_text(
        "Managed AI mode enabled. Hey boss - talk naturally and I will route strategy + analysis for you.",
        reply_markup=persistent_menu_kb(),
    )


async def cmd_agent_off(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    increment_counter("managed_agent.command.agent_off")
    set_managed_agent_enabled(telegram_id, False)
    await update.message.reply_text(
        "Managed AI mode disabled. Back to standard Nadobro flow.",
        reply_markup=persistent_menu_kb(),
    )


async def cmd_agent_status(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    increment_counter("managed_agent.command.agent_status")
    state = get_managed_agent_state(telegram_id)
    enabled = bool(state.get("enabled"))
    updated_at = state.get("updated_at") or "n/a"
    global_enabled = is_managed_agent_globally_enabled()
    effective_enabled = enabled and global_enabled
    status = "ON" if effective_enabled else "OFF"
    global_status = "ENABLED" if global_enabled else "DISABLED"
    await update.message.reply_text(
        f"Managed AI mode: {status}\nGlobal switch: {global_status}\nLast update: {updated_at}",
        reply_markup=persistent_menu_kb(),
    )
