from telegram import Update
from telegram.ext import CallbackContext

from src.nadobro.services.admin_service import is_admin
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.invite_service import (
    generate_invite_codes,
    get_invite_code_status,
    get_user_invite_status,
    grant_private_access,
    revoke_invite_code,
)


def _args(context: CallbackContext) -> list[str]:
    return list(getattr(context, "args", None) or [])


def _is_int(value: str) -> bool:
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _fmt_dt(value) -> str:
    return str(value or "-")[:19]


async def _require_admin(update: Update) -> bool:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return False
    return True


async def cmd_invite_generate(update: Update, context: CallbackContext):
    if not await _require_admin(update):
        return
    args = _args(context)
    count = 1
    note_parts = args
    if args and _is_int(args[0]):
        count = int(args[0])
        note_parts = args[1:]
    note = " ".join(note_parts).strip() or None

    try:
        codes = await run_blocking(
            generate_invite_codes,
            update.effective_user.id,
            count,
            note=note,
        )
    except Exception as exc:
        await update.message.reply_text(f"Failed to generate invite codes: {exc}")
        return

    lines = [f"Generated {len(codes)} invite code(s):"]
    lines.extend(code["code"] for code in codes)
    if note:
        lines.append(f"\nNote: {note}")
    await update.message.reply_text("\n".join(lines))


async def cmd_invite_status(update: Update, context: CallbackContext):
    if not await _require_admin(update):
        return
    args = _args(context)
    if not args:
        await update.message.reply_text("Usage: /invite_status <code|telegram_id>")
        return

    target = args[0]
    if _is_int(target):
        status = await run_blocking(get_user_invite_status, int(target))
        lines = [
            f"User: {status.get('telegram_id')}",
            f"Username: {status.get('telegram_username') or '-'}",
            f"Access: {'granted' if status.get('private_access_granted') else 'not granted'}",
            f"Granted at: {_fmt_dt(status.get('private_access_granted_at'))}",
            f"Granted by: {status.get('private_access_granted_by') or '-'}",
            f"Code prefix: {status.get('code_prefix') or '-'}",
            f"Note: {status.get('note') or '-'}",
        ]
        await update.message.reply_text("\n".join(lines))
        return

    status = await run_blocking(get_invite_code_status, target)
    if not status:
        await update.message.reply_text("Invite code not found.")
        return
    lines = [
        f"Code prefix: {status.get('code_prefix')}",
        f"Created by: {status.get('created_by')}",
        f"Created for: {status.get('created_for_telegram_id') or '-'}",
        f"Redemptions: {status.get('redemption_count')}/{status.get('max_redemptions')}",
        f"Redeemed by: {status.get('redeemed_by') or '-'}",
        f"Redeemed username: {status.get('redeemed_username') or '-'}",
        f"Redeemed at: {_fmt_dt(status.get('redeemed_at'))}",
        f"Expires at: {_fmt_dt(status.get('expires_at'))}",
        f"Revoked at: {_fmt_dt(status.get('revoked_at'))}",
        f"Note: {status.get('note') or '-'}",
    ]
    await update.message.reply_text("\n".join(lines))


async def cmd_invite_revoke(update: Update, context: CallbackContext):
    if not await _require_admin(update):
        return
    args = _args(context)
    if not args:
        await update.message.reply_text("Usage: /invite_revoke <code>")
        return
    ok, msg = await run_blocking(revoke_invite_code, update.effective_user.id, args[0])
    await update.message.reply_text(msg)


async def cmd_invite_grant(update: Update, context: CallbackContext):
    if not await _require_admin(update):
        return
    args = _args(context)
    if not args or not _is_int(args[0]):
        await update.message.reply_text("Usage: /invite_grant <telegram_id> [note]")
        return
    note = " ".join(args[1:]).strip() or None
    ok, msg = await run_blocking(
        grant_private_access,
        update.effective_user.id,
        int(args[0]),
        note,
    )
    await update.message.reply_text(msg)
