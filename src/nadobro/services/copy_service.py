import logging
from datetime import datetime
from typing import Optional

from src.nadobro.models.database import (
    upsert_copy_trader,
    get_copy_trader,
    get_copy_trader_by_wallet,
    get_active_copy_traders,
    get_curated_copy_traders,
    deactivate_copy_trader,
    create_copy_mirror,
    get_copy_mirror,
    get_user_active_mirrors,
    get_mirrors_for_trader,
    stop_copy_mirror,
    update_mirror_last_synced,
    count_user_active_mirrors,
    insert_copy_trade,
    copy_trade_exists,
    get_copy_trades_by_user,
    get_copy_trades_by_mirror,
    get_active_trader_wallets,
)
from src.nadobro.services.copy_asset_map import (
    hl_coin_to_nado_product_id,
    is_supported_coin,
    get_nado_product_name,
)
from src.nadobro.services.hl_client import get_hl_client
from src.nadobro.services.user_service import get_user, get_user_nado_client
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.bot_runtime import get_runtime_passphrase
from src.nadobro.config import get_product_max_leverage

logger = logging.getLogger(__name__)

MAX_MIRRORS_PER_USER = 5
MIN_BUDGET_USD = 10.0
MAX_BUDGET_USD = 10000.0
MIN_RISK_FACTOR = 0.1
MAX_RISK_FACTOR = 5.0
MIN_COPY_SIZE_USD = 1.0

_bot_app = None


def set_copy_bot_app(app):
    global _bot_app
    _bot_app = app


async def _notify_user(telegram_id: int, text: str):
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text)
    except Exception as e:
        logger.warning("Copy notify failed for %s: %s", telegram_id, e)


def add_trader(wallet_address: str, label: str = "", is_curated: bool = False) -> tuple[bool, str, int | None]:
    if not wallet_address or len(wallet_address) < 10:
        return False, "Invalid wallet address.", None
    wallet = wallet_address.strip()
    if not wallet.startswith("0x"):
        return False, "Wallet address must start with 0x.", None
    trader_id = upsert_copy_trader(wallet, label=label, is_curated=is_curated)
    if not trader_id:
        return False, "Failed to save trader.", None
    return True, f"Trader added: {label or wallet[:10]}...", trader_id


def remove_trader(trader_id: int) -> tuple[bool, str]:
    trader = get_copy_trader(trader_id)
    if not trader:
        return False, "Trader not found."
    mirrors = get_mirrors_for_trader(trader_id)
    for m in mirrors:
        stop_copy_mirror(m["id"])
    deactivate_copy_trader(trader_id)
    label = trader.get("label") or trader["wallet_address"][:10]
    return True, f"Trader {label} removed. {len(mirrors)} mirror(s) stopped."


def start_copy(
    telegram_id: int,
    trader_id: int,
    budget_usd: float = 100.0,
    risk_factor: float = 1.0,
    max_leverage: float = 10.0,
) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not registered."
    if not user.linked_signer_address:
        return False, "Please link a wallet first."

    trader = get_copy_trader(trader_id)
    if not trader or not trader.get("active"):
        return False, "Trader not found or inactive."

    if budget_usd < MIN_BUDGET_USD or budget_usd > MAX_BUDGET_USD:
        return False, f"Budget must be between ${MIN_BUDGET_USD} and ${MAX_BUDGET_USD}."
    if risk_factor < MIN_RISK_FACTOR or risk_factor > MAX_RISK_FACTOR:
        return False, f"Risk factor must be between {MIN_RISK_FACTOR}x and {MAX_RISK_FACTOR}x."

    count = count_user_active_mirrors(telegram_id)
    if count >= MAX_MIRRORS_PER_USER:
        return False, f"Maximum {MAX_MIRRORS_PER_USER} simultaneous copy traders allowed."

    mirror_id = create_copy_mirror(
        user_id=telegram_id,
        trader_id=trader_id,
        budget_usd=budget_usd,
        risk_factor=risk_factor,
        max_leverage=max_leverage,
    )
    if not mirror_id:
        return False, "Failed to create copy mirror."

    label = trader.get("label") or trader["wallet_address"][:10]
    return True, (
        f"Now copying {label}\n"
        f"Budget: ${budget_usd:.0f} | Risk: {risk_factor}x | Max Leverage: {max_leverage}x"
    )


def stop_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror already stopped."
    stop_copy_mirror(mirror_id)
    return True, "Copy trading stopped for this trader."


def stop_all_copies(telegram_id: int) -> tuple[bool, str]:
    mirrors = get_user_active_mirrors(telegram_id)
    if not mirrors:
        return False, "No active copy mirrors."
    for m in mirrors:
        stop_copy_mirror(m["id"])
    return True, f"Stopped {len(mirrors)} copy mirror(s)."


def get_user_copies(telegram_id: int) -> list[dict]:
    mirrors = get_user_active_mirrors(telegram_id)
    result = []
    for m in mirrors:
        trades = get_copy_trades_by_mirror(m["id"], limit=10)
        filled = sum(1 for t in trades if t.get("status") == "filled")
        failed = sum(1 for t in trades if t.get("status") == "failed")
        result.append({
            "mirror_id": m["id"],
            "trader_label": m.get("label") or m["wallet_address"][:10],
            "wallet": m["wallet_address"],
            "budget_usd": m["budget_usd"],
            "risk_factor": m["risk_factor"],
            "max_leverage": m["max_leverage"],
            "recent_filled": filled,
            "recent_failed": failed,
            "created_at": m.get("created_at"),
        })
    return result


def get_user_mirrors(telegram_id: int) -> list[dict]:
    return get_user_copies(telegram_id)


def get_available_traders() -> list[dict]:
    traders = get_active_copy_traders()
    return [
        {
            "id": t["id"],
            "wallet": t["wallet_address"],
            "label": t.get("label") or t["wallet_address"][:10],
            "is_curated": t.get("is_curated", False),
        }
        for t in traders
    ]


async def process_hl_fill(wallet_address: str, fill: dict):
    trader = get_copy_trader_by_wallet(wallet_address)
    if not trader:
        return

    mirrors = get_mirrors_for_trader(trader["id"])
    if not mirrors:
        return

    coin = fill.get("coin", "")
    if not is_supported_coin(coin):
        logger.debug("Unsupported coin %s from HL fill, skipping", coin)
        for m in mirrors:
            await _notify_user(
                m["user_id"],
                f"ℹ️ Copy Trade Skipped\n"
                f"Trader opened {coin} — not available on Nado DEX.\n"
                f"Supported: BTC, ETH, SOL, XRP, BNB, LINK, AVAX, DOGE",
            )
        return

    nado_product_id = hl_coin_to_nado_product_id(coin)
    if nado_product_id is None:
        return

    fill_tid = int(fill.get("tid", 0))
    hl_side = fill.get("side", "").upper()
    hl_size = abs(float(fill.get("sz", 0)))
    hl_price = float(fill.get("px", 0))

    if hl_size <= 0 or hl_price <= 0:
        return

    is_buy = hl_side == "B" or hl_side == "BUY"
    is_close = fill.get("closedPnl") and float(fill.get("closedPnl", "0")) != 0

    hl_client = get_hl_client()
    leader_equity = await hl_client.get_account_equity(wallet_address)

    for mirror in mirrors:
        user_id = mirror["user_id"]

        if fill_tid and copy_trade_exists(fill_tid, user_id):
            continue

        try:
            await _execute_mirror_trade(
                user_id=user_id,
                mirror=mirror,
                coin=coin,
                nado_product_id=nado_product_id,
                fill_tid=fill_tid,
                is_buy=is_buy,
                is_close=is_close,
                hl_size=hl_size,
                hl_price=hl_price,
                leader_equity=leader_equity,
            )
        except Exception as e:
            logger.error(
                "Copy trade failed for user %s mirror %s: %s",
                user_id, mirror["id"], e, exc_info=True,
            )
            insert_copy_trade({
                "user_id": user_id,
                "mirror_id": mirror["id"],
                "hl_fill_tid": fill_tid,
                "hl_coin": coin,
                "nado_product_id": nado_product_id,
                "side": "long" if is_buy else "short",
                "hl_size": hl_size,
                "hl_price": hl_price,
                "status": "failed",
                "error_message": str(e)[:300],
            })

        if fill_tid:
            update_mirror_last_synced(mirror["id"], fill_tid)


async def _execute_mirror_trade(
    user_id: int,
    mirror: dict,
    coin: str,
    nado_product_id: int,
    fill_tid: int,
    is_buy: bool,
    is_close: bool,
    hl_size: float,
    hl_price: float,
    leader_equity: float | None,
):
    budget_usd = float(mirror.get("budget_usd", 100))
    risk_factor = float(mirror.get("risk_factor", 1.0))
    max_leverage = float(mirror.get("max_leverage", 10.0))

    if leader_equity and leader_equity > 0:
        size_ratio = budget_usd / leader_equity
    else:
        logger.warning(
            "Cannot fetch leader equity for %s, skipping copy for user %s",
            mirror.get("wallet_address", "?")[:10], user_id,
        )
        return

    nado_size = hl_size * size_ratio * risk_factor

    nado_notional = nado_size * hl_price
    if nado_notional > budget_usd:
        nado_size = budget_usd / hl_price
        nado_notional = budget_usd
        logger.debug("Capped copy size to budget $%.0f for user %s", budget_usd, user_id)

    if nado_notional < MIN_COPY_SIZE_USD:
        logger.debug(
            "Copy trade too small ($%.2f) for user %s, skipping",
            nado_notional, user_id,
        )
        return

    product_name = get_nado_product_name(nado_product_id)
    product_key = coin.upper()

    product_max_lev = get_product_max_leverage(product_key)
    leverage = min(max_leverage, product_max_lev)

    user = get_user(user_id)
    if not user:
        return
    network = user.network_mode.value

    passphrase = get_runtime_passphrase(user_id, network)

    if is_close:
        from src.nadobro.services.trade_service import close_position

        side_str = "long" if is_buy else "short"
        copy_trade_id = insert_copy_trade({
            "user_id": user_id,
            "mirror_id": mirror["id"],
            "hl_fill_tid": fill_tid,
            "hl_coin": coin,
            "nado_product_id": nado_product_id,
            "side": side_str,
            "hl_size": hl_size,
            "hl_price": hl_price,
            "nado_size": nado_size,
            "status": "pending",
        })

        result = close_position(
            telegram_id=user_id,
            product=product_key,
            passphrase=passphrase,
        )

        if result.get("success"):
            from src.nadobro.db import execute as db_execute
            db_execute(
                "UPDATE copy_trades SET status = 'filled', nado_price = %s, filled_at = %s WHERE id = %s",
                (result.get("price", 0), datetime.utcnow().isoformat(), copy_trade_id),
            )
            trader_label = mirror.get("label") or mirror.get("wallet_address", "")[:10]
            await _notify_user(
                user_id,
                f"📋 Copy Close Filled\n"
                f"Trader: {trader_label}\n"
                f"Closed {product_name} position",
            )
        else:
            error = result.get("error", "Unknown error")
            from src.nadobro.db import execute as db_execute
            db_execute(
                "UPDATE copy_trades SET status = 'failed', error_message = %s WHERE id = %s",
                (error[:300], copy_trade_id),
            )
        return

    side_str = "long" if is_buy else "short"

    copy_trade_id = insert_copy_trade({
        "user_id": user_id,
        "mirror_id": mirror["id"],
        "hl_fill_tid": fill_tid,
        "hl_coin": coin,
        "nado_product_id": nado_product_id,
        "side": side_str,
        "hl_size": hl_size,
        "hl_price": hl_price,
        "nado_size": nado_size,
        "status": "pending",
    })

    result = execute_market_order(
        telegram_id=user_id,
        product=product_key,
        size=nado_size,
        is_long=is_buy,
        leverage=leverage,
        slippage_pct=1.5,
        enforce_rate_limit=False,
        passphrase=passphrase,
    )

    if result.get("success"):
        from src.nadobro.db import execute as db_execute
        db_execute(
            "UPDATE copy_trades SET status = 'filled', nado_price = %s, nado_trade_id = %s, filled_at = %s WHERE id = %s",
            (result.get("price", 0), result.get("trade_id"), datetime.utcnow().isoformat(), copy_trade_id),
        )
        trader_label = mirror.get("label") or mirror.get("wallet_address", "")[:10]
        await _notify_user(
            user_id,
            f"📋 Copy Trade Filled\n"
            f"Trader: {trader_label}\n"
            f"{'🟢 BUY' if is_buy else '🔴 SELL'} {nado_size:.6f} {product_name}\n"
            f"HL: ${hl_price:,.2f} → Nado: ${result.get('price', 0):,.2f}",
        )
        logger.info(
            "Copy trade filled: user=%s mirror=%s %s %.6f %s",
            user_id, mirror["id"], side_str, nado_size, product_name,
        )
    else:
        error = result.get("error", "Unknown error")
        from src.nadobro.db import execute as db_execute
        db_execute(
            "UPDATE copy_trades SET status = 'failed', error_message = %s WHERE id = %s",
            (error[:300], copy_trade_id),
        )
        await _notify_user(
            user_id,
            f"⚠️ Copy Trade Failed\n"
            f"Trader: {mirror.get('label') or mirror.get('wallet_address', '')[:10]}\n"
            f"{coin} {'BUY' if is_buy else 'SELL'}: {error[:200]}",
        )
        logger.warning(
            "Copy trade failed: user=%s mirror=%s error=%s",
            user_id, mirror["id"], error[:200],
        )
