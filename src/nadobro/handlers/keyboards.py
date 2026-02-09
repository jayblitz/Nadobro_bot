from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from src.nadobro.config import PRODUCTS

PERP_PRODUCTS = [name for name, info in PRODUCTS.items() if info["type"] == "perp"]


def _product_grid(callback_prefix: str, cols: int = 3) -> list:
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=f"{callback_prefix}{name}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📈 Trade", callback_data="menu:trade"),
            InlineKeyboardButton("📊 Portfolio", callback_data="menu:portfolio"),
        ],
        [
            InlineKeyboardButton("💹 Market", callback_data="menu:market"),
            InlineKeyboardButton("🔔 Alerts", callback_data="menu:alerts"),
        ],
        [
            InlineKeyboardButton("👛 Account", callback_data="menu:account"),
            InlineKeyboardButton("❓ Help", callback_data="menu:help"),
        ],
    ])


def trade_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Long", callback_data="trade:long"),
            InlineKeyboardButton("🔴 Short", callback_data="trade:short"),
        ],
        [
            InlineKeyboardButton("📗 Limit Long", callback_data="trade:limit_long"),
            InlineKeyboardButton("📕 Limit Short", callback_data="trade:limit_short"),
        ],
        [
            InlineKeyboardButton("🎯 Take Profit", callback_data="trade:tp"),
            InlineKeyboardButton("🛑 Stop Loss", callback_data="trade:sl"),
        ],
        [
            InlineKeyboardButton("❌ Close Position", callback_data="trade:close"),
            InlineKeyboardButton("❌ Close All", callback_data="trade:close_all"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:main"),
        ],
    ])


def product_keyboard(action: str) -> InlineKeyboardMarkup:
    rows = _product_grid(f"product:{action}:")
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="nav:trade")])
    return InlineKeyboardMarkup(rows)


def portfolio_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Positions", callback_data="portfolio:positions"),
            InlineKeyboardButton("💰 Balance", callback_data="portfolio:balance"),
        ],
        [
            InlineKeyboardButton("📜 History", callback_data="portfolio:history"),
            InlineKeyboardButton("📊 Analytics", callback_data="portfolio:analytics"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:main"),
        ],
    ])


def market_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("💹 All Prices", callback_data="market:all_prices"),
            InlineKeyboardButton("📈 Funding Rates", callback_data="market:funding"),
        ],
    ]
    rows.extend(_product_grid("market:price:"))
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def alerts_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔔 Set Alert", callback_data="alerts:set"),
            InlineKeyboardButton("📋 My Alerts", callback_data="alerts:view"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:main"),
        ],
    ])


def alert_product_keyboard() -> InlineKeyboardMarkup:
    rows = _product_grid("alert_product:")
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="nav:alerts")])
    return InlineKeyboardMarkup(rows)


def account_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👛 Wallet Info", callback_data="account:wallet"),
            InlineKeyboardButton("🔄 Switch Network", callback_data="account:mode"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:main"),
        ],
    ])


def network_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧪 Testnet", callback_data="network:testnet"),
            InlineKeyboardButton("🌐 Mainnet", callback_data="network:mainnet"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:account"),
        ],
    ])


def confirm_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{action}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{action}"),
        ],
    ])


def close_product_keyboard() -> InlineKeyboardMarkup:
    rows = _product_grid("close:product:")
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="nav:trade")])
    return InlineKeyboardMarkup(rows)


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀️ Main Menu", callback_data="nav:main"),
        ],
    ])
