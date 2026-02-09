from telegram import InlineKeyboardButton, InlineKeyboardMarkup


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
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("BTC", callback_data=f"product:{action}:BTC"),
            InlineKeyboardButton("ETH", callback_data=f"product:{action}:ETH"),
            InlineKeyboardButton("SOL", callback_data=f"product:{action}:SOL"),
        ],
        [
            InlineKeyboardButton("ARB", callback_data=f"product:{action}:ARB"),
            InlineKeyboardButton("OP", callback_data=f"product:{action}:OP"),
            InlineKeyboardButton("DOGE", callback_data=f"product:{action}:DOGE"),
        ],
        [
            InlineKeyboardButton("LINK", callback_data=f"product:{action}:LINK"),
            InlineKeyboardButton("AVAX", callback_data=f"product:{action}:AVAX"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:trade"),
        ],
    ])


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
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💹 All Prices", callback_data="market:all_prices"),
            InlineKeyboardButton("📈 Funding Rates", callback_data="market:funding"),
        ],
        [
            InlineKeyboardButton("BTC", callback_data="market:price:BTC"),
            InlineKeyboardButton("ETH", callback_data="market:price:ETH"),
            InlineKeyboardButton("SOL", callback_data="market:price:SOL"),
        ],
        [
            InlineKeyboardButton("ARB", callback_data="market:price:ARB"),
            InlineKeyboardButton("OP", callback_data="market:price:OP"),
            InlineKeyboardButton("DOGE", callback_data="market:price:DOGE"),
        ],
        [
            InlineKeyboardButton("LINK", callback_data="market:price:LINK"),
            InlineKeyboardButton("AVAX", callback_data="market:price:AVAX"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:main"),
        ],
    ])


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
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("BTC", callback_data="alert_product:BTC"),
            InlineKeyboardButton("ETH", callback_data="alert_product:ETH"),
            InlineKeyboardButton("SOL", callback_data="alert_product:SOL"),
        ],
        [
            InlineKeyboardButton("ARB", callback_data="alert_product:ARB"),
            InlineKeyboardButton("OP", callback_data="alert_product:OP"),
            InlineKeyboardButton("DOGE", callback_data="alert_product:DOGE"),
        ],
        [
            InlineKeyboardButton("LINK", callback_data="alert_product:LINK"),
            InlineKeyboardButton("AVAX", callback_data="alert_product:AVAX"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:alerts"),
        ],
    ])


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
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("BTC", callback_data="close:product:BTC"),
            InlineKeyboardButton("ETH", callback_data="close:product:ETH"),
            InlineKeyboardButton("SOL", callback_data="close:product:SOL"),
        ],
        [
            InlineKeyboardButton("ARB", callback_data="close:product:ARB"),
            InlineKeyboardButton("OP", callback_data="close:product:OP"),
            InlineKeyboardButton("DOGE", callback_data="close:product:DOGE"),
        ],
        [
            InlineKeyboardButton("LINK", callback_data="close:product:LINK"),
            InlineKeyboardButton("AVAX", callback_data="close:product:AVAX"),
        ],
        [
            InlineKeyboardButton("◀️ Back", callback_data="nav:trade"),
        ],
    ])


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀️ Main Menu", callback_data="nav:main"),
        ],
    ])
