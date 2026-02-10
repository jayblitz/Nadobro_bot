from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from src.nadobro.config import PRODUCTS

PERP_PRODUCTS = [name for name, info in PRODUCTS.items() if info["type"] == "perp"]

SIZE_PRESETS = {
    "BTC": [0.001, 0.005, 0.01, 0.05, 0.1],
    "ETH": [0.01, 0.05, 0.1, 0.5, 1.0],
    "SOL": [0.1, 0.5, 1, 5, 10],
    "XRP": [10, 50, 100, 500, 1000],
    "BNB": [0.01, 0.05, 0.1, 0.5, 1],
    "LINK": [1, 5, 10, 50, 100],
    "DOGE": [100, 500, 1000, 5000, 10000],
    "AVAX": [0.1, 0.5, 1, 5, 10],
}


def main_menu_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Buy / Long", callback_data="trade:long"),
            InlineKeyboardButton("🔴 Sell / Short", callback_data="trade:short"),
        ],
        [
            InlineKeyboardButton("📋 Positions", callback_data="pos:view"),
            InlineKeyboardButton("👛 Wallet", callback_data="wallet:view"),
        ],
        [
            InlineKeyboardButton("💹 Markets", callback_data="mkt:prices"),
            InlineKeyboardButton("🔔 Alerts", callback_data="alert:menu"),
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings:view"),
            InlineKeyboardButton("❓ Help", callback_data="nav:help"),
        ],
        [
            InlineKeyboardButton("↻ Refresh", callback_data="nav:refresh"),
        ],
    ])


def trade_product_kb(action):
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=f"product:{action}:{name}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def trade_size_kb(product, action):
    presets = SIZE_PRESETS.get(product.upper(), [1, 5, 10, 50, 100])
    rows = []
    row = []
    for s in presets:
        label = str(int(s)) if s == int(s) else str(s)
        row.append(InlineKeyboardButton(label, callback_data=f"size:{action}:{product}:{s}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    row.append(InlineKeyboardButton("✏️ Custom", callback_data=f"size:{action}:{product}:custom"))
    rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"trade:{action}")])
    return InlineKeyboardMarkup(rows)


def trade_leverage_kb(product, action, size):
    leverages = [1, 2, 3, 5, 10, 20, 50]
    rows = []
    row = []
    for lev in leverages:
        row.append(InlineKeyboardButton(f"{lev}x", callback_data=f"leverage:{action}:{product}:{size}:{lev}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"product:{action}:{product}")])
    return InlineKeyboardMarkup(rows)


def trade_confirm_kb(trade_id="pending"):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm Trade", callback_data=f"exec_trade:{trade_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_trade"),
        ],
    ])


def positions_kb(positions):
    rows = []
    seen = set()
    for p in positions:
        pname = p.get("product_name", "").replace("-PERP", "")
        if pname and pname not in seen:
            seen.add(pname)
            rows.append([InlineKeyboardButton(f"❌ Close {pname}-PERP", callback_data=f"pos:close:{pname}")])
    if positions:
        rows.append([InlineKeyboardButton("❌ Close All Positions", callback_data="pos:close_all")])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def wallet_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 Balance", callback_data="wallet:balance"),
        ],
        [
            InlineKeyboardButton("🧪 Testnet", callback_data="wallet:network:testnet"),
            InlineKeyboardButton("🌐 Mainnet", callback_data="wallet:network:mainnet"),
        ],
        [
            InlineKeyboardButton("🚰 Faucet", url="https://testnet.nado.xyz/portfolio/faucet"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def alerts_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔔 Set Alert", callback_data="alert:set"),
            InlineKeyboardButton("📋 My Alerts", callback_data="alert:view"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def alert_product_kb():
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=f"alert:product:{name}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data="alert:menu")])
    return InlineKeyboardMarkup(rows)


def alert_delete_kb(alerts):
    rows = []
    for a in alerts:
        rows.append([InlineKeyboardButton(
            f"🗑 #{a['id']} {a['product']} {a['condition']} ${a['target']:,.2f}",
            callback_data=f"alert:del:{a['id']}"
        )])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="alert:menu")])
    return InlineKeyboardMarkup(rows)


def settings_kb(leverage=1, slippage=1):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"⚡ Default Leverage: {leverage}x", callback_data="settings:leverage_menu"),
        ],
        [
            InlineKeyboardButton(f"📊 Slippage: {slippage}%", callback_data="settings:slippage_menu"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def settings_leverage_kb():
    leverages = [1, 2, 3, 5, 10, 20, 50]
    rows = []
    row = []
    for lev in leverages:
        row.append(InlineKeyboardButton(f"{lev}x", callback_data=f"settings:leverage:{lev}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data="settings:view")])
    return InlineKeyboardMarkup(rows)


def settings_slippage_kb():
    slippages = [0.5, 1, 2, 3, 5]
    row = []
    for s in slippages:
        label = f"{s}%"
        row.append(InlineKeyboardButton(label, callback_data=f"settings:slippage:{s}"))
    return InlineKeyboardMarkup([
        row,
        [InlineKeyboardButton("◀ Back", callback_data="settings:view")],
    ])


def close_product_kb():
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=f"pos:close:{name}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def confirm_close_all_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Close All", callback_data="pos:confirm_close_all"),
            InlineKeyboardButton("❌ Cancel", callback_data="nav:main"),
        ],
    ])


def back_kb(target="main"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ Back", callback_data=f"nav:{target}")],
    ])
