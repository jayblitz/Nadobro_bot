from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
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


REPLY_BUTTON_MAP = {
    "📊 Trade": "nav:trade",
    "📋 Positions": "pos:view",
    "👛 Wallet": "wallet:view",
    "💹 Markets": "mkt:menu",
    "📈 Strategies": "nav:strategy_hub",
    "🔔 Alerts": "alert:menu",
    "⚙️ Settings": "settings:view",
    "🔄 Mode": "nav:mode",
    "🟢 Long": "trade_flow:direction:long",
    "🔴 Short": "trade_flow:direction:short",
    "◀ Home": "trade_flow:home",
    "📈 Market": "trade_flow:order_type:market",
    "📉 Limit": "trade_flow:order_type:limit",
    "◀ Back": "trade_flow:back",
    "1x": "trade_flow:leverage:1",
    "2x": "trade_flow:leverage:2",
    "3x": "trade_flow:leverage:3",
    "5x": "trade_flow:leverage:5",
    "10x": "trade_flow:leverage:10",
    "20x": "trade_flow:leverage:20",
    "✏️ Custom": "trade_flow:size:custom",
    "📐 Set TP/SL": "trade_flow:tpsl:set",
    "⏭ Skip": "trade_flow:tpsl:skip",
    "Set TP": "trade_flow:tpsl:set_tp",
    "Set SL": "trade_flow:tpsl:set_sl",
    "✅ Done": "trade_flow:tpsl:done",
    "✅ Confirm Trade": "trade_flow:confirm",
    "❌ Cancel": "trade_flow:cancel",
}

for name in PERP_PRODUCTS:
    REPLY_BUTTON_MAP[name] = f"trade_flow:product:{name}"

for preset_product, presets in SIZE_PRESETS.items():
    for s in presets:
        label = str(int(s)) if s == int(s) else str(s)
        REPLY_BUTTON_MAP[label] = f"trade_flow:size:{label}"


def persistent_menu_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Trade"), KeyboardButton("📋 Positions")],
            [KeyboardButton("👛 Wallet"), KeyboardButton("💹 Markets")],
            [KeyboardButton("📈 Strategies"), KeyboardButton("🔔 Alerts")],
            [KeyboardButton("⚙️ Settings"), KeyboardButton("🔄 Mode")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def trade_direction_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🟢 Long"), KeyboardButton("🔴 Short")],
            [KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_order_type_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📈 Market"), KeyboardButton("📉 Limit")],
            [KeyboardButton("◀ Back")],
        ],
        resize_keyboard=True,
    )


def trade_product_reply_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("BTC"), KeyboardButton("ETH"), KeyboardButton("SOL"), KeyboardButton("XRP")],
            [KeyboardButton("BNB"), KeyboardButton("LINK"), KeyboardButton("DOGE"), KeyboardButton("AVAX")],
            [KeyboardButton("◀ Back")],
        ],
        resize_keyboard=True,
    )


def trade_leverage_reply_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("1x"), KeyboardButton("2x"), KeyboardButton("3x")],
            [KeyboardButton("5x"), KeyboardButton("10x"), KeyboardButton("20x")],
            [KeyboardButton("◀ Back")],
        ],
        resize_keyboard=True,
    )


def trade_size_reply_kb(product):
    presets = SIZE_PRESETS.get(product.upper(), [1, 5, 10, 50, 100])
    rows = []
    row = []
    for s in presets:
        label = str(int(s)) if s == int(s) else str(s)
        row.append(KeyboardButton(label))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([KeyboardButton("✏️ Custom")])
    rows.append([KeyboardButton("◀ Back")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def trade_tpsl_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📐 Set TP/SL"), KeyboardButton("⏭ Skip")],
            [KeyboardButton("◀ Back")],
        ],
        resize_keyboard=True,
    )


def trade_tpsl_edit_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Set TP"), KeyboardButton("Set SL")],
            [KeyboardButton("✅ Done"), KeyboardButton("◀ Back")],
        ],
        resize_keyboard=True,
    )


def trade_confirm_reply_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("✅ Confirm Trade"), KeyboardButton("❌ Cancel")],
        ],
        resize_keyboard=True,
    )


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
    leverages = [1, 2, 3, 5, 10, 20, 40]
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
            InlineKeyboardButton("🔑 Import Testnet Key", callback_data="wallet:import:testnet"),
            InlineKeyboardButton("🔑 Import Mainnet Key", callback_data="wallet:import:mainnet"),
        ],
        [
            InlineKeyboardButton("♻️ Rotate Active Key", callback_data="wallet:rotate"),
            InlineKeyboardButton("🗑 Remove Active Key", callback_data="wallet:remove_active"),
        ],
        [
            InlineKeyboardButton("👁️ Review Private Key", callback_data="wallet:view_key"),
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
            InlineKeyboardButton("🛡 Risk Profile", callback_data="settings:risk_menu"),
        ],
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
    leverages = [1, 2, 3, 5, 10, 20, 40]
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


def risk_profile_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛡 Conservative", callback_data="settings:risk:conservative"),
            InlineKeyboardButton("⚖️ Balanced", callback_data="settings:risk:balanced"),
        ],
        [
            InlineKeyboardButton("🔥 Aggressive", callback_data="settings:risk:aggressive"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="settings:view"),
        ],
    ])


def strategy_hub_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📈 Market Maker", callback_data="strategy:preview:mm"),
            InlineKeyboardButton("🧮 Grid", callback_data="strategy:preview:grid"),
        ],
        [
            InlineKeyboardButton("⚖️ Delta Neutral", callback_data="strategy:preview:dn"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def markets_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💹 Price Grid", callback_data="mkt:prices"),
            InlineKeyboardButton("📊 Funding", callback_data="mkt:funding"),
        ],
        [
            InlineKeyboardButton("🔴 Live Last Price", callback_data="mkt:live_menu"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def live_price_asset_kb():
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=f"mkt:live:{name}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ Back", callback_data="mkt:menu")])
    return InlineKeyboardMarkup(rows)


def live_price_controls_kb(product: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛑 Stop Live", callback_data="mkt:live_stop"),
            InlineKeyboardButton("Switch Asset", callback_data="mkt:live_menu"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="mkt:menu"),
        ],
    ])


def strategy_action_kb(strategy_id: str, selected_product: str = "BTC"):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Mark Active", callback_data=f"strategy:activate:{strategy_id}"),
            InlineKeyboardButton("⚙️ Tune Risk", callback_data="settings:risk_menu"),
        ],
        [
            InlineKeyboardButton("🧩 Edit Strategy Params", callback_data=f"strategy:config:{strategy_id}"),
        ],
        [
            InlineKeyboardButton("BTC", callback_data=f"strategy:pair:{strategy_id}:BTC"),
            InlineKeyboardButton("ETH", callback_data=f"strategy:pair:{strategy_id}:ETH"),
            InlineKeyboardButton("SOL", callback_data=f"strategy:pair:{strategy_id}:SOL"),
        ],
        [
            InlineKeyboardButton(
                f"🚀 Start {selected_product.upper()}",
                callback_data=f"strategy:start:{strategy_id}:{selected_product.upper()}",
            ),
        ],
        [
            InlineKeyboardButton("🔄 Refresh Analytics", callback_data=f"strategy:preview:{strategy_id}"),
        ],
        [
            InlineKeyboardButton("📡 Bot Status", callback_data="strategy:status"),
            InlineKeyboardButton("🛑 Stop Bot", callback_data="strategy:stop"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub"),
        ],
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


def mode_kb(current_network="testnet"):
    testnet_label = "🧪 Testnet ✅" if current_network == "testnet" else "🧪 Testnet"
    mainnet_label = "🌐 Mainnet ✅" if current_network == "mainnet" else "🌐 Mainnet"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(testnet_label, callback_data="mode:testnet"),
            InlineKeyboardButton(mainnet_label, callback_data="mode:mainnet"),
        ],
    ])


def back_kb(target="main"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ Back", callback_data=f"nav:{target}")],
    ])


def onboarding_nav_kb(step: str, allow_skip: bool = False, allow_back: bool = True):
    rows = []
    nav_row = []
    if allow_back:
        nav_row.append(InlineKeyboardButton("◀ Back", callback_data="onboarding:back"))
    nav_row.append(InlineKeyboardButton("Next ▶", callback_data="onboarding:next"))
    rows.append(nav_row)
    if allow_skip:
        rows.append([InlineKeyboardButton("Skip this step", callback_data="onboarding:skip")])
    rows.append([InlineKeyboardButton("Go to Dashboard", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def onboarding_mode_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧪 Use Testnet", callback_data="onboarding:set_mode:testnet"),
            InlineKeyboardButton("🌐 Use Mainnet", callback_data="onboarding:set_mode:mainnet"),
        ],
        [
            InlineKeyboardButton("Next ▶", callback_data="onboarding:next"),
        ],
        [
            InlineKeyboardButton("Go to Dashboard", callback_data="nav:main"),
        ],
    ])


def onboarding_key_kb(network: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"🔑 Import {network.upper()} Key",
                callback_data=f"wallet:import:{network}",
            ),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="onboarding:back"),
            InlineKeyboardButton("Next ▶", callback_data="onboarding:next"),
        ],
        [
            InlineKeyboardButton("Open Wallet", callback_data="wallet:view"),
        ],
    ])


def onboarding_funding_kb(network: str):
    faucet_url = "https://testnet.nado.xyz/portfolio/faucet" if network == "testnet" else "https://nado.xyz"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("↻ Check Again", callback_data="onboarding:check_funding")],
        [InlineKeyboardButton("Open Funding Page", url=faucet_url)],
        [
            InlineKeyboardButton("◀ Back", callback_data="onboarding:back"),
            InlineKeyboardButton("Next ▶", callback_data="onboarding:next"),
        ],
    ])


def onboarding_risk_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛡 Conservative", callback_data="onboarding:set_risk:conservative"),
            InlineKeyboardButton("⚖️ Balanced", callback_data="onboarding:set_risk:balanced"),
        ],
        [InlineKeyboardButton("🔥 Aggressive", callback_data="onboarding:set_risk:aggressive")],
        [
            InlineKeyboardButton("◀ Back", callback_data="onboarding:back"),
            InlineKeyboardButton("Next ▶", callback_data="onboarding:next"),
        ],
        [InlineKeyboardButton("Skip this step", callback_data="onboarding:skip")],
    ])


def onboarding_template_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📈 MM Starter", callback_data="onboarding:set_template:mm"),
            InlineKeyboardButton("🧮 Grid Starter", callback_data="onboarding:set_template:grid"),
        ],
        [InlineKeyboardButton("⚖️ DN Starter", callback_data="onboarding:set_template:dn")],
        [
            InlineKeyboardButton("◀ Back", callback_data="onboarding:back"),
            InlineKeyboardButton("Next ▶", callback_data="onboarding:next"),
        ],
        [InlineKeyboardButton("Skip this step", callback_data="onboarding:skip")],
    ])
