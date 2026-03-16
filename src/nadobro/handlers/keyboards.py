from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from src.nadobro.config import PRODUCTS, DUAL_MODE_CARD_FLOW, get_product_max_leverage

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


HOME_BTN_TRADE = "🤖 Trade Console"
HOME_BTN_PORTFOLIO = "📁 Portfolio Deck"
HOME_BTN_HOME = "🏠 Home"
HOME_BTN_WALLET = "💼 Wallet Vault"
HOME_BTN_MARKETS = "📡 Market Radar"
HOME_BTN_STRATEGIES = "🧠 Strategy Lab"
HOME_BTN_ALERTS = "🔔 Alert Engine"
HOME_BTN_SETTINGS = "⚙️ Control Panel"
HOME_BTN_MODE = "🌐 Execution Mode"


REPLY_BUTTON_MAP = {
    HOME_BTN_HOME: "nav:main",
    HOME_BTN_TRADE: "nav:trade",
    HOME_BTN_PORTFOLIO: "portfolio:view",
    HOME_BTN_WALLET: "wallet:view",
    HOME_BTN_MARKETS: "mkt:menu",
    HOME_BTN_STRATEGIES: "nav:strategy_hub",
    HOME_BTN_ALERTS: "alert:menu",
    HOME_BTN_SETTINGS: "settings:view",
    HOME_BTN_MODE: "nav:mode",
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

# Backward-compatible aliases for older keyboards/client emoji fallbacks.
REPLY_BUTTON_MAP.update({
    "📋 Positions": "pos:view",
    "📁 Portfolio": "portfolio:view",
    "Portfolio": "portfolio:view",
    "👛 Wallet": "wallet:view",
    "Trade": "nav:trade",
    "Home": "nav:main",
    "Positions": "pos:view",
    "Wallet": "wallet:view",
    "Markets": "mkt:menu",
    "Strategies": "nav:strategy_hub",
    "Alerts": "alert:menu",
    "Settings": "settings:view",
    "Mode": "nav:mode",
})

for name in PERP_PRODUCTS:
    REPLY_BUTTON_MAP[name] = f"trade_flow:product:{name}"

for preset_product, presets in SIZE_PRESETS.items():
    for s in presets:
        label = str(int(s)) if s == int(s) else str(s)
        REPLY_BUTTON_MAP[label] = f"trade_flow:size:{label}"


# --- New onboarding (language + ToS) ---
def onboarding_language_kb():
    """2 columns: 🇬🇧 English, 🇨🇳 Chinese; 🇫🇷 Français, 🇸🇦 العربية; 🇷🇺 Русский, 🇰🇷 Korean"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="onb:lang:en"),
            InlineKeyboardButton("🇨🇳 Chinese", callback_data="onb:lang:zh"),
        ],
        [
            InlineKeyboardButton("🇫🇷 Français", callback_data="onb:lang:fr"),
            InlineKeyboardButton("🇸🇦 العربية", callback_data="onb:lang:ar"),
        ],
        [
            InlineKeyboardButton("🇷🇺 Русский", callback_data="onb:lang:ru"),
            InlineKeyboardButton("🇰🇷 Korean", callback_data="onb:lang:ko"),
        ],
    ])


def onboarding_accept_tos_kb():
    """Big green Let's Get It button"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Let's Get It 🔥", callback_data="onb:accept_tos")],
    ])


def persistent_menu_kb():
    if DUAL_MODE_CARD_FLOW:
        return ReplyKeyboardMarkup(
            [[KeyboardButton(HOME_BTN_HOME)]],
            resize_keyboard=True,
            is_persistent=True,
        )
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(HOME_BTN_TRADE), KeyboardButton(HOME_BTN_PORTFOLIO)],
            [KeyboardButton(HOME_BTN_WALLET), KeyboardButton(HOME_BTN_MARKETS)],
            [KeyboardButton(HOME_BTN_STRATEGIES), KeyboardButton(HOME_BTN_ALERTS)],
            [KeyboardButton(HOME_BTN_SETTINGS), KeyboardButton(HOME_BTN_MODE)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def home_card_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🤖 Trade Console", callback_data="card:trade:start"),
            InlineKeyboardButton("📁 Portfolio Deck", callback_data="portfolio:view"),
        ],
        [
            InlineKeyboardButton("💼 Wallet Vault", callback_data="wallet:view"),
            InlineKeyboardButton("📡 Market Radar", callback_data="mkt:menu"),
        ],
        [
            InlineKeyboardButton("🧠 Strategy Lab", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("🔔 Alert Engine", callback_data="alert:menu"),
        ],
        [
            InlineKeyboardButton("⚙️ Control Panel", callback_data="settings:view"),
            InlineKeyboardButton("🌐 Execution Mode", callback_data="home:mode"),
        ],
    ])


def portfolio_kb(has_positions: bool = False):
    rows = [
        [InlineKeyboardButton("📌 Open Positions", callback_data="pos:view")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="portfolio:view")],
    ]
    if has_positions:
        rows.insert(1, [InlineKeyboardButton("❌ Close All Positions", callback_data="pos:close_all")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


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
            [KeyboardButton("◀ Back"), KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_product_reply_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("BTC"), KeyboardButton("ETH"), KeyboardButton("SOL"), KeyboardButton("XRP")],
            [KeyboardButton("BNB"), KeyboardButton("LINK"), KeyboardButton("DOGE"), KeyboardButton("AVAX")],
            [KeyboardButton("◀ Back"), KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_leverage_reply_kb(product: str = "BTC"):
    leverages = [lev for lev in [1, 2, 3, 5, 10, 20, 40] if lev <= get_product_max_leverage(product)]
    rows = []
    if leverages[:3]:
        rows.append([KeyboardButton(f"{lev}x") for lev in leverages[:3]])
    if leverages[3:]:
        rows.append([KeyboardButton(f"{lev}x") for lev in leverages[3:]])
    rows.append([KeyboardButton("◀ Back"), KeyboardButton("◀ Home")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


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
    rows.append([KeyboardButton("◀ Back"), KeyboardButton("◀ Home")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def trade_tpsl_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📐 Set TP/SL"), KeyboardButton("⏭ Skip")],
            [KeyboardButton("◀ Back"), KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_tpsl_edit_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Set TP"), KeyboardButton("Set SL")],
            [KeyboardButton("✅ Done"), KeyboardButton("◀ Back"), KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_confirm_reply_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("✅ Confirm Trade"), KeyboardButton("❌ Cancel")],
            [KeyboardButton("◀ Home")],
        ],
        resize_keyboard=True,
    )


def trade_card_cb(session_id: str, action: str, value: str = "") -> str:
    if value:
        return f"card:trade:{session_id}:{action}:{value}"
    return f"card:trade:{session_id}:{action}"


def trade_card_start_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Trade", callback_data="card:trade:start")],
    ])


def trade_card_direction_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Long", callback_data=trade_card_cb(session_id, "direction", "long")),
            InlineKeyboardButton("🔴 Short", callback_data=trade_card_cb(session_id, "direction", "short")),
        ],
        [
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
            InlineKeyboardButton("❌ Cancel", callback_data=trade_card_cb(session_id, "cancel")),
        ],
    ])


def trade_card_order_type_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📈 Market", callback_data=trade_card_cb(session_id, "order", "market")),
            InlineKeyboardButton("📉 Limit", callback_data=trade_card_cb(session_id, "order", "limit")),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_product_kb(session_id: str):
    rows = []
    row = []
    for name in PERP_PRODUCTS:
        row.append(InlineKeyboardButton(name, callback_data=trade_card_cb(session_id, "product", name)))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
        InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
    ])
    return InlineKeyboardMarkup(rows)


def trade_card_leverage_kb(session_id: str, product: str = "BTC"):
    max_leverage = get_product_max_leverage(product)
    leverages = [lev for lev in [1, 2, 3, 5, 10, 20, 40] if lev <= max_leverage]
    rows = []
    row = []
    for lev in leverages:
        row.append(InlineKeyboardButton(f"{lev}x", callback_data=trade_card_cb(session_id, "lev", str(lev))))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
        InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
    ])
    return InlineKeyboardMarkup(rows)


def trade_card_size_kb(session_id: str, product: str):
    presets = SIZE_PRESETS.get(product.upper(), [1, 5, 10, 50, 100])
    rows = []
    row = []
    for s in presets:
        label = str(int(s)) if s == int(s) else str(s)
        row.append(InlineKeyboardButton(label, callback_data=trade_card_cb(session_id, "size", label)))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Custom", callback_data=trade_card_cb(session_id, "size_custom"))])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
        InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
    ])
    return InlineKeyboardMarkup(rows)


def trade_card_limit_price_input_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_tpsl_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📐 Set TP/SL", callback_data=trade_card_cb(session_id, "tpsl", "edit")),
            InlineKeyboardButton("⏭ Skip", callback_data=trade_card_cb(session_id, "tpsl", "skip")),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_tpsl_edit_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Set TP", callback_data=trade_card_cb(session_id, "tp_prompt")),
            InlineKeyboardButton("Set SL", callback_data=trade_card_cb(session_id, "sl_prompt")),
        ],
        [
            InlineKeyboardButton("✅ Done", callback_data=trade_card_cb(session_id, "tpsl_done")),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_text_input_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_confirm_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm Trade", callback_data=trade_card_cb(session_id, "confirm")),
            InlineKeyboardButton("❌ Cancel", callback_data=trade_card_cb(session_id, "cancel")),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Back Home", callback_data=trade_card_cb(session_id, "home")),
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
    max_leverage = get_product_max_leverage(product)
    leverages = [lev for lev in [1, 2, 3, 5, 10, 20, 40] if lev <= max_leverage]
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
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def wallet_kb():
    """Default wallet keyboard (linked-signer model)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Balance", callback_data="wallet:balance")],
        [InlineKeyboardButton("🔄 Revoke", callback_data="wallet:revoke_steps")],
        [InlineKeyboardButton("◀ Back", callback_data="nav:main")],
    ])


def wallet_revoke_confirm_kb():
    """Confirm revoke: reset the bot's stored signer and clear data."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, Reset My Key", callback_data="wallet:revoke_confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="wallet:view")],
    ])


def wallet_kb_not_linked():
    """When wallet is not linked — only Back (user replies DONE in chat)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ Back", callback_data="nav:main")],
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
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="alert:menu"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def alert_delete_kb(alerts):
    rows = []
    for a in alerts:
        rows.append([InlineKeyboardButton(
            f"🗑 #{a['id']} {a['product']} {a['condition']} ${a['target']:,.2f}",
            callback_data=f"alert:del:{a['id']}",
        )])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="alert:menu"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def settings_kb(leverage=1, slippage=1):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛡 Risk Profile", callback_data="settings:risk_menu"),
        ],
        [
            InlineKeyboardButton("🌐 Language", callback_data="settings:language_menu"),
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
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="settings:view"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def settings_slippage_kb():
    slippages = [0.5, 1, 2, 3, 5]
    row = []
    for s in slippages:
        label = f"{s}%"
        row.append(InlineKeyboardButton(label, callback_data=f"settings:slippage:{s}"))
    return InlineKeyboardMarkup([
        row,
        [
            InlineKeyboardButton("◀ Back", callback_data="settings:view"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
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
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def settings_language_kb(current_language: str = "en"):
    current = (current_language or "en").lower()
    options = [
        ("en", "🇬🇧 English"),
        ("zh", "🇨🇳 Chinese"),
        ("fr", "🇫🇷 Français"),
        ("ar", "🇸🇦 العربية"),
        ("ru", "🇷🇺 Русский"),
        ("ko", "🇰🇷 Korean"),
    ]

    rows = []
    row = []
    for code, label in options:
        suffix = " ✅" if current == code else ""
        row.append(InlineKeyboardButton(f"{label}{suffix}", callback_data=f"settings:language:{code}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="settings:view"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def strategy_hub_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🤖 MM Bot", callback_data="strategy:preview:mm"),
            InlineKeyboardButton("🧮 Grid Reactor", callback_data="strategy:preview:grid"),
        ],
        [
            InlineKeyboardButton("⚖️ Mirror DN", callback_data="strategy:preview:dn"),
            InlineKeyboardButton("🔁 Volume Engine", callback_data="strategy:preview:vol"),
        ],
        [
            InlineKeyboardButton("🧠 Bro Mode", callback_data="strategy:preview:bro"),
        ],
        [
            InlineKeyboardButton("🔁 Copy Trading", callback_data="copy:hub"),
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
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="mkt:menu"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def live_price_controls_kb(product: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛑 Stop Live", callback_data="mkt:live_stop"),
            InlineKeyboardButton("Switch Asset", callback_data="mkt:live_menu"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="mkt:menu"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def strategy_action_kb(strategy_id: str, selected_product: str = "BTC"):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Arm Strategy", callback_data=f"strategy:activate:{strategy_id}"),
            InlineKeyboardButton("⚙️ Tune Risk", callback_data="settings:risk_menu"),
        ],
        [
            InlineKeyboardButton("🧩 Edit Parameters", callback_data=f"strategy:config:{strategy_id}"),
        ],
        [
            InlineKeyboardButton("BTC", callback_data=f"strategy:pair:{strategy_id}:BTC"),
            InlineKeyboardButton("ETH", callback_data=f"strategy:pair:{strategy_id}:ETH"),
            InlineKeyboardButton("SOL", callback_data=f"strategy:pair:{strategy_id}:SOL"),
        ],
        [
            InlineKeyboardButton(
                f"🚀 Launch {selected_product.upper()}",
                callback_data=f"strategy:start:{strategy_id}:{selected_product.upper()}",
            ),
        ],
        [
            InlineKeyboardButton("🔄 Refresh Dashboard", callback_data=f"strategy:preview:{strategy_id}"),
        ],
        [
            InlineKeyboardButton("📡 Runtime Status", callback_data="strategy:status"),
            InlineKeyboardButton("🛑 Stop Runtime", callback_data="strategy:stop"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def bro_action_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚀 Launch Bro Mode", callback_data="strategy:start:bro:MULTI"),
        ],
        [
            InlineKeyboardButton("⚙️ Configure", callback_data="bro:config"),
            InlineKeyboardButton("📊 Status", callback_data="bro:status"),
        ],
        [
            InlineKeyboardButton("🐺 HOWL Report", callback_data="bro:howl"),
        ],
        [
            InlineKeyboardButton("📡 Runtime Status", callback_data="strategy:status"),
            InlineKeyboardButton("🛑 Stop Runtime", callback_data="strategy:stop"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def bro_config_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 Budget", callback_data="bro:set:budget_usd"),
            InlineKeyboardButton("⚡ Risk Level", callback_data="bro:set:risk_level"),
        ],
        [
            InlineKeyboardButton("🎯 Confidence", callback_data="bro:set:min_confidence"),
            InlineKeyboardButton("📐 Max Leverage", callback_data="bro:set:leverage_cap"),
        ],
        [
            InlineKeyboardButton("📊 TP/SL", callback_data="bro:set:tp_sl"),
            InlineKeyboardButton("🔢 Max Positions", callback_data="bro:set:max_positions"),
        ],
        [
            InlineKeyboardButton("Conservative", callback_data="bro:risk:conservative"),
            InlineKeyboardButton("Balanced", callback_data="bro:risk:balanced"),
            InlineKeyboardButton("Aggressive", callback_data="bro:risk:aggressive"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="strategy:preview:bro"),
        ],
    ])


def howl_approval_kb(suggestion_count: int):
    rows = []
    for i in range(suggestion_count):
        rows.append([
            InlineKeyboardButton(f"✅ Apply #{i+1}", callback_data=f"howl:approve:{i}"),
            InlineKeyboardButton(f"❌ Reject #{i+1}", callback_data=f"howl:reject:{i}"),
        ])
    rows.append([
        InlineKeyboardButton("✅ Apply All", callback_data="howl:approve_all"),
        InlineKeyboardButton("❌ Dismiss All", callback_data="howl:dismiss"),
    ])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="strategy:preview:bro"),
    ])
    return InlineKeyboardMarkup(rows)


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
        [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
    ])


def back_kb(target="main"):
    label = "🏠 Home" if target == "main" else "◀ Back"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=f"nav:{target}")],
    ])


def copy_hub_kb(traders: list, is_admin_user: bool = False):
    rows = []
    for t in traders:
        label = t.get("label", t["wallet"][:10])
        curated = " ⭐" if t.get("is_curated") else ""
        rows.append([InlineKeyboardButton(
            f"{label}{curated}",
            callback_data=f"copy:trader:{t['id']}",
        )])
    rows.append([InlineKeyboardButton("📋 My Copies", callback_data="copy:dashboard")])
    rows.append([InlineKeyboardButton("➕ Add Custom Wallet", callback_data="copy:add_custom")])
    if is_admin_user:
        rows.append([InlineKeyboardButton("⚙️ Manage Traders", callback_data="copy:admin:menu")])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub")])
    return InlineKeyboardMarkup(rows)


def copy_trader_preview_kb(trader_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶ Start Copying", callback_data=f"copy:start:{trader_id}")],
        [InlineKeyboardButton("◀ Back", callback_data="copy:hub")],
    ])


def copy_budget_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("$50", callback_data="copy:budget:50"),
            InlineKeyboardButton("$100", callback_data="copy:budget:100"),
            InlineKeyboardButton("$250", callback_data="copy:budget:250"),
        ],
        [
            InlineKeyboardButton("$500", callback_data="copy:budget:500"),
            InlineKeyboardButton("$1000", callback_data="copy:budget:1000"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
    ])


def copy_risk_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("0.5x", callback_data="copy:risk:0.5"),
            InlineKeyboardButton("1x", callback_data="copy:risk:1"),
            InlineKeyboardButton("1.5x", callback_data="copy:risk:1.5"),
        ],
        [
            InlineKeyboardButton("2x", callback_data="copy:risk:2"),
            InlineKeyboardButton("3x", callback_data="copy:risk:3"),
            InlineKeyboardButton("5x", callback_data="copy:risk:5"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
    ])


def copy_leverage_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("5x", callback_data="copy:lev:5"),
            InlineKeyboardButton("10x", callback_data="copy:lev:10"),
            InlineKeyboardButton("20x", callback_data="copy:lev:20"),
        ],
        [
            InlineKeyboardButton("30x", callback_data="copy:lev:30"),
            InlineKeyboardButton("40x", callback_data="copy:lev:40"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
    ])


def copy_confirm_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm & Start", callback_data="copy:confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
    ])


def copy_dashboard_kb(mirrors: list, lang: str = "en"):
    from src.nadobro.i18n import localize_label
    rows = []
    for m in mirrors:
        label = m.get("trader_label", "???")[:16]
        is_paused = m.get("paused", False)
        if is_paused:
            rows.append([
                InlineKeyboardButton(f"{localize_label('▶ Resume', lang)} {label}", callback_data=f"copy:resume:{m['mirror_id']}"),
                InlineKeyboardButton(f"{localize_label('🛑 Stop', lang)} {label}", callback_data=f"copy:stop:{m['mirror_id']}"),
            ])
        else:
            rows.append([
                InlineKeyboardButton(f"{localize_label('⏸ Pause', lang)} {label}", callback_data=f"copy:pause:{m['mirror_id']}"),
                InlineKeyboardButton(f"{localize_label('🛑 Stop', lang)} {label}", callback_data=f"copy:stop:{m['mirror_id']}"),
            ])
    if not mirrors:
        rows.append([InlineKeyboardButton(localize_label("ℹ️ No active copies", lang), callback_data="copy:hub")])
    rows.append([
        InlineKeyboardButton(localize_label("🔄 Refresh", lang), callback_data="copy:dashboard"),
        InlineKeyboardButton(localize_label("◀ Back", lang), callback_data="copy:hub"),
    ])
    return InlineKeyboardMarkup(rows)


def copy_admin_menu_kb(traders: list, lang: str = "en"):
    from src.nadobro.i18n import localize_label
    rows = []
    for t in traders:
        label = t.get("label", t["wallet"][:10])
        rows.append([
            InlineKeyboardButton(f"{localize_label('❌ Remove', lang)} {label}", callback_data=f"copy:admin:remove:{t['id']}"),
        ])
    rows.append([InlineKeyboardButton(localize_label("➕ Add Trader", lang), callback_data="copy:admin:add")])
    rows.append([InlineKeyboardButton(localize_label("◀ Back", lang), callback_data="copy:hub")])
    return InlineKeyboardMarkup(rows)


