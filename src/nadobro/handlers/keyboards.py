from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from src.nadobro.config import PRODUCTS, DUAL_MODE_CARD_FLOW, get_product_max_leverage, get_perp_products

PERP_PRODUCTS = [name for name, info in PRODUCTS.items() if info["type"] == "perp"]


def _perp_products(network: str = "mainnet", client=None):
    return get_perp_products(network=network, client=client)

SIZE_PRESETS = {
    "BTC": [0.001, 0.005, 0.01, 0.05, 0.1],
    "ETH": [0.01, 0.05, 0.1, 0.5, 1.0],
    "SOL": [0.1, 0.5, 1, 5, 10],
    "XRP": [10, 50, 100, 500, 1000],
    "BNB": [0.01, 0.05, 0.1, 0.5, 1],
    "LINK": [1, 5, 10, 50, 100],
    "DOGE": [100, 500, 1000, 5000, 10000],
}


HOME_MODULE_EMOJIS = {
    "trade": "🤖",
    "portfolio": "📁",
    "home": "🏠",
    "wallet": "💼",
    "points": "🏆",
    "strategies": "🧠",
    "alerts": "🔔",
    "settings": "⚙️",
    "mode": "🌐",
}

HOME_BTN_TRADE = f"{HOME_MODULE_EMOJIS['trade']} Trade Console"
HOME_BTN_PORTFOLIO = f"{HOME_MODULE_EMOJIS['portfolio']} Portfolio Deck"
HOME_BTN_HOME = f"{HOME_MODULE_EMOJIS['home']} Home"
HOME_BTN_WALLET = f"{HOME_MODULE_EMOJIS['wallet']} Wallet Vault"
HOME_BTN_POINTS = f"{HOME_MODULE_EMOJIS['points']} Nado Points"
HOME_BTN_REFER = "🎁 Refer Friends"
HOME_BTN_STRATEGIES = f"{HOME_MODULE_EMOJIS['strategies']} Strategy Lab"
HOME_BTN_ALERTS = f"{HOME_MODULE_EMOJIS['alerts']} Alert Engine"
HOME_BTN_SETTINGS = f"{HOME_MODULE_EMOJIS['settings']} Control Panel"
HOME_BTN_MODE = f"{HOME_MODULE_EMOJIS['mode']} Execution Mode"


REPLY_BUTTON_MAP = {
    HOME_BTN_HOME: "nav:main",
    HOME_BTN_TRADE: "nav:trade",
    HOME_BTN_PORTFOLIO: "portfolio:view",
    HOME_BTN_WALLET: "wallet:view",
    HOME_BTN_POINTS: "points:view",
    HOME_BTN_REFER: "refer:view",
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
    "Points": "points:view",
    "🎁 Refer Friends": "refer:view",
    "Refer Friends": "refer:view",
    "📡 Market Radar": "points:view",
    "Market Radar": "points:view",
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


def private_access_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Request Access", url="https://t.me/jaynadobro")],
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
            [KeyboardButton(HOME_BTN_WALLET), KeyboardButton(HOME_BTN_POINTS)],
            [KeyboardButton(HOME_BTN_STRATEGIES), KeyboardButton(HOME_BTN_REFER)],
            [KeyboardButton(HOME_BTN_ALERTS), KeyboardButton(HOME_BTN_SETTINGS)],
            [KeyboardButton(HOME_BTN_MODE)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def home_card_kb():
    rows = [
        [
            InlineKeyboardButton("🤖 Trade Console", callback_data="card:trade:start"),
            InlineKeyboardButton("📁 Portfolio Deck", callback_data="portfolio:view"),
        ],
        [
            InlineKeyboardButton("💼 Wallet Vault", callback_data="wallet:view"),
            InlineKeyboardButton("🏆 Nado Points", callback_data="points:view"),
        ],
        [
            InlineKeyboardButton("🎁 Refer Friends", callback_data="refer:view"),
            InlineKeyboardButton("🔔 Alert Engine", callback_data="alert:menu"),
        ],
        [
            InlineKeyboardButton("🧠 Strategy Lab", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("⚙️ Control Panel", callback_data="settings:view"),
        ],
        [
            InlineKeyboardButton("🌐 Execution Mode", callback_data="home:mode"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def bro_answer_kb(mode: str | None = None):
    rows = []
    if mode in {"strategy_design", "educational_guide"}:
        rows.append([
            InlineKeyboardButton("🧠 Strategy Lab", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("🤖 Trade Console", callback_data="card:trade:start"),
        ])
    elif mode == "market_analysis":
        rows.append([
            InlineKeyboardButton("🏆 Market Radar", callback_data="points:view"),
            InlineKeyboardButton("📁 Portfolio", callback_data="portfolio:view"),
        ])
    elif mode == "debugging":
        rows.append([
            InlineKeyboardButton("📁 Portfolio", callback_data="portfolio:view"),
            InlineKeyboardButton("⚙️ Control Panel", callback_data="settings:view"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("🧠 Strategy Lab", callback_data="nav:strategy_hub"),
            InlineKeyboardButton("📁 Portfolio", callback_data="portfolio:view"),
        ])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def status_kb(is_running: bool = False, strategy_label: str | None = None):
    """Inline actions for /status with strategy-aware stop control."""
    rows = [[InlineKeyboardButton("🔄 Refresh", callback_data="status:refresh")]]
    if is_running:
        stop_label = "🛑 Stop Strategy"
        if strategy_label:
            stop_label = f"🛑 Stop {str(strategy_label).upper()}"
        rows.append([InlineKeyboardButton(stop_label[:32], callback_data="status:stop")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def portfolio_kb(has_positions: bool = False):
    rows = [
        [
            InlineKeyboardButton("📌 Open Positions", callback_data="pos:view"),
            InlineKeyboardButton("📜 Trade History", callback_data="portfolio:history"),
        ],
        [
            InlineKeyboardButton("📊 Performance", callback_data="portfolio:analytics"),
            InlineKeyboardButton("🔄 Refresh", callback_data="portfolio:refresh"),
        ],
    ]
    if has_positions:
        rows.append([InlineKeyboardButton("❌ Close All Positions", callback_data="pos:close_all")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def referral_kb(can_generate: bool = False):
    rows = []
    if can_generate:
        rows.append([InlineKeyboardButton("🎟 Generate Invite Code", callback_data="refer:generate")])
    rows.append([
        InlineKeyboardButton("🔄 Refresh", callback_data="refer:view"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def portfolio_history_kb(page: int = 0, has_more: bool = False):
    rows = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀ Prev", callback_data=f"portfolio:history:{page - 1}"))
    if has_more:
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"portfolio:history:{page + 1}"))
    if nav_row:
        rows.append(nav_row)
    rows.append([
        InlineKeyboardButton("📁 Back to Portfolio", callback_data="portfolio:view"),
        InlineKeyboardButton("📊 View Performance", callback_data="portfolio:analytics"),
    ])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def portfolio_analytics_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📁 Back to Portfolio", callback_data="portfolio:view"),
            InlineKeyboardButton("📜 View History", callback_data="portfolio:history"),
        ],
        [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
    ])


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


def trade_product_reply_kb(network: str = "mainnet"):
    products = _perp_products(network=network)
    rows = []
    row = []
    for name in products:
        row.append(KeyboardButton(name))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([KeyboardButton("◀ Back"), KeyboardButton("◀ Home")])
    return ReplyKeyboardMarkup(
        rows,
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
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
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
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_product_kb(session_id: str, network: str = "mainnet"):
    rows = []
    row = []
    for name in _perp_products(network=network):
        row.append(InlineKeyboardButton(name, callback_data=trade_card_cb(session_id, "product", name)))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
        InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
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
        InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
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
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
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
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
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
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_text_input_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_card_confirm_kb(session_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⏱ Auto-close at…", callback_data=trade_card_cb(session_id, "time_limit")),
        ],
        [
            InlineKeyboardButton("✅ Confirm Trade", callback_data=trade_card_cb(session_id, "confirm")),
            InlineKeyboardButton("❌ Cancel", callback_data=trade_card_cb(session_id, "cancel")),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data=trade_card_cb(session_id, "back")),
            InlineKeyboardButton("🏠 Home", callback_data=trade_card_cb(session_id, "home")),
        ],
    ])


def trade_product_kb(action, network: str = "mainnet"):
    rows = []
    row = []
    for name in _perp_products(network=network):
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
    rows = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="pos:view")],
    ]
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
            InlineKeyboardButton("🔔 Create Alert", callback_data="alert:set"),
            InlineKeyboardButton("📋 Active Alerts", callback_data="alert:view"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def alert_product_kb(network: str = "mainnet"):
    rows = []
    row = []
    for name in _perp_products(network=network):
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


def alert_condition_kb(product):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📈 Price Above", callback_data=f"alert:cond:{product}:above"),
            InlineKeyboardButton("📉 Price Below", callback_data=f"alert:cond:{product}:below"),
        ],
        [
            InlineKeyboardButton("💹 Funding Above", callback_data=f"alert:cond:{product}:funding_above"),
            InlineKeyboardButton("💸 Funding Below", callback_data=f"alert:cond:{product}:funding_below"),
        ],
        [
            InlineKeyboardButton("🟢 PnL Above", callback_data=f"alert:cond:{product}:pnl_above"),
            InlineKeyboardButton("🔴 PnL Below", callback_data=f"alert:cond:{product}:pnl_below"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="alert:set"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


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
            InlineKeyboardButton(f"⚡ Leverage: {leverage}x", callback_data="settings:leverage_menu"),
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
            InlineKeyboardButton("🤖 GRID", callback_data="strategy:preview:grid"),
            InlineKeyboardButton("🧮 Reverse GRID", callback_data="strategy:preview:rgrid"),
        ],
        [
            InlineKeyboardButton("⚡ Dynamic GRID", callback_data="strategy:preview:dgrid"),
        ],
        [
            InlineKeyboardButton("⚖️ Mirror DN", callback_data="strategy:preview:dn"),
            InlineKeyboardButton("🔁 Volume Engine", callback_data="strategy:preview:vol"),
        ],
        [
            InlineKeyboardButton("🧠 Strategy Studio", callback_data="studio:home"),
        ],
        [
            InlineKeyboardButton("🔁 Copy Trading", callback_data="copy:hub"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="nav:main"),
        ],
    ])


def points_scope_kb(scope: str = "week"):
    scope_norm = (scope or "week").lower()
    week_label = "✅ 7d" if scope_norm == "week" else "7d"
    month_label = "✅ 30d" if scope_norm == "month" else "30d"
    all_label = "✅ All" if scope_norm == "all" else "All"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(week_label, callback_data="points:scope:week"),
            InlineKeyboardButton(month_label, callback_data="points:scope:month"),
            InlineKeyboardButton(all_label, callback_data="points:scope:all"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="points:refresh"),
            InlineKeyboardButton("❌ Cancel", callback_data="points:cancel"),
        ],
        [
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def points_followup_options_kb(options: list[str]):
    rows = []
    clean = [str(opt).strip() for opt in (options or []) if str(opt).strip()]
    for i in range(0, len(clean), 2):
        row = []
        for j, label in enumerate(clean[i:i + 2]):
            row.append(InlineKeyboardButton(label[:32], callback_data=f"points:replyopt:{i + j}"))
        if row:
            rows.append(row)
    rows.append([InlineKeyboardButton("❌ Cancel Request", callback_data="points:cancel")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def strategy_action_kb(
    strategy_id: str,
    selected_product: str = "BTC",
    available_products: list[str] | None = None,
    is_running: bool = False,
    vol_market: str = "perp",
):
    if strategy_id == "dn":
        products = [p.upper() for p in (available_products or ["BTC", "ETH"])]
    else:
        products = [p.upper() for p in (available_products or _perp_products() or ["BTC", "ETH", "SOL"])]
    if not products:
        products = ["BTC", "ETH"] if strategy_id == "dn" else ["BTC", "ETH", "SOL"]
    selected = str(selected_product or products[0]).upper()
    if selected not in products:
        selected = products[0]
    featured = list(products[:3])
    if selected not in featured:
        featured = [selected] + featured[:2]
    pair_buttons = [
        InlineKeyboardButton(
            f"{'✅ ' if product == selected else ''}{product}",
            callback_data=f"strategy:pair:{strategy_id}:{product}",
        )
        for product in featured[:3]
    ]
    if strategy_id == "vol":
        vm = str(vol_market or "perp").strip().lower()
        if vm not in ("perp", "spot"):
            vm = "perp"
        if vm == "perp":
            start_row = [
                InlineKeyboardButton("▶ Start Long", callback_data=f"strategy:start:{strategy_id}:{selected}:long"),
                InlineKeyboardButton("▶ Start Short", callback_data=f"strategy:start:{strategy_id}:{selected}:short"),
            ]
        else:
            start_row = [
                InlineKeyboardButton(
                    f"▶ Start {strategy_id.upper()}",
                    callback_data=f"strategy:start:{strategy_id}:{selected}",
                ),
            ]
        market_row = [
            InlineKeyboardButton(
                ("✅ " if vm == "perp" else "") + "Perp",
                callback_data="strategy:volmarket:vol:perp",
            ),
            InlineKeyboardButton(
                ("✅ " if vm == "spot" else "") + "Spot",
                callback_data="strategy:volmarket:vol:spot",
            ),
        ]
        rows = [
            start_row,
            market_row,
            [
                InlineKeyboardButton("🎯 Choose Asset", callback_data=f"strategy:custom:{strategy_id}:0"),
                InlineKeyboardButton("⚙️ Advanced", callback_data=f"strategy:config:{strategy_id}"),
            ],
            [*pair_buttons],
        ]
    else:
        rows = [
            [
                InlineKeyboardButton(
                    f"▶ Start {strategy_id.upper()}",
                    callback_data=f"strategy:start:{strategy_id}:{selected}",
                ),
            ],
            [
                InlineKeyboardButton("🎯 Choose Asset", callback_data=f"strategy:custom:{strategy_id}:0"),
                InlineKeyboardButton("⚙️ Advanced", callback_data=f"strategy:config:{strategy_id}"),
            ],
            [*pair_buttons],
        ]
    if is_running:
        rows.insert(0, [
            InlineKeyboardButton("🔄 Refresh", callback_data="strategy:status"),
            InlineKeyboardButton(f"🛑 Stop {strategy_id.upper()[:8]}", callback_data="strategy:stop"),
        ])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def strategy_product_picker_kb(
    strategy_id: str,
    selected_product: str = "BTC",
    available_products: list[str] | None = None,
    page: int = 0,
):
    products = [p.upper() for p in (available_products or _perp_products() or ["BTC", "ETH", "SOL"])]
    if not products:
        products = ["BTC", "ETH", "SOL"]
    selected = str(selected_product or products[0]).upper()
    if selected not in products:
        selected = products[0]

    per_page = 12
    max_page = max(0, (len(products) - 1) // per_page)
    page = max(0, min(int(page or 0), max_page))
    start = page * per_page
    chunk = products[start:start + per_page]

    rows = []
    row = []
    for product in chunk:
        label = f"✅ {product}" if product == selected else product
        row.append(InlineKeyboardButton(label, callback_data=f"strategy:pair:{strategy_id}:{product}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀ Prev", callback_data=f"strategy:custom:{strategy_id}:{page - 1}"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"strategy:custom:{strategy_id}:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy_id}")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


def bro_action_kb(is_running: bool = False):
    rows = [
        [InlineKeyboardButton("▶ Start Alpha Agent", callback_data="strategy:start:bro:MULTI")],
        [
            InlineKeyboardButton("⚙️ Advanced", callback_data="bro:config"),
            InlineKeyboardButton("📋 Game Plan", callback_data="bro:gameplan"),
        ],
        [InlineKeyboardButton("🐺 HOWL Report", callback_data="bro:howl")],
    ]
    if is_running:
        rows.insert(0, [
            InlineKeyboardButton("🔄 Refresh", callback_data="strategy:status"),
            InlineKeyboardButton("🛑 Stop BRO", callback_data="strategy:stop"),
        ])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
    ])
    return InlineKeyboardMarkup(rows)


def bro_config_menu_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("😎 Preset", callback_data="bro:config_section:preset"),
            InlineKeyboardButton("🛡 Risk", callback_data="bro:config_section:risk"),
        ],
        [
            InlineKeyboardButton("📊 Exits", callback_data="bro:config_section:exits"),
        ],
        [
            InlineKeyboardButton("◀ Back", callback_data="strategy:preview:bro"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def bro_config_section_kb(section: str):
    if section == "preset":
        rows = [
            [
                InlineKeyboardButton("😎 Chill", callback_data="bro:profile:chill"),
                InlineKeyboardButton("🤙 Normal", callback_data="bro:profile:normal"),
                InlineKeyboardButton("🔥 Degen", callback_data="bro:profile:degen"),
            ],
        ]
    elif section == "risk":
        rows = [
            [
                InlineKeyboardButton("Conservative", callback_data="bro:risk:conservative"),
                InlineKeyboardButton("Balanced", callback_data="bro:risk:balanced"),
                InlineKeyboardButton("Aggressive", callback_data="bro:risk:aggressive"),
            ],
            [
                InlineKeyboardButton("💰 Budget", callback_data="bro:set:budget_usd"),
                InlineKeyboardButton("🎯 Confidence", callback_data="bro:set:min_confidence"),
            ],
            [
                InlineKeyboardButton("📐 Max Leverage", callback_data="bro:set:leverage_cap"),
                InlineKeyboardButton("🔢 Max Positions", callback_data="bro:set:max_positions"),
            ],
            [
                InlineKeyboardButton("⚡ Risk Style", callback_data="bro:config_section:risk_style"),
            ],
        ]
    elif section == "risk_style":
        rows = [
            [
                InlineKeyboardButton("Conservative", callback_data="bro:set_text:risk_level:conservative"),
                InlineKeyboardButton("Balanced", callback_data="bro:set_text:risk_level:balanced"),
            ],
            [
                InlineKeyboardButton("Aggressive", callback_data="bro:set_text:risk_level:aggressive"),
            ],
        ]
    else:
        rows = [
            [
                InlineKeyboardButton("📊 TP / SL", callback_data="bro:set:tp_sl"),
            ],
        ]

    rows.append([InlineKeyboardButton("◀ Back", callback_data="bro:config")])
    return InlineKeyboardMarkup(rows)


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


def close_product_kb(network: str = "mainnet"):
    rows = []
    row = []
    for name in _perp_products(network=network):
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
    callback_data = target if ":" in str(target) else f"nav:{target}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=callback_data)],
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


def copy_cumulative_sl_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("5%", callback_data="copy:csl:5"),
            InlineKeyboardButton("10%", callback_data="copy:csl:10"),
            InlineKeyboardButton("20%", callback_data="copy:csl:20"),
        ],
        [
            InlineKeyboardButton("50%", callback_data="copy:csl:50"),
            InlineKeyboardButton("⏭ Skip (No SL)", callback_data="copy:csl:0"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
    ])


def copy_cumulative_tp_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("10%", callback_data="copy:ctp:10"),
            InlineKeyboardButton("25%", callback_data="copy:ctp:25"),
            InlineKeyboardButton("50%", callback_data="copy:ctp:50"),
        ],
        [
            InlineKeyboardButton("100%", callback_data="copy:ctp:100"),
            InlineKeyboardButton("⏭ Skip (No TP)", callback_data="copy:ctp:0"),
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


