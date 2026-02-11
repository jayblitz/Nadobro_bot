import os
from typing import Optional

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
XAI_API_KEY = os.environ.get("XAI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SESSION_SECRET = os.environ.get("SESSION_SECRET", "default-session-secret")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip()]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", SESSION_SECRET)

NADO_TESTNET_WS = "wss://gateway.test.nado.xyz/v1/ws"
NADO_TESTNET_REST = "https://gateway.test.nado.xyz/v1"
NADO_MAINNET_WS = "wss://gateway.prod.nado.xyz/v1/ws"
NADO_MAINNET_REST = "https://gateway.prod.nado.xyz/v1"

NADO_TESTNET_ARCHIVE = "https://archive.test.nado.xyz/v1"
NADO_MAINNET_ARCHIVE = "https://archive.prod.nado.xyz/v1"

NADO_TESTNET_TRIGGER = "https://trigger.test.nado.xyz/v1"
NADO_MAINNET_TRIGGER = "https://trigger.prod.nado.xyz/v1"

PRODUCTS = {
    "USDT0": {"id": 0, "type": "spot"},
    "BTC": {"id": 2, "type": "perp", "symbol": "BTC-PERP"},
    "ETH": {"id": 4, "type": "perp", "symbol": "ETH-PERP"},
    "SOL": {"id": 8, "type": "perp", "symbol": "SOL-PERP"},
    "XRP": {"id": 10, "type": "perp", "symbol": "XRP-PERP"},
    "BNB": {"id": 14, "type": "perp", "symbol": "BNB-PERP"},
    "LINK": {"id": 16, "type": "perp", "symbol": "LINK-PERP"},
    "DOGE": {"id": 22, "type": "perp", "symbol": "DOGE-PERP"},
    "AVAX": {"id": 18, "type": "perp", "symbol": "AVAX-PERP"},
}

PRODUCT_ALIASES = {}
for name, info in PRODUCTS.items():
    PRODUCT_ALIASES[name.lower()] = info["id"]
    if "symbol" in info:
        PRODUCT_ALIASES[info["symbol"].lower()] = info["id"]
        PRODUCT_ALIASES[info["symbol"].replace("-PERP", "").lower() + "-perp"] = info["id"]

def get_product_id(name: str) -> Optional[int]:
    return PRODUCT_ALIASES.get(name.lower().strip())

def get_product_name(product_id: int) -> str:
    for name, info in PRODUCTS.items():
        if info["id"] == product_id:
            return info.get("symbol", name)
    return f"ID:{product_id}"

RATE_LIMIT_SECONDS = 60
MAX_LEVERAGE = 50
MIN_TRADE_SIZE_USD = 1.0
