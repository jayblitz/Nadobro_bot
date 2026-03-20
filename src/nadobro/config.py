import os
from typing import Optional

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
XAI_API_KEY = os.environ.get("XAI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip()]

NADO_TESTNET_REST = "https://gateway.test.nado.xyz/v1"
NADO_MAINNET_REST = "https://gateway.prod.nado.xyz/v1"

NADO_TESTNET_ARCHIVE = "https://archive.test.nado.xyz/v1"
NADO_MAINNET_ARCHIVE = "https://archive.prod.nado.xyz/v1"

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

# Spot product ids currently supported for DN hedge legs.
SPOT_PRODUCT_IDS = {
    "USDT0": 0,
    "BTC": 1,
    "ETH": 3,
}

PRODUCT_ALIASES = {}
for name, info in PRODUCTS.items():
    PRODUCT_ALIASES[name.lower()] = info["id"]
    if "symbol" in info:
        PRODUCT_ALIASES[info["symbol"].lower()] = info["id"]
        PRODUCT_ALIASES[info["symbol"].replace("-PERP", "").lower() + "-perp"] = info["id"]

def _default_catalog_network() -> str:
    return os.environ.get("NADO_PRODUCT_CATALOG_DEFAULT_NETWORK", "mainnet").strip().lower() or "mainnet"


def get_product_id(name: str, network: str = None, client=None) -> Optional[int]:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_product_id as _catalog_product_id

        pid = _catalog_product_id(name, network=network_name, client=client)
        if pid is not None:
            return pid
    except Exception:
        pass
    return PRODUCT_ALIASES.get((name or "").lower().strip())

def get_product_name(product_id: int, network: str = None, client=None) -> str:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_product_name as _catalog_product_name

        resolved = _catalog_product_name(product_id, network=network_name, client=client)
        if resolved and not resolved.startswith("ID:"):
            return resolved
    except Exception:
        pass
    for name, info in PRODUCTS.items():
        if info["id"] == product_id:
            return info.get("symbol", name)
    return f"ID:{product_id}"


def get_spot_product_id(name: str) -> Optional[int]:
    return SPOT_PRODUCT_IDS.get((name or "").upper().strip())

RATE_LIMIT_SECONDS = 5
MAX_LEVERAGE = 40
MIN_TRADE_SIZE_USD = 1.0

# Asset-specific leverage caps. BTC/ETH support 40x, others are capped lower.
PRODUCT_MAX_LEVERAGE = {
    "BTC": 40,
    "ETH": 40,
    "SOL": 20,
    "XRP": 20,
    "BNB": 20,
    "LINK": 20,
    "DOGE": 20,
    "AVAX": 20,
}


def get_product_max_leverage(product: str, network: str = None, client=None) -> int:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_product_max_leverage as _catalog_max_leverage

        return int(_catalog_max_leverage(product, network=network_name, client=client))
    except Exception:
        product_key = (product or "").upper().strip()
        if product_key not in PRODUCT_MAX_LEVERAGE:
            return 1  # Safe default for unknown products
        return int(PRODUCT_MAX_LEVERAGE[product_key])


def get_perp_products(network: str = None, client=None) -> list[str]:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import list_perp_names

        products = list_perp_names(network=network_name, client=client)
        if products:
            return products
    except Exception:
        pass
    return [name for name, info in PRODUCTS.items() if info.get("type") == "perp"]

EST_FEE_RATE = 0.0003
EST_FILL_EFFICIENCY = 0.45
DUAL_MODE_CARD_FLOW = os.environ.get("DUAL_MODE_CARD_FLOW", "true").strip().lower() in ("1", "true", "yes", "on")