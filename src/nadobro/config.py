import logging
import os
from typing import Optional

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
_MINIAPP_DISABLED = (os.environ.get("BOT_DISABLE_MINIAPP") or "").strip().lower() in ("1", "true", "yes", "on")
MINIAPP_URL = "" if _MINIAPP_DISABLED else (os.environ.get("MINIAPP_URL") or "").strip()
XAI_API_KEY = os.environ.get("XAI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "nadobro")
X_API_BEARER_TOKEN = os.environ.get("X_API_BEARER_TOKEN")
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


def get_spot_product_id(name: str, network: str = None, client=None) -> Optional[int]:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_spot_product_id as _catalog_spot_product_id

        pid = _catalog_spot_product_id(name, network=network_name)
        if pid is not None:
            return pid
    except Exception:
        pass
    return SPOT_PRODUCT_IDS.get((name or "").upper().strip())


def get_spot_metadata(name: str, network: str = None) -> dict:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_spot_metadata as _catalog_spot_metadata

        metadata = _catalog_spot_metadata(name, network=network_name)
        if metadata:
            return metadata
    except Exception:
        pass
    spot_pid = SPOT_PRODUCT_IDS.get((name or "").upper().strip())
    if spot_pid is None:
        return {}
    return {"id": int(spot_pid), "symbol": str(name or "").upper().strip()}

RATE_LIMIT_SECONDS = 5
MAX_LEVERAGE = 50
MIN_TRADE_SIZE_USD = 1.0

# Fallback leverage caps when the live catalog is unavailable (Nado mainnet overrides via API).
PRODUCT_MAX_LEVERAGE = {
    "BTC": 50,
    "ETH": 50,
    "SOL": 40,
    "XRP": 20,
    "BNB": 20,
    "LINK": 20,
    "DOGE": 20,
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


def get_dn_pair(product: str, network: str = None, client=None) -> dict:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import get_dn_pair as _catalog_dn_pair

        pair = _catalog_dn_pair(product, network=network_name, client=client)
        if pair:
            return pair
    except Exception:
        pass
    spot_pid = get_spot_product_id(product, network=network_name, client=client)
    perp_pid = get_product_id(product, network=network_name, client=client)
    if spot_pid is None or perp_pid is None:
        return {}
    product_key = (product or "").upper().replace("-PERP", "").strip()
    return {
        "product": product_key,
        "underlying_key": product_key,
        "perp_product_id": int(perp_pid),
        "perp_symbol": get_product_name(perp_pid, network=network_name, client=client),
        "spot_product_id": int(spot_pid),
        "spot_symbol": product_key,
        "spot_trading_status": "live",
        "perp_trading_status": "live",
        "spot_market_hours": None,
        "perp_market_hours": None,
        "entry_allowed": True,
        "entry_block_reason": "",
    }


def get_dn_products(network: str = None, client=None) -> list[str]:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import list_dn_product_names as _catalog_dn_products

        products = _catalog_dn_products(network=network_name, client=client)
        if products:
            return products
    except Exception:
        pass
    return [name for name in ("BTC", "ETH") if get_spot_product_id(name, network=network_name, client=client) is not None]


def is_product_isolated_only(product: str, network: str = None, client=None) -> bool:
    network_name = str(network or _default_catalog_network())
    try:
        from src.nadobro.services.product_catalog import is_product_isolated_only as _catalog_isolated_only

        return bool(_catalog_isolated_only(product, network=network_name, client=client))
    except Exception:
        return False

EST_FEE_RATE = 0.0003
EST_FILL_EFFICIENCY = 0.45
DUAL_MODE_CARD_FLOW = os.environ.get("DUAL_MODE_CARD_FLOW", "true").strip().lower() in ("1", "true", "yes", "on")

NADO_BUILDER_ID_ENV = "NADO_BUILDER_ID"
NADO_BUILDER_FEE_RATE_ENV = "NADO_BUILDER_FEE_RATE"
NADO_BUILDER_FEE_RATE_1_BPS = 10  # 0.1 bps units


def get_nado_builder_routing_config() -> tuple[int, int]:
    """Return validated (builder_id, builder_fee_rate) for order routing.

    Safety-first behavior:
    - Builder routing is mandatory for order placement.
    - Fee rate is locked to 1 bps (10 units) to avoid accidental fee changes.
    """
    builder_id_raw = (os.environ.get(NADO_BUILDER_ID_ENV) or "").strip()
    if not builder_id_raw:
        raise ValueError(f"{NADO_BUILDER_ID_ENV} is required for order placement.")

    try:
        builder_id = int(builder_id_raw)
    except ValueError as exc:
        raise ValueError(f"{NADO_BUILDER_ID_ENV} must be an integer in [1, 65535].") from exc

    if builder_id < 1 or builder_id > 65535:
        raise ValueError(f"{NADO_BUILDER_ID_ENV} must be in [1, 65535].")

    fee_rate_raw = (os.environ.get(NADO_BUILDER_FEE_RATE_ENV) or str(NADO_BUILDER_FEE_RATE_1_BPS)).strip()
    try:
        fee_rate = int(fee_rate_raw)
    except ValueError as exc:
        raise ValueError(f"{NADO_BUILDER_FEE_RATE_ENV} must be an integer in [0, 1023].") from exc

    if fee_rate < 0 or fee_rate > 1023:
        raise ValueError(f"{NADO_BUILDER_FEE_RATE_ENV} must be in [0, 1023].")

    if fee_rate != NADO_BUILDER_FEE_RATE_1_BPS:
        raise ValueError(
            f"{NADO_BUILDER_FEE_RATE_ENV} must be {NADO_BUILDER_FEE_RATE_1_BPS} (1 bps) for safe routing."
        )

    return builder_id, fee_rate


_logger = logging.getLogger(__name__)
if MINIAPP_URL and not MINIAPP_URL.lower().startswith("https://"):
    _logger.warning(
        "MINIAPP_URL must use https for Telegram Web Apps (Mini App may not open).",
    )

