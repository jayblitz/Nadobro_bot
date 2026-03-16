import logging

logger = logging.getLogger(__name__)

HL_TO_NADO = {
    "BTC": 2,
    "ETH": 4,
    "SOL": 8,
    "XRP": 10,
    "BNB": 14,
    "LINK": 16,
    "AVAX": 18,
    "DOGE": 22,
}

NADO_TO_HL = {v: k for k, v in HL_TO_NADO.items()}

NADO_PRODUCT_NAMES = {
    2: "BTC-PERP",
    4: "ETH-PERP",
    8: "SOL-PERP",
    10: "XRP-PERP",
    14: "BNB-PERP",
    16: "LINK-PERP",
    18: "AVAX-PERP",
    22: "DOGE-PERP",
}


def hl_coin_to_nado_product_id(coin: str) -> int | None:
    return HL_TO_NADO.get(coin.upper().strip())


def nado_product_id_to_hl_coin(product_id: int) -> str | None:
    return NADO_TO_HL.get(product_id)


def is_supported_coin(coin: str) -> bool:
    return coin.upper().strip() in HL_TO_NADO


def get_supported_coins() -> list[str]:
    return list(HL_TO_NADO.keys())


def get_nado_product_name(product_id: int) -> str:
    return NADO_PRODUCT_NAMES.get(product_id, f"ID:{product_id}")
