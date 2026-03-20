import logging
from src.nadobro.config import get_perp_products, get_product_id

logger = logging.getLogger(__name__)

def _maps(network: str = "mainnet", client=None) -> tuple[dict[str, int], dict[int, str]]:
    hl_to_nado: dict[str, int] = {}
    nado_to_hl: dict[int, str] = {}
    for coin in get_perp_products(network=network, client=client):
        pid = get_product_id(coin, network=network, client=client)
        if pid is None:
            continue
        hl_to_nado[coin] = pid
        nado_to_hl[pid] = coin
    return hl_to_nado, nado_to_hl


def hl_coin_to_nado_product_id(coin: str, network: str = "mainnet", client=None) -> int | None:
    hl_to_nado, _ = _maps(network=network, client=client)
    return hl_to_nado.get(coin.upper().strip())


def nado_product_id_to_hl_coin(product_id: int, network: str = "mainnet", client=None) -> str | None:
    _, nado_to_hl = _maps(network=network, client=client)
    return nado_to_hl.get(product_id)


def is_supported_coin(coin: str, network: str = "mainnet", client=None) -> bool:
    hl_to_nado, _ = _maps(network=network, client=client)
    return coin.upper().strip() in hl_to_nado


def get_supported_coins(network: str = "mainnet", client=None) -> list[str]:
    hl_to_nado, _ = _maps(network=network, client=client)
    return list(hl_to_nado.keys())


def get_nado_product_name(product_id: int, network: str = "mainnet", client=None) -> str:
    coin = nado_product_id_to_hl_coin(product_id, network=network, client=client)
    return f"{coin}-PERP" if coin else f"ID:{product_id}"
