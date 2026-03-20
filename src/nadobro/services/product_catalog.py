import logging
import os
import time
from typing import Optional

import requests

from src.nadobro.config import (
    PRODUCTS,
    PRODUCT_MAX_LEVERAGE,
    NADO_MAINNET_REST,
    NADO_TESTNET_REST,
)

logger = logging.getLogger(__name__)

_CATALOG_TTL_SECONDS = int(os.environ.get("NADO_PRODUCT_CATALOG_TTL_SECONDS", "60"))
_DYNAMIC_DEFAULT_MAX_LEVERAGE = int(os.environ.get("NADO_DYNAMIC_DEFAULT_MAX_LEVERAGE", "20"))
_REQUEST_TIMEOUT_SECONDS = float(os.environ.get("NADO_HTTP_TIMEOUT_SECONDS", "6"))

_catalog_cache: dict[str, dict] = {}
_rest_session = requests.Session()


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in ("1", "true", "yes", "y", "on")


def _rest_url(network: str) -> str:
    return NADO_MAINNET_REST if str(network).lower() == "mainnet" else NADO_TESTNET_REST


def _normalize_symbol(raw_symbol: str, product_id: int) -> tuple[str, str]:
    text = (raw_symbol or "").strip().upper()
    if not text:
        return f"P{product_id}", f"P{product_id}-PERP"
    if text.endswith("-PERP"):
        base = text[:-5]
    elif text.endswith("PERP"):
        base = text.replace("_", "-").replace("/", "-").replace(" ", "")
        base = base[:-4].rstrip("-")
    else:
        base = text.replace("_", "-").replace("/", "-").replace(" ", "")
    base = base.strip("-")
    if not base:
        base = f"P{product_id}"
    return base, f"{base}-PERP"


def _build_static_catalog() -> dict:
    perps: dict[str, dict] = {}
    by_id: dict[int, str] = {}
    aliases: dict[str, str] = {}
    for name, info in PRODUCTS.items():
        if info.get("type") != "perp":
            continue
        pid = int(info["id"])
        symbol = str(info.get("symbol") or f"{name}-PERP").upper()
        base, norm_symbol = _normalize_symbol(symbol, pid)
        key = name.upper().strip()
        perps[key] = {
            "id": pid,
            "type": "perp",
            "symbol": norm_symbol,
            "base": key,
            "dynamic": False,
            "max_leverage": int(PRODUCT_MAX_LEVERAGE.get(key, _DYNAMIC_DEFAULT_MAX_LEVERAGE)),
            "isolated_only": False,
        }
        by_id[pid] = key
        aliases[key.lower()] = key
        aliases[norm_symbol.lower()] = key
        aliases[f"{key.lower()}-perp"] = key
    return {"perps": perps, "by_id": by_id, "aliases": aliases}


def _fetch_all_products(network: str, client=None) -> list[dict]:
    if client is not None:
        try:
            data = client._query_rest("all_products") or {}  # noqa: SLF001
            if data.get("status") == "success":
                return ((data.get("data") or {}).get("perp_products") or [])
        except Exception as e:
            logger.warning("catalog: client all_products failed on %s: %s", network, e)
    try:
        url = f"{_rest_url(network)}/query"
        resp = _rest_session.get(
            url,
            params={"type": "all_products"},
            headers={"Accept-Encoding": "gzip"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        data = resp.json() if resp is not None else {}
        if data.get("status") == "success":
            return ((data.get("data") or {}).get("perp_products") or [])
    except Exception as e:
        logger.warning("catalog: rest all_products failed on %s: %s", network, e)
    return []


def _fetch_symbol_rows(network: str, client=None) -> list[dict]:
    if client is not None:
        try:
            data = client._query_rest("symbols") or {}  # noqa: SLF001
            if data.get("status") == "success":
                symbols = ((data.get("data") or {}).get("symbols") or {})
                if isinstance(symbols, dict):
                    return [row for row in symbols.values() if isinstance(row, dict)]
        except Exception as e:
            logger.warning("catalog: client symbols failed on %s: %s", network, e)
    try:
        url = f"{_rest_url(network)}/query"
        resp = _rest_session.get(
            url,
            params={"type": "symbols"},
            headers={"Accept-Encoding": "gzip"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        data = resp.json() if resp is not None else {}
        if data.get("status") == "success":
            symbols = ((data.get("data") or {}).get("symbols") or {})
            if isinstance(symbols, dict):
                return [row for row in symbols.values() if isinstance(row, dict)]
    except Exception as e:
        logger.warning("catalog: rest symbols failed on %s: %s", network, e)
    return []


def _build_dynamic_catalog(network: str, client=None) -> Optional[dict]:
    raw_products = _fetch_all_products(network, client=client)
    raw_symbols = _fetch_symbol_rows(network, client=client)
    if not raw_products and not raw_symbols:
        return None

    catalog = _build_static_catalog()
    perps = dict(catalog["perps"])
    by_id = dict(catalog["by_id"])
    aliases = dict(catalog["aliases"])

    rows_by_id: dict[int, dict] = {}
    for row in raw_products:
        try:
            pid = int(row.get("product_id"))
        except (TypeError, ValueError):
            continue
        rows_by_id[pid] = row

    merged_rows: list[dict] = []
    if raw_symbols:
        # `symbols` includes canonical symbol strings and trading metadata.
        for symbol_row in raw_symbols:
            if str(symbol_row.get("type", "")).lower() != "perp":
                continue
            merged_rows.append(symbol_row)
    else:
        # Fallback: still support legacy payloads where all_products carried symbols.
        merged_rows = raw_products

    for row in merged_rows:
        try:
            pid = int(row.get("product_id"))
        except (TypeError, ValueError):
            continue
        product_row = rows_by_id.get(pid, {})
        book_info = row.get("book_info") or {}
        raw_symbol = (
            row.get("symbol")
            or row.get("name")
            or row.get("ticker")
            or row.get("product_symbol")
            or book_info.get("symbol")
            or ""
        )
        base, norm_symbol = _normalize_symbol(str(raw_symbol), pid)
        if base.startswith("P") and pid in by_id:
            base = by_id[pid]
            norm_symbol = perps[base]["symbol"]
        product_book = product_row.get("book_info") or {}
        max_lev = (
            row.get("max_leverage")
            or row.get("max_leverage_x")
            or book_info.get("max_leverage")
            or product_row.get("max_leverage")
            or product_row.get("max_leverage_x")
            or product_book.get("max_leverage")
            or PRODUCT_MAX_LEVERAGE.get(base, _DYNAMIC_DEFAULT_MAX_LEVERAGE)
        )
        try:
            max_lev = int(float(max_lev))
        except (TypeError, ValueError):
            max_lev = _DYNAMIC_DEFAULT_MAX_LEVERAGE
        max_lev = max(1, max_lev)
        isolated_only = _as_bool(
            row.get("isolated_only")
            or book_info.get("isolated_only")
            or product_row.get("isolated_only")
            or product_book.get("isolated_only")
        )

        key = base.upper().strip()
        perps[key] = {
            "id": pid,
            "type": "perp",
            "symbol": norm_symbol,
            "base": key,
            "dynamic": True,
            "max_leverage": int(max_lev),
            "isolated_only": isolated_only,
        }
        by_id[pid] = key
        aliases[key.lower()] = key
        aliases[norm_symbol.lower()] = key
        aliases[f"{key.lower()}-perp"] = key

    return {"perps": perps, "by_id": by_id, "aliases": aliases}


def get_catalog(network: str = "mainnet", client=None, refresh: bool = False) -> dict:
    key = str(network or "mainnet").lower()
    if not refresh:
        cached = _catalog_cache.get(key)
        if cached and (time.time() - cached["ts"] < _CATALOG_TTL_SECONDS):
            return cached["data"]

    data = _build_dynamic_catalog(key, client=client) or _build_static_catalog()
    _catalog_cache[key] = {"data": data, "ts": time.time()}
    return data


def list_perp_names(network: str = "mainnet", client=None, refresh: bool = False) -> list[str]:
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    entries = list((catalog.get("perps") or {}).items())
    entries.sort(key=lambda kv: int((kv[1] or {}).get("id", 0)))
    return [name for name, _ in entries]


def get_product_id(name: str, network: str = "mainnet", client=None, refresh: bool = False) -> Optional[int]:
    if not name:
        return None
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    key = (catalog.get("aliases") or {}).get(str(name).lower().strip())
    if not key:
        return None
    row = (catalog.get("perps") or {}).get(key)
    if not row:
        return None
    try:
        return int(row.get("id"))
    except (TypeError, ValueError):
        return None


def get_product_name(product_id: int, network: str = "mainnet", client=None, refresh: bool = False) -> str:
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    key = (catalog.get("by_id") or {}).get(int(product_id))
    if not key:
        return f"ID:{product_id}"
    row = (catalog.get("perps") or {}).get(key) or {}
    return str(row.get("symbol") or f"{key}-PERP")


def get_product_max_leverage(
    product: str,
    network: str = "mainnet",
    client=None,
    refresh: bool = False,
) -> int:
    key = (product or "").upper().strip()
    if not key:
        return 1
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    row = (catalog.get("perps") or {}).get(key)
    if not row:
        # Resolve aliases when a caller passes BTC-PERP, etc.
        resolved = (catalog.get("aliases") or {}).get(key.lower())
        row = (catalog.get("perps") or {}).get(resolved) if resolved else None
    if not row:
        return 1
    try:
        return max(1, int(row.get("max_leverage", _DYNAMIC_DEFAULT_MAX_LEVERAGE)))
    except (TypeError, ValueError):
        return max(1, int(_DYNAMIC_DEFAULT_MAX_LEVERAGE))


def get_product_metadata(product: str, network: str = "mainnet", client=None, refresh: bool = False) -> dict:
    key = (product or "").upper().strip()
    if not key:
        return {}
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    row = (catalog.get("perps") or {}).get(key)
    if not row:
        resolved = (catalog.get("aliases") or {}).get(key.lower())
        row = (catalog.get("perps") or {}).get(resolved) if resolved else None
    return dict(row or {})


def is_product_isolated_only(product: str, network: str = "mainnet", client=None, refresh: bool = False) -> bool:
    row = get_product_metadata(product=product, network=network, client=client, refresh=refresh)
    return _as_bool(row.get("isolated_only"))


def is_product_id_isolated_only(
    product_id: int, network: str = "mainnet", client=None, refresh: bool = False
) -> bool:
    try:
        pid = int(product_id)
    except (TypeError, ValueError):
        return False
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    key = (catalog.get("by_id") or {}).get(pid)
    if not key:
        return False
    row = (catalog.get("perps") or {}).get(key) or {}
    return _as_bool(row.get("isolated_only"))

