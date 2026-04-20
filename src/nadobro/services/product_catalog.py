import logging
import os
import time
from typing import Optional

import requests

from src.nadobro.config import (
    PRODUCTS,
    PRODUCT_MAX_LEVERAGE,
    SPOT_PRODUCT_IDS,
    NADO_MAINNET_ARCHIVE,
    NADO_MAINNET_REST,
    NADO_TESTNET_ARCHIVE,
    NADO_TESTNET_REST,
)

logger = logging.getLogger(__name__)

_CATALOG_TTL_SECONDS = int(os.environ.get("NADO_PRODUCT_CATALOG_TTL_SECONDS", "60"))
_DYNAMIC_DEFAULT_MAX_LEVERAGE = int(os.environ.get("NADO_DYNAMIC_DEFAULT_MAX_LEVERAGE", "20"))
_REQUEST_TIMEOUT_SECONDS = float(os.environ.get("NADO_HTTP_TIMEOUT_SECONDS", "6"))

_catalog_cache: dict[str, dict] = {}
_spot_catalog_cache: dict[str, dict] = {}
_dn_pair_cache: dict[str, dict] = {}
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


def _archive_v2_url(network: str) -> str:
    base = NADO_MAINNET_ARCHIVE if str(network).lower() == "mainnet" else NADO_TESTNET_ARCHIVE
    return str(base).rstrip("/").replace("/v1", "/v2")


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


def _is_live_trading_status(value) -> bool:
    """Return True when symbol trading status is live/tradable."""
    status = str(value or "").strip().lower()
    if not status:
        # If status is missing, keep backward-compatible behavior.
        return True
    return status in {"live", "trading", "tradable", "active", "enabled"}


def _is_entry_trading_status(value) -> bool:
    status = str(value or "").strip().lower()
    if not status:
        return True
    return status == "live"


def _market_is_open(row: dict) -> bool:
    market_hours = row.get("market_hours")
    if not isinstance(market_hours, dict) or not market_hours:
        return True
    return bool(market_hours.get("is_open", True))


def _dn_underlying_key(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if text.endswith("-PERP"):
        text = text[:-5]
    text = text.replace("_", "-").replace("/", "-").strip("-")
    exact_aliases = {
        "KBTC": "BTC",
        "WBTC": "BTC",
        "BTC": "BTC",
        "WETH": "ETH",
        "ETH": "ETH",
    }
    if text in exact_aliases:
        return exact_aliases[text]
    compact = text.replace("-", "")
    if compact.startswith("WB") and compact.endswith("X") and len(compact) > 3:
        return compact[2:-1]
    # xStocks wrapped Backed spot symbols (e.g. wbNVDA -> NVDA) pair with NVDA-PERP.
    if compact.startswith("WB") and len(compact) >= 5:
        return compact[2:]
    return compact or text


def _derive_max_leverage_from_weight_x18(weight_x18) -> Optional[int]:
    """Derive max leverage from initial asset weight: lev = 1 / (1 - w)."""
    try:
        w = float(int(weight_x18)) / 1e18
    except (TypeError, ValueError):
        return None
    if w <= 0 or w >= 1:
        return None
    try:
        lev = 1.0 / (1.0 - w)
    except ZeroDivisionError:
        return None
    if lev <= 0:
        return None
    return max(1, int(round(lev)))


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


def _rebuild_perp_indexes(perps: dict[str, dict]) -> tuple[dict[int, str], dict[str, str]]:
    """Rebuild by_id and aliases from a perps map."""
    by_id: dict[int, str] = {}
    aliases: dict[str, str] = {}
    for key, row in perps.items():
        try:
            pid = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        by_id[pid] = key
        norm_symbol = str(row.get("symbol") or f"{key}-PERP")
        aliases[key.lower()] = key
        aliases[norm_symbol.lower()] = key
        aliases[f"{key.lower()}-perp"] = key
    return by_id, aliases


def _build_static_spot_catalog() -> dict:
    spots: dict[str, dict] = {}
    by_id: dict[int, str] = {}
    aliases: dict[str, str] = {}
    for name, pid in SPOT_PRODUCT_IDS.items():
        key = str(name or "").upper().strip()
        if not key:
            continue
        symbol = key
        spots[key] = {
            "id": int(pid),
            "type": "spot",
            "symbol": symbol,
            "base": key,
            "dynamic": False,
            "underlying_key": _dn_underlying_key(symbol),
            "trading_status": "live",
            "market_hours": None,
        }
        by_id[int(pid)] = key
        aliases[key.lower()] = key
        aliases[symbol.lower()] = key
        aliases[_dn_underlying_key(symbol).lower()] = key
    return {"spots": spots, "by_id": by_id, "aliases": aliases}


def _fetch_v2_symbols_map(network: str, product_type: str | None = None) -> dict[str, dict]:
    params = {}
    if product_type:
        params["product_type"] = product_type
    try:
        resp = _rest_session.get(
            f"{_archive_v2_url(network)}/symbols",
            params=params,
            headers={"Accept-Encoding": "gzip"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        data = resp.json() if resp is not None else {}
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    except Exception as e:
        logger.warning("catalog: v2 symbols failed on %s (%s): %s", network, product_type or "all", e)
    return {}


def _build_dynamic_spot_catalog(network: str) -> Optional[dict]:
    rows = _fetch_v2_symbols_map(network, product_type="spot")
    if not rows:
        return None
    spots: dict[str, dict] = {}
    by_id: dict[int, str] = {}
    aliases: dict[str, str] = {}
    for raw_symbol, row in rows.items():
        try:
            pid = int(row.get("product_id"))
        except (TypeError, ValueError):
            continue
        symbol = str(row.get("symbol") or raw_symbol or f"S{pid}").upper().strip()
        if not symbol:
            continue
        key = symbol
        underlying_key = _dn_underlying_key(symbol)
        spots[key] = {
            "id": pid,
            "type": "spot",
            "symbol": symbol,
            "base": key,
            "dynamic": True,
            "underlying_key": underlying_key,
            "trading_status": str(row.get("trading_status") or ""),
            "market_hours": row.get("market_hours"),
            "exchange_rate_x18": row.get("exchange_rate_x18"),
        }
        by_id[pid] = key
        aliases[key.lower()] = key
        aliases[symbol.lower()] = key
        if underlying_key:
            aliases[underlying_key.lower()] = key
    return {"spots": spots, "by_id": by_id, "aliases": aliases}


def get_spot_catalog(network: str = "mainnet", refresh: bool = False) -> dict:
    key = str(network or "mainnet").lower()
    if not refresh:
        cached = _spot_catalog_cache.get(key)
        if cached and (time.time() - cached["ts"] < _CATALOG_TTL_SECONDS):
            return cached["data"]
    data = _build_dynamic_spot_catalog(key) or _build_static_spot_catalog()
    _spot_catalog_cache[key] = {"data": data, "ts": time.time()}
    return data


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
    rows_by_pid: dict[int, dict] = {}
    if client is not None:
        try:
            data = client._query_rest("symbols") or {}  # noqa: SLF001
            if data.get("status") == "success":
                symbols = ((data.get("data") or {}).get("symbols") or {})
                if isinstance(symbols, dict):
                    for row in symbols.values():
                        if not isinstance(row, dict):
                            continue
                        try:
                            rows_by_pid[int(row.get("product_id"))] = row
                        except (TypeError, ValueError):
                            continue
        except Exception as e:
            logger.warning("catalog: client symbols failed on %s: %s", network, e)
    if not rows_by_pid:
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
                    for row in symbols.values():
                        if not isinstance(row, dict):
                            continue
                        try:
                            rows_by_pid[int(row.get("product_id"))] = row
                        except (TypeError, ValueError):
                            continue
        except Exception as e:
            logger.warning("catalog: rest symbols failed on %s: %s", network, e)

    # V2 symbols currently expose a broader perp set than the legacy gateway feed.
    # Merge them in so downstream product resolution and DN pairing see the same market set.
    for row in _fetch_v2_symbols_map(network, product_type="perp").values():
        if not isinstance(row, dict):
            continue
        try:
            rows_by_pid[int(row.get("product_id"))] = row
        except (TypeError, ValueError):
            continue

    return list(rows_by_pid.values())


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
        # Filter out symbols that are listed but not currently tradable/live.
        if not _is_live_trading_status(row.get("trading_status")):
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
        derived_max_lev = (
            _derive_max_leverage_from_weight_x18(row.get("long_weight_initial_x18"))
            or _derive_max_leverage_from_weight_x18(product_row.get("long_weight_initial_x18"))
            or _derive_max_leverage_from_weight_x18(book_info.get("long_weight_initial_x18"))
            or _derive_max_leverage_from_weight_x18(product_book.get("long_weight_initial_x18"))
        )
        max_lev = (
            row.get("max_leverage")
            or row.get("max_leverage_x")
            or book_info.get("max_leverage")
            or product_row.get("max_leverage")
            or product_row.get("max_leverage_x")
            or product_book.get("max_leverage")
            or derived_max_lev
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

    # When Nado returns all_products, drop static-only perps not present on-chain (stale config).
    if raw_products and rows_by_id:
        allowed_ids = set(rows_by_id.keys())
        for row in merged_rows:
            try:
                allowed_ids.add(int(row.get("product_id")))
            except (TypeError, ValueError):
                continue
        stale = [k for k, v in perps.items() if int(v.get("id", -1)) not in allowed_ids]
        for k in stale:
            del perps[k]
        by_id, aliases = _rebuild_perp_indexes(perps)

    return {"perps": perps, "by_id": by_id, "aliases": aliases}


def _build_dn_pair_catalog(network: str, client=None) -> Optional[dict]:
    perp_catalog = get_catalog(network=network, client=client, refresh=True)
    spot_catalog = get_spot_catalog(network=network, refresh=True)
    v2_symbols = _fetch_v2_symbols_map(network)
    if not (perp_catalog.get("perps") and spot_catalog.get("spots")):
        return None

    spot_by_underlying: dict[str, list[dict]] = {}
    for key, row in (spot_catalog.get("spots") or {}).items():
        spot_row = dict(row or {})
        spot_row["key"] = key
        underlying_key = str(spot_row.get("underlying_key") or "").upper()
        if not underlying_key:
            continue
        spot_by_underlying.setdefault(underlying_key, []).append(spot_row)

    v2_perp_rows_by_symbol = {
        str(k).upper().strip(): v for k, v in v2_symbols.items() if str((v or {}).get("type") or "").lower() == "perp"
    }
    pairs: dict[str, dict] = {}
    aliases: dict[str, str] = {}
    by_perp_id: dict[int, str] = {}
    by_spot_id: dict[int, str] = {}

    entries = list((perp_catalog.get("perps") or {}).items())
    entries.sort(key=lambda kv: int((kv[1] or {}).get("id", 0)))
    for product_key, row in entries:
        perp_row = dict(row or {})
        perp_symbol = str(perp_row.get("symbol") or f"{product_key}-PERP").upper().strip()
        v2_perp_row = v2_perp_rows_by_symbol.get(perp_symbol, {})
        underlying_key = _dn_underlying_key(product_key) or _dn_underlying_key(perp_symbol)
        if not underlying_key:
            continue
        spot_candidates = list(spot_by_underlying.get(underlying_key) or [])
        if not spot_candidates:
            continue
        spot_candidates.sort(key=lambda item: int(item.get("id", 0)))
        spot_row = spot_candidates[0]
        spot_status = str(spot_row.get("trading_status") or "")
        perp_status = str(v2_perp_row.get("trading_status") or perp_row.get("trading_status") or "")
        spot_open = _market_is_open(spot_row)
        perp_open = _market_is_open(v2_perp_row) if v2_perp_row else _market_is_open(perp_row)
        pair = {
            "product": str(product_key).upper().strip(),
            "underlying_key": underlying_key,
            "perp_product_id": int(perp_row.get("id")),
            "perp_symbol": perp_symbol,
            "spot_product_id": int(spot_row.get("id")),
            "spot_symbol": str(spot_row.get("symbol") or spot_row.get("key") or "").upper(),
            "exchange_rate_x18": spot_row.get("exchange_rate_x18"),
            "spot_trading_status": spot_status,
            "perp_trading_status": perp_status,
            "spot_market_hours": spot_row.get("market_hours"),
            "perp_market_hours": v2_perp_row.get("market_hours") if v2_perp_row else None,
            "entry_allowed": (
                _is_entry_trading_status(spot_status)
                and _is_entry_trading_status(perp_status)
                and spot_open
                and perp_open
            ),
        }
        if not _is_entry_trading_status(spot_status):
            pair["entry_block_reason"] = f"Spot market status is {spot_status or 'not tradable'}."
        elif not _is_entry_trading_status(perp_status):
            pair["entry_block_reason"] = f"Perp market status is {perp_status or 'not tradable'}."
        elif not spot_open:
            pair["entry_block_reason"] = "Spot market is currently closed."
        elif not perp_open:
            pair["entry_block_reason"] = "Perp market is currently closed."
        else:
            pair["entry_block_reason"] = ""
        pair_key = str(pair["product"]).upper()
        pairs[pair_key] = pair
        by_perp_id[int(pair["perp_product_id"])] = pair_key
        by_spot_id[int(pair["spot_product_id"])] = pair_key
        aliases[pair_key.lower()] = pair_key
        aliases[perp_symbol.lower()] = pair_key
        aliases[str(pair["spot_symbol"]).lower()] = pair_key
        if underlying_key:
            aliases[underlying_key.lower()] = pair_key
    if not pairs:
        return None
    return {"pairs": pairs, "aliases": aliases, "by_perp_id": by_perp_id, "by_spot_id": by_spot_id}


def get_catalog(network: str = "mainnet", client=None, refresh: bool = False) -> dict:
    key = str(network or "mainnet").lower()
    if not refresh:
        cached = _catalog_cache.get(key)
        if cached and (time.time() - cached["ts"] < _CATALOG_TTL_SECONDS):
            return cached["data"]

    data = _build_dynamic_catalog(key, client=client) or _build_static_catalog()
    _catalog_cache[key] = {"data": data, "ts": time.time()}
    return data


def get_dn_pair_catalog(network: str = "mainnet", client=None, refresh: bool = False) -> dict:
    key = str(network or "mainnet").lower()
    if not refresh:
        cached = _dn_pair_cache.get(key)
        if cached and (time.time() - cached["ts"] < _CATALOG_TTL_SECONDS):
            return cached["data"]
    data = _build_dn_pair_catalog(key, client=client)
    if data is None:
        # Conservative fallback for legacy BTC/ETH behavior when live metadata is unavailable.
        data = {
            "pairs": {
                "BTC": {
                    "product": "BTC",
                    "underlying_key": "BTC",
                    "perp_product_id": 2,
                    "perp_symbol": "BTC-PERP",
                    "spot_product_id": int(SPOT_PRODUCT_IDS.get("BTC", 1)),
                    "spot_symbol": "BTC",
                    "spot_trading_status": "live",
                    "perp_trading_status": "live",
                    "spot_market_hours": None,
                    "perp_market_hours": None,
                    "entry_allowed": True,
                    "entry_block_reason": "",
                },
                "ETH": {
                    "product": "ETH",
                    "underlying_key": "ETH",
                    "perp_product_id": 4,
                    "perp_symbol": "ETH-PERP",
                    "spot_product_id": int(SPOT_PRODUCT_IDS.get("ETH", 3)),
                    "spot_symbol": "ETH",
                    "spot_trading_status": "live",
                    "perp_trading_status": "live",
                    "spot_market_hours": None,
                    "perp_market_hours": None,
                    "entry_allowed": True,
                    "entry_block_reason": "",
                },
            },
            "aliases": {
                "btc": "BTC",
                "btc-perp": "BTC",
                "eth": "ETH",
                "eth-perp": "ETH",
            },
            "by_perp_id": {2: "BTC", 4: "ETH"},
            "by_spot_id": {
                int(SPOT_PRODUCT_IDS.get("BTC", 1)): "BTC",
                int(SPOT_PRODUCT_IDS.get("ETH", 3)): "ETH",
            },
        }
    _dn_pair_cache[key] = {"data": data, "ts": time.time()}
    return data


def list_perp_names(network: str = "mainnet", client=None, refresh: bool = False) -> list[str]:
    catalog = get_catalog(network=network, client=client, refresh=refresh)
    entries = list((catalog.get("perps") or {}).items())
    entries.sort(key=lambda kv: int((kv[1] or {}).get("id", 0)))
    return [name for name, _ in entries]


def list_dn_product_names(network: str = "mainnet", client=None, refresh: bool = False) -> list[str]:
    catalog = get_dn_pair_catalog(network=network, client=client, refresh=refresh)
    entries = list((catalog.get("pairs") or {}).items())
    entries.sort(key=lambda kv: int((kv[1] or {}).get("perp_product_id", 0)))
    return [name for name, _ in entries]


def get_dn_pair(product: str, network: str = "mainnet", client=None, refresh: bool = False) -> dict:
    if not product:
        return {}
    catalog = get_dn_pair_catalog(network=network, client=client, refresh=refresh)
    aliases = catalog.get("aliases") or {}
    key = aliases.get(str(product).lower().strip()) or str(product).upper().strip()
    return dict((catalog.get("pairs") or {}).get(key) or {})


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


def get_spot_product_id(name: str, network: str = "mainnet", refresh: bool = False) -> Optional[int]:
    if not name:
        return None
    dn_pair = get_dn_pair(name, network=network, refresh=refresh)
    if dn_pair.get("spot_product_id") is not None:
        return int(dn_pair["spot_product_id"])
    catalog = get_spot_catalog(network=network, refresh=refresh)
    aliases = catalog.get("aliases") or {}
    key = aliases.get(str(name).lower().strip())
    if not key:
        return None
    row = (catalog.get("spots") or {}).get(key)
    if not row:
        return None
    try:
        return int(row.get("id"))
    except (TypeError, ValueError):
        return None


def get_spot_metadata(name: str, network: str = "mainnet", refresh: bool = False) -> dict:
    if not name:
        return {}
    dn_pair = get_dn_pair(name, network=network, refresh=refresh)
    if dn_pair.get("spot_product_id") is not None:
        return {
            "id": int(dn_pair["spot_product_id"]),
            "symbol": str(dn_pair.get("spot_symbol") or ""),
            "trading_status": str(dn_pair.get("spot_trading_status") or ""),
            "market_hours": dn_pair.get("spot_market_hours"),
            "exchange_rate_x18": dn_pair.get("exchange_rate_x18"),
        }
    catalog = get_spot_catalog(network=network, refresh=refresh)
    aliases = catalog.get("aliases") or {}
    key = aliases.get(str(name).lower().strip())
    if not key:
        return {}
    return dict((catalog.get("spots") or {}).get(key) or {})


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

