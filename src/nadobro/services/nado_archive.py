"""
Nado Archive Indexer client.

Queries the archive API for actual fill data (prices, fees, realized PnL)
after trades are placed via the Nado SDK (which only returns a digest).

Archive docs: https://docs.nado.xyz/developer-resources/api/archive-indexer
"""
import logging
import time
import requests
from typing import Optional

from src.nadobro.config import NADO_TESTNET_ARCHIVE, NADO_MAINNET_ARCHIVE
from src.nadobro.services.log_redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT = 8.0
_MAX_RETRIES = 2
_RETRY_BASE_SECONDS = 0.3

# Reuse the shared session from nado_client for connection pooling.
_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"Accept-Encoding": "gzip"})
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session


def archive_url_for_network(network: str) -> str:
    return NADO_MAINNET_ARCHIVE if network == "mainnet" else NADO_TESTNET_ARCHIVE


def _from_x18(value) -> float:
    if value is None:
        return 0.0
    try:
        v = float(value) if not isinstance(value, (int, float)) else float(value)
        if abs(v) >= 1e9:
            return v / 1e18
        return v
    except (ValueError, TypeError):
        return 0.0


def _post(url: str, payload: dict) -> dict | list | None:
    session = _get_session()
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = session.post(url, json=payload, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", "?")
            body = ""
            try:
                body = (e.response.text or "")[:200]
            except Exception:
                pass
            logger.warning("Archive API HTTP %s (attempt %d): %s", status, attempt + 1, redact_sensitive_text(body))
            if attempt >= _MAX_RETRIES:
                return None
        except requests.RequestException as e:
            logger.warning("Archive API request failed (attempt %d): %s", attempt + 1, e)
            if attempt >= _MAX_RETRIES:
                return None
        time.sleep(_RETRY_BASE_SECONDS * (2 ** attempt))
    return None


def _orders_list_from_archive_response(result) -> list:
    """Normalize archive POST bodies that may return orders at top level or under data."""
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        return []
    o = result.get("orders")
    if isinstance(o, list):
        return o
    data = result.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        inner = data.get("orders")
        if isinstance(inner, list):
            return inner
    return []


def _matches_list_from_archive_response(result) -> list:
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        return []
    m = result.get("matches")
    if isinstance(m, list):
        return m
    data = result.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        inner = data.get("matches")
        if isinstance(inner, list):
            return inner
    return []


def _isolated_subaccounts_list_from_response(result) -> list:
    """Normalize archive POST bodies for isolated_subaccounts queries."""
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        return []
    rows = result.get("isolated_subaccounts") or result.get("isolatedSubaccounts")
    if isinstance(rows, list):
        return rows
    data = result.get("data")
    if isinstance(data, dict):
        inner = data.get("isolated_subaccounts") or data.get("isolatedSubaccounts")
        if isinstance(inner, list):
            return inner
    if isinstance(data, list):
        return data
    return []


def isolated_subaccount_from_row(row: dict, parent_subaccount_hex: str = "") -> str:
    """Extract an isolated child subaccount from known archive response shapes."""
    if not isinstance(row, dict):
        return ""
    parent = (parent_subaccount_hex or "").strip().lower()
    for key in (
        "isolated_subaccount",
        "isolatedSubaccount",
        "isolated_subaccount_hex",
        "isolatedSubaccountHex",
        "child_subaccount",
        "childSubaccount",
        "subaccount_hex",
        "subaccountHex",
        "subaccount",
    ):
        value = (row.get(key) or "").strip()
        if not value:
            continue
        if parent and value.lower() == parent:
            continue
        return value
    return ""


def query_isolated_subaccounts_for_parent(
    network: str,
    parent_subaccount_hex: str,
    limit: int = 100,
) -> list[dict]:
    """
    Isolated margin positions live on child subaccounts. List those linked to the
    default (parent) subaccount.

    https://docs.nado.xyz/developer-resources/api/archive-indexer/isolated-subaccounts
    """
    parent = (parent_subaccount_hex or "").strip()
    if not parent:
        return []
    url = archive_url_for_network(network)
    # Archive API expects a single hex string here, not an array (see Nado docs).
    payload = {"isolated_subaccounts": {"subaccount": parent, "limit": min(max(1, int(limit)), 500)}}
    result = _post(url, payload)
    return _isolated_subaccounts_list_from_response(result) or []


def _pick(d: dict, *keys, default=0):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _parse_order(order: dict) -> dict:
    """Parse a single order from the archive orders response."""
    base_filled = _from_x18(_pick(order, "base_filled", "baseFilled", default=0))
    quote_filled = _from_x18(_pick(order, "quote_filled", "quoteFilled", default=0))
    fee = _from_x18(_pick(order, "fee", default=0))
    realized_pnl = _from_x18(_pick(order, "realized_pnl", "realizedPnl", default=0))
    closed_amount = _from_x18(_pick(order, "closed_amount", "closedAmount", default=0))
    closed_net_entry = _from_x18(_pick(order, "closed_net_entry", "closedNetEntry", default=0))

    fill_price = 0.0
    if abs(base_filled) > 0 and abs(quote_filled) > 0:
        fill_price = abs(quote_filled / base_filled)

    is_filled = abs(base_filled) > 0

    return {
        "digest": order.get("digest", ""),
        "product_id": order.get("product_id"),
        "fill_price": fill_price,
        "fill_size": abs(base_filled),
        "quote_filled": abs(quote_filled),
        "fee": abs(fee),
        "builder_fee": _from_x18(order.get("builder_fee", 0)),
        "realized_pnl": realized_pnl,
        "closed_amount": abs(closed_amount),
        "closed_net_entry": closed_net_entry,
        "first_fill_ts": order.get("first_fill_timestamp"),
        "last_fill_ts": order.get("last_fill_timestamp"),
        "is_filled": is_filled,
        "isolated": order.get("isolated", False),
        "prev_position": _from_x18(order.get("prev_position", 0)),
        "original_amount": _from_x18(order.get("amount", 0)),
        "original_price": _from_x18(order.get("price_x18", 0)),
    }


def _parse_match(match: dict) -> dict:
    """Parse a single match from the archive matches response."""
    base_filled = _from_x18(match.get("base_filled", 0))
    quote_filled = _from_x18(match.get("quote_filled", 0))
    fee = _from_x18(match.get("fee", 0))
    realized_pnl = _from_x18(match.get("realized_pnl", 0))

    fill_price = 0.0
    if abs(base_filled) > 0 and abs(quote_filled) > 0:
        fill_price = abs(quote_filled / base_filled)

    return {
        "submission_idx": match.get("submission_idx"),
        "digest": match.get("digest", ""),
        "is_taker": match.get("is_taker", False),
        "fill_price": fill_price,
        "fill_size": abs(base_filled),
        "quote_filled": abs(quote_filled),
        "fee": abs(fee),
        "sequencer_fee": _from_x18(match.get("sequencer_fee", 0)),
        "builder_fee": _from_x18(match.get("builder_fee", 0)),
        "realized_pnl": realized_pnl,
        "closed_amount": _from_x18(match.get("closed_amount", 0)),
        "closed_net_entry": _from_x18(match.get("closed_net_entry", 0)),
        "isolated": match.get("isolated", False),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def query_order_by_digest(
    network: str,
    digest: str,
    max_wait_seconds: float = 2.0,
    poll_interval: float = 0.5,
) -> Optional[dict]:
    """
    Query the Nado archive for fill data of a specific order by its digest.

    For IOC (market) orders that fill instantly, the archive may take a short
    time to index the fill.  This function polls up to *max_wait_seconds*.

    Returns parsed fill data dict or None if not found / not filled.
    """
    url = archive_url_for_network(network)
    payload = {"orders": {"digests": [digest]}}
    deadline = time.time() + max_wait_seconds
    attempt = 0

    while True:
        attempt += 1
        result = _post(url, payload)
        if result:
            orders = _orders_list_from_archive_response(result)
            if isinstance(orders, list):
                for o in orders:
                    if not isinstance(o, dict):
                        continue
                    parsed = _parse_order(o)
                    if parsed["is_filled"]:
                        logger.info(
                            "Archive fill resolved for digest %s: price=%.6f size=%.6f fee=%.6f pnl=%.6f (attempt %d)",
                            digest[:16], parsed["fill_price"], parsed["fill_size"],
                            parsed["fee"], parsed["realized_pnl"], attempt,
                        )
                        return parsed

        if time.time() >= deadline:
            logger.debug("Archive fill not resolved for digest %s after %.1fs", digest[:16], max_wait_seconds)
            return None
        time.sleep(poll_interval)


def query_orders_by_subaccount(
    network: str,
    subaccount_hex: str,
    product_ids: list[int] | None = None,
    limit: int = 100,
    max_time: int | None = None,
) -> list[dict]:
    """Query recent orders for a subaccount from the archive."""
    url = archive_url_for_network(network)
    params: dict = {
        "subaccounts": [subaccount_hex],
        "limit": min(limit, 500),
    }
    if product_ids:
        params["product_ids"] = product_ids
    if max_time:
        params["max_time"] = max_time

    result = _post(url, {"orders": params})
    if not result:
        return []

    orders_raw = _orders_list_from_archive_response(result)
    if not isinstance(orders_raw, list):
        return []
    return [_parse_order(o) for o in orders_raw if isinstance(o, dict)]


def query_matches_by_subaccount(
    network: str,
    subaccount_hex: str,
    product_ids: list[int] | None = None,
    limit: int = 50,
    max_time: int | None = None,
) -> list[dict]:
    """Query recent matches/fills for a subaccount from the archive."""
    url = archive_url_for_network(network)
    params: dict = {
        "subaccounts": [subaccount_hex],
        "limit": min(limit, 500),
    }
    if product_ids:
        params["product_ids"] = product_ids
    if max_time:
        params["max_time"] = max_time

    result = _post(url, {"matches": params})
    if not result:
        return []

    matches_raw = _matches_list_from_archive_response(result)
    if not isinstance(matches_raw, list):
        return []
    return [_parse_match(m) for m in matches_raw if isinstance(m, dict)]


def query_funding_payments(
    network: str,
    subaccount_hex: str,
    product_ids: list[int],
    limit: int = 100,
) -> list[dict]:
    """Query interest and funding payments for a subaccount."""
    url = archive_url_for_network(network)
    payload = {
        "interest_and_funding_payments": {
            "subaccount": subaccount_hex,
            "product_ids": product_ids,
            "limit": min(limit, 100),
        }
    }
    result = _post(url, payload)
    if not result or not isinstance(result, dict):
        return []

    payments = []
    for key in ("funding_payments", "interest_payments"):
        for p in result.get(key, []):
            if not isinstance(p, dict):
                continue
            payments.append({
                "type": "funding" if key == "funding_payments" else "interest",
                "product_id": p.get("product_id"),
                "timestamp": p.get("timestamp"),
                "amount": _from_x18(p.get("amount", 0)),
                "balance_amount": _from_x18(p.get("balance_amount", 0)),
                "rate": _from_x18(p.get("rate_x18", 0)),
                "oracle_price": _from_x18(p.get("oracle_price_x18", 0)),
            })
    return payments
