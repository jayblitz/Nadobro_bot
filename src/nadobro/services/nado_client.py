import logging
import time
import os
import random
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Optional
from src.nadobro.config import (
    NADO_TESTNET_REST, NADO_MAINNET_REST,
    NADO_TESTNET_ARCHIVE, NADO_MAINNET_ARCHIVE,
    get_product_name, get_perp_products, get_product_id,
    get_nado_builder_routing_config,
)

logger = logging.getLogger(__name__)

_price_cache = {}
_PRICE_CACHE_TTL = 5
_ALL_PRODUCTS_CACHE = {}
_ALL_PRODUCTS_TTL = 20
_FUNDING_CACHE = {}
_FUNDING_TTL = 10
_size_increment_cache = {}
_price_increment_cache = {}
_size_increment_x18_cache = {}
_price_increment_x18_cache = {}
_min_size_x18_cache = {}
_REQUEST_TIMEOUT_SECONDS = float(os.environ.get("NADO_HTTP_TIMEOUT_SECONDS", "6"))
_FANOUT_WORKERS = int(os.environ.get("NADO_FANOUT_WORKERS", "8"))
_REST_MAX_RETRIES = int(os.environ.get("NADO_REST_MAX_RETRIES", "2"))
_REST_RETRY_BASE_SECONDS = float(os.environ.get("NADO_REST_RETRY_BASE_SECONDS", "0.25"))
_REST_RETRY_JITTER_SECONDS = float(os.environ.get("NADO_REST_RETRY_JITTER_SECONDS", "0.2"))
_REST_POOL_CONNECTIONS = int(os.environ.get("NADO_HTTP_POOL_CONNECTIONS", "64"))
_REST_POOL_MAXSIZE = int(os.environ.get("NADO_HTTP_POOL_MAXSIZE", "64"))
_OPEN_ORDERS_CACHE_TTL = float(os.environ.get("NADO_OPEN_ORDERS_CACHE_TTL_SECONDS", "2.0"))
_POSITIONS_FALLBACK_TTL = float(os.environ.get("NADO_POSITIONS_FALLBACK_TTL_SECONDS", "6.0"))
_POSITIONS_FALLBACK_MAX_PRODUCTS = int(os.environ.get("NADO_POSITIONS_FALLBACK_MAX_PRODUCTS", "16"))
_rest_session = requests.Session()
_rest_session.mount(
    "https://",
    HTTPAdapter(pool_connections=max(8, _REST_POOL_CONNECTIONS), pool_maxsize=max(8, _REST_POOL_MAXSIZE)),
)
_rest_session.mount(
    "http://",
    HTTPAdapter(pool_connections=max(8, _REST_POOL_CONNECTIONS), pool_maxsize=max(8, _REST_POOL_MAXSIZE)),
)
_open_orders_cache: dict[tuple[str, str, int], dict] = {}
_positions_fallback_cache: dict[tuple[str, str], dict] = {}


class NadoClient:
    def __init__(self, private_key: str, network: str = "testnet", main_address: str = None):
        self.private_key = private_key
        self.network = network
        self.client = None
        self.subaccount_hex = None
        self.address = None
        self.main_address = main_address
        self._initialized = False
        self._derive_address()

    @classmethod
    def from_address(cls, address: str, network: str = "testnet") -> "NadoClient":
        instance = cls.__new__(cls)
        instance.private_key = None
        instance.network = network
        instance.client = None
        instance.address = address
        instance.main_address = address
        instance._initialized = False
        try:
            from nado_protocol.utils.bytes32 import subaccount_to_hex
            instance.subaccount_hex = subaccount_to_hex(address, "default")
        except ImportError:
            default_bytes = "default".encode().hex()
            instance.subaccount_hex = address.lower() + default_bytes + "0" * (24 - len(default_bytes))
        return instance

    def _compute_subaccount_hex(self, address: str) -> str:
        try:
            from nado_protocol.utils.bytes32 import subaccount_to_hex
            return subaccount_to_hex(address, "default")
        except ImportError:
            default_bytes = "default".encode().hex()
            return address.lower() + default_bytes + "0" * (24 - len(default_bytes))

    def _derive_address(self):
        try:
            from eth_account import Account
            acct = Account.from_key(self.private_key)
            self.address = acct.address
            query_addr = self.main_address or self.address
            self.subaccount_hex = self._compute_subaccount_hex(query_addr)
        except Exception as e:
            logger.error(f"Failed to derive address from private key: {e}")

    def initialize(self):
        if self._initialized:
            return True
        try:
            from nado_protocol.client import create_nado_client, NadoClientMode

            mode = NadoClientMode.TESTNET if self.network == "testnet" else NadoClientMode.MAINNET
            self.client = create_nado_client(mode, self.private_key)
            self.address = self.client.context.signer.address
            query_addr = self.main_address or self.address
            self.subaccount_hex = self._compute_subaccount_hex(query_addr)
            self._initialized = True
            self.private_key = None  # Clear raw key after SDK init
            logger.info(
                "Nado client initialized: signer=%s, query=%s, network=%s",
                self.address, query_addr, self.network,
            )
            return True
        except ImportError:
            logger.warning("nado_protocol not installed, using REST API fallback")
            self._initialized = False
            return False
        except Exception as e:
            logger.error(f"Failed to initialize Nado client: {e}")
            self._initialized = False
            return False

    def _rest_url(self):
        return NADO_MAINNET_REST if self.network == "mainnet" else NADO_TESTNET_REST

    def _archive_url(self):
        return NADO_MAINNET_ARCHIVE if self.network == "mainnet" else NADO_TESTNET_ARCHIVE

    @staticmethod
    def _parse_json_response(resp) -> Optional[dict]:
        if resp is None:
            return None
        try:
            return resp.json()
        except ValueError:
            snippet = ""
            try:
                snippet = (resp.text or "").strip().replace("\n", " ")[:180]
            except Exception:
                snippet = ""
            logger.warning(
                "REST returned non-JSON status=%s content_type=%s body=%r",
                getattr(resp, "status_code", "?"),
                (getattr(resp, "headers", {}) or {}).get("content-type"),
                snippet,
            )
            return None

    def _query_rest(self, query_type: str, extra_params: Optional[dict] = None) -> Optional[dict]:
        params = {"type": query_type}
        if extra_params:
            params.update(extra_params)
        url = f"{self._rest_url()}/query"
        headers = {"Accept-Encoding": "gzip"}
        use_post = query_type in {"market_prices", "orders"} or isinstance((extra_params or {}).get("product_ids"), list)
        max_attempts = max(1, _REST_MAX_RETRIES + 1)
        for attempt in range(max_attempts):
            try:
                if use_post:
                    resp = _rest_session.post(url, json=params, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
                else:
                    resp = _rest_session.get(url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
                data = self._parse_json_response(resp)
                if data is not None:
                    return data
                if attempt < (max_attempts - 1):
                    sleep_s = (_REST_RETRY_BASE_SECONDS * (2 ** attempt)) + random.uniform(0.0, _REST_RETRY_JITTER_SECONDS)
                    time.sleep(sleep_s)
                    continue
                return None
            except requests.RequestException as e:
                if attempt >= (max_attempts - 1):
                    logger.error("REST query failed type=%s attempts=%s: %s", query_type, max_attempts, e)
                    return None
                sleep_s = (_REST_RETRY_BASE_SECONDS * (2 ** attempt)) + random.uniform(0.0, _REST_RETRY_JITTER_SECONDS)
                time.sleep(sleep_s)
            except Exception as e:
                logger.error("REST query failed type=%s unexpected: %s", query_type, e)
                return None
        return None

    def get_market_price(self, product_id: int) -> dict:
        cache_key = f"{self.network}:{product_id}"
        cached = _price_cache.get(cache_key)
        if cached and (time.time() - cached["ts"] < _PRICE_CACHE_TTL):
            return cached["data"]

        if self._initialized and self.client:
            try:
                from nado_protocol.utils.math import from_x18
                mp = self.client.context.engine_client.get_market_price(product_id)
                bid = from_x18(int(mp.bid_x18))
                ask = from_x18(int(mp.ask_x18)) if hasattr(mp, 'ask_x18') else bid
                result = {"bid": float(bid), "ask": float(ask), "mid": float((bid + ask) / 2)}
                _price_cache[cache_key] = {"data": result, "ts": time.time()}
                return result
            except Exception as e:
                logger.error(f"SDK get_market_price failed: {e}")

        try:
            data = self._query_rest("market_price", {"product_id": product_id}) or {}
            if data.get("status") == "success":
                bid = int(data["data"]["bid_x18"]) / 1e18
                ask = int(data["data"].get("ask_x18", data["data"]["bid_x18"])) / 1e18
                result = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}
                _price_cache[cache_key] = {"data": result, "ts": time.time()}
                return result
        except Exception as e:
            logger.error(f"REST get_market_price failed: {e}")

        return {"bid": 0, "ask": 0, "mid": 0}

    def get_all_market_prices(self) -> dict:
        prices = {}
        try:
            product_ids = []
            for name in get_perp_products(network=self.network, client=self):
                pid = get_product_id(name, network=self.network, client=self)
                if pid is not None:
                    product_ids.append(int(pid))
            if product_ids:
                data = self._query_rest("market_prices", {"product_ids": product_ids}) or {}
            else:
                data = {}
            if data.get("status") == "success":
                payload = data.get("data", {}) or {}
                rows = payload.get("market_prices")
                if rows is None and isinstance(payload, list):
                    rows = payload
                if rows is None and isinstance(payload, dict):
                    rows = payload.get("prices") or payload.get("markets")
                if isinstance(rows, dict):
                    rows = list(rows.values())
                for row in rows or []:
                    if not isinstance(row, dict):
                        continue
                    try:
                        pid = int(row.get("product_id"))
                    except Exception:
                        continue
                    name = str(get_product_name(pid, network=self.network, client=self)).replace("-PERP", "")
                    bid = self._from_x18_dynamic(row.get("bid_x18") or row.get("bid") or row.get("price_x18") or row.get("price"))
                    ask = self._from_x18_dynamic(row.get("ask_x18") or row.get("ask") or row.get("price_x18") or row.get("price"))
                    if bid <= 0 and ask > 0:
                        bid = ask
                    if ask <= 0 and bid > 0:
                        ask = bid
                    mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
                    if mid > 0:
                        prices[name] = {"bid": float(bid), "ask": float(ask), "mid": float(mid)}
                if prices:
                    return prices
        except Exception as e:
            logger.debug("market_prices bulk query unavailable, falling back to fanout: %s", e)

        perp_products = []
        for name in get_perp_products(network=self.network, client=self):
            pid = get_product_id(name, network=self.network, client=self)
            if pid is not None:
                perp_products.append((name, pid))
        with ThreadPoolExecutor(max_workers=max(1, _FANOUT_WORKERS)) as pool:
            futures = {pool.submit(self.get_market_price, pid): name for name, pid in perp_products}
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    prices[name] = fut.result()
                except Exception:
                    prices[name] = {"bid": 0, "ask": 0, "mid": 0}
        return prices

    def get_balance(self) -> dict:
        if self._initialized and self.client:
            try:
                from nado_protocol.utils.math import from_x18
                info = self.client.context.engine_client.get_subaccount_info(self.subaccount_hex)
                balances = {}
                if info.exists and info.spot_balances:
                    for sb in info.spot_balances:
                        bal = from_x18(int(sb.balance.amount))
                        balances[sb.product_id] = float(bal)
                return {"exists": info.exists, "balances": balances}
            except Exception as e:
                logger.error(f"SDK get_balance failed: {e}")

        try:
            data = self._query_rest("subaccount_info", {"subaccount": self.subaccount_hex}) or {}
            if data.get("status") == "success":
                data_payload = data.get("data", {}) or {}
                exists_field = data_payload.get("exists")
                balances = {}
                for sb in data_payload.get("spot_balances", []):
                    bal = int(sb["balance"]["amount"]) / 1e18
                    balances[sb["product_id"]] = bal
                exists = bool(exists_field) if exists_field is not None else bool(balances)
                return {"exists": exists, "balances": balances}
        except Exception as e:
            logger.error(f"REST get_balance failed: {e}")

        return {"exists": False, "balances": {}}

    def get_open_orders(self, product_id: int, refresh: bool = False) -> list:
        cache_key = (self.network, str(self.subaccount_hex or ""), int(product_id))
        if not refresh:
            cached = _open_orders_cache.get(cache_key)
            if cached and (time.time() - float(cached.get("ts", 0))) < _OPEN_ORDERS_CACHE_TTL:
                return list(cached.get("data") or [])
        if self._initialized and self.client:
            try:
                from nado_protocol.utils.math import from_x18
                orders_data = self.client.context.engine_client.get_subaccount_open_orders(product_id, self.subaccount_hex)
                orders = []
                for o in orders_data.orders:
                    amount = from_x18(int(o.amount))
                    price = from_x18(int(o.price_x18))
                    orders.append({
                        "digest": o.digest,
                        "amount": float(amount),
                        "price": float(price),
                        "side": "LONG" if float(amount) > 0 else "SHORT",
                        "product_id": product_id,
                        "product_name": get_product_name(product_id),
                    })
                _open_orders_cache[cache_key] = {"data": orders, "ts": time.time()}
                return orders
            except Exception as e:
                logger.error(f"SDK get_open_orders failed: {e}")
        try:
            # Read-only/runtime clients rely on REST, so keep parity with SDK path.
            data = self._query_rest(
                "subaccount_orders",
                {"sender": self.subaccount_hex, "product_id": product_id},
            ) or {}
            if data.get("status") == "success":
                payload = data.get("data", {}) or {}
                rows = payload.get("orders")
                if rows is None and isinstance(payload, list):
                    rows = payload
                if rows is None:
                    rows = data.get("orders")
                orders = []
                for o in rows or []:
                    if not isinstance(o, dict):
                        continue
                    digest = o.get("digest") or o.get("order_digest") or o.get("id")
                    amount_raw = (
                        o.get("amount")
                        or o.get("size")
                        or o.get("amount_x18")
                        or o.get("size_x18")
                        or 0
                    )
                    price_raw = o.get("price") or o.get("price_x18") or 0
                    amount = self._from_x18_dynamic(amount_raw)
                    price = self._from_x18_dynamic(price_raw)
                    if not digest:
                        continue
                    orders.append(
                        {
                            "digest": str(digest),
                            "amount": abs(float(amount)),
                            "price": float(price),
                            "side": self._normalize_side(
                                raw_side=o.get("side"),
                                raw_is_long=o.get("is_long"),
                                raw_direction=o.get("direction"),
                                signed_amount=float(amount),
                            ),
                            "product_id": product_id,
                            "product_name": get_product_name(product_id),
                        }
                    )
                _open_orders_cache[cache_key] = {"data": orders, "ts": time.time()}
                return orders
        except Exception as e:
            logger.error("REST get_open_orders failed: %s", e)
        _open_orders_cache[cache_key] = {"data": [], "ts": time.time()}
        return []

    def get_all_open_orders(self, refresh: bool = False) -> list[dict]:
        """
        Fetch open orders for all perp products concurrently.

        This avoids serial per-product REST calls on Portfolio refresh paths.
        """
        product_pairs = []
        for name in get_perp_products(network=self.network, client=self):
            pid = get_product_id(name, network=self.network, client=self)
            if pid is not None:
                product_pairs.append((name, int(pid)))

        if not product_pairs:
            return []

        rows: list[dict] = []
        max_workers = max(1, min(len(product_pairs), _FANOUT_WORKERS))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self.get_open_orders, pid, refresh): (name, pid)
                for name, pid in product_pairs
            }
            for fut in as_completed(futures):
                _name, pid = futures[fut]
                try:
                    for order in fut.result() or []:
                        normalized = dict(order)
                        normalized["product_id"] = int(normalized.get("product_id") or pid)
                        if not normalized.get("product_name"):
                            normalized["product_name"] = get_product_name(pid, network=self.network, client=self)
                        rows.append(normalized)
                except Exception:
                    continue
        return rows

    @staticmethod
    def _from_x18_dynamic(value) -> float:
        if value is None:
            return 0.0
        try:
            if isinstance(value, str):
                raw = value.strip()
                if not raw:
                    return 0.0
                fv = float(raw)
            else:
                fv = float(value)
            # Nado payloads can mix x18 integers and plain floats.
            # Treat very large magnitudes as x18; otherwise keep human units.
            if abs(fv) >= 1e9:
                return fv / 1e18
            return fv
        except (ValueError, TypeError) as e:
            logging.getLogger(__name__).warning("_from_x18_dynamic: invalid value %r: %s", value, e)
            return 0.0

    @staticmethod
    def _normalize_side(raw_side=None, raw_is_long=None, raw_direction=None, signed_amount: float = 0.0) -> str:
        if raw_is_long is not None:
            return "LONG" if bool(raw_is_long) else "SHORT"

        def _from_text(value):
            if value is None:
                return None
            text = str(value).strip().upper()
            if not text:
                return None
            if text in ("LONG", "BUY", "BID", "BULL", "L"):
                return "LONG"
            if text in ("SHORT", "SELL", "ASK", "BEAR", "S"):
                return "SHORT"
            return None

        side = _from_text(raw_side)
        if side:
            return side
        side = _from_text(raw_direction)
        if side:
            return side
        return "LONG" if float(signed_amount or 0.0) >= 0 else "SHORT"

    @staticmethod
    def _nested_position_container(obj):
        if obj is None:
            return None
        for key in ("subaccount_info", "subaccountInfo", "account", "result", "data"):
            if isinstance(obj, dict):
                nested = obj.get(key)
            else:
                nested = getattr(obj, key, None)
            if nested:
                return nested
        return None

    def _extract_unrealized_pnl_sdk(self, p, balance_obj=None) -> float | None:
        """Best-effort uPnL from SDK objects (matches Nado when the API exposes it)."""
        for obj in (p, balance_obj):
            if obj is None:
                continue
            for attr in (
                "unrealized_pnl",
                "unrealizedPnl",
                "unsettled_pnl",
                "unsettledPnl",
                "u_pnl",
                "perp_pnl",
                "perpPnl",
            ):
                raw = getattr(obj, attr, None)
                if raw is not None:
                    try:
                        return float(self._from_x18_dynamic(raw))
                    except Exception:
                        continue
        return None

    def _extract_unrealized_pnl_rest(self, p: dict, balance_dict: dict | None) -> float | None:
        """Best-effort uPnL from REST dicts (matches Nado when the API exposes it)."""
        for obj in (p, balance_dict or {}):
            if not isinstance(obj, dict):
                continue
            for key in (
                "unrealized_pnl",
                "unrealizedPnl",
                "unsettled_pnl",
                "unsettledPnl",
                "u_pnl",
                "perp_pnl",
                "perpPnl",
                "unrealized_pnl_x18",
                "unrealizedPnlX18",
            ):
                if obj.get(key) is not None:
                    try:
                        return float(self._from_x18_dynamic(obj[key]))
                    except Exception:
                        continue
        return None

    def _extract_liquidation_price_sdk(self, p, balance_obj) -> float | None:
        """Best-effort liquidation / est. liq price from SDK position objects."""
        for obj in (p, balance_obj):
            if obj is None:
                continue
            for attr in (
                "liquidation_price_x18",
                "liquidationPriceX18",
                "liquidation_price",
                "liquidationPrice",
                "est_liquidation_price_x18",
                "estLiquidationPriceX18",
                "est_liquidation_price",
                "estLiquidationPrice",
                "liq_price_x18",
                "liqPriceX18",
                "liq_price",
                "liqPrice",
            ):
                raw = getattr(obj, attr, None)
                if raw is not None:
                    try:
                        v = float(self._from_x18_dynamic(raw))
                        if v > 0:
                            return v
                    except Exception:
                        continue
        return None

    def _extract_liquidation_price_rest(self, p: dict, balance_dict: dict | None) -> float | None:
        for obj in (p, balance_dict or {}):
            if not isinstance(obj, dict):
                continue
            for key in (
                "liquidation_price_x18",
                "liquidationPriceX18",
                "liquidation_price",
                "liquidationPrice",
                "est_liquidation_price_x18",
                "estLiquidationPriceX18",
                "est_liquidation_price",
                "estLiquidationPrice",
                "liq_price_x18",
                "liqPriceX18",
                "liq_price",
                "liqPrice",
            ):
                if obj.get(key) is not None:
                    try:
                        v = float(self._from_x18_dynamic(obj[key]))
                        if v > 0:
                            return v
                    except Exception:
                        continue
        return None

    def _extract_positions_from_sdk_info(self, info) -> list:
        positions = []
        if not info:
            return positions
        candidate_lists = []
        for attr in ("perp_positions", "positions", "perp_balances", "perpPositions", "perpBalances"):
            val = getattr(info, attr, None)
            if val:
                candidate_lists.append(val)
        if not candidate_lists:
            nested = self._nested_position_container(info)
            if nested is not None and nested is not info:
                return self._extract_positions_from_sdk_info(nested)
            return positions

        for plist in candidate_lists:
            for p in plist:
                product_id = (
                    getattr(p, "product_id", None)
                    or getattr(p, "productId", None)
                    or getattr(p, "pid", None)
                )
                if product_id is None:
                    continue
                balance_obj = getattr(p, "balance", None)
                amount_raw = None
                v_quote_raw = None
                if balance_obj is not None:
                    amount_raw = (
                        getattr(balance_obj, "amount", None)
                        or getattr(balance_obj, "amount_x18", None)
                        or getattr(balance_obj, "amountX18", None)
                        or getattr(balance_obj, "balance_amount", None)
                        or getattr(balance_obj, "balanceAmount", None)
                        or getattr(balance_obj, "balance_amount_x18", None)
                        or getattr(balance_obj, "balanceAmountX18", None)
                        or getattr(balance_obj, "size", None)
                        or getattr(balance_obj, "size_x18", None)
                        or getattr(balance_obj, "sizeX18", None)
                    )
                    v_quote_raw = (
                        getattr(balance_obj, "v_quote_balance", None)
                        or getattr(balance_obj, "vQuoteBalance", None)
                        or getattr(balance_obj, "v_quote_balance_x18", None)
                        or getattr(balance_obj, "vQuoteBalanceX18", None)
                    )
                if amount_raw is None:
                    amount_raw = (
                        getattr(p, "amount", None)
                        or getattr(p, "amount_x18", None)
                        or getattr(p, "amountX18", None)
                        or getattr(p, "balance_amount", None)
                        or getattr(p, "balanceAmount", None)
                        or getattr(p, "balance_amount_x18", None)
                        or getattr(p, "balanceAmountX18", None)
                        or getattr(p, "size", None)
                        or getattr(p, "size_x18", None)
                        or getattr(p, "sizeX18", None)
                        or getattr(p, "base_amount", None)
                        or getattr(p, "base_amount_x18", None)
                        or getattr(p, "baseAmount", None)
                        or getattr(p, "baseAmountX18", None)
                    )
                amount = self._from_x18_dynamic(amount_raw)
                if abs(amount) <= 0:
                    continue

                price_raw = (
                    getattr(p, "entry_price_x18", None)
                    or getattr(p, "entryPriceX18", None)
                    or getattr(p, "avg_entry_price_x18", None)
                    or getattr(p, "avgEntryPriceX18", None)
                    or getattr(p, "price_x18", None)
                    or getattr(p, "priceX18", None)
                    or getattr(p, "entry_price", None)
                    or getattr(p, "entryPrice", None)
                    or 0
                )
                price = self._from_x18_dynamic(price_raw)
                v_quote_val = self._from_x18_dynamic(v_quote_raw) if v_quote_raw is not None else None
                if (not price or price <= 0) and v_quote_val is not None:
                    if abs(amount) > 0:
                        price = abs(v_quote_val / amount)

                side_hint = self._normalize_side(
                    raw_side=getattr(p, "side", None) or (getattr(balance_obj, "side", None) if balance_obj is not None else None),
                    raw_is_long=getattr(p, "is_long", None) if getattr(p, "is_long", None) is not None else getattr(p, "isLong", None),
                    raw_direction=getattr(p, "direction", None),
                    signed_amount=amount,
                )
                abs_amount = abs(float(amount))
                signed_amount = abs_amount if side_hint == "LONG" else -abs_amount
                if amount != 0 and ((amount > 0 and side_hint == "SHORT") or (amount < 0 and side_hint == "LONG")):
                    logger.info(
                        "Position side override for product %s: amount_sign=%s side_hint=%s",
                        product_id,
                        "LONG" if amount > 0 else "SHORT",
                        side_hint,
                    )

                pos = {
                    "product_id": int(product_id),
                    "product_name": get_product_name(int(product_id)),
                    "amount": abs_amount,
                    "signed_amount": signed_amount,
                    "price": float(price),
                    "side": side_hint,
                }
                if v_quote_val is not None:
                    pos["v_quote_balance"] = float(v_quote_val)
                upnl = self._extract_unrealized_pnl_sdk(p, balance_obj)
                if upnl is not None:
                    pos["unrealized_pnl"] = upnl
                liq = self._extract_liquidation_price_sdk(p, balance_obj)
                if liq is not None:
                    pos["liquidation_price"] = liq
                positions.append(pos)
        return positions

    def _extract_positions_from_rest_payload(self, payload: dict) -> list:
        positions = []
        if not payload:
            return positions
        lists = []
        for key in ("perp_positions", "positions", "perp_balances", "perpPositions", "perpBalances"):
            val = payload.get(key)
            if isinstance(val, list) and val:
                lists.append(val)
        if not lists:
            nested = self._nested_position_container(payload)
            if isinstance(nested, dict) and nested is not payload:
                return self._extract_positions_from_rest_payload(nested)
            return positions

        for plist in lists:
            for p in plist:
                try:
                    product_id = int(
                        p.get("product_id")
                        or p.get("productId")
                        or p.get("pid")
                    )
                except Exception:
                    continue
                balance_dict = p.get("balance") if isinstance(p.get("balance"), dict) else None
                amount_raw = None
                v_quote_raw = None
                if balance_dict:
                    amount_raw = (
                        balance_dict.get("amount")
                        or balance_dict.get("amount_x18")
                        or balance_dict.get("amountX18")
                        or balance_dict.get("balance_amount")
                        or balance_dict.get("balanceAmount")
                        or balance_dict.get("balance_amount_x18")
                        or balance_dict.get("balanceAmountX18")
                        or balance_dict.get("size")
                        or balance_dict.get("size_x18")
                        or balance_dict.get("sizeX18")
                    )
                    v_quote_raw = (
                        balance_dict.get("v_quote_balance")
                        or balance_dict.get("vQuoteBalance")
                        or balance_dict.get("v_quote_balance_x18")
                        or balance_dict.get("vQuoteBalanceX18")
                    )
                if amount_raw is None:
                    amount_raw = (
                        p.get("amount")
                        or p.get("amount_x18")
                        or p.get("amountX18")
                        or p.get("balance_amount")
                        or p.get("balanceAmount")
                        or p.get("balance_amount_x18")
                        or p.get("balanceAmountX18")
                        or p.get("size")
                        or p.get("size_x18")
                        or p.get("sizeX18")
                        or p.get("base_amount")
                        or p.get("base_amount_x18")
                        or p.get("baseAmount")
                        or p.get("baseAmountX18")
                        or 0
                    )
                amount = self._from_x18_dynamic(amount_raw)
                if abs(amount) <= 0:
                    continue
                price_raw = (
                    p.get("entry_price_x18")
                    or p.get("entryPriceX18")
                    or p.get("avg_entry_price_x18")
                    or p.get("avgEntryPriceX18")
                    or p.get("price_x18")
                    or p.get("priceX18")
                    or p.get("entry_price")
                    or p.get("entryPrice")
                    or 0
                )
                price = self._from_x18_dynamic(price_raw)
                v_quote_val = self._from_x18_dynamic(v_quote_raw) if v_quote_raw is not None else None
                if (not price or price <= 0) and v_quote_val is not None:
                    if abs(amount) > 0:
                        price = abs(v_quote_val / amount)

                side_hint = self._normalize_side(
                    raw_side=p.get("side") or (balance_dict.get("side") if balance_dict else None),
                    raw_is_long=p.get("is_long") if p.get("is_long") is not None else p.get("isLong"),
                    raw_direction=p.get("direction"),
                    signed_amount=amount,
                )
                abs_amount = abs(float(amount))
                signed_amount = abs_amount if side_hint == "LONG" else -abs_amount
                if amount != 0 and ((amount > 0 and side_hint == "SHORT") or (amount < 0 and side_hint == "LONG")):
                    logger.info(
                        "REST position side override for product %s: amount_sign=%s side_hint=%s",
                        product_id,
                        "LONG" if amount > 0 else "SHORT",
                        side_hint,
                    )
                pos = {
                    "product_id": product_id,
                    "product_name": get_product_name(product_id),
                    "amount": abs_amount,
                    "signed_amount": signed_amount,
                    "price": float(price),
                    "side": side_hint,
                }
                if v_quote_val is not None:
                    pos["v_quote_balance"] = float(v_quote_val)
                upnl = self._extract_unrealized_pnl_rest(p, balance_dict)
                if upnl is not None:
                    pos["unrealized_pnl"] = upnl
                liq = self._extract_liquidation_price_rest(p, balance_dict)
                if liq is not None:
                    pos["liquidation_price"] = liq
                positions.append(pos)
        return positions

    def get_all_positions(self) -> list:
        # Prefer true perp positions from subaccount info.
        subaccount_info_succeeded = False
        if self._initialized and self.client:
            try:
                info = self.client.context.engine_client.get_subaccount_info(self.subaccount_hex)
                subaccount_info_succeeded = True
                sdk_positions = self._extract_positions_from_sdk_info(info)
                return sdk_positions
            except Exception as e:
                logger.warning(f"SDK get_all_positions via subaccount_info failed: {e}")

        try:
            data = self._query_rest("subaccount_info", {"subaccount": self.subaccount_hex}) or {}
            if data.get("status") == "success":
                subaccount_info_succeeded = True
                payload = data.get("data", {}) or {}
                if isinstance(payload, dict):
                    for nested_key in ("subaccount_info", "subaccountInfo", "account", "result"):
                        nested = payload.get(nested_key)
                        if isinstance(nested, dict) and nested:
                            payload = nested
                            break
                rest_positions = self._extract_positions_from_rest_payload(payload)
                return rest_positions
        except Exception as e:
            logger.warning(f"REST get_all_positions via subaccount_info failed: {e}")

        # If subaccount_info read succeeded and returned no perp positions, do not
        # fan out into per-product order scans.
        if subaccount_info_succeeded:
            return []

        # Fallback to open orders for backward compatibility.
        fallback_key = (self.network, str(self.subaccount_hex or ""))
        cached = _positions_fallback_cache.get(fallback_key)
        if cached and (time.time() - float(cached.get("ts", 0))) < _POSITIONS_FALLBACK_TTL:
            return list(cached.get("data") or [])

        positions = []
        products = list(get_perp_products(network=self.network, client=self) or [])
        max_products = max(1, _POSITIONS_FALLBACK_MAX_PRODUCTS)
        if len(products) > max_products:
            logger.warning(
                "get_all_positions fallback truncating product scan %s -> %s to reduce REST load",
                len(products),
                max_products,
            )
            products = products[:max_products]
        for name in products:
            pid = get_product_id(name, network=self.network, client=self)
            if pid is None:
                continue
            orders = self.get_open_orders(pid)
            positions.extend(orders)
        _positions_fallback_cache[fallback_key] = {"data": positions, "ts": time.time()}
        return positions

    def verify_linked_signer(self, expected_signer_address: str = None) -> dict:
        expected = (expected_signer_address or self.address or "").lower()
        result = {
            "verified": False,
            "current_signer": None,
            "expected_signer": expected,
            "error": None,
        }

        if self._initialized and self.client:
            try:
                ls_data = self.client.context.engine_client.get_linked_signer(self.subaccount_hex)
                current = getattr(ls_data, "signer", None) or getattr(ls_data, "linked_signer", None)
                if current and hasattr(current, "address"):
                    current = current.address
                current = self._normalize_signer_address(current)
                result["current_signer"] = current
                if current and expected:
                    result["verified"] = current.lower() == expected[:42].lower()
                logger.info("Linked signer check via SDK: current=%s expected=%s verified=%s",
                            current, expected, result["verified"])
                return result
            except Exception as e:
                logger.warning("SDK get_linked_signer failed: %s", e)

        try:
            data = self._query_rest("linked_signer", {"subaccount": self.subaccount_hex}) or {}
            if data.get("status") == "success":
                signer_raw = data.get("data", {})
                current = None
                if isinstance(signer_raw, dict):
                    current = signer_raw.get("linked_signer") or signer_raw.get("signer")
                elif isinstance(signer_raw, str):
                    current = signer_raw
                current = self._normalize_signer_address(current)
                result["current_signer"] = current
                if current and expected:
                    result["verified"] = current.lower() == expected[:42].lower()
                logger.info("Linked signer check via REST: current=%s expected=%s verified=%s",
                            current, expected, result["verified"])
            else:
                result["error"] = data.get("error", "Unknown error from exchange")
                logger.warning("REST linked_signer query returned non-success: %s", data)
        except Exception as e:
            result["error"] = str(e)
            logger.warning("REST get_linked_signer failed: %s", e)

        return result

    @staticmethod
    def _normalize_signer_address(raw) -> Optional[str]:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            hex_str = raw.hex()
            if len(hex_str) >= 40:
                addr_hex = hex_str[-40:]
            else:
                return None
            addr = "0x" + addr_hex
        elif isinstance(raw, str):
            addr = raw.strip().lower()
        else:
            return None
        if not addr.startswith("0x"):
            addr = "0x" + addr
        if len(addr) > 42:
            addr = "0x" + addr[-40:]
        if len(addr) != 42:
            return None
        if addr == "0x" + "0" * 40:
            return None
        return addr.lower()

    def _friendly_error(self, error_str: str) -> str:
        err_lower = error_str.lower()
        compact = err_lower.replace("_", "").replace("-", "")
        if "ipqueryonly" in compact:
            diag = ""
            try:
                signer_addr = self.address
                if signer_addr and self.subaccount_hex:
                    check = self.verify_linked_signer(signer_addr)
                    current = check.get("current_signer")
                    if check.get("error"):
                        diag = f"\n\n🔍 Diagnostic: Could not verify linked signer ({check['error']})"
                    elif not current:
                        diag = (
                            f"\n\n🔍 Diagnostic: NO linked signer found on the exchange for this subaccount. "
                            f"You must link the bot's 1CT key first.\n"
                            f"Bot's 1CT signer address: {signer_addr}"
                        )
                    elif check["verified"]:
                        diag = (
                            f"\n\n🔍 Diagnostic: Bot's 1CT signer IS linked correctly ({signer_addr[:10]}...). "
                            f"The rejection is likely an IP restriction on the exchange gateway. "
                            f"The bot server IP may be blocked for write operations."
                        )
                    else:
                        diag = (
                            f"\n\n🔍 Diagnostic: SIGNER MISMATCH!\n"
                            f"• Exchange has: {current[:10]}... linked\n"
                            f"• Bot's signer: {signer_addr[:10]}...\n"
                            f"Go to Nado Settings → 1-Click Trading → disable → "
                            f"then Advanced 1CT → paste the bot's key → enable and save."
                        )
            except Exception as de:
                logger.warning("Linked signer diagnostic failed: %s", de)

            return (
                "The exchange restricted this trade (ip_query_only). This usually means:\n"
                "1. Your 1CT signer key is not linked on Nado — go to Settings → 1-Click Trading on the Nado web app, paste your 1CT private key, enable the toggle, and save.\n"
                "2. Your subaccount may not be initialized — deposit at least $5 USDT0 at https://testnet.nado.xyz/portfolio/faucet\n"
                "3. If already linked and funded, the bot's server IP may be restricted by the exchange."
                + diag
            )
        if "insufficient" in err_lower or "margin" in err_lower:
            return "Insufficient margin. Please deposit more funds."
        if "product" in err_lower and "not found" in err_lower:
            return "This product is not currently available on the exchange."
        if "blocked" in err_lower:
            return "Order was blocked by the exchange. Your wallet may need funds deposited on-chain first."
        if "nonce" in err_lower:
            return "Order timing issue. Please try again."
        if "rate" in err_lower and "limit" in err_lower:
            return "Too many requests. Please wait a moment and try again."
        if "invalid order price" in err_lower and "price_increment_x18" in err_lower:
            return "Order price did not match exchange tick size. Price was auto-adjusted if possible; please retry."
        if "2122" in err_lower or "isolated-only" in err_lower or "marketisolatedonlymode" in compact:
            return (
                "This market is isolated-only. The bot now sends isolated orders, "
                "but this trade may still fail if isolated margin is too low."
            )
        return error_str

    @staticmethod
    def _extract_price_increment_from_error(error_str: str, product_id: int) -> Optional[float]:
        import re
        pattern = rf"price_increment_x18 for product {product_id}:\s*(\d+)"
        m = re.search(pattern, error_str)
        if not m:
            return None
        try:
            return int(m.group(1)) / 1e18
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_size_increment_from_error(error_str: str, product_id: int) -> Optional[float]:
        import re
        patterns = [
            rf"size_increment_x18 for product {product_id}:\s*(\d+)",
            r"size_increment_x18[^0-9]*(\d+)",
            r"size_increment[^0-9]*([0-9]*\.?[0-9]+(?:e-?\d+)?)",
        ]
        for pattern in patterns:
            m = re.search(pattern, error_str, flags=re.IGNORECASE)
            if not m:
                continue
            raw = m.group(1)
            try:
                if "x18" in pattern:
                    return int(raw) / 1e18
                return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_min_size_x18_from_error(error_str: str, product_id: int) -> Optional[int]:
        import re
        patterns = [
            rf"min_size for product {product_id}:\s*(\d+)",
            r"min_size[^0-9]*(\d+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, error_str, flags=re.IGNORECASE)
            if not m:
                continue
            try:
                return int(m.group(1))
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _is_size_increment_error(error_str: str) -> bool:
        lowered = (error_str or "").lower()
        return "invalid order amount" in lowered and "size_increment" in lowered

    @staticmethod
    def _is_min_notional_error(error_str: str) -> bool:
        lowered = (error_str or "").lower()
        return "invalid order size" in lowered and "min_size" in lowered

    @staticmethod
    def _align_price_to_increment(price: float, increment: float, is_buy: bool, order_type: str) -> float:
        if increment <= 0:
            return price
        d_price = Decimal(str(price))
        d_inc = Decimal(str(increment))
        ratio = d_price / d_inc
        if order_type == "ioc":
            rounding = ROUND_CEILING if is_buy else ROUND_FLOOR
        else:
            rounding = ROUND_HALF_UP
        ticks = ratio.to_integral_value(rounding=rounding)
        aligned = ticks * d_inc
        return float(aligned)

    @staticmethod
    def _align_size_to_increment(size: float, increment: float) -> float:
        if increment <= 0:
            return size
        d_size = Decimal(str(size))
        d_inc = Decimal(str(increment))
        ticks = (d_size / d_inc).to_integral_value(rounding=ROUND_FLOOR)
        aligned = ticks * d_inc
        return float(aligned)

    @staticmethod
    def _align_size_up_to_increment(size: float, increment: float) -> float:
        if increment <= 0:
            return size
        d_size = Decimal(str(size))
        d_inc = Decimal(str(increment))
        ticks = (d_size / d_inc).to_integral_value(rounding=ROUND_CEILING)
        aligned = ticks * d_inc
        return float(aligned)

    @staticmethod
    def _to_x18_int(value: float) -> int:
        return int((Decimal(str(value)) * Decimal("1000000000000000000")).to_integral_value(rounding=ROUND_HALF_UP))

    @staticmethod
    def _to_x6_int(value: float) -> int:
        return int((Decimal(str(value)) * Decimal("1000000")).to_integral_value(rounding=ROUND_HALF_UP))

    @staticmethod
    def _build_order_appendix(
        order_type_int: int,
        isolated: bool = False,
        reduce_only: bool = False,
        margin_x6: int = 0,
        builder_id: int = 0,
        builder_fee_rate: int = 0,
    ) -> int:
        # Bit layout:
        # value[64..127], builder[48..63], builder_fee_rate[38..47], reserved[14..37],
        # trigger[12..13], reduce_only[11], order_type[9..10], isolated[8], version[0..7]
        version = 1
        appendix = int(version & 0xFF)
        if isolated:
            appendix |= (1 << 8)
        appendix |= ((int(order_type_int) & 0x03) << 9)
        if reduce_only:
            appendix |= (1 << 11)
        appendix |= ((int(builder_fee_rate) & 0x3FF) << 38)
        appendix |= ((int(builder_id) & 0xFFFF) << 48)
        if isolated and margin_x6 > 0:
            appendix |= (int(margin_x6) << 64)
        return appendix

    @staticmethod
    def _align_x18_to_increment(value_x18: int, increment_x18: int) -> int:
        if increment_x18 <= 0:
            return value_x18
        sign = -1 if value_x18 < 0 else 1
        abs_v = abs(int(value_x18))
        aligned = (abs_v // int(increment_x18)) * int(increment_x18)
        return sign * aligned

    @staticmethod
    def _align_x18_up_to_increment(value_x18: int, increment_x18: int) -> int:
        if increment_x18 <= 0:
            return value_x18
        sign = -1 if value_x18 < 0 else 1
        abs_v = abs(int(value_x18))
        inc = int(increment_x18)
        aligned = ((abs_v + inc - 1) // inc) * inc
        return sign * aligned

    def _fallback_size_candidates(self, size: float) -> list[float]:
        # Conservative ladder from fine to coarse increments.
        # Includes 1/2/5 * 10^n steps to match common exchange lot sizes like 5e-5.
        increments = []
        for exp in range(-8, 1):
            base = 10 ** exp
            increments.extend([1 * base, 2 * base, 5 * base])
        candidates = []
        for inc in increments:
            aligned = self._align_size_to_increment(size, inc)
            if aligned > 0 and aligned not in candidates:
                candidates.append(aligned)
        return candidates

    def _warm_product_increment_cache(self, product_id: int) -> None:
        key = (self.network, product_id)
        if key in _size_increment_x18_cache and key in _price_increment_x18_cache:
            return
        try:
            data = self._query_rest("all_products") or {}
            if data.get("status") != "success":
                return
            perp_products = (data.get("data", {}) or {}).get("perp_products", []) or []
            for p in perp_products:
                pid = p.get("product_id")
                try:
                    pid = int(pid)
                except (TypeError, ValueError):
                    continue
                book_info = p.get("book_info", {}) or {}
                size_inc_x18 = p.get("size_increment_x18") or book_info.get("size_increment")
                price_inc_x18 = p.get("price_increment_x18") or book_info.get("price_increment_x18")
                min_size_x18 = p.get("min_size_x18") or book_info.get("min_size_x18") or p.get("min_size")
                if size_inc_x18 is not None:
                    try:
                        size_inc_x18_int = int(size_inc_x18)
                        _size_increment_x18_cache[(self.network, pid)] = size_inc_x18_int
                        _size_increment_cache[(self.network, pid)] = size_inc_x18_int / 1e18
                    except (TypeError, ValueError):
                        pass
                if price_inc_x18 is not None:
                    try:
                        price_inc_x18_int = int(price_inc_x18)
                        _price_increment_x18_cache[(self.network, pid)] = price_inc_x18_int
                        _price_increment_cache[(self.network, pid)] = price_inc_x18_int / 1e18
                    except (TypeError, ValueError):
                        pass
                if min_size_x18 is not None:
                    try:
                        _min_size_x18_cache[(self.network, pid)] = int(min_size_x18)
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            logger.debug("Could not warm product increment cache for %s: %s", product_id, e)

    def place_order(
        self,
        product_id: int,
        size: float,
        price: float,
        order_type: str = "default",
        is_buy: bool = True,
        isolated_only: bool = False,
        isolated_margin: Optional[float] = None,
        reduce_only: bool = False,
        _retry_count: int = 0,
    ) -> dict:
        if not self._initialized or not self.client:
            return {"success": False, "error": "Client not initialized. Please try /start again."}

        try:
            try:
                builder_id, builder_fee_rate = get_nado_builder_routing_config()
            except ValueError as cfg_err:
                logger.error("Builder routing misconfiguration; rejecting order: %s", cfg_err)
                return {"success": False, "error": f"Builder routing misconfigured: {cfg_err}"}

            self._warm_product_increment_cache(product_id)
            size_increment = _size_increment_cache.get((self.network, product_id))
            size_increment_x18 = _size_increment_x18_cache.get((self.network, product_id))
            min_size_x18 = _min_size_x18_cache.get((self.network, product_id))
            if size_increment and size_increment > 0:
                aligned_size = self._align_size_to_increment(size, size_increment)
                if aligned_size <= 0:
                    return {
                        "success": False,
                        "error": (
                            f"Order size {size} is below minimum increment for product {product_id} "
                            f"(size increment {size_increment})."
                        ),
                    }
                size = aligned_size

            price_increment = _price_increment_cache.get((self.network, product_id))
            price_increment_x18 = _price_increment_x18_cache.get((self.network, product_id))
            if price_increment and price_increment > 0:
                price = self._align_price_to_increment(price, price_increment, is_buy, order_type)

            from nado_protocol.engine_client.types.execute import PlaceOrderParams, OrderParams
            from nado_protocol.utils.expiration import get_expiration_timestamp
            from nado_protocol.utils.nonce import gen_order_nonce

            appendix_order_type_int = {
                "default": 0,
                "ioc": 1,
                "fok": 2,
                "post_only": 3,
            }.get(order_type, 0)

            amount = size if is_buy else -size
            expiration_secs = 10 if order_type == "ioc" else 3600

            amount_x18 = self._to_x18_int(amount)
            if size_increment_x18 and size_increment_x18 > 0:
                amount_x18 = self._align_x18_to_increment(amount_x18, int(size_increment_x18))
                if amount_x18 == 0:
                    return {
                        "success": False,
                        "error": (
                            f"Order size {size} is below minimum increment for product {product_id} "
                            f"(size increment {size_increment})."
                        ),
                    }

            price_x18 = self._to_x18_int(price)
            if price_increment_x18 and price_increment_x18 > 0:
                price_x18 = self._align_x18_to_increment(price_x18, int(price_increment_x18))

            # Proactively enforce exchange minimum notional before submission.
            if min_size_x18 and min_size_x18 > 0 and price_x18 > 0 and amount_x18 != 0:
                min_notional_x36 = int(min_size_x18) * (10 ** 18)
                current_notional_x36 = abs(int(amount_x18)) * int(price_x18)
                if current_notional_x36 < min_notional_x36:
                    required_abs_amount_x18 = (min_notional_x36 + int(price_x18) - 1) // int(price_x18)
                    bumped_amount_x18 = required_abs_amount_x18 if amount_x18 > 0 else -required_abs_amount_x18
                    if size_increment_x18 and size_increment_x18 > 0:
                        bumped_amount_x18 = self._align_x18_up_to_increment(
                            bumped_amount_x18,
                            int(size_increment_x18),
                        )
                    if abs(int(bumped_amount_x18)) > abs(int(amount_x18)):
                        logger.info(
                            "Bumping order amount for min notional product_id=%s amount_x18=%s->%s min_size_x18=%s",
                            product_id,
                            amount_x18,
                            bumped_amount_x18,
                            min_size_x18,
                        )
                        amount_x18 = int(bumped_amount_x18)
                        size = abs(float(amount_x18) / 1e18)

            isolated_margin_x6 = 0
            if isolated_only:
                if isolated_margin is None:
                    isolated_margin = abs(float(size) * float(price))
                isolated_margin_x6 = max(0, self._to_x6_int(float(isolated_margin)))

            order = OrderParams(
                sender=self.subaccount_hex,
                priceX18=price_x18,
                amount=amount_x18,
                expiration=get_expiration_timestamp(expiration_secs),
                nonce=gen_order_nonce(),
                appendix=self._build_order_appendix(
                    appendix_order_type_int,
                    isolated=bool(isolated_only),
                    reduce_only=bool(reduce_only),
                    margin_x6=isolated_margin_x6,
                    builder_id=builder_id,
                    builder_fee_rate=builder_fee_rate,
                ),
            )

            params = PlaceOrderParams(product_id=product_id, order=order)
            result = self.client.market.place_order(params)

            if hasattr(result, 'data') and result.data:
                if hasattr(result.data, 'digest') and result.data.digest:
                    return {
                        "success": True,
                        "digest": result.data.digest,
                        "product_id": product_id,
                        "size": size,
                        "price": price,
                        "side": "LONG" if is_buy else "SHORT",
                    }

            result_str = str(result)
            lowered_result = result_str.lower()
            if (
                (hasattr(result, "status") and str(getattr(result, "status")).lower() == "failure")
                or ('"status":"failure"' in lowered_result)
                or ("'status': 'failure'" in lowered_result)
            ):
                return {"success": False, "error": self._friendly_error(result_str)}
            if "blocked" in result_str.lower() or "reason" in result_str.lower():
                return {"success": False, "error": self._friendly_error(result_str)}

            return {
                "success": False,
                "error": self._friendly_error(
                    "Exchange did not confirm order acceptance (missing digest/status)."
                ),
            }
        except Exception as e:
            err_str = str(e)
            increment = self._extract_price_increment_from_error(err_str, product_id)
            size_increment = self._extract_size_increment_from_error(err_str, product_id)
            min_size_x18 = self._extract_min_size_x18_from_error(err_str, product_id)

            if increment and increment > 0:
                _price_increment_cache[(self.network, product_id)] = increment
            if size_increment and size_increment > 0:
                _size_increment_cache[(self.network, product_id)] = size_increment
            if min_size_x18 and min_size_x18 > 0:
                _min_size_x18_cache[(self.network, product_id)] = int(min_size_x18)

            # Retry with normalized size/price when exchange returns increment errors.
            if _retry_count < 3 and ((increment and increment > 0) or (size_increment and size_increment > 0)):
                retry_size = size
                retry_price = price
                if size_increment and size_increment > 0:
                    retry_size = self._align_size_to_increment(size, size_increment)
                if increment and increment > 0:
                    retry_price = self._align_price_to_increment(price, increment, is_buy, order_type)

                if retry_size > 0 and (abs(retry_size - size) > 1e-12 or abs(retry_price - price) > 1e-12):
                    logger.info(
                        "Retrying place_order with aligned increments product_id=%s size=%s->%s price=%s->%s",
                        product_id,
                        size,
                        retry_size,
                        price,
                        retry_price,
                    )
                    try:
                        return self.place_order(
                            product_id=product_id,
                            size=retry_size,
                            price=retry_price,
                            order_type=order_type,
                            is_buy=is_buy,
                            isolated_only=isolated_only,
                            isolated_margin=isolated_margin,
                            reduce_only=reduce_only,
                            _retry_count=_retry_count + 1,
                        )
                    except Exception as retry_e:
                        logger.error(f"place_order retry failed: {retry_e}")
                        return {"success": False, "error": self._friendly_error(str(retry_e))}

            # Retry with bumped size when exchange enforces min notional.
            if _retry_count < 3 and self._is_min_notional_error(err_str):
                required_min_x18 = _min_size_x18_cache.get((self.network, product_id)) or min_size_x18
                if required_min_x18 and required_min_x18 > 0 and price > 0:
                    required_notional = float(required_min_x18) / 1e18
                    # Add tiny safety buffer so downstream tick/rounding does not dip below threshold.
                    target_size = (required_notional / float(price)) * 1.01
                    if size_increment and size_increment > 0:
                        target_size = self._align_size_up_to_increment(target_size, size_increment)
                    retry_size = max(float(size), float(target_size))
                    if retry_size > size:
                        logger.info(
                            "Retrying place_order with min-notional bump product_id=%s size=%s->%s min_notional=%s",
                            product_id,
                            size,
                            retry_size,
                            required_notional,
                        )
                        try:
                            return self.place_order(
                                product_id=product_id,
                                size=retry_size,
                                price=price,
                                order_type=order_type,
                                is_buy=is_buy,
                                isolated_only=isolated_only,
                                isolated_margin=isolated_margin,
                                reduce_only=reduce_only,
                                _retry_count=_retry_count + 1,
                            )
                        except Exception as retry_e:
                            logger.error(f"place_order min-notional retry failed: {retry_e}")
                            return {"success": False, "error": self._friendly_error(str(retry_e))}

            # Some exchange error payloads truncate size_increment value. In that case
            # probe a small ladder of common increments to find a valid divisible amount.
            if _retry_count < 6 and self._is_size_increment_error(err_str):
                for candidate_size in self._fallback_size_candidates(size):
                    if abs(candidate_size - size) <= 1e-12:
                        continue
                    logger.info(
                        "Retrying place_order with fallback size alignment product_id=%s size=%s->%s",
                        product_id,
                        size,
                        candidate_size,
                    )
                    try:
                        retry_result = self.place_order(
                            product_id=product_id,
                            size=candidate_size,
                            price=price,
                            order_type=order_type,
                            is_buy=is_buy,
                            isolated_only=isolated_only,
                            isolated_margin=isolated_margin,
                            reduce_only=reduce_only,
                            _retry_count=_retry_count + 1,
                        )
                        if retry_result.get("success"):
                            return retry_result
                    except Exception as retry_e:
                        logger.error(f"place_order fallback retry failed: {retry_e}")
                    # Keep trying broader candidate increments until one succeeds.
                    continue

            if increment and increment > 0:
                aligned_price = self._align_price_to_increment(price, increment, is_buy, order_type)
                if aligned_price > 0 and abs(aligned_price - price) > 1e-12:
                    logger.info(
                        "Retrying place_order with aligned tick price product_id=%s original=%s aligned=%s increment=%s",
                        product_id,
                        price,
                        aligned_price,
                        increment,
                    )
                    try:
                        return self.place_order(
                            product_id=product_id,
                            size=size,
                            price=aligned_price,
                            order_type=order_type,
                            is_buy=is_buy,
                            isolated_only=isolated_only,
                            isolated_margin=isolated_margin,
                            reduce_only=reduce_only,
                            _retry_count=_retry_count + 1,
                        )
                    except Exception as retry_e:
                        logger.error(f"place_order retry failed: {retry_e}")
                        return {"success": False, "error": self._friendly_error(str(retry_e))}

            logger.error(f"place_order failed: {e}")
            return {"success": False, "error": self._friendly_error(err_str)}

    def place_market_order(
        self,
        product_id: int,
        size: float,
        is_buy: bool = True,
        slippage_pct: float = 1.0,
        isolated_only: bool = False,
        isolated_margin: Optional[float] = None,
        reduce_only: bool = False,
    ) -> dict:
        mp = self.get_market_price(product_id)
        if mp["mid"] == 0:
            return {"success": False, "error": "Could not fetch market price"}
        try:
            slippage_pct = float(slippage_pct)
        except (TypeError, ValueError):
            slippage_pct = 1.0
        slippage_pct = max(0.1, min(slippage_pct, 10.0))
        multiplier = 1.0 + (slippage_pct / 100.0)
        price = mp["ask"] * multiplier if is_buy else mp["bid"] / multiplier
        return self.place_order(
            product_id,
            size,
            price,
            order_type="ioc",
            is_buy=is_buy,
            isolated_only=isolated_only,
            isolated_margin=isolated_margin,
            reduce_only=reduce_only,
        )

    def place_limit_order(
        self,
        product_id: int,
        size: float,
        price: float,
        is_buy: bool = True,
        isolated_only: bool = False,
        isolated_margin: Optional[float] = None,
        reduce_only: bool = False,
    ) -> dict:
        return self.place_order(
            product_id,
            size,
            price,
            order_type="default",
            is_buy=is_buy,
            isolated_only=isolated_only,
            isolated_margin=isolated_margin,
            reduce_only=reduce_only,
        )

    def cancel_order(self, product_id: int, digest: str) -> dict:
        if not self._initialized or not self.client:
            return {"success": False, "error": "Client not initialized"}

        try:
            from nado_protocol.engine_client.types.execute import CancelOrdersParams

            cancel_params = CancelOrdersParams(
                sender=self.subaccount_hex,
                productIds=[product_id],
                digests=[digest],
            )
            self.client.market.cancel_orders(cancel_params)
            return {"success": True, "digest": digest}
        except Exception as e:
            logger.error(f"cancel_order failed: {e}")
            return {"success": False, "error": str(e)}

    def cancel_all_orders(self, product_id: int) -> dict:
        orders = self.get_open_orders(product_id)
        results = []
        for o in orders:
            r = self.cancel_order(product_id, o["digest"])
            results.append(r)
        return {"success": True, "cancelled": len([r for r in results if r["success"]])}

    def get_all_funding_rates(self) -> dict:
        cache_key = f"{self.network}:funding"
        cached = _FUNDING_CACHE.get(cache_key)
        if cached and (time.time() - cached["ts"] < _FUNDING_TTL):
            return cached["data"]
        try:
            data = self._query_rest("all_products") or {}
            if data.get("status") == "success":
                rates = {}
                for prod in data["data"].get("perp_products", []):
                    pid = prod.get("product_id")
                    funding = int(prod.get("cum_funding_x18", 0)) / 1e18
                    rates[pid] = {"product_id": pid, "funding_rate": funding}
                _FUNDING_CACHE[cache_key] = {"data": rates, "ts": time.time()}
                return rates
        except Exception as e:
            logger.error(f"get_all_funding_rates failed: {e}")
        return {}

    def get_funding_rate(self, product_id: int) -> Optional[dict]:
        rates = self.get_all_funding_rates()
        return rates.get(product_id)

    def get_product_market_stats(self, product_id: int) -> dict:
        """
        Best-effort market stats for a product from Nado gateway payloads.

        Fields are optional because API payload keys can differ by environment.
        """
        stats = {
            "product_id": int(product_id),
            "product_name": get_product_name(int(product_id), network=self.network, client=self),
            "bid": 0.0,
            "ask": 0.0,
            "mid": 0.0,
            "spread": 0.0,
            "spread_bps": 0.0,
            "funding_rate": None,
            "volume_24h_usd": None,
            "open_interest": None,
            "change_24h_pct": None,
            "high_24h": None,
            "low_24h": None,
            "mark_price": None,
            "index_price": None,
        }

        try:
            px = self.get_market_price(int(product_id)) or {}
            bid = float(px.get("bid") or 0)
            ask = float(px.get("ask") or 0)
            mid = float(px.get("mid") or 0)
            spread = (ask - bid) if (ask > 0 and bid > 0) else 0.0
            spread_bps = (spread / mid * 10000.0) if mid > 0 else 0.0
            stats.update(
                {
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread": spread,
                    "spread_bps": spread_bps,
                }
            )
        except Exception:
            pass

        try:
            fr = self.get_funding_rate(int(product_id)) or {}
            if isinstance(fr, dict) and fr.get("funding_rate") is not None:
                stats["funding_rate"] = float(fr.get("funding_rate") or 0)
        except Exception:
            pass

        try:
            data = self._query_rest("all_products") or {}
            if data.get("status") != "success":
                return stats
            rows = ((data.get("data") or {}).get("perp_products") or [])
            if not isinstance(rows, list):
                return stats
            row = None
            for candidate in rows:
                try:
                    if int(candidate.get("product_id")) == int(product_id):
                        row = candidate
                        break
                except Exception:
                    continue
            if not isinstance(row, dict):
                return stats

            book = row.get("book_info") or {}

            def _pick(*keys):
                for key in keys:
                    if key in row and row.get(key) is not None:
                        return row.get(key)
                    if key in book and book.get(key) is not None:
                        return book.get(key)
                return None

            volume_raw = _pick(
                "volume_24h",
                "quote_volume_24h",
                "notional_volume_24h",
                "turnover_24h",
                "total_volume_24h",
                "volume24h",
            )
            oi_raw = _pick("open_interest", "open_interest_x18", "oi", "oi_x18")
            change_raw = _pick(
                "change_24h_pct",
                "price_change_24h_pct",
                "percent_change_24h",
                "change24h",
            )
            high_raw = _pick("high_24h", "price_high_24h")
            low_raw = _pick("low_24h", "price_low_24h")
            mark_raw = _pick("mark_price", "mark_price_x18")
            index_raw = _pick("index_price", "index_price_x18", "oracle_price", "oracle_price_x18")

            if volume_raw is not None:
                stats["volume_24h_usd"] = float(self._from_x18_dynamic(volume_raw))
            if oi_raw is not None:
                stats["open_interest"] = float(self._from_x18_dynamic(oi_raw))
            if change_raw is not None:
                stats["change_24h_pct"] = float(self._from_x18_dynamic(change_raw))
            if high_raw is not None:
                stats["high_24h"] = float(self._from_x18_dynamic(high_raw))
            if low_raw is not None:
                stats["low_24h"] = float(self._from_x18_dynamic(low_raw))
            if mark_raw is not None:
                stats["mark_price"] = float(self._from_x18_dynamic(mark_raw))
            if index_raw is not None:
                stats["index_price"] = float(self._from_x18_dynamic(index_raw))
        except Exception as e:
            logger.debug("get_product_market_stats all_products lookup failed for %s: %s", product_id, e)

        return stats

    def get_all_products_info(self) -> dict:
        cache_key = f"{self.network}:products"
        cached = _ALL_PRODUCTS_CACHE.get(cache_key)
        if cached and (time.time() - cached["ts"] < _ALL_PRODUCTS_TTL):
            return cached["data"]
        try:
            if self._initialized and self.client:
                products = self.client.context.engine_client.get_all_products()
                data = {
                    "perp": [{"id": p.product_id} for p in products.perp_products],
                    "spot": [{"id": p.product_id} for p in products.spot_products],
                }
                _ALL_PRODUCTS_CACHE[cache_key] = {"data": data, "ts": time.time()}
                return data
        except Exception as e:
            logger.error(f"get_all_products_info failed: {e}")
        return {"perp": [], "spot": []}


_client_cache: dict[str, NadoClient] = {}


def _cache_key_for(address: str, network: str) -> str:
    return f"{address.lower()}_{network}"


def get_nado_client(private_key: str, network: str = "testnet", main_address: str = None) -> NadoClient:
    client = NadoClient(private_key, network, main_address=main_address)
    # Derive the signer address before initialization for cache key
    signer_address = client.address
    if signer_address:
        cache_key = _cache_key_for(signer_address, network)
        cached = _client_cache.get(cache_key)
        if cached:
            if main_address and cached.main_address != main_address:
                cached.main_address = main_address
                cached.subaccount_hex = cached._compute_subaccount_hex(main_address)
            return cached
    client.initialize()
    if signer_address:
        _client_cache[cache_key] = client
    return client


def clear_client_cache(address: str = None, network: str = None):
    if address and network:
        cache_key = _cache_key_for(address, network)
        _client_cache.pop(cache_key, None)
    else:
        _client_cache.clear()
