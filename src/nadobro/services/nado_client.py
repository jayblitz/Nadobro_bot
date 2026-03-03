import logging
import time
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Optional
from src.nadobro.config import (
    NADO_TESTNET_REST, NADO_MAINNET_REST,
    NADO_TESTNET_ARCHIVE, NADO_MAINNET_ARCHIVE,
    PRODUCTS, get_product_name
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
_REQUEST_TIMEOUT_SECONDS = float(os.environ.get("NADO_HTTP_TIMEOUT_SECONDS", "6"))
_FANOUT_WORKERS = int(os.environ.get("NADO_FANOUT_WORKERS", "8"))
_rest_session = requests.Session()


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

    def _query_rest(self, query_type: str, extra_params: dict | None = None) -> Optional[dict]:
        params = {"type": query_type}
        if extra_params:
            params.update(extra_params)
        try:
            url = f"{self._rest_url()}/query"
            headers = {"Accept-Encoding": "gzip"}
            resp = _rest_session.get(url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
            return resp.json()
        except Exception as e:
            logger.error("REST query failed type=%s: %s", query_type, e)
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
        perp_products = [(name, info["id"]) for name, info in PRODUCTS.items() if info["type"] == "perp"]
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

    def get_open_orders(self, product_id: int) -> list:
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
                return orders
            except Exception as e:
                logger.error(f"SDK get_open_orders failed: {e}")
        return []

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

    def _extract_positions_from_sdk_info(self, info) -> list:
        positions = []
        if not info:
            return positions
        candidate_lists = []
        for attr in ("perp_positions", "positions", "perp_balances"):
            val = getattr(info, attr, None)
            if val:
                candidate_lists.append(val)
        if not candidate_lists:
            return positions

        for plist in candidate_lists:
            for p in plist:
                product_id = getattr(p, "product_id", None)
                if product_id is None:
                    continue
                balance_obj = getattr(p, "balance", None)
                amount_raw = None
                v_quote_raw = None
                if balance_obj is not None:
                    amount_raw = getattr(balance_obj, "amount", None)
                    v_quote_raw = getattr(balance_obj, "v_quote_balance", None)
                if amount_raw is None:
                    amount_raw = (
                        getattr(p, "amount", None)
                        or getattr(p, "amount_x18", None)
                        or getattr(p, "size", None)
                        or getattr(p, "size_x18", None)
                    )
                amount = self._from_x18_dynamic(amount_raw)
                if abs(amount) <= 0:
                    continue

                price_raw = (
                    getattr(p, "entry_price_x18", None)
                    or getattr(p, "avg_entry_price_x18", None)
                    or getattr(p, "price_x18", None)
                    or getattr(p, "entry_price", None)
                    or 0
                )
                price = self._from_x18_dynamic(price_raw)
                v_quote_val = self._from_x18_dynamic(v_quote_raw) if v_quote_raw is not None else None
                if (not price or price <= 0) and v_quote_val is not None:
                    if abs(amount) > 0:
                        price = abs(v_quote_val / amount)

                side_hint = self._normalize_side(
                    raw_side=getattr(p, "side", None) or (getattr(balance_obj, "side", None) if balance_obj is not None else None),
                    raw_is_long=getattr(p, "is_long", None),
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
                positions.append(pos)
        return positions

    def _extract_positions_from_rest_payload(self, payload: dict) -> list:
        positions = []
        if not payload:
            return positions
        lists = []
        for key in ("perp_positions", "positions", "perp_balances"):
            val = payload.get(key)
            if isinstance(val, list) and val:
                lists.append(val)
        if not lists:
            return positions

        for plist in lists:
            for p in plist:
                try:
                    product_id = int(p.get("product_id"))
                except Exception:
                    continue
                balance_dict = p.get("balance") if isinstance(p.get("balance"), dict) else None
                amount_raw = None
                v_quote_raw = None
                if balance_dict:
                    amount_raw = balance_dict.get("amount")
                    v_quote_raw = balance_dict.get("v_quote_balance")
                if amount_raw is None:
                    amount_raw = (
                        p.get("amount")
                        or p.get("amount_x18")
                        or p.get("size")
                        or p.get("size_x18")
                        or 0
                    )
                amount = self._from_x18_dynamic(amount_raw)
                if abs(amount) <= 0:
                    continue
                price_raw = (
                    p.get("entry_price_x18")
                    or p.get("avg_entry_price_x18")
                    or p.get("price_x18")
                    or p.get("entry_price")
                    or 0
                )
                price = self._from_x18_dynamic(price_raw)
                v_quote_val = self._from_x18_dynamic(v_quote_raw) if v_quote_raw is not None else None
                if (not price or price <= 0) and v_quote_val is not None:
                    if abs(amount) > 0:
                        price = abs(v_quote_val / amount)

                side_hint = self._normalize_side(
                    raw_side=p.get("side") or (balance_dict.get("side") if balance_dict else None),
                    raw_is_long=p.get("is_long"),
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
                positions.append(pos)
        return positions

    def get_all_positions(self) -> list:
        # Prefer true perp positions from subaccount info.
        if self._initialized and self.client:
            try:
                info = self.client.context.engine_client.get_subaccount_info(self.subaccount_hex)
                sdk_positions = self._extract_positions_from_sdk_info(info)
                if sdk_positions:
                    return sdk_positions
            except Exception as e:
                logger.warning(f"SDK get_all_positions via subaccount_info failed: {e}")

        try:
            data = self._query_rest("subaccount_info", {"subaccount": self.subaccount_hex}) or {}
            if data.get("status") == "success":
                payload = data.get("data", {}) or {}
                rest_positions = self._extract_positions_from_rest_payload(payload)
                if rest_positions:
                    return rest_positions
        except Exception as e:
            logger.warning(f"REST get_all_positions via subaccount_info failed: {e}")

        # Fallback to open orders for backward compatibility.
        positions = []
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                orders = self.get_open_orders(info["id"])
                positions.extend(orders)
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
    def _is_size_increment_error(error_str: str) -> bool:
        lowered = (error_str or "").lower()
        return "invalid order amount" in lowered and "size_increment" in lowered

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
    def _to_x18_int(value: float) -> int:
        return int((Decimal(str(value)) * Decimal("1000000000000000000")).to_integral_value(rounding=ROUND_HALF_UP))

    @staticmethod
    def _align_x18_to_increment(value_x18: int, increment_x18: int) -> int:
        if increment_x18 <= 0:
            return value_x18
        sign = -1 if value_x18 < 0 else 1
        abs_v = abs(int(value_x18))
        aligned = (abs_v // int(increment_x18)) * int(increment_x18)
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
        except Exception as e:
            logger.debug("Could not warm product increment cache for %s: %s", product_id, e)

    def place_order(
        self,
        product_id: int,
        size: float,
        price: float,
        order_type: str = "default",
        is_buy: bool = True,
        _retry_count: int = 0,
    ) -> dict:
        if not self._initialized or not self.client:
            return {"success": False, "error": "Client not initialized. Please try /start again."}

        try:
            self._warm_product_increment_cache(product_id)
            size_increment = _size_increment_cache.get((self.network, product_id))
            size_increment_x18 = _size_increment_x18_cache.get((self.network, product_id))
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
            from nado_protocol.utils.math import to_x18
            from nado_protocol.utils.order import build_appendix
            from nado_protocol.utils.expiration import OrderType, get_expiration_timestamp
            from nado_protocol.utils.nonce import gen_order_nonce

            ot_map = {
                "default": OrderType.DEFAULT,
                "ioc": OrderType.IOC,
                "fok": OrderType.FOK,
                "post_only": OrderType.POST_ONLY,
            }
            ot = ot_map.get(order_type, OrderType.DEFAULT)

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

            order = OrderParams(
                sender=self.subaccount_hex,
                priceX18=price_x18,
                amount=amount_x18,
                expiration=get_expiration_timestamp(expiration_secs),
                nonce=gen_order_nonce(),
                appendix=build_appendix(ot),
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
            if "blocked" in result_str.lower() or "reason" in result_str.lower():
                return {"success": False, "error": self._friendly_error(result_str)}

            return {
                "success": True,
                "digest": "unknown",
                "product_id": product_id,
                "size": size,
                "price": price,
                "side": "LONG" if is_buy else "SHORT",
            }
        except Exception as e:
            err_str = str(e)
            increment = self._extract_price_increment_from_error(err_str, product_id)
            size_increment = self._extract_size_increment_from_error(err_str, product_id)

            if increment and increment > 0:
                _price_increment_cache[(self.network, product_id)] = increment
            if size_increment and size_increment > 0:
                _size_increment_cache[(self.network, product_id)] = size_increment

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
                            _retry_count=_retry_count + 1,
                        )
                    except Exception as retry_e:
                        logger.error(f"place_order retry failed: {retry_e}")
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
                            _retry_count=_retry_count + 1,
                        )
                    except Exception as retry_e:
                        logger.error(f"place_order retry failed: {retry_e}")
                        return {"success": False, "error": self._friendly_error(str(retry_e))}

            logger.error(f"place_order failed: {e}")
            return {"success": False, "error": self._friendly_error(err_str)}

    def place_market_order(self, product_id: int, size: float, is_buy: bool = True, slippage_pct: float = 1.0) -> dict:
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
        return self.place_order(product_id, size, price, order_type="ioc", is_buy=is_buy)

    def place_limit_order(self, product_id: int, size: float, price: float, is_buy: bool = True) -> dict:
        return self.place_order(product_id, size, price, order_type="default", is_buy=is_buy)

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


def _cache_key_for(private_key: str, network: str) -> str:
    import hashlib
    key_hash = hashlib.sha256(private_key.encode()).hexdigest()[:16]
    return f"{key_hash}_{network}"


def get_nado_client(private_key: str, network: str = "testnet", main_address: str = None) -> NadoClient:
    cache_key = _cache_key_for(private_key, network)
    cached = _client_cache.get(cache_key)
    if cached:
        if main_address and cached.main_address != main_address:
            cached.main_address = main_address
            cached.subaccount_hex = cached._compute_subaccount_hex(main_address)
        return cached
    client = NadoClient(private_key, network, main_address=main_address)
    client.initialize()
    _client_cache[cache_key] = client
    return client


def clear_client_cache(private_key: str = None, network: str = None):
    if private_key and network:
        cache_key = _cache_key_for(private_key, network)
        _client_cache.pop(cache_key, None)
    else:
        _client_cache.clear()
