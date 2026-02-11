import logging
import time
import requests
from typing import Optional
from src.nadobro.config import (
    NADO_TESTNET_REST, NADO_MAINNET_REST,
    NADO_TESTNET_ARCHIVE, NADO_MAINNET_ARCHIVE,
    PRODUCTS, get_product_name
)

logger = logging.getLogger(__name__)


class NadoClient:
    def __init__(self, private_key: str, network: str = "testnet"):
        self.private_key = private_key
        self.network = network
        self.client = None
        self.subaccount_hex = None
        self.address = None
        self._initialized = False
        self._derive_address()

    def _derive_address(self):
        try:
            from eth_account import Account
            acct = Account.from_key(self.private_key)
            self.address = acct.address
            try:
                from nado_protocol.utils.bytes32 import subaccount_to_hex
                self.subaccount_hex = subaccount_to_hex(self.address, "default")
            except ImportError:
                default_bytes = "default".encode().hex()
                self.subaccount_hex = self.address.lower() + default_bytes + "0" * (24 - len(default_bytes))
        except Exception as e:
            logger.error(f"Failed to derive address from private key: {e}")

    def initialize(self):
        if self._initialized:
            return True
        try:
            from nado_protocol.client import create_nado_client, NadoClientMode
            from nado_protocol.utils.bytes32 import subaccount_to_hex

            mode = NadoClientMode.TESTNET if self.network == "testnet" else NadoClientMode.MAINNET
            self.client = create_nado_client(mode, self.private_key)
            self.address = self.client.context.signer.address
            self.subaccount_hex = subaccount_to_hex(self.address, "default")
            self._initialized = True
            logger.info(f"Nado client initialized for {self.address} on {self.network}")
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

    def get_market_price(self, product_id: int) -> dict:
        if self._initialized and self.client:
            try:
                from nado_protocol.utils.math import from_x18
                mp = self.client.context.engine_client.get_market_price(product_id)
                bid = from_x18(int(mp.bid_x18))
                ask = from_x18(int(mp.ask_x18)) if hasattr(mp, 'ask_x18') else bid
                return {"bid": float(bid), "ask": float(ask), "mid": float((bid + ask) / 2)}
            except Exception as e:
                logger.error(f"SDK get_market_price failed: {e}")

        try:
            url = f"{self._rest_url()}/query"
            params = {"type": "market_price", "product_id": product_id}
            headers = {"Accept-Encoding": "gzip"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
            if data.get("status") == "success":
                bid = int(data["data"]["bid_x18"]) / 1e18
                ask = int(data["data"].get("ask_x18", data["data"]["bid_x18"])) / 1e18
                return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}
        except Exception as e:
            logger.error(f"REST get_market_price failed: {e}")

        return {"bid": 0, "ask": 0, "mid": 0}

    def get_all_market_prices(self) -> dict:
        prices = {}
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                try:
                    p = self.get_market_price(info["id"])
                    prices[name] = p
                except Exception:
                    pass
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
            url = f"{self._rest_url()}/query"
            params = {"type": "subaccount_info", "subaccount": self.subaccount_hex}
            headers = {"Accept-Encoding": "gzip"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
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

    def get_all_positions(self) -> list:
        positions = []
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                orders = self.get_open_orders(info["id"])
                positions.extend(orders)
        return positions

    @staticmethod
    def _friendly_error(error_str: str) -> str:
        err_lower = error_str.lower()
        compact = err_lower.replace("_", "").replace("-", "")
        if "ipqueryonly" in compact:
            return "Your wallet needs funds deposited on Nado DEX before trading. Please deposit USDT0 at https://testnet.nado.xyz/portfolio/faucet"
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
        return error_str

    def place_order(self, product_id: int, size: float, price: float, order_type: str = "default", is_buy: bool = True) -> dict:
        if not self._initialized or not self.client:
            return {"success": False, "error": "Client not initialized. Please try /start again."}

        try:
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

            order = OrderParams(
                sender=self.subaccount_hex,
                priceX18=to_x18(price),
                amount=to_x18(amount),
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
            logger.error(f"place_order failed: {e}")
            return {"success": False, "error": self._friendly_error(str(e))}

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
        try:
            url = f"{self._rest_url()}/query"
            params = {"type": "all_products"}
            headers = {"Accept-Encoding": "gzip"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
            if data.get("status") == "success":
                rates = {}
                for prod in data["data"].get("perp_products", []):
                    pid = prod.get("product_id")
                    funding = int(prod.get("cum_funding_x18", 0)) / 1e18
                    rates[pid] = {"product_id": pid, "funding_rate": funding}
                return rates
        except Exception as e:
            logger.error(f"get_all_funding_rates failed: {e}")
        return {}

    def get_funding_rate(self, product_id: int) -> Optional[dict]:
        rates = self.get_all_funding_rates()
        return rates.get(product_id)

    def get_all_products_info(self) -> dict:
        try:
            if self._initialized and self.client:
                products = self.client.context.engine_client.get_all_products()
                return {
                    "perp": [{"id": p.product_id} for p in products.perp_products],
                    "spot": [{"id": p.product_id} for p in products.spot_products],
                }
        except Exception as e:
            logger.error(f"get_all_products_info failed: {e}")
        return {"perp": [], "spot": []}


_client_cache: dict[str, NadoClient] = {}


def get_nado_client(private_key: str, network: str = "testnet") -> NadoClient:
    cache_key = f"{private_key[:10]}_{network}"
    if cache_key not in _client_cache:
        client = NadoClient(private_key, network)
        client.initialize()
        _client_cache[cache_key] = client
    return _client_cache[cache_key]


def clear_client_cache(private_key: str = None, network: str = None):
    if private_key and network:
        cache_key = f"{private_key[:10]}_{network}"
        _client_cache.pop(cache_key, None)
    else:
        _client_cache.clear()
