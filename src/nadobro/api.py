import logging
from flask import Flask, request, jsonify, render_template, send_from_directory
from functools import wraps
from src.nadobro.webapp_auth import get_telegram_user_from_init_data
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info,
    switch_network, get_user,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.config import PRODUCTS, get_product_name

logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder="../../templates",
    static_folder="../../static",
)
app.config["JSON_SORT_KEYS"] = False


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        init_data = request.headers.get("X-Telegram-Init-Data", "")
        if not init_data:
            init_data = request.args.get("initData", "")

        user_data = get_telegram_user_from_init_data(init_data)
        if not user_data:
            return jsonify({"error": "Unauthorized"}), 401

        request.telegram_user = user_data
        request.telegram_id = user_data.get("id")
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory(app.static_folder, filename)


@app.route("/api/user", methods=["GET"])
@require_auth
def api_user():
    telegram_id = request.telegram_id
    username = request.telegram_user.get("username", "")

    user, is_new, mnemonic = get_or_create_user(telegram_id, username)

    result = {
        "telegram_id": telegram_id,
        "is_new": is_new,
        "network": user.network_mode.value,
        "wallet_address": user.wallet_address_testnet if user.network_mode.value == "testnet" else user.wallet_address_mainnet,
        "testnet_address": user.wallet_address_testnet,
        "mainnet_address": user.wallet_address_mainnet,
        "total_trades": user.total_trades or 0,
        "total_volume": user.total_volume_usd or 0,
    }
    if is_new and mnemonic:
        result["mnemonic"] = mnemonic

    return jsonify(result)


@app.route("/api/balance", methods=["GET"])
@require_auth
def api_balance():
    client = get_user_nado_client(request.telegram_id)
    if not client:
        return jsonify({"error": "Wallet not initialized"}), 400

    balance = client.get_balance()
    if not balance.get("exists"):
        user = get_user(request.telegram_id)
        addr = user.wallet_address_testnet if user.network_mode.value == "testnet" else user.wallet_address_mainnet
        return jsonify({
            "exists": False,
            "wallet_address": addr,
            "message": "Deposit funds to start trading",
        })

    usdt = balance.get("balances", {}).get(0, 0)
    other_balances = {}
    for pid, bal in balance.get("balances", {}).items():
        if pid != 0 and bal != 0:
            other_balances[get_product_name(pid)] = bal

    return jsonify({
        "exists": True,
        "usdt_balance": usdt,
        "other_balances": other_balances,
    })


@app.route("/api/positions", methods=["GET"])
@require_auth
def api_positions():
    client = get_user_nado_client(request.telegram_id)
    if not client:
        return jsonify({"error": "Wallet not initialized"}), 400

    positions = client.get_all_positions()
    return jsonify({"positions": positions})


@app.route("/api/prices", methods=["GET"])
@require_auth
def api_prices():
    client = get_user_nado_client(request.telegram_id)
    if not client:
        return jsonify({"error": "Wallet not initialized"}), 400

    prices = client.get_all_market_prices()
    result = {}
    for name, p in prices.items():
        result[name] = {
            "bid": p["bid"],
            "ask": p["ask"],
            "mid": p["mid"],
            "symbol": f"{name}-PERP",
        }
    return jsonify({"prices": result})


@app.route("/api/trade", methods=["POST"])
@require_auth
def api_trade():
    if is_trading_paused():
        return jsonify({"error": "Trading is temporarily paused"}), 503

    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    product = data.get("product", "").upper()
    size = data.get("size")
    action = data.get("action", "long")
    leverage = data.get("leverage", 1.0)
    price = data.get("price")
    order_type = data.get("order_type", "market")

    if not product or not size:
        return jsonify({"error": "Product and size are required"}), 400

    try:
        size = float(size)
        leverage = float(leverage)
        if price:
            price = float(price)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid numeric values"}), 400

    is_long = action in ("long", "limit_long")

    if order_type == "limit" and price:
        result = execute_limit_order(
            request.telegram_id, product, size, price,
            is_long=is_long, leverage=leverage
        )
    else:
        result = execute_market_order(
            request.telegram_id, product, size,
            is_long=is_long, leverage=leverage
        )

    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 400


@app.route("/api/close", methods=["POST"])
@require_auth
def api_close():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    product = data.get("product")
    close_all = data.get("close_all", False)

    if close_all:
        result = close_all_positions(request.telegram_id)
    elif product:
        result = close_position(request.telegram_id, product.upper())
    else:
        return jsonify({"error": "Specify product or close_all"}), 400

    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 400


@app.route("/api/history", methods=["GET"])
@require_auth
def api_history():
    limit = request.args.get("limit", 20, type=int)
    trades = get_trade_history(request.telegram_id, limit=limit)
    return jsonify({"trades": trades})


@app.route("/api/analytics", methods=["GET"])
@require_auth
def api_analytics():
    stats = get_trade_analytics(request.telegram_id)
    return jsonify(stats)


@app.route("/api/wallet", methods=["GET"])
@require_auth
def api_wallet():
    info = get_user_wallet_info(request.telegram_id)
    if not info:
        return jsonify({"error": "User not found"}), 404
    return jsonify(info)


@app.route("/api/network", methods=["POST"])
@require_auth
def api_network():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    network = data.get("network", "").lower()
    if network not in ("testnet", "mainnet"):
        return jsonify({"error": "Invalid network. Use testnet or mainnet."}), 400

    success, msg = switch_network(request.telegram_id, network)
    if success:
        user = get_user(request.telegram_id)
        return jsonify({
            "success": True,
            "network": network,
            "wallet_address": user.wallet_address_mainnet if network == "mainnet" else user.wallet_address_testnet,
            "message": msg,
        })
    else:
        return jsonify({"success": False, "error": msg}), 400


@app.route("/api/alerts", methods=["GET"])
@require_auth
def api_get_alerts():
    alerts = get_user_alerts(request.telegram_id)
    return jsonify({"alerts": alerts})


@app.route("/api/alerts", methods=["POST"])
@require_auth
def api_create_alert():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid request"}), 400

    product = data.get("product", "").upper()
    condition = data.get("condition", "").lower()
    target = data.get("target")

    if not product or not condition or target is None:
        return jsonify({"error": "Product, condition, and target are required"}), 400

    try:
        target = float(target)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid target value"}), 400

    result = create_alert(request.telegram_id, product, condition, target)
    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 400


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
@require_auth
def api_delete_alert(alert_id):
    result = delete_alert(request.telegram_id, alert_id)
    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 400


@app.route("/api/products", methods=["GET"])
def api_products():
    products = []
    for name, info in PRODUCTS.items():
        if info["type"] == "perp":
            products.append({
                "name": name,
                "id": info["id"],
                "symbol": info.get("symbol", name),
                "type": info["type"],
            })
    return jsonify({"products": products})


@app.after_request
def add_headers(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
