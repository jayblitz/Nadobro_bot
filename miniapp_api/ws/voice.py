"""Gemini Multimodal Live API WebSocket proxy for voice trading.

Architecture:
  Browser <-> miniapp_api /ws/voice <-> Gemini Live API (default: 3.1 Flash Live preview)

The Gemini API key stays server-side.  Audio frames are proxied bidirectionally.
When Gemini calls a function (trade, portfolio, etc.) we execute it server-side
against the existing service layer and return results to Gemini for narration.
"""

import asyncio
import json
import logging
import time
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from miniapp_api.auth import AuthError, validate_init_data
from miniapp_api.config import GEMINI_API_KEY, GEMINI_MODEL, get_product_id
from miniapp_api.ip_utils import client_ip_from_scope
from miniapp_api.rate_limit import check_rate_limit
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.user_service import (
    get_or_create_user,
    get_user_nado_client as _get_nado_client,
)
from src.nadobro.services.trade_service import (
    execute_market_order,
    close_position,
    close_all_positions,
)
from src.nadobro.models.database import NetworkMode
from src.nadobro.config import MIN_TRADE_SIZE_USD

logger = logging.getLogger(__name__)

router = APIRouter()

# Session timeout: 5 minutes of silence
_SESSION_TIMEOUT = 300

# Max WebSocket JSON payload (auth + audio metadata); prevents huge JSON DoS.
_MAX_WS_JSON_BYTES = 512 * 1024


async def _receive_json_capped(ws: WebSocket, *, max_bytes: int = _MAX_WS_JSON_BYTES) -> dict:
    """Parse one WebSocket text/bytes JSON message with a size cap."""
    message = await ws.receive()
    if message.get("type") != "websocket.receive":
        raise WebSocketDisconnect()
    if "bytes" in message:
        raw = message["bytes"]
        if len(raw) > max_bytes:
            raise ValueError("message too large")
        return json.loads(raw.decode("utf-8"))
    if "text" in message:
        text = message["text"]
        if len(text.encode("utf-8")) > max_bytes:
            raise ValueError("message too large")
        return json.loads(text)
    raise ValueError("empty websocket frame")


def _client_safe_error() -> str:
    """Do not leak stack traces or internals to the browser."""
    return "Request failed. Please try again."


# -------------------------------------------------------------------
# Gemini function-calling tools exposed to the voice model
# -------------------------------------------------------------------
VOICE_TOOLS = [
    {
        "name": "prepare_trade_order",
        "description": (
            "Stage a market order for confirmation only — DO NOT execute. "
            "Use when the user asks to open a position (e.g. 'long 10 WTI 5x market'). "
            "After this, wait for the user to say confirm or decline."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Asset symbol (BTC, ETH, WTI, etc.)"},
                "side": {"type": "string", "enum": ["long", "short"]},
                "size_usd": {"type": "number", "description": "Position size in USD"},
                "leverage": {"type": "number", "description": "Leverage multiplier, default 1"},
            },
            "required": ["product", "side", "size_usd"],
        },
    },
    {
        "name": "confirm_trade_order",
        "description": (
            "Execute the previously staged trade after the user clearly confirms "
            "(e.g. 'confirm', 'yes', 'execute'). Only call if a prepare_trade_order was done."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "cancel_trade_order",
        "description": (
            "Discard the staged trade when the user declines (e.g. 'decline', 'cancel', 'stop')."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "close_position",
        "description": "Close an open position fully or partially.",
        "parameters": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Asset symbol"},
                "close_pct": {"type": "number", "description": "Percentage to close (1-100), default 100"},
            },
            "required": ["product"],
        },
    },
    {
        "name": "close_all_positions",
        "description": "Emergency close all open positions.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "get_portfolio",
        "description": "Get current positions, balance, and PnL summary.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "get_price",
        "description": "Get the current market price of a crypto asset.",
        "parameters": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Asset symbol"},
            },
            "required": ["product"],
        },
    },
]


def _build_system_prompt(username: str) -> str:
    """Build Gemini system prompt with NadoBro's Bro personality."""
    return f"""You are NadoBro — a confident, knowledgeable crypto trading assistant with a friendly "bro" personality.

The user's name is {username}. When they greet you (e.g. "Yo Bro", "Hey Bro"), respond warmly: "Hi {username}, how can I help you today?"

Your capabilities:
- Stage and confirm trades (long/short perpetual futures) on Nado DEX — same rules as text chat
- Check portfolio, positions, and balances
- Get current market prices
- Close positions (full or partial)

Available trading pairs include BTC, ETH, SOL, XRP, BNB, LINK, DOGE, WTI, and other listed perps.

Rules for NEW positions (opening trades):
- NEVER execute a market order on first request. Always call prepare_trade_order first.
- After prepare_trade_order returns, tell the user clearly, for example: "Executing Trade — Long 10 WTI with leverage 5. Say confirm to execute or Decline to stop."
- When they say confirm / yes / go ahead, call confirm_trade_order.
- When they say decline / cancel / stop, call cancel_trade_order.
- If they change the trade details before confirming, call prepare_trade_order again with the new parameters.

Rules after a trade is filled:
- Read back side, product, size, leverage, and fill price briefly.

General:
- Be concise but friendly. Use the bro persona — casual, confident, helpful.
- If the user's request is unclear, ask for clarification.
- Keep responses short for voice — 1-2 sentences max unless they ask for details.
- When reporting prices, round to reasonable decimals (2 for BTC/ETH, 4 for others).
- Always mention leverage if it's not 1x.
"""


async def _execute_function(
    func_name: str,
    args: dict,
    telegram_id: int,
    network: str,
    session: dict[str, Any],
) -> dict[str, Any]:
    """Execute a Gemini function call against real services."""
    try:
        if func_name == "prepare_trade_order":
            product = str(args["product"]).upper().strip()
            side = str(args["side"]).lower().strip()
            size_usd = float(args["size_usd"])
            leverage = float(args.get("leverage", 1) or 1)
            if side not in ("long", "short"):
                return {"status": "error", "error": "Side must be long or short"}
            if size_usd < float(MIN_TRADE_SIZE_USD):
                return {
                    "status": "error",
                    "error": f"Minimum order size is ${MIN_TRADE_SIZE_USD} USD.",
                }
            pid = get_product_id(product, network=network)
            if pid is None:
                return {"status": "error", "error": f"Unknown or unsupported product: {product}"}

            session["pending_trade"] = {
                "product": product,
                "side": side,
                "size_usd": size_usd,
                "leverage": leverage,
            }
            action = "Long" if side == "long" else "Short"
            summary = (
                f"Executing Trade ({action} {size_usd:g} {product} with leverage {leverage:g}). "
                "Say confirm to execute or Decline to stop."
            )
            return {
                "status": "awaiting_confirmation",
                "product": product,
                "side": side,
                "size_usd": size_usd,
                "leverage": leverage,
                "summary": summary,
            }

        elif func_name == "confirm_trade_order":
            pending = session.get("pending_trade")
            if not pending:
                return {"status": "error", "error": "No staged trade. Describe your order first."}

            product = pending["product"]
            side = pending["side"]
            size_usd = float(pending["size_usd"])
            leverage = float(pending.get("leverage", 1))
            is_long = side == "long"

            result = await run_blocking(
                execute_market_order,
                telegram_id,
                product,
                size_usd,
                is_long,
                leverage=leverage,
                enforce_rate_limit=True,
            )
            session["pending_trade"] = None
            if result.get("success"):
                return {
                    "status": "filled",
                    "product": product,
                    "side": side,
                    "size_usd": size_usd,
                    "leverage": leverage,
                    "fill_price": result.get("fill_price") or result.get("price"),
                    "digest": result.get("digest"),
                }
            return {"status": "failed", "error": result.get("error", "Trade failed")}

        elif func_name == "cancel_trade_order":
            had = session.pop("pending_trade", None)
            if not had:
                return {"status": "idle", "message": "No staged trade to cancel."}
            return {"status": "cancelled", "message": "Staged trade discarded."}

        elif func_name == "close_position":
            product = args["product"].upper()
            close_pct = float(args.get("close_pct", 100))

            # Get current position to determine size
            client = await run_blocking(_get_nado_client, telegram_id, network)
            if not client:
                return {"status": "failed", "error": "No exchange client"}

            positions = await run_blocking(client.get_all_positions)
            target = None
            for pos in (positions or []):
                if pos.get("product_name", "").upper() == product:
                    target = pos
                    break

            if not target:
                return {"status": "failed", "error": f"No open {product} position"}

            total_size = abs(float(target.get("amount", 0)))
            close_size = total_size * (close_pct / 100) if close_pct < 100 else None

            result = await run_blocking(
                close_position, telegram_id, product,
                size=close_size, network=network,
            )
            if result.get("success"):
                return {"status": "closed", "product": product, "close_pct": close_pct}
            return {"status": "failed", "error": result.get("error", "Close failed")}

        elif func_name == "close_all_positions":
            result = await run_blocking(close_all_positions, telegram_id, network=network)
            return {"status": "closed_all", "results": result}

        elif func_name == "get_portfolio":
            client = await run_blocking(_get_nado_client, telegram_id, network)
            if not client:
                return {"status": "error", "error": "No exchange client"}

            positions = await run_blocking(client.get_all_positions) or []
            balance_data = await run_blocking(client.get_balance)
            balances = balance_data.get("balances", {}) if balance_data else {}
            usdt_balance = float(balances.get(0, 0))

            pos_summary = []
            for p in positions:
                pos_summary.append({
                    "product": p.get("product_name"),
                    "side": p.get("side"),
                    "size": abs(float(p.get("amount", 0))),
                    "entry_price": float(p.get("price", 0)),
                })

            return {
                "balance_usdt": usdt_balance,
                "positions": pos_summary,
                "position_count": len(positions),
            }

        elif func_name == "get_price":
            product = args["product"].upper()
            pid = get_product_id(product, network=network)
            if pid is None:
                return {"status": "error", "error": f"Unknown product: {product}"}

            client = await run_blocking(_get_nado_client, telegram_id, network)
            if not client:
                return {"status": "error", "error": "No exchange client"}

            price = await run_blocking(client.get_market_price, pid)
            return {
                "product": product,
                "bid": price.get("bid"),
                "ask": price.get("ask"),
                "mid": price.get("mid"),
            }

        return {"status": "error", "error": f"Unknown function: {func_name}"}

    except Exception as exc:
        logger.exception("Voice function %s failed", func_name)
        return {"status": "error", "error": _client_safe_error()}


@router.websocket("/ws/voice")
async def voice_ws(ws: WebSocket):
    """WebSocket endpoint for Gemini voice proxy.

    Client sends:
      - {"type": "auth", "init_data": "..."} — first message, authenticate
      - {"type": "audio", "data": "<base64 pcm>"} — audio chunks
      - {"type": "text", "text": "..."} — text input fallback
      - {"type": "end"} — end session

    Server sends:
      - {"type": "auth_ok", "username": "..."}
      - {"type": "audio", "data": "<base64 pcm>"} — Gemini audio response
      - {"type": "text", "text": "..."} — transcript
      - {"type": "function_call", "name": "...", "result": {...}} — function execution result
      - {"type": "error", "message": "..."}
    """
    try:
        if not await run_blocking(check_rate_limit, client_ip_from_scope(ws.scope)):
            await ws.close(code=1008)
            return
    except Exception:
        logger.exception("Voice rate limit check failed; allowing connection")

    await ws.accept()

    if not GEMINI_API_KEY or len(GEMINI_API_KEY) < 12:
        await ws.send_json({
            "type": "error",
            "message": "Voice AI not configured. Set GEMINI_API_KEY (Google AI Studio) on the server.",
            "code": "missing_gemini_key",
        })
        await ws.close()
        return

    # Step 1: Authenticate
    try:
        auth_msg = await asyncio.wait_for(_receive_json_capped(ws), timeout=10)
    except (asyncio.TimeoutError, WebSocketDisconnect):
        await ws.close()
        return
    except (ValueError, json.JSONDecodeError):
        await ws.send_json({"type": "error", "message": "Invalid message"})
        await ws.close()
        return

    if auth_msg.get("type") != "auth" or not auth_msg.get("init_data"):
        await ws.send_json({"type": "error", "message": "First message must be auth"})
        await ws.close()
        return

    try:
        tg_user = validate_init_data(auth_msg["init_data"])
    except AuthError as exc:
        await ws.send_json({"type": "error", "message": f"Auth failed: {exc}"})
        await ws.close()
        return

    user_row, _, _ = await run_blocking(get_or_create_user, tg_user.id, tg_user.username)
    if not user_row:
        await ws.send_json({"type": "error", "message": "User not found"})
        await ws.close()
        return

    network = "mainnet"
    if hasattr(user_row, "network_mode"):
        nm = user_row.network_mode
        if isinstance(nm, NetworkMode):
            network = nm.value
        else:
            network = str(nm or "mainnet")

    username = tg_user.first_name or tg_user.username or "Bro"

    session: dict[str, Any] = {"pending_trade": None}

    # Step 2: Connect to Gemini Live API BEFORE auth_ok so the client does not show
    # a greeting when the upstream connection will fail (e.g. invalid API key on Fly).
    gemini_ws = None
    try:
        import websockets

        # URL-encode key so +, /, & in secrets cannot break the query string.
        key_q = quote(GEMINI_API_KEY, safe="")
        gemini_url = (
            "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage"
            ".v1beta.GenerativeService.BidiGenerateContent"
            f"?key={key_q}"
        )

        gemini_ws = await websockets.connect(gemini_url)

        setup_msg = {
            "setup": {
                "model": f"models/{GEMINI_MODEL}",
                "system_instruction": {
                    "parts": [{"text": _build_system_prompt(username)}]
                },
                "tools": [
                    {
                        "function_declarations": VOICE_TOOLS,
                    }
                ],
                "generation_config": {
                    "response_modalities": ["AUDIO", "TEXT"],
                    "speech_config": {
                        "voice_config": {
                            "prebuilt_voice_config": {"voice_name": "Puck"}
                        }
                    },
                },
            }
        }
        await gemini_ws.send(json.dumps(setup_msg))

        setup_resp = await asyncio.wait_for(gemini_ws.recv(), timeout=15)
        setup_data = json.loads(setup_resp)
        logger.info("Gemini session established: %s", json.dumps(setup_data)[:200])

        if isinstance(setup_data, dict) and setup_data.get("error"):
            raise RuntimeError(str(setup_data["error"]))

    except Exception as exc:
        err_s = str(exc).lower()
        logger.error("Failed to connect to Gemini: %s", exc, exc_info=True)
        if gemini_ws:
            try:
                await gemini_ws.close()
            except Exception:
                pass
            gemini_ws = None
        if "api key" in err_s or "api_key" in err_s or "not found" in err_s:
            msg = (
                "Voice AI could not start: the Gemini API key on the server is missing or rejected. "
                "Add a valid key from Google AI Studio (GEMINI_API_KEY) and redeploy."
            )
            code = "invalid_gemini_key"
        else:
            msg = "Voice AI connection failed. Try again in a moment."
            code = "gemini_connect_failed"
        await ws.send_json({"type": "error", "message": msg, "code": code})
        await ws.close()
        return

    await ws.send_json({"type": "auth_ok", "username": username})
    logger.info("Voice session started for %s (tid=%d)", username, tg_user.id)

    # Step 3: Bidirectional proxy
    last_activity = time.time()

    async def client_to_gemini():
        """Forward audio/text from client to Gemini."""
        nonlocal last_activity
        try:
            while True:
                msg = await asyncio.wait_for(
                    _receive_json_capped(ws), timeout=_SESSION_TIMEOUT
                )
                last_activity = time.time()
                msg_type = msg.get("type")

                if msg_type == "audio":
                    # Forward audio chunk to Gemini
                    audio_data = msg.get("data", "")
                    gemini_msg = {
                        "realtime_input": {
                            "media_chunks": [
                                {
                                    "data": audio_data,
                                    "mime_type": "audio/pcm;rate=16000",
                                }
                            ]
                        }
                    }
                    await gemini_ws.send(json.dumps(gemini_msg))

                elif msg_type == "text":
                    # Text fallback — send as content turn
                    gemini_msg = {
                        "client_content": {
                            "turns": [
                                {
                                    "role": "user",
                                    "parts": [{"text": msg.get("text", "")}],
                                }
                            ],
                            "turn_complete": True,
                        }
                    }
                    await gemini_ws.send(json.dumps(gemini_msg))

                elif msg_type == "end":
                    break

        except (asyncio.TimeoutError, WebSocketDisconnect):
            pass
        except (ValueError, json.JSONDecodeError):
            logger.warning("Client->Gemini: invalid or oversized JSON")
        except Exception as exc:
            logger.warning("Client->Gemini error: %s", exc)

    async def gemini_to_client():
        """Forward Gemini responses (audio, text, function calls) to client."""
        nonlocal last_activity
        try:
            async for raw_msg in gemini_ws:
                last_activity = time.time()
                data = json.loads(raw_msg)

                server_content = data.get("serverContent")
                tool_call = data.get("toolCall")

                if server_content:
                    parts = []
                    model_turn = server_content.get("modelTurn", {})
                    for part in model_turn.get("parts", []):
                        if "text" in part:
                            await ws.send_json({
                                "type": "text",
                                "text": part["text"],
                            })
                        elif "inlineData" in part:
                            inline = part["inlineData"]
                            await ws.send_json({
                                "type": "audio",
                                "data": inline.get("data", ""),
                                "mime_type": inline.get("mimeType", "audio/pcm;rate=24000"),
                            })

                    if server_content.get("turnComplete"):
                        await ws.send_json({"type": "turn_complete"})

                elif tool_call:
                    # Gemini wants to call a function — execute it
                    function_calls = tool_call.get("functionCalls", [])
                    responses = []

                    for fc in function_calls:
                        fname = fc.get("name", "")
                        fargs = fc.get("args", {})
                        fid = fc.get("id", "")

                        _args_preview = json.dumps(fargs, default=str)[:200]
                        logger.info("Voice function call: %s(%s)", fname, _args_preview)

                        result = await _execute_function(
                            fname, fargs, tg_user.id, network, session
                        )

                        # Notify client about function execution
                        await ws.send_json({
                            "type": "function_call",
                            "name": fname,
                            "args": fargs,
                            "result": result,
                        })

                        responses.append({
                            "id": fid,
                            "name": fname,
                            "response": {"result": result},
                        })

                    # Send function results back to Gemini
                    tool_response = {
                        "tool_response": {
                            "function_responses": responses,
                        }
                    }
                    await gemini_ws.send(json.dumps(tool_response))

        except (WebSocketDisconnect, Exception) as exc:
            if not isinstance(exc, WebSocketDisconnect):
                logger.warning("Gemini->Client error: %s", exc)

    try:
        # Run both directions concurrently
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(client_to_gemini()),
                asyncio.create_task(gemini_to_client()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        logger.info("Voice session ended for %s", username)
        if gemini_ws:
            await gemini_ws.close()
        try:
            await ws.close()
        except Exception:
            pass
