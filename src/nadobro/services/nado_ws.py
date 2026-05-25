from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any

from src.nadobro.services.ws_health import mark_connected, mark_disconnected, touch

logger = logging.getLogger(__name__)

# Debounce WS invalidations — coalesce bursts into one sync per window.
_DEBOUNCE_SECONDS = float(__import__("os").environ.get("NADO_WS_DEBOUNCE_SECONDS", "2.0"))
_pending_sync: dict[tuple[int, str], float] = {}


def ws_url_for_network(network: str) -> str:
    env = "prod" if str(network) == "mainnet" else "test"
    return f"wss://gateway.{env}.nado.xyz/v1/ws"


@dataclass
class PortfolioWsSubscription:
    user_id: int
    network: str
    subaccount: str


class NadoPortfolioWs:
    """WebSocket invalidation layer; polling sync is fallback when WS is down."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[int, str], asyncio.Task] = {}

    def subscribe(self, sub: PortfolioWsSubscription) -> None:
        key = (int(sub.user_id), str(sub.network))
        existing = self._tasks.get(key)
        if existing and not existing.done():
            return
        self._tasks[key] = asyncio.create_task(self._run(sub), name=f"portfolio-ws-{sub.user_id}-{sub.network}")

    async def unsubscribe(self, user_id: int, network: str) -> None:
        key = (int(user_id), str(network))
        mark_disconnected(user_id, network)
        task = self._tasks.pop(key, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def stop(self) -> None:
        for key in list(self._tasks):
            uid, net = key
            mark_disconnected(uid, net)
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, sub: PortfolioWsSubscription) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect_once(sub)
                backoff = 1.0
            except asyncio.CancelledError:
                mark_disconnected(sub.user_id, sub.network)
                raise
            except Exception as exc:
                mark_disconnected(sub.user_id, sub.network)
                logger.warning("portfolio ws disconnected user=%s network=%s: %s", sub.user_id, sub.network, exc)
                await asyncio.sleep(backoff + random.uniform(0, backoff * 0.2))
                backoff = min(60.0, backoff * 2)
                await self._schedule_sync(sub.user_id, sub.network, force=True, reason="ws_reconnect")

    async def _connect_once(self, sub: PortfolioWsSubscription) -> None:
        import websockets

        async with websockets.connect(ws_url_for_network(sub.network), ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "subaccount": sub.subaccount,
                "channels": ["position_change", "order_update", "fill", "funding_payment", "mark_price"],
            }))
            mark_connected(sub.user_id, sub.network)
            await self._schedule_sync(sub.user_id, sub.network, force=True, reason="ws_connect")
            async for raw in ws:
                event = _json(raw)
                if _event_type(event) == "mark_price":
                    _push_mark_prices(sub.network, event)
                if _should_invalidate(event):
                    touch(sub.user_id, sub.network)
                    await self._schedule_sync(
                        sub.user_id,
                        sub.network,
                        force=False,
                        reason=f"ws_{_event_type(event) or 'event'}",
                    )

    async def _schedule_sync(self, user_id: int, network: str, *, force: bool, reason: str) -> None:
        key = (int(user_id), str(network))
        now = time.monotonic()
        if not force:
            last = _pending_sync.get(key, 0.0)
            if now - last < _DEBOUNCE_SECONDS:
                return
        _pending_sync[key] = now
        from src.nadobro.services.nado_sync import sync_user
        try:
            await sync_user(user_id, network=network, reason=reason, force=force)
        except Exception:
            logger.debug("portfolio ws sync failed user=%s reason=%s", user_id, reason, exc_info=True)


def _json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _should_invalidate(event: dict[str, Any]) -> bool:
    event_type = _event_type(event)
    return event_type in {"position_change", "order_update", "fill", "funding_payment", "mark_price"}


def _event_type(event: dict[str, Any]) -> str:
    current: Any = event
    for _ in range(4):
        if not isinstance(current, dict):
            return ""
        event_type = str(current.get("type") or current.get("event") or "")
        if event_type:
            return event_type
        current = current.get("payload") or current.get("data") or current.get("message")
    return ""


def _push_mark_prices(network: str, event: dict[str, Any]) -> None:
    """Best-effort mark price push into the shared market feed cache."""
    try:
        from src.nadobro.config import get_product_name
        from src.nadobro.services.market_feed import update_from_ws

        payload = event.get("payload") or event.get("data") or event
        if not isinstance(payload, dict):
            return
        try:
            pid = int(payload.get("product_id") or 0)
        except (TypeError, ValueError):
            return
        if pid <= 0:
            return
        price_raw = payload.get("mark_price_x18") or payload.get("price_x18") or payload.get("mark_price")
        if price_raw is None:
            return
        try:
            from nado_protocol.utils.math import from_x18
            mid = float(from_x18(int(price_raw)))
        except Exception:
            try:
                mid = float(price_raw)
            except (TypeError, ValueError):
                return
        if mid <= 0:
            return
        name = str(get_product_name(pid, network=network)).replace("-PERP", "")
        update_from_ws(network, {name: {"bid": mid, "ask": mid, "mid": mid}})
    except Exception:
        logger.debug("mark_price ws push failed", exc_info=True)


portfolio_ws = NadoPortfolioWs()
