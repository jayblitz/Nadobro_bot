from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from typing import Any

from src.nadobro.services.nado_sync import sync_user

logger = logging.getLogger(__name__)


def ws_url_for_network(network: str) -> str:
    env = "prod" if str(network) == "mainnet" else "test"
    return f"wss://gateway.{env}.nado.xyz/v1/ws"


@dataclass
class PortfolioWsSubscription:
    user_id: int
    network: str
    subaccount: str


class NadoPortfolioWs:
    """WebSocket invalidation layer; polling sync remains authoritative."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[int, str], asyncio.Task] = {}

    def subscribe(self, sub: PortfolioWsSubscription) -> None:
        key = (int(sub.user_id), str(sub.network))
        existing = self._tasks.get(key)
        if existing and not existing.done():
            return
        self._tasks[key] = asyncio.create_task(self._run(sub), name=f"portfolio-ws-{sub.user_id}-{sub.network}")

    async def unsubscribe(self, user_id: int, network: str) -> None:
        task = self._tasks.pop((int(user_id), str(network)), None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def stop(self) -> None:
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
                raise
            except Exception as exc:
                logger.warning("portfolio ws disconnected user=%s network=%s: %s", sub.user_id, sub.network, exc)
                await asyncio.sleep(backoff + random.uniform(0, backoff * 0.2))
                backoff = min(60.0, backoff * 2)
                try:
                    await sync_user(sub.user_id, network=sub.network, reason="ws_reconnect", force=True)
                except Exception:
                    logger.debug("portfolio ws reconnect sync failed", exc_info=True)

    async def _connect_once(self, sub: PortfolioWsSubscription) -> None:
        import websockets

        async with websockets.connect(ws_url_for_network(sub.network), ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "subaccount": sub.subaccount,
                "channels": ["position_change", "order_update", "fill", "funding_payment"],
            }))
            await sync_user(sub.user_id, network=sub.network, reason="ws_connect", force=True)
            async for raw in ws:
                event = _json(raw)
                if _should_invalidate(event):
                    await sync_user(sub.user_id, network=sub.network, reason=f"ws_{_event_type(event) or 'event'}", force=True)


def _json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _should_invalidate(event: dict[str, Any]) -> bool:
    event_type = _event_type(event)
    return event_type in {"position_change", "order_update", "fill", "funding_payment"}


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


portfolio_ws = NadoPortfolioWs()
