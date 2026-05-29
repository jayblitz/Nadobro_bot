from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any

from src.nadobro.services.ws_health import mark_connected, mark_disconnected, touch

logger = logging.getLogger(__name__)

# Debounce WS invalidations — coalesce bursts into one sync per window.
_DEBOUNCE_SECONDS = float(os.environ.get("NADO_WS_DEBOUNCE_SECONDS", "2.0"))
_pending_sync: dict[tuple[int, str], float] = {}

# Subscriptions WS v2 streams we care about for portfolio invalidation. Per the
# Nado docs these are subaccount-scoped; ``order_update`` and ``fill`` require a
# prior ``authenticate`` on the connection. ``product_id: null`` means "all
# products" so a single subscription per stream covers every market.
#   docs: https://docs.nado.xyz/developer-resources/api/subscriptions/streams
_PORTFOLIO_STREAMS = ("order_update", "fill", "position_change", "funding_payment")

# Event ``type`` (or ``reason``) values that mean our cached portfolio snapshot
# is now stale and should be refreshed.
_INVALIDATING_EVENTS = {"order_update", "fill", "position_change", "funding_payment"}


def subscribe_url_for_network(network: str) -> str:
    """Subscriptions (streams) websocket endpoint.

    IMPORTANT: this is ``/v1/subscribe`` — the live-data subscriptions socket —
    NOT ``/v1/ws`` which is the gateway *action* socket for executes/queries.
    Subscribing to streams on ``/v1/ws`` silently yields no data, which is why
    the portfolio WS never went healthy and every poll fell back to the full
    REST read storm. (Audit 2026-05-29.)
    """
    env = "prod" if str(network) == "mainnet" else "test"
    return f"wss://gateway.{env}.nado.xyz/v1/subscribe"


# Back-compat alias: some callers/tests import ``ws_url_for_network``.
def ws_url_for_network(network: str) -> str:
    return subscribe_url_for_network(network)


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

    def _build_auth_message(self, sub: PortfolioWsSubscription) -> dict[str, Any] | None:
        """Sign the per-connection ``authenticate`` message using the user's
        signing client. Returns None for read-only users (no signer) — they
        simply won't get the authenticated streams and stay on REST polling.
        """
        try:
            from src.nadobro.services.user_service import get_user_nado_client

            client = get_user_nado_client(int(sub.user_id), network=sub.network)
            if client is None:
                return None
            return client.sign_stream_authentication(sender=sub.subaccount)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "portfolio ws auth signing failed user=%s network=%s: %s",
                sub.user_id, sub.network, exc,
            )
            return None

    async def _connect_once(self, sub: PortfolioWsSubscription) -> None:
        import websockets

        # ``compression="deflate"`` makes the client advertise the required
        # ``Sec-WebSocket-Extensions: permessage-deflate`` in the handshake.
        async with websockets.connect(
            subscribe_url_for_network(sub.network),
            ping_interval=20,
            ping_timeout=20,
            compression="deflate",
        ) as ws:
            # 1) Authenticate (required before order_update / fill).
            auth_msg = await asyncio.to_thread(self._build_auth_message, sub)
            if auth_msg is not None:
                await ws.send(json.dumps(auth_msg))

            # 2) Subscribe to each portfolio stream as its own message. The
            #    subscriptions API takes ONE ``stream`` per ``subscribe`` (not a
            #    ``channels`` array) and echoes the ``id`` back in the response.
            for idx, stream_type in enumerate(_PORTFOLIO_STREAMS, start=1):
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "stream": {
                        "type": stream_type,
                        "product_id": None,  # all products
                        "subaccount": sub.subaccount,
                    },
                    "id": idx,
                }))

            mark_connected(sub.user_id, sub.network)
            await self._schedule_sync(sub.user_id, sub.network, force=True, reason="ws_connect")

            async for raw in ws:
                event = _json(raw)
                # Any inbound frame (event, subscribe ack, heartbeat) proves the
                # socket is alive — keep health fresh so sync_user keeps skipping
                # the REST poll.
                touch(sub.user_id, sub.network)
                if _is_auth_or_error(event):
                    _log_control_frame(sub, event)
                    continue
                # Phase C: drive the per-order lifecycle store off the stream so
                # the engine can stop polling order_status on every tick.
                _route_lifecycle(event)
                if _should_invalidate(event):
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


def _is_auth_or_error(event: dict[str, Any]) -> bool:
    """Control frames: authenticate ack, subscribe ack, or an error response."""
    if not isinstance(event, dict):
        return False
    if "error" in event or "result" in event:
        return True
    method = str(event.get("method") or "")
    return method in {"authenticate", "subscribe", "unsubscribe"}


def _log_control_frame(sub: PortfolioWsSubscription, event: dict[str, Any]) -> None:
    if event.get("error"):
        logger.warning(
            "portfolio ws control error user=%s network=%s: %s",
            sub.user_id, sub.network, event.get("error"),
        )
    else:
        logger.debug("portfolio ws control frame user=%s: %s", sub.user_id, event)


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    """Unwrap a stream frame to the dict that holds the event fields."""
    current: Any = event
    for _ in range(4):
        if not isinstance(current, dict):
            return {}
        if any(k in current for k in ("digest", "reason", "id", "filled_qty")):
            return current
        nxt = current.get("payload") or current.get("data") or current.get("message")
        if nxt is None:
            return current
        current = nxt
    return event if isinstance(event, dict) else {}


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _route_lifecycle(event: dict[str, Any]) -> None:
    """Feed order_update / fill stream events into the lifecycle store."""
    etype = _event_type(event)
    if etype not in ("order_update", "fill"):
        return
    try:
        from src.nadobro.engine import order_lifecycle

        body = _payload(event)
        tag = _as_int(body.get("id"))
        if etype == "order_update":
            order_lifecycle.apply_order_update(
                digest=body.get("digest"), reason=body.get("reason"), tag=tag,
            )
        else:  # fill — carries only ``id`` (our Phase-B tag), no digest
            order_lifecycle.apply_fill(tag=tag, digest=body.get("digest"))
    except Exception:  # noqa: BLE001 - lifecycle is best-effort; never break the WS loop
        logger.debug("lifecycle routing failed", exc_info=True)


def _should_invalidate(event: dict[str, Any]) -> bool:
    return _event_type(event) in _INVALIDATING_EVENTS


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
