# DEPRECATED: This module is being replaced by Nado-native copy trading.
# It will be removed in a future version.

import asyncio
import json
import logging
import time

import websockets

from src.nadobro.models.database import get_active_trader_wallets, get_mirrors_for_trader, get_copy_trader_by_wallet, update_mirror_last_synced

logger = logging.getLogger(__name__)


# Stub for removed function - HL copy trading replaced by Nado-native
async def _deprecated_process_hl_fill(wallet, fill):
    logger.warning("HL copy trading is deprecated. Use Nado-native copy trading instead.")

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
HEARTBEAT_INTERVAL = 30.0
WALLET_REFRESH_INTERVAL = 60.0


class HLWebSocketManager:

    def __init__(self):
        self._ws = None
        self._task: asyncio.Task | None = None
        self._subscribed_wallets: set[str] = set()
        self._running = False
        self._reconnect_count = 0

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    @property
    def subscribed_count(self) -> int:
        return len(self._subscribed_wallets)

    def start(self):
        if self.is_running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("HL WebSocket manager started")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        self._subscribed_wallets.clear()
        logger.info("HL WebSocket manager stopped")

    async def _run_loop(self):
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("HL WS loop cancelled")
                break
            except Exception as e:
                self._reconnect_count += 1
                delay = min(
                    RECONNECT_BASE_DELAY * (2 ** min(self._reconnect_count, 6)),
                    RECONNECT_MAX_DELAY,
                )
                logger.warning(
                    "HL WS disconnected (attempt %d): %s — reconnecting in %.1fs",
                    self._reconnect_count, e, delay,
                )
                await asyncio.sleep(delay)

        await self._close_ws()

    async def _connect_and_listen(self):
        await self._close_ws()

        logger.info("Connecting to HL WebSocket...")
        self._ws = await websockets.connect(
            HL_WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        self._reconnect_count = 0
        self._subscribed_wallets.clear()
        logger.info("HL WebSocket connected")

        await self._subscribe_active_wallets()
        asyncio.create_task(self._backfill_missed_fills())

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        refresh_task = asyncio.create_task(self._wallet_refresh_loop())

        try:
            async for raw_msg in self._ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw_msg)
                    await self._handle_message(msg)
                except json.JSONDecodeError:
                    logger.debug("Non-JSON WS message: %s", str(raw_msg)[:100])
                except Exception as e:
                    logger.error("WS message handler error: %s", e, exc_info=True)
        finally:
            heartbeat_task.cancel()
            refresh_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await refresh_task
            except (asyncio.CancelledError, Exception):
                pass

    async def _close_ws(self):
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _subscribe_active_wallets(self):
        try:
            wallets = get_active_trader_wallets()
        except Exception as e:
            logger.error("Failed to fetch active trader wallets: %s", e)
            return

        for wallet in wallets:
            if wallet not in self._subscribed_wallets:
                await self._subscribe_wallet(wallet)

    async def _subscribe_wallet(self, wallet: str):
        if not self._ws:
            return
        try:
            sub_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "userFills",
                    "user": wallet,
                },
            }
            await self._ws.send(json.dumps(sub_msg))
            self._subscribed_wallets.add(wallet)
            logger.info("Subscribed to HL fills for wallet %s", wallet[:10])
        except Exception as e:
            logger.error("Failed to subscribe wallet %s: %s", wallet[:10], e)

    async def _unsubscribe_wallet(self, wallet: str):
        if not self._ws:
            return
        try:
            unsub_msg = {
                "method": "unsubscribe",
                "subscription": {
                    "type": "userFills",
                    "user": wallet,
                },
            }
            await self._ws.send(json.dumps(unsub_msg))
            self._subscribed_wallets.discard(wallet)
            logger.info("Unsubscribed from HL fills for wallet %s", wallet[:10])
        except Exception as e:
            logger.error("Failed to unsubscribe wallet %s: %s", wallet[:10], e)

    async def _handle_message(self, msg: dict):
        channel = msg.get("channel")
        data = msg.get("data")

        if channel == "subscriptionResponse":
            method = data.get("method") if isinstance(data, dict) else None
            logger.debug("HL WS subscription response: %s", method)
            return

        if channel == "userFills":
            if not isinstance(data, dict):
                return
            user = data.get("user", "")
            fills = data.get("fills", [])
            if not fills:
                return
            asyncio.create_task(self._dispatch_fills(user, fills))

        if channel == "pong":
            return

    async def _dispatch_fills(self, wallet: str, fills: list):
        process_hl_fill = _deprecated_process_hl_fill
        for fill in fills:
            try:
                await process_hl_fill(wallet, fill)
            except Exception as e:
                logger.error(
                    "Error processing HL fill for %s: %s",
                    wallet[:10], e, exc_info=True,
                )

    async def _heartbeat_loop(self):
        while self._running and self._ws:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self._ws:
                    await self._ws.send(json.dumps({"method": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Heartbeat error: %s", e)
                break

    async def _wallet_refresh_loop(self):
        while self._running and self._ws:
            try:
                await asyncio.sleep(WALLET_REFRESH_INTERVAL)
                await self._sync_subscriptions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Wallet refresh error: %s", e)

    async def _sync_subscriptions(self):
        try:
            active_wallets = set(get_active_trader_wallets())
        except Exception as e:
            logger.error("Failed to fetch wallets for sync: %s", e)
            return

        to_subscribe = active_wallets - self._subscribed_wallets
        to_unsubscribe = self._subscribed_wallets - active_wallets

        for wallet in to_subscribe:
            await self._subscribe_wallet(wallet)
        for wallet in to_unsubscribe:
            await self._unsubscribe_wallet(wallet)

    async def _backfill_missed_fills(self):
        from src.nadobro.services.hl_client import get_hl_client
        process_hl_fill = _deprecated_process_hl_fill
        try:
            wallets = get_active_trader_wallets()
        except Exception as e:
            logger.error("Backfill: failed to get wallets: %s", e)
            return

        hl = get_hl_client()
        for wallet in wallets:
            try:
                trader = get_copy_trader_by_wallet(wallet)
                if not trader:
                    continue
                mirrors = get_mirrors_for_trader(trader["id"])
                if not mirrors:
                    continue
                max_synced_tid = max(int(m.get("last_synced_fill_tid", 0) or 0) for m in mirrors)
                if max_synced_tid <= 0:
                    continue
                fills = await hl.get_user_fills(wallet)
                if not fills:
                    continue
                missed = [f for f in fills if int(f.get("tid", 0)) > max_synced_tid]
                missed.sort(key=lambda f: int(f.get("tid", 0)))
                if missed:
                    logger.info("Backfilling %d missed fills for wallet %s", len(missed), wallet[:10])
                for fill in missed:
                    try:
                        await process_hl_fill(wallet, fill)
                    except Exception as e:
                        logger.error("Backfill fill error for %s: %s", wallet[:10], e)
            except Exception as e:
                logger.error("Backfill error for wallet %s: %s", wallet[:10], e)


_manager: HLWebSocketManager | None = None


def get_ws_manager() -> HLWebSocketManager:
    global _manager
    if _manager is None:
        _manager = HLWebSocketManager()
    return _manager


def start_copy_ws():
    mgr = get_ws_manager()
    mgr.start()


def stop_copy_ws():
    mgr = get_ws_manager()
    mgr.stop()
