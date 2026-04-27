import logging
import os
import time
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from src.nadobro.services.trade_service import (
    get_local_position_hints,
    get_open_limit_orders,
    get_trade_analytics,
)
from src.nadobro.services.user_service import get_user, get_user_readonly_client

logger = logging.getLogger(__name__)

_SNAPSHOT_TTL_SECONDS = float(os.environ.get("PORTFOLIO_SNAPSHOT_TTL_SECONDS", "2.0"))
_SNAPSHOT_CACHE_MAX_ENTRIES = int(os.environ.get("PORTFOLIO_SNAPSHOT_CACHE_MAX_ENTRIES", "256"))
_snapshot_cache: dict[tuple[int, str], dict[str, Any]] = {}


@dataclass
class PortfolioSnapshot:
    """Bot-first portfolio read model used by Telegram and future API/workflow consumers."""

    telegram_id: int
    network: str
    positions: list[dict] = field(default_factory=list)
    prices: dict | None = None
    stats: dict = field(default_factory=dict)
    open_orders: list[dict] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    from_cache: bool = False


class PortfolioSnapshotUnavailable(RuntimeError):
    pass


def _snapshot_clone(snapshot: PortfolioSnapshot, *, from_cache: bool | None = None) -> PortfolioSnapshot:
    cloned = PortfolioSnapshot(
        telegram_id=snapshot.telegram_id,
        network=snapshot.network,
        positions=deepcopy(snapshot.positions),
        prices=deepcopy(snapshot.prices),
        stats=deepcopy(snapshot.stats),
        open_orders=deepcopy(snapshot.open_orders),
        timings=dict(snapshot.timings),
        from_cache=snapshot.from_cache if from_cache is None else from_cache,
    )
    return cloned


def _prune_snapshot_cache(now: float | None = None) -> None:
    now = time.time() if now is None else now
    stale = [
        key for key, value in _snapshot_cache.items()
        if now - float(value.get("ts", 0.0)) >= _SNAPSHOT_TTL_SECONDS
    ]
    for key in stale:
        _snapshot_cache.pop(key, None)
    while len(_snapshot_cache) > max(1, _SNAPSHOT_CACHE_MAX_ENTRIES):
        oldest = min(_snapshot_cache.items(), key=lambda item: float(item[1].get("ts", 0.0)))[0]
        _snapshot_cache.pop(oldest, None)


def clear_portfolio_snapshot_cache(telegram_id: int | None = None, network: str | None = None) -> None:
    if telegram_id is None:
        _snapshot_cache.clear()
        return
    prefix = int(telegram_id)
    for key in list(_snapshot_cache.keys()):
        if key[0] == prefix and (network is None or key[1] == str(network)):
            _snapshot_cache.pop(key, None)


def _merge_position_hints(telegram_id: int, network: str, live_positions: list[dict]) -> list[dict]:
    positions = list(live_positions or [])
    try:
        live_product_ids = {
            int(p.get("product_id"))
            for p in positions
            if p.get("product_id") is not None
        }
        missing_hints = [
            p for p in get_local_position_hints(telegram_id, network=network)
            if int(p.get("product_id")) not in live_product_ids
        ]
        if missing_hints:
            positions.extend(missing_hints)
    except Exception as e:
        logger.debug("portfolio local position hints failed user=%s err=%s", telegram_id, e)
    return positions


def get_portfolio_snapshot(telegram_id: int, *, force_refresh: bool = False) -> PortfolioSnapshot:
    started = time.perf_counter()
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    cache_key = (int(telegram_id), network)
    now = time.time()

    _prune_snapshot_cache(now)
    if not force_refresh:
        cached = _snapshot_cache.get(cache_key)
        if cached and now - float(cached.get("ts", 0.0)) < _SNAPSHOT_TTL_SECONDS:
            return _snapshot_clone(cached["snapshot"], from_cache=True)

    client = get_user_readonly_client(telegram_id, network=network)
    if not client:
        raise PortfolioSnapshotUnavailable("Wallet not initialized")

    timings: dict[str, float] = {}
    try:
        t0 = time.perf_counter()
        live_positions = client.get_all_positions() or []
        timings["positions"] = time.perf_counter() - t0
    except Exception as e:
        raise PortfolioSnapshotUnavailable(f"Portfolio positions unavailable: {e}") from e

    prices = None
    try:
        t0 = time.perf_counter()
        prices = client.get_all_market_prices()
        timings["prices"] = time.perf_counter() - t0
    except Exception as e:
        logger.debug("portfolio prices unavailable user=%s err=%s", telegram_id, e)
        timings["prices"] = 0.0

    try:
        t0 = time.perf_counter()
        stats = get_trade_analytics(telegram_id)
        timings["stats"] = time.perf_counter() - t0
    except Exception as e:
        logger.warning("portfolio stats unavailable user=%s err=%s", telegram_id, e)
        stats = {}
        timings["stats"] = 0.0

    try:
        t0 = time.perf_counter()
        open_orders = get_open_limit_orders(telegram_id, refresh=force_refresh)
        timings["orders"] = time.perf_counter() - t0
    except Exception as e:
        logger.warning("portfolio open orders unavailable user=%s err=%s", telegram_id, e)
        open_orders = []
        timings["orders"] = 0.0

    positions = _merge_position_hints(telegram_id, network, live_positions)
    timings["total"] = time.perf_counter() - started
    snapshot = PortfolioSnapshot(
        telegram_id=int(telegram_id),
        network=network,
        positions=positions,
        prices=prices,
        stats=stats or {},
        open_orders=open_orders or [],
        timings=timings,
    )
    _snapshot_cache[cache_key] = {"snapshot": _snapshot_clone(snapshot), "ts": time.time()}
    _prune_snapshot_cache()
    return snapshot
