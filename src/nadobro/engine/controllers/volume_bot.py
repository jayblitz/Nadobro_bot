"""Volume Bot controller — spot-only wash-cycle. Hard-coded to spot
(leverage 1); perp configs are rejected at construction. The list of
*supported pairs* is sourced from the live Nado spot catalog (see
``services.product_catalog.list_volume_spot_bases``) per execution mode, so
new testnet/mainnet listings (e.g. ``QQQX``, ``SPYX``) are picked up
automatically without code edits. Each cycle runs a MAKER TWAP buy half then a
MAKER TWAP sell-cleanup half, and the controller repeats cycles until the user's
cumulative ``target_volume_usd`` is met (or a single round-trip when no target
is set). A hard ``max_cycles`` ceiling bounds fee burn if the target is mis-set,
and the session SL/TP rail (bot_runtime) halts a losing run on the user's
``sl_pct``.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.twap_executor import TWAPExecutor, TWAPExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import TradeType, _dec

# Quote-like symbols that must never be selected as a base for Volume.
# Kept in sync with ``product_catalog._QUOTE_LIKE_SYMBOLS``.
_QUOTE_LIKE_BASES = frozenset({"USDC", "USDC0", "USDT", "USDT0", "USD"})


class VolumeBotController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="volume_bot", **kwargs)  # type: ignore[arg-type]
        raw_pair = str(self.cfg("trading_pair") or "").strip().upper()
        market = str(self.cfg("market", "spot")).lower()
        leverage = int(self.cfg("leverage", 1))
        if market != "spot":
            raise ValueError("VolumeBotController is spot-only (market must be 'spot')")
        if leverage != 1:
            raise ValueError("VolumeBotController is spot-only (leverage must be 1)")
        if not raw_pair:
            raise ValueError("VolumeBotController requires a non-empty trading_pair")
        # Reject perps explicitly so a mis-routed configuration fails loudly.
        if raw_pair.endswith("-PERP") or raw_pair.endswith("PERP"):
            raise ValueError(f"{raw_pair} is a perp; VolumeBotController is spot-only")
        # Reject a quote-only "pair" (e.g. trading_pair="USDC" means trade USDC
        # against the quote, which is a no-op stable/stable trade and the venue
        # has no spot book for it). The base is everything before the dash if a
        # dashed pair (e.g. ``KBTC-USDC0`` -> base ``KBTC``) was supplied.
        base = raw_pair.split("-", 1)[0]
        if base in _QUOTE_LIKE_BASES:
            raise ValueError(
                f"{raw_pair} is a quote-like asset and not a valid Volume spot base"
            )
        self.trading_pair = raw_pair
        self.total_amount_quote = _dec(self.cfg("total_amount_quote", "50"))
        self.total_duration = float(self.cfg("total_duration", 600))
        self.order_interval = float(self.cfg("order_interval", 60))
        # VOL-LOOP fix: keep cycling buy->sell until the user's cumulative VOLUME
        # target is met, instead of doing one round-trip and idling forever.
        # 0 / unset = legacy single round-trip. VOL-NO-CAP fix: a hard max-cycles
        # safety ceiling so a mis-set target can't burn fees unbounded.
        self.target_volume_usd = _dec(self.cfg("target_volume_usd", "0"))
        self.max_cycles = max(1, int(self.cfg("max_cycles", 100) or 100))
        self.session_volume_usd: Decimal = Decimal(0)
        self.cycles_completed = 0
        # Completion signal drained by run_engine_cycle so bot_runtime can
        # finalize the session (was: never set, so the strategy idled "running").
        self.completed = False
        self.stop_reason = ""
        self.phase = "buying"
        self.buy_id: Optional[str] = None
        self.sell_id: Optional[str] = None

    def _twap(self, side: TradeType, amount_quote: Decimal) -> TWAPExecutor:
        cfg = TWAPExecutorConfig(
            self.trading_pair, side, amount_quote, self.total_duration,
            self.order_interval, mode="MAKER", leverage=1,
        )
        return TWAPExecutor(cfg, user_id=self.user_id, controller_id=self.id,
                            adapter=self.adapter, inventory=self.inventory)

    def _target_reached(self) -> bool:
        return self.target_volume_usd > 0 and self.session_volume_usd >= self.target_volume_usd

    def _complete(self, reason: str) -> None:
        """Finalize the run: signal completion and stop the controller so the
        engine tears it down instead of ticking a do-nothing 'done' phase."""
        self.phase = "done"
        self.completed = True
        self.stop_reason = reason
        self._set_stopped()

    async def _start_buy_cycle(self) -> bool:
        ex = self._twap(TradeType.BUY, self.total_amount_quote)
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=self.total_amount_quote)
        )
        if ok:
            self.buy_id = ex.id
            self.sell_id = None
            self.phase = "buying"
        return ok

    async def on_start(self) -> None:
        await self._start_buy_cycle()

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        if self.phase == "buying" and self.buy_id is not None:
            buy_ex = self.orchestrator.get(self.buy_id)
            if buy_ex is not None and buy_ex.is_terminated:
                bought = getattr(buy_ex, "filled_quote", Decimal(0)) or Decimal(0)
                self.session_volume_usd += bought
                if bought > 0:
                    sell = self._twap(TradeType.SELL, bought)
                    ok = await self.spawn_executor(
                        sell, ExecutorRequest(order_amount_quote=bought)
                    )
                    if ok:
                        self.sell_id = sell.id
                        self.phase = "selling"
                    else:
                        # Couldn't place the sell — finish rather than strand a
                        # bought position un-cleaned (it remains for the user).
                        self._complete("sell_spawn_failed")
                else:
                    self._complete("no_fill")
        elif self.phase == "selling" and self.sell_id is not None:
            sell_ex = self.orchestrator.get(self.sell_id)
            if sell_ex is not None and sell_ex.is_terminated:
                self.session_volume_usd += getattr(sell_ex, "filled_quote", Decimal(0)) or Decimal(0)
                self.cycles_completed += 1
                # Decide: loop again or finish. Legacy single round-trip when no
                # target is set; otherwise loop until target volume or the
                # safety cap.
                if self.target_volume_usd <= 0:
                    self._complete("round_trip_complete")
                elif self._target_reached():
                    self._complete("target_volume_hit")
                elif self.cycles_completed >= self.max_cycles:
                    self._complete("max_cycles")
                else:
                    if not await self._start_buy_cycle():
                        self._complete("buy_spawn_failed")
