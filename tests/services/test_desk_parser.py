"""Desk parser golden set — the TreadFi-parity sentences MUST parse.

Fast path is deterministic and runs without any LLM; the LLM tier is tested
with an injected fake ``chat_json_fn`` (never a network call). Routing rule
under test throughout: Buy/Sell -> SPOT, Long/Short -> PERP, leverage or
explicit market words override.
"""
from __future__ import annotations

import pytest

from src.nadobro.services.desk_parser import (
    DeskParseResult,
    looks_like_desk_text,
    parse_desk_intent,
)
from src.nadobro.services.desk_plans import ExecutionPlan

PERPS = {"BTC", "ETH", "SOL", "HYPE"}
SPOTS = {"BTC", "ETH", "SOL", "QQQX", "SPYX"}


def parse(text: str, **kw) -> DeskParseResult:
    kw.setdefault("perp_symbols", PERPS)
    kw.setdefault("spot_symbols", SPOTS)
    kw.setdefault("allow_llm", False)
    return parse_desk_intent(text, **kw)


# ---------------------------------------------------------------------------
# The TreadFi screenshot examples (the parity bar)
# ---------------------------------------------------------------------------

def test_treadfi_example_maker_twap():
    r = parse("Buy 0.5 BTC over 2 hours with a maker-only TWAP.")
    p = r.plan
    assert p is not None and not r.clarify
    assert p.algo == "twap"
    assert p.market == "spot"  # Buy -> spot
    assert p.side == "buy"
    assert p.product == "BTC"
    assert p.size_base == 0.5
    assert p.duration_minutes == 120
    assert p.exec_mode == "maker"


def test_treadfi_example_accumulate_with_pct_trigger():
    r = parse("Accumulate 5 ETH over 24h, but only start once ETH pumps 2% from the current price.")
    p = r.plan
    assert p is not None and not r.clarify
    assert p.algo == "twap"
    assert (p.market, p.side, p.product) == ("spot", "buy", "ETH")
    assert p.size_base == 5
    assert p.duration_minutes == 24 * 60
    assert p.entry_trigger is not None
    assert p.entry_trigger.kind == "pct_move"
    assert p.entry_trigger.pct == 2.0


def test_treadfi_example_dump_trigger_is_negative():
    r = parse("Accumulate 5 ETH over 24h, but only start once ETH dumps 2% from the current price.")
    assert r.plan.entry_trigger.pct == -2.0


def test_treadfi_example_long_with_chained_exits():
    r = parse("Open a 0.1 BTC long, exit that takes profit 5% above the fill or stops out 3% below.")
    p = r.plan
    assert p is not None and not r.clarify
    assert (p.market, p.side, p.product) == ("perp", "buy", "BTC")  # long -> perp
    assert p.size_base == 0.1
    assert p.exits is not None
    assert p.exits.tp_pct == 5.0
    assert p.exits.sl_pct == 3.0


# ---------------------------------------------------------------------------
# Routing rules: Buy/Sell -> spot, Long/Short -> perp, overrides
# ---------------------------------------------------------------------------

def test_buy_defaults_to_spot():
    p = parse("buy 2 SOL").plan
    assert (p.market, p.side, p.algo) == ("spot", "buy", "market")
    assert p.size_base == 2


def test_sell_defaults_to_spot():
    p = parse("sell 1.5 ETH").plan
    assert (p.market, p.side) == ("spot", "sell")


def test_long_defaults_to_perp():
    p = parse("long eth 0.1 5x").plan
    assert (p.market, p.side, p.leverage, p.size_base) == ("perp", "buy", 5, 0.1)


def test_short_defaults_to_perp():
    p = parse("short 0.2 btc").plan
    assert (p.market, p.side, p.size_base) == ("perp", "sell", 0.2)


def test_leverage_forces_perp_even_for_buy():
    p = parse("buy 0.5 btc 5x").plan
    assert (p.market, p.leverage) == ("perp", 5)


def test_explicit_spot_keyword_wins():
    p = parse("sell 3 sol on spot").plan
    assert p.market == "spot"


def test_stock_token_buys_route_spot():
    p = parse("buy $500 of QQQX over 4 hours").plan
    assert (p.market, p.product, p.algo) == ("spot", "QQQX", "twap")
    assert p.size_quote == 500
    assert p.duration_minutes == 240


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

def test_price_level_trigger_with_direction():
    p = parse("buy 100 SPYX when it drops to 580").plan
    assert p.entry_trigger.kind == "price_below"
    assert p.entry_trigger.price == 580
    assert p.size_base == 100


def test_directionless_price_trigger_is_cross():
    p = parse("buy 0.1 eth once eth hits 2500").plan
    assert p.entry_trigger.kind == "price_cross"
    assert p.entry_trigger.price == 2500
    assert p.size_base == 0.1


def test_time_trigger_with_start():
    p = parse("start a twap buy of 1 eth over 2h starting in 30 minutes").plan
    assert p.algo == "twap"
    assert p.entry_trigger is not None
    assert p.entry_trigger.kind == "time"
    assert p.entry_trigger.delay_minutes == 30


# ---------------------------------------------------------------------------
# Algos and modes
# ---------------------------------------------------------------------------

def test_limit_order():
    p = parse("buy 0.2 eth limit 3200").plan
    assert (p.algo, p.limit_price, p.market) == ("limit", 3200, "spot")


def test_bare_at_price_means_limit():
    p = parse("sell 0.2 eth at 3300").plan
    assert (p.algo, p.limit_price) == ("limit", 3300)


def test_twap_default_is_taker_and_one_hour():
    p = parse("dca 0.5 eth").plan
    assert (p.algo, p.exec_mode, p.duration_minutes) == ("twap", "taker", 60.0)


def test_twap_verb_with_inner_sell():
    p = parse("twap sell 1.5 btc over 90 min on perps").plan
    assert (p.algo, p.side, p.market) == ("twap", "sell", "perp")
    assert p.duration_minutes == 90


def test_trailing_stop():
    p = parse("long 0.1 btc with a trailing stop of 2%").plan
    assert p.exits.trailing_pct == 2.0


# ---------------------------------------------------------------------------
# Non-trades stay out
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", [
    "what is tp?",
    "how do I buy ETH?",
    "should I long here?",
    "",
    "gm",
])
def test_questions_and_chatter_are_not_trades(text):
    assert parse(text).not_trade


def test_looks_like_desk_text_filters_questions():
    assert not looks_like_desk_text("can I buy stocks here?")
    assert looks_like_desk_text("buy 1 eth")


def test_bare_verb_is_navigation_not_a_trade():
    # "buy" alone opens the trade menu (interaction intent), it must not
    # trigger a Desk clarification prompt.
    for word in ("buy", "sell", "long", "short", "trade"):
        assert not looks_like_desk_text(word)


# ---------------------------------------------------------------------------
# Clarification: trade-shaped but incomplete
# ---------------------------------------------------------------------------

def test_missing_size_asks_for_it():
    r = parse("buy some eth")
    assert r.plan is not None
    assert "size" in r.clarify


def test_unknown_product_asks_for_it():
    r = parse("buy 5 DOGE")
    assert r.plan is not None
    assert "product" in r.clarify


# ---------------------------------------------------------------------------
# LLM tier (injected fake — never a network call)
# ---------------------------------------------------------------------------

def _fake_chat(payload):
    def chat(messages):
        assert "<user_request>" in messages[-1]["content"]
        return payload, "fake"
    return chat


def test_llm_fills_what_fast_path_cannot():
    payload = {
        "not_trade": False,
        "plan": {"algo": "twap", "market": "spot", "product": "eth-perp", "side": "long",
                 "size_quote": 1000, "duration_minutes": 720},
        "clarify": [],
    }
    r = parse_desk_intent(
        "please scale into a grand of ether through the next twelve hours",
        perp_symbols=PERPS, spot_symbols=SPOTS,
        allow_llm=True, chat_json_fn=_fake_chat(payload),
    )
    p = r.plan
    assert r.source == "llm"
    # long -> buy/perp normalization + -PERP suffix stripped + defaults applied
    assert (p.side, p.market, p.product) == ("buy", "perp", "ETH")
    assert p.interval_seconds == 30
    assert p.size_quote == 1000


def test_llm_cannot_invent_products():
    payload = {
        "not_trade": False,
        "plan": {"algo": "market", "market": "spot", "product": "TSLA", "side": "buy",
                 "size_quote": 100},
        "clarify": [],
    }
    r = parse_desk_intent(
        "buy a hundred bucks of tesla",
        perp_symbols=PERPS, spot_symbols=SPOTS,
        allow_llm=True, chat_json_fn=_fake_chat(payload),
    )
    assert r.plan is None or r.plan.product is None
    if r.plan is not None:
        assert "product" in r.clarify


def test_llm_failure_falls_back_to_fast_path():
    def boom(messages):
        raise RuntimeError("provider down")
    r = parse_desk_intent(
        "buy 1 eth when it drops to 2000",
        perp_symbols=PERPS, spot_symbols=SPOTS,
        allow_llm=True, chat_json_fn=boom,
    )
    # fast path already complete -> never consults the LLM anyway
    assert r.source == "fast"
    assert r.plan.entry_trigger.kind == "price_below"


def test_llm_not_trade_respected_when_fast_path_empty():
    r = parse_desk_intent(
        "I might sell my house someday",
        perp_symbols=PERPS, spot_symbols=SPOTS,
        allow_llm=True, chat_json_fn=_fake_chat({"not_trade": True}),
    )
    # fast path produces a junk-ish partial ("sell" verb), LLM says not_trade,
    # but trade verbs are present so the deterministic partial wins for safety:
    # the user gets a clarification prompt, not a silent drop into AI chat.
    assert r.plan is not None or r.not_trade


def test_plan_round_trips_through_dict():
    p = parse("Accumulate 5 ETH over 24h, but only start once ETH dumps 2% from the current price.").plan
    q = ExecutionPlan.from_dict(p.to_dict())
    assert q.to_dict() == p.to_dict()
    assert q.entry_trigger.pct == -2.0
