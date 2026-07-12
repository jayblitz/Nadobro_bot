"""Tests for documented Nado rate-limit weights (nado_weights)."""
from __future__ import annotations

from src.nadobro.venue.nado_weights import execute_weight, query_weight


def test_fixed_query_weights():
    assert query_weight("status") == 1
    assert query_weight("all_products") == 5
    assert query_weight("isolated_positions") == 10
    assert query_weight("linked_signer") == 5
    assert query_weight("nlp_pool_info") == 20


def test_subaccount_info_variants():
    assert query_weight("subaccount_info") == 2
    assert query_weight("subaccount_info", {"with_txns": True}) == 10
    assert query_weight("subaccount_info", {"with_txns": True, "pre_state": True}) == 15


def test_market_prices_scales_with_product_count():
    assert query_weight("market_price") == 1
    assert query_weight("market_prices", {"product_ids": [1, 2, 3, 4, 5]}) == 5
    # Empty/missing list still charges at least 1.
    assert query_weight("market_prices", {"product_ids": []}) == 1


def test_orders_weight_is_two_per_product():
    assert query_weight("subaccount_orders", {"product_id": 2}) == 2
    assert query_weight("orders", {"product_ids": [1, 2, 3]}) == 6


def test_archive_variable_weights_round_up():
    # candlesticks = 1 + limit/20 -> ceil
    assert query_weight("candlesticks", {"limit": 200}) == 11
    # matches = 2 + limit*subs/10
    assert query_weight("matches", {"limit": 200, "subaccounts": ["a"]}) == 22
    # archive orders = 2 + limit*subs/20
    assert query_weight("archive_orders", {"limit": 200, "subaccounts": ["a"]}) == 12


def test_unknown_query_uses_conservative_default():
    assert query_weight("totally_made_up") == 5


def test_place_order_leverage_weights():
    assert execute_weight("place_order") == 1
    assert execute_weight("place_order", {"spot_leverage": True}) == 1
    assert execute_weight("place_order", {"spot_leverage": False}) == 20


def test_cancel_weights():
    assert execute_weight("cancel_orders") == 1
    assert execute_weight("cancel_orders", {"digests": ["a", "b", "c"]}) == 3
    assert execute_weight("cancel_product_orders") == 50
    assert execute_weight("cancel_product_orders", {"product_ids": [1, 2]}) == 10


def test_no_leverage_place_naturally_caps_at_30_per_min():
    """20 weight x 30 places == 600/min, the documented no-leverage cap."""
    assert execute_weight("place_order", {"spot_leverage": False}) * 30 == 600


def test_other_execute_weights():
    assert execute_weight("link_signer") == 30
    assert execute_weight("liquidate_subaccount") == 20
    assert execute_weight("mint_nlp") == 10
    assert execute_weight("transfer_quote") == 10
    assert execute_weight("unknown_execute") == 20
