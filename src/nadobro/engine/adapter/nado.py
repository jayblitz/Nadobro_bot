"""Nado execution adapter — wraps ``connectors/nado`` and the 1CT Linked
Signer to provide ``place_order``, ``cancel_order``, ``order_status``,
``fill_stream``, ``order_book``, ``mid_price``, and ``tick_size`` /
``lot_size`` / ``min_notional`` helpers.

This is the ONLY module in the engine permitted to import from
``src/nadobro/connectors/nado/``. See
``tests/lint/test_adapter_isolation.py``.

Implemented in Phase 1.
"""
