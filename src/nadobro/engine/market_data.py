"""Market Data — cached access to the Nado order book, candles, funding, and
mark price. Consumed by controllers and routines; always routed through the
engine adapter, never directly through ``connectors/nado/``.

Implemented in Phase 4.
"""
