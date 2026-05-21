"""Order Executor — places a single order using one of LIMIT, LIMIT_MAKER,
MARKET, or LIMIT_CHASER strategies.

LIMIT_CHASER repositions when mid moves past ``refresh_threshold``, capped at
50 replacements. Fills are routed into Inventory on terminate.

Implemented in Phase 1.
"""
