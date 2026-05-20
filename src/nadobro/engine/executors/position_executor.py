"""Position Executor — places an entry order and manages the resulting
position using the Triple Barrier method (take_profit, stop_loss, time_limit,
optional trailing_stop with activation_price + trailing_delta).

States: OPENING → ACTIVE_POSITION → CLOSING → TERMINATED. Barrier evaluation
order on each tick: time_limit, stop_loss, trailing_stop (if armed),
take_profit. First barrier hit wins; the closing order uses that barrier's
order type and sets ``close_type`` accordingly.

Implemented in Phase 1.
"""
