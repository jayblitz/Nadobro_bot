"""Grid Executor — multi-level grid with a per-level state machine:
NOT_ACTIVE → OPEN_ORDER_PLACED → OPEN_ORDER_FILLED → CLOSE_ORDER_PLACED →
COMPLETE. Uses ``activation_bounds`` to keep orders only near mid.

Implemented in Phase 3.
"""
