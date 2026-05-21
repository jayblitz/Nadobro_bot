"""Risk Engine — pre-tick gates (daily_pnl floor, drawdown cap, daily cost
cap) and per-executor gates (max_open_executors, max_single_order_quote,
max_position_size_quote). Single kill switch persisted to DB so process
restarts honor it.

Implemented in Phase 1.
"""
