"""Position Hold inventory — per-(user_id, trading_pair, controller_id)
aggregation of buy/sell base and quote amounts plus cumulative fees, with
derived breakeven, realized PnL (matched min of base in/out), and unrealized
PnL. Fills are applied atomically inside a DB transaction.

Implemented in Phase 1.
"""
