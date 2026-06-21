-- Backfill: recompute strategy_sessions PnL/volume/fees aggregates with the
-- corrected, venue-derived, flat-aware logic (see fix/pnl-venue-realized-derivation).
--
-- WHY: this venue's indexer match carries NO per-fill realized PnL, so the old
-- code recorded realized_pnl_x18 = 0 on every fill and either showed PnL 0 (flat
-- runs) or booked raw notional as PnL (the "-$506"). Separately, the account-wide
-- stop flatten wrote ONE oversized synthetic `source='manual'` close, mis-tagged
-- to a session, inflating its signed cash flow. The corrected metric:
--   * count ONLY engine fills (exclude source='manual'),
--   * realized PnL = signed cash flow when the run is flat (|net_base| <= 1e-9),
--     else 0 (carried by the venue position's unrealized PnL).
-- This mirrors the fixed get_session_live_metrics so persisted == live.
--
-- APPLIED TO PRODUCTION (NadoBro / gevenrdkwrodvojsszmr) 2026-06-20.
-- Result: realized_pnl sum across affected sessions -1418.67 -> -0.18.
-- REVERSIBLE: pre-backfill values are snapshotted in
-- strategy_sessions_pnl_backfill_bak_20260620 (see restore block at the end).

-- 1) Reversible snapshot of every column the backfill may change.
CREATE TABLE IF NOT EXISTS strategy_sessions_pnl_backfill_bak_20260620 AS
SELECT id, user_id, strategy, network,
       realized_pnl, total_volume_usd, total_fees_paid, total_funding_paid,
       total_orders_filled, total_orders_cancelled, win_count, loss_count,
       now() AS backed_up_at
FROM strategy_sessions;

-- 2) Recompute mainnet engine sessions from trades_mainnet (exclude manual rows).
WITH agg AS (
  SELECT s.id,
    COUNT(*) FILTER (WHERE t.status IN ('filled','closed','partially_filled')) AS filled,
    COUNT(*) FILTER (WHERE t.status='cancelled') AS cancelled,
    COALESCE(SUM(ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0)),0) AS volume,
    COALESCE(SUM(COALESCE(t.fill_fee,t.fees,0)+COALESCE(t.builder_fee,0)),0) AS fees,
    COALESCE(SUM(CASE WHEN t.side='long'  THEN  ABS(COALESCE(NULLIF(t.base_filled_x18,0)/1e18,t.fill_size,t.size,0))
                      WHEN t.side='short' THEN -ABS(COALESCE(NULLIF(t.base_filled_x18,0)/1e18,t.fill_size,t.size,0)) ELSE 0 END),0) AS net_base,
    COALESCE(SUM(CASE WHEN t.side='short' THEN  ABS(COALESCE(NULLIF(t.quote_filled_x18,0)/1e18, ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0)))
                      WHEN t.side='long'  THEN -ABS(COALESCE(NULLIF(t.quote_filled_x18,0)/1e18, ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0))) ELSE 0 END),0) AS signed_cash
  FROM strategy_sessions s
  JOIN trades_mainnet t ON t.strategy_session_id = s.id AND COALESCE(t.source,'') <> 'manual'
  WHERE s.network='mainnet' AND s.strategy IN ('grid','rgrid','dgrid','mid','vol','dn')
  GROUP BY s.id
)
UPDATE strategy_sessions s SET
  realized_pnl = CASE WHEN ABS(a.net_base) <= 1e-9 THEN a.signed_cash ELSE 0 END,
  total_volume_usd = a.volume,
  total_fees_paid = a.fees,
  total_orders_filled = a.filled,
  total_orders_cancelled = a.cancelled
FROM agg a WHERE s.id = a.id;

-- 3) Recompute testnet engine sessions from trades_testnet (same logic).
WITH agg AS (
  SELECT s.id,
    COUNT(*) FILTER (WHERE t.status IN ('filled','closed','partially_filled')) AS filled,
    COUNT(*) FILTER (WHERE t.status='cancelled') AS cancelled,
    COALESCE(SUM(ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0)),0) AS volume,
    COALESCE(SUM(COALESCE(t.fill_fee,t.fees,0)+COALESCE(t.builder_fee,0)),0) AS fees,
    COALESCE(SUM(CASE WHEN t.side='long'  THEN  ABS(COALESCE(NULLIF(t.base_filled_x18,0)/1e18,t.fill_size,t.size,0))
                      WHEN t.side='short' THEN -ABS(COALESCE(NULLIF(t.base_filled_x18,0)/1e18,t.fill_size,t.size,0)) ELSE 0 END),0) AS net_base,
    COALESCE(SUM(CASE WHEN t.side='short' THEN  ABS(COALESCE(NULLIF(t.quote_filled_x18,0)/1e18, ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0)))
                      WHEN t.side='long'  THEN -ABS(COALESCE(NULLIF(t.quote_filled_x18,0)/1e18, ABS(COALESCE(t.fill_size,t.size,0))*COALESCE(NULLIF(t.fill_price,0),t.price,0))) ELSE 0 END),0) AS signed_cash
  FROM strategy_sessions s
  JOIN trades_testnet t ON t.strategy_session_id = s.id AND COALESCE(t.source,'') <> 'manual'
  WHERE s.network='testnet' AND s.strategy IN ('grid','rgrid','dgrid','mid','vol','dn')
  GROUP BY s.id
)
UPDATE strategy_sessions s SET
  realized_pnl = CASE WHEN ABS(a.net_base) <= 1e-9 THEN a.signed_cash ELSE 0 END,
  total_volume_usd = a.volume,
  total_fees_paid = a.fees,
  total_orders_filled = a.filled,
  total_orders_cancelled = a.cancelled
FROM agg a WHERE s.id = a.id;

-- 4) (verify) before/after for changed sessions:
--   SELECT s.id, b.realized_pnl AS old, s.realized_pnl AS new
--   FROM strategy_sessions s
--   JOIN strategy_sessions_pnl_backfill_bak_20260620 b ON b.id = s.id
--   WHERE round(b.realized_pnl::numeric,2) IS DISTINCT FROM round(s.realized_pnl::numeric,2);
--
-- ROLLBACK (restore pre-backfill values):
--   UPDATE strategy_sessions s SET
--     realized_pnl = b.realized_pnl, total_volume_usd = b.total_volume_usd,
--     total_fees_paid = b.total_fees_paid, total_funding_paid = b.total_funding_paid,
--     total_orders_filled = b.total_orders_filled,
--     total_orders_cancelled = b.total_orders_cancelled,
--     win_count = b.win_count, loss_count = b.loss_count
--   FROM strategy_sessions_pnl_backfill_bak_20260620 b WHERE b.id = s.id;
