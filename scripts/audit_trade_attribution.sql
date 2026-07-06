-- Trade-attribution audit (read-only) — run against the production DB
-- (psql "$SUPABASE_DATABASE_URL" -f scripts/audit_trade_attribution.sql)
-- Substitute :uid with the telegram user id, and adjust the trades table
-- (trades_mainnet / trades_testnet) to the network being audited.

-- 1) Phantom-History candidates: venue-confirmed fills tagged manual with no
--    session. Rows here that coincide with strategy activity are strategy/close
--    fills that arrived UNTAGGED (the bug fixed by intent-linking closes +
--    gating the window fallback). After the fix, new rows here should be ONLY
--    genuine manual trades.
SELECT id, product_name, side, fill_size, fill_price, fill_fee,
       order_digest, submission_idx, filled_at
FROM trades_mainnet
WHERE user_id = :uid
  AND submission_idx IS NOT NULL
  AND COALESCE(source, 'manual') = 'manual'
  AND strategy_session_id IS NULL
ORDER BY filled_at DESC
LIMIT 50;

-- 2) Swallowed manual fills: session-tagged but source=manual — invisible on
--    EVERY surface (History excludes session-tagged; rollup excludes manual).
--    These are the user's manual trades absorbed by the window fallback, or
--    engine fills the fallback attributed without relabeling. Historical rows
--    predating the fix will remain until repaired.
SELECT id, product_name, side, fill_size, fill_price, strategy_session_id,
       source, order_digest, submission_idx, filled_at
FROM trades_mainnet
WHERE user_id = :uid
  AND strategy_session_id IS NOT NULL
  AND COALESCE(source, 'manual') = 'manual'
  AND submission_idx IS NOT NULL
ORDER BY filled_at DESC
LIMIT 50;

-- 3) Open↔close linkage for manual trades: every close row should carry
--    open_trade_id; a NULL here means the close was recorded without a
--    resolvable open (find_open_trade heuristic miss).
SELECT id, open_trade_id, product_name, side, size, close_price, realized_pnl,
       order_digest, closed_at
FROM trades_mainnet
WHERE user_id = :uid
  AND order_type ILIKE '%close%'
ORDER BY closed_at DESC
LIMIT 50;

-- 4) Close digests now tagged: after the fix, every bot close leaves an
--    order_intents row (intent_id 'close:<network>:<digest>'). Verify new
--    closes appear here; zero rows for post-deploy closes = link regression.
SELECT intent_id, order_digest, value, updated_at
FROM order_intents
WHERE intent_id LIKE 'close:%'
ORDER BY updated_at DESC
LIMIT 30;

-- 5) Session totals vs raw fills: for a given session :sid, the stored rollup
--    should equal the recomputation over its non-manual fills (incl. the close
--    turnover). A mismatch means fills are still arriving unattributed.
SELECT s.id, s.strategy, s.total_volume_usd AS stored_volume,
       s.total_fees_paid AS stored_fees, s.stopped_at,
       agg.fills, agg.volume AS recomputed_volume, agg.fees AS recomputed_fees
FROM strategy_sessions s
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS fills,
           COALESCE(SUM(COALESCE(ABS(NULLIF(quote_filled_x18, 0)) / 1e18,
                    ABS(COALESCE(fill_size, size, 0)) * COALESCE(NULLIF(fill_price, 0), price, 0))), 0) AS volume,
           COALESCE(SUM(COALESCE(NULLIF(fee_x18, 0) / 1e18,
                    COALESCE(fill_fee, fees, 0) + COALESCE(builder_fee, 0))), 0) AS fees
    FROM trades_mainnet t
    WHERE t.strategy_session_id = s.id
      AND COALESCE(t.source, '') <> 'manual'
) agg ON TRUE
WHERE s.id = :sid;

-- 6) Fills of a session that landed AFTER stopped_at (the close turnover):
--    these must be source='strategy' + session-tagged to count. Any 'manual'
--    or NULL-session row here is a leaked flatten fill (pre-fix data).
SELECT t.id, t.source, t.strategy_session_id, t.order_digest, t.fill_size,
       t.fill_price, t.filled_at, s.stopped_at
FROM trades_mainnet t
JOIN strategy_sessions s ON s.id = :sid
WHERE t.user_id = :uid
  AND t.product_id = s.product_id
  AND t.filled_at >= s.stopped_at
  AND t.filled_at < s.stopped_at + INTERVAL '10 minutes'
  AND t.submission_idx IS NOT NULL
ORDER BY t.filled_at;
