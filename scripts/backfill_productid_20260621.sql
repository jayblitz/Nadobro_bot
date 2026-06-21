-- Backfill product_id on product-less manual venue fills (product_id = 0).
--
-- WHY: this venue's indexer match (SDK IndexerMatch) carries NO product_id, and
-- nado_sync._write_matches previously inserted manual/desk fills with product_id=0
-- (product_name 'ID:0'). The per-trade PnL card (compute_round_trips) keys its FIFO
-- on product_id, so product_id=0 fills can't be paired per product. The forward fix
-- (resolve product_id at insert from the recorder row / open_orders) ships in
-- nado_sync. This script recovers what history it can from open_orders by digest.
--
-- APPLIED TO PRODUCTION (NadoBro / gevenrdkwrodvojsszmr) 2026-06-21.
-- Recovered: 268 mainnet rows (testnet open_orders had no matching digests -> 0).
-- The rest remain product_id=0 (open_orders no longer holds those digests; only a
-- venue fill-ledger re-pull could recover them).
-- REVERSIBLE: pre-backfill product_id/product_name snapshotted in
-- trades_productid_backfill_bak_20260621 (restore block at the end).

-- 1) Reversible snapshot of every product-less manual fill (both networks).
CREATE TABLE IF NOT EXISTS trades_productid_backfill_bak_20260621 AS
SELECT id, 'mainnet'::text AS network, product_id, product_name, order_digest, now() AS backed_up_at
FROM trades_mainnet
WHERE COALESCE(product_id,0)=0 AND COALESCE(source,'manual')='manual' AND submission_idx IS NOT NULL
UNION ALL
SELECT id, 'testnet'::text AS network, product_id, product_name, order_digest, now()
FROM trades_testnet
WHERE COALESCE(product_id,0)=0 AND COALESCE(source,'manual')='manual' AND submission_idx IS NOT NULL;

-- 2) Recover product_id + name from the live open_orders row for the digest.
UPDATE trades_mainnet t SET
  product_id   = o.product_id,
  product_name = COALESCE(NULLIF(o.pair, ''), t.product_name)
FROM open_orders o
WHERE o.order_digest = t.order_digest AND o.user_id = t.user_id AND o.network = 'mainnet'
  AND COALESCE(o.product_id, 0) <> 0
  AND COALESCE(t.product_id, 0) = 0 AND COALESCE(t.source, 'manual') = 'manual'
  AND t.submission_idx IS NOT NULL;

UPDATE trades_testnet t SET
  product_id   = o.product_id,
  product_name = COALESCE(NULLIF(o.pair, ''), t.product_name)
FROM open_orders o
WHERE o.order_digest = t.order_digest AND o.user_id = t.user_id AND o.network = 'testnet'
  AND COALESCE(o.product_id, 0) <> 0
  AND COALESCE(t.product_id, 0) = 0 AND COALESCE(t.source, 'manual') = 'manual'
  AND t.submission_idx IS NOT NULL;

-- ROLLBACK (restore pre-backfill values):
--   UPDATE trades_mainnet t SET product_id = b.product_id, product_name = b.product_name
--   FROM trades_productid_backfill_bak_20260621 b WHERE b.id = t.id AND b.network='mainnet';
--   UPDATE trades_testnet t SET product_id = b.product_id, product_name = b.product_name
--   FROM trades_productid_backfill_bak_20260621 b WHERE b.id = t.id AND b.network='testnet';
