-- Repair fills whose HUMAN columns were never backfilled.
--
-- The submit-time fill resolve can miss (indexer lag) and the retry queue can
-- expire; the venue match sync then stamped the recorder row with
-- submission_idx + the x18 amounts but left fill_price/fill_size/fill_fee at
-- 0/NULL. Every surface that reads fill_size * fill_price (History round
-- trips, session volume rollups) then saw $0 legs: round trips rendered as
-- "entry @ $0.00" with realized PnL equal to the full exit notional, and
-- session volume undercounted.
--
-- The venue's own quote/base ARE stored on those rows (x18), so the human
-- columns are derived from them here. Rows without x18 amounts carry no venue
-- price and are left untouched (the read layer skips price-less fills rather
-- than fabricating $0 entries). Idempotent: the CASE guards only fill missing
-- values, so a second run matches nothing.

UPDATE trades_testnet SET
  fill_size = CASE WHEN COALESCE(fill_size, 0) = 0
      THEN ABS(base_filled_x18::numeric) / 1e18 ELSE fill_size END,
  fill_price = CASE WHEN COALESCE(fill_price, 0) = 0
      THEN ABS(quote_filled_x18::numeric / NULLIF(base_filled_x18, 0)::numeric) ELSE fill_price END,
  price = CASE WHEN COALESCE(price, 0) = 0
      THEN ABS(quote_filled_x18::numeric / NULLIF(base_filled_x18, 0)::numeric) ELSE price END,
  fill_fee = CASE WHEN COALESCE(fill_fee, 0) = 0
      THEN COALESCE(ABS(fee_x18::numeric), 0) / 1e18 ELSE fill_fee END
WHERE submission_idx IS NOT NULL
  AND COALESCE(base_filled_x18, 0) <> 0
  AND COALESCE(quote_filled_x18, 0) <> 0
  AND (COALESCE(fill_price, 0) = 0 OR COALESCE(fill_size, 0) = 0 OR COALESCE(price, 0) = 0);

UPDATE trades_mainnet SET
  fill_size = CASE WHEN COALESCE(fill_size, 0) = 0
      THEN ABS(base_filled_x18::numeric) / 1e18 ELSE fill_size END,
  fill_price = CASE WHEN COALESCE(fill_price, 0) = 0
      THEN ABS(quote_filled_x18::numeric / NULLIF(base_filled_x18, 0)::numeric) ELSE fill_price END,
  price = CASE WHEN COALESCE(price, 0) = 0
      THEN ABS(quote_filled_x18::numeric / NULLIF(base_filled_x18, 0)::numeric) ELSE price END,
  fill_fee = CASE WHEN COALESCE(fill_fee, 0) = 0
      THEN COALESCE(ABS(fee_x18::numeric), 0) / 1e18 ELSE fill_fee END
WHERE submission_idx IS NOT NULL
  AND COALESCE(base_filled_x18, 0) <> 0
  AND COALESCE(quote_filled_x18, 0) <> 0
  AND (COALESCE(fill_price, 0) = 0 OR COALESCE(fill_size, 0) = 0 OR COALESCE(price, 0) = 0);
