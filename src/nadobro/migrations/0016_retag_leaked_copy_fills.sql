-- Re-tag copy fills that leaked into the History tab as source='manual'.
--
-- Root cause (fixed forward in nado_sync._write_matches): the venue-match
-- enrich gate was ``source IN ('strategy','manual')``, which skipped the
-- source='copy' recorder row. A copied fill's venue match therefore inserted a
-- NEW source='manual' duplicate carrying a submission_idx — and
-- compute_round_trips (History, manual-only) counts exactly
-- ``source='manual' AND submission_idx IS NOT NULL``, so every copied trade
-- surfaced in History (which is for NORMAL trades only) and double-counted.
--
-- These leaked duplicates are identifiable precisely: a source='manual' row
-- whose order_digest ALSO appears on a source='copy' row for the SAME user (a
-- legitimate manual trade has a unique digest shared with nothing). Re-tag them
-- to 'copy' so they leave History while their venue data is preserved.
-- Idempotent: a second run matches nothing (the rows are already 'copy').

UPDATE trades_testnet m SET source = 'copy'
WHERE m.source = 'manual'
  AND m.order_digest IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM trades_testnet c
    WHERE c.source = 'copy'
      AND c.order_digest = m.order_digest
      AND c.user_id = m.user_id
      AND c.id <> m.id
  );

UPDATE trades_mainnet m SET source = 'copy'
WHERE m.source = 'manual'
  AND m.order_digest IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM trades_mainnet c
    WHERE c.source = 'copy'
      AND c.order_digest = m.order_digest
      AND c.user_id = m.user_id
      AND c.id <> m.id
  );
