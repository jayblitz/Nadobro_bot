-- Portfolio history: tag every equity sample with the network it was taken on.
-- The sampler follows each user's ACTIVE network (SnapshotAccountProvider ->
-- get_portfolio_snapshot), so without this column a testnet<->mainnet switch
-- produced one interleaved equity series under PRIMARY KEY (user_id, ts).
-- Pre-existing rows were all effectively mainnet-or-unknown; they default to
-- 'mainnet' (the sampler's historical bias) rather than being dropped.
-- Idempotent Postgres migration.

ALTER TABLE engine_portfolio_history
    ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT 'mainnet';

-- Extend the primary key to (user_id, network, ts). Safe on existing data:
-- the old key (user_id, ts) is strictly finer than the new one once network
-- is a constant default, so no duplicates can exist. Guarded so re-running
-- the migration (or the inline startup DDL) is a no-op.
DO $$
DECLARE
    pk_cols text;
BEGIN
    SELECT string_agg(a.attname, ',' ORDER BY k.ord) INTO pk_cols
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
    WHERE t.relname = 'engine_portfolio_history' AND c.contype = 'p';

    IF pk_cols IS DISTINCT FROM 'user_id,network,ts' THEN
        EXECUTE 'ALTER TABLE engine_portfolio_history DROP CONSTRAINT IF EXISTS engine_portfolio_history_pkey';
        EXECUTE 'ALTER TABLE engine_portfolio_history ADD PRIMARY KEY (user_id, network, ts)';
    END IF;
END $$;
