-- Copy trading privacy scoping (2026-05).
--
-- Custom copy-trade wallets added by one user must not be visible to other
-- users. We add `owner_user_id` to `copy_traders`:
--   * NULL = public/curated entry (managed by admins).
--   * non-NULL = private entry visible only to that telegram_id.
--
-- The legacy single-column UNIQUE on `wallet_address` would force every
-- user adding the same address to share a row (leaking visibility); we
-- replace it with two partial unique indexes that keep curated entries
-- globally unique while letting many users privately copy the same wallet.

ALTER TABLE copy_traders
    ADD COLUMN IF NOT EXISTS owner_user_id BIGINT;

-- Legacy personal wallets were created before owner scoping existed. If a
-- wallet was mirrored by exactly one user, preserve it as that user's private
-- row; otherwise deactivate non-curated ownerless rows so they no longer leak
-- through the public/curated query path.
UPDATE copy_traders ct
SET owner_user_id = owners.user_id
FROM (
    SELECT trader_id, MIN(user_id)::BIGINT AS user_id
    FROM copy_mirrors
    GROUP BY trader_id
    HAVING COUNT(DISTINCT user_id) = 1
) owners
WHERE ct.id = owners.trader_id
  AND ct.owner_user_id IS NULL
  AND COALESCE(ct.is_curated, false) = false;

UPDATE copy_traders
SET active = false
WHERE owner_user_id IS NULL
  AND COALESCE(is_curated, false) = false
  AND active = true;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'copy_traders_wallet_address_key'
    ) THEN
        ALTER TABLE copy_traders
            DROP CONSTRAINT copy_traders_wallet_address_key;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS copy_traders_curated_wallet_uq
    ON copy_traders (wallet_address)
    WHERE owner_user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS copy_traders_owner_wallet_uq
    ON copy_traders (owner_user_id, wallet_address)
    WHERE owner_user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_copy_traders_owner
    ON copy_traders (owner_user_id)
    WHERE owner_user_id IS NOT NULL;
