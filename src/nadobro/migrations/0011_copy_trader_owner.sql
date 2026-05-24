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
