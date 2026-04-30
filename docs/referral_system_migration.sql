-- Nadobro Referral System Migration
-- Adds Nado-style volume-earned referral invite codes and direct referred volume tracking.

BEGIN;

ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS public_code TEXT;
ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS code_type TEXT NOT NULL DEFAULT 'private_access';
ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS referrer_user_id BIGINT;
ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS earned_volume_threshold_usd DOUBLE PRECISION;
ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS sequence_number INT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_invite_codes_public_code
  ON invite_codes (public_code)
  WHERE public_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_invite_codes_referrer
  ON invite_codes (referrer_user_id, code_type, active);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'invite_codes_code_type_check'
  ) THEN
    ALTER TABLE invite_codes
      ADD CONSTRAINT invite_codes_code_type_check
      CHECK (code_type IN ('private_access', 'referral'));
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS referrals (
  id BIGSERIAL PRIMARY KEY,
  referrer_user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
  referred_user_id BIGINT NOT NULL UNIQUE REFERENCES users(telegram_id) ON DELETE CASCADE,
  invite_code_id BIGINT REFERENCES invite_codes(id),
  referred_username TEXT,
  referred_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  referred_trade_count INT NOT NULL DEFAULT 0,
  first_trade_at TIMESTAMPTZ,
  last_trade_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  CHECK (referrer_user_id <> referred_user_id),
  CHECK (referred_volume_usd >= 0),
  CHECK (referred_trade_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_referrals_referrer
  ON referrals (referrer_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_referrals_referred
  ON referrals (referred_user_id);

CREATE TABLE IF NOT EXISTS referral_volume_events (
  id BIGSERIAL PRIMARY KEY,
  referral_id BIGINT NOT NULL REFERENCES referrals(id) ON DELETE CASCADE,
  referrer_user_id BIGINT NOT NULL,
  referred_user_id BIGINT NOT NULL,
  volume_usd DOUBLE PRECISION NOT NULL,
  source TEXT NOT NULL DEFAULT 'trade_stats',
  created_at TIMESTAMPTZ DEFAULT now(),
  CHECK (volume_usd >= 0)
);

CREATE INDEX IF NOT EXISTS idx_referral_volume_events_referrer
  ON referral_volume_events (referrer_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_referral_volume_events_referred
  ON referral_volume_events (referred_user_id, created_at DESC);

-- Sample data: one earned referral invite and one linked referred user.
-- Replace Telegram IDs before running against a real environment.
INSERT INTO users (telegram_id, telegram_username, total_volume_usd, private_access_granted)
VALUES
  (100001, 'referrer_demo', 12500, true),
  (100002, 'referred_demo', 2500, true)
ON CONFLICT (telegram_id) DO UPDATE
SET telegram_username = EXCLUDED.telegram_username;

INSERT INTO invite_codes (
  code_hash,
  public_code,
  code_type,
  code_prefix,
  created_by,
  referrer_user_id,
  note,
  max_redemptions,
  redemption_count,
  redeemed_by,
  redeemed_username,
  redeemed_at,
  earned_volume_threshold_usd,
  sequence_number
)
VALUES (
  'sample_hash_replace_with_service_generated_hash',
  'NADODEMO',
  'referral',
  'NAD',
  100001,
  100001,
  'sample referral invite',
  1,
  1,
  100002,
  'referred_demo',
  now(),
  10000,
  1
)
ON CONFLICT DO NOTHING;

INSERT INTO referrals (referrer_user_id, referred_user_id, referred_username, referred_volume_usd, referred_trade_count)
VALUES (100001, 100002, 'referred_demo', 2500, 3)
ON CONFLICT (referred_user_id) DO UPDATE
SET referred_volume_usd = EXCLUDED.referred_volume_usd,
    referred_trade_count = EXCLUDED.referred_trade_count,
    updated_at = now();

COMMIT;
