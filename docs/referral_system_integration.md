# Nadobro Referral System (Open-Access Edition)

## What This Adds

Nadobro's referral system was simplified in May 2026:

- The private-alpha invite gate is removed. **Anyone can open the bot.**
- Every user can claim exactly one globally-unique vanity code.
- The code is **immutable once claimed** and can be redeemed an unlimited number of times.
- No volume threshold, no maximum-code cap.
- Two different users cannot claim the same code.

Referral rows still link a new user back to the direct referrer, so direct-referred volume continues to be tracked on `referrals` / `referral_volume_events`.

## Code Rules

- Length: **3-20 characters** (`MIN_CODE_LEN` / `MAX_CODE_LEN` in `referral_service`).
- Characters: `A-Z` and `0-9` (normalized to uppercase; dashes and underscores are stripped).
- Reserved blocklist: `ADMIN`, `NADOBRO`, `NADO`, `SUPPORT`, `STAFF`, `TEAM`, `MOD`, `OFFICIAL`, `HELP`, `ROOT`, `OWNER`, `BOT`, `SYSTEM`, `NULL`, `NONE`, `TEST`.
- Users may also tap *Auto-Generate* to receive an 8-character code from the same uniqueness pool.

## Database

Run-once SQL is folded into `src/nadobro/db.py::init_db()` and is safe to re-apply:

```sql
ALTER TABLE users ALTER COLUMN private_access_granted SET DEFAULT true;
UPDATE users SET private_access_granted = true
WHERE private_access_granted IS DISTINCT FROM true;
```

The legacy `invite_codes` table is preserved (it stores both historical access invites and active referral vanity codes). The `idx_invite_codes_public_code` unique index enforces "one code per string" across all users. The legacy `docs/referral_system_migration.sql` is retained for historical reference only - all referral DDL now ships with `init_db()`.

## Runtime Configuration

- `BOT_USERNAME`: used to build `https://t.me/<bot>?start=ref_<CODE>` links. Defaults to `Nadbro_bot` and is validated against Telegram `getMe()` at startup.
- `INVITE_CODE_PEPPER` / `ENCRYPTION_KEY`: required so `invite_codes.code_hash` cannot be forged from the public code alone. `NADOBRO_ALLOW_DEV_INVITE_PEPPER=true` enables a hardcoded dev pepper for local testing only.

The old `REFERRAL_VOLUME_PER_INVITE_USD` and `REFERRAL_MAX_INVITE_CODES` variables are no longer read.

## User Flow

1. New user opens the bot (no access code needed) or follows `https://t.me/<bot>?start=ref_<CODE>`.
2. `cmd_start` best-effort calls `redeem_referral_code` for the deep-link payload, then runs normal onboarding.
3. Existing user taps *Refer Friends* and either:
   - *Claim Custom Code* -> types their desired code (validated, persisted, deep link returned).
   - *Auto-Generate* -> mints an 8-char code via the same uniqueness check.
4. Their referrer sees the new redemption in the Referral Deck; subsequent trades by the referred user flow into `record_referred_volume()`.

## Telegram UX

- Reply keyboard button: `🎁 Refer Friends`
- Callback routes:
  - `refer:view` - render the Referral Deck.
  - `refer:claim` - prompt for a custom code (sets `pending_referral_claim` on `user_data`).
  - `refer:autogen` - call `auto_generate_referral_code` and re-render.
- Free-text fallback: pasting `ref_<CODE>` in chat will redeem the code if the user has no referrer yet.

## Production Notes

- Self-referrals are blocked.
- One referred Telegram account can only be linked to one referrer (the existing `referrals.referred_user_id UNIQUE` constraint).
- Referral volume tracking remains direct-only and event-sourced via `referral_volume_events`.
- Users who unlocked multiple legacy "earned" codes keep all of them (their inbound redemptions still resolve). The dashboard shows their first active code as their canonical share code and refuses any new claim.
- The `/invite_generate`, `/invite_status`, `/invite_revoke`, `/invite_grant` admin commands were removed. Use direct DB updates or restore the file from git history if access control needs to be re-introduced.
