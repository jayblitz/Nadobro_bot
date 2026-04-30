# Nadobro Referral System Integration Guide

## What This Adds

Nadobro now supports Nado-style referral invites:

- Users earn `1` referral invite code per `$10,000` in their own trading volume.
- Each user can earn up to `1000` referral invite codes.
- Referral codes grant private access and link the new user to the direct referrer.
- Direct referred volume is updated whenever the referred user’s trading volume is committed.

Private alpha admin invite codes still work. Referral invite codes are stored in the same `invite_codes` table with `code_type = 'referral'`.

## Database Deployment

Run the migration in:

```bash
psql "$DATABASE_URL" -f docs/referral_system_migration.sql
```

For hard-reset environments, `src/nadobro/db.py:init_db()` creates the same schema and `scripts/reset_db.py` drops the referral tables before recreating the database.

## Runtime Configuration

Optional environment variables:

- `REFERRAL_VOLUME_PER_INVITE_USD`: default `10000`
- `REFERRAL_MAX_INVITE_CODES`: default `1000`
- `TELEGRAM_BOT_USERNAME` or `BOT_USERNAME`: used to build `https://t.me/<bot>?start=ref_<code>` links

## User Flow

1. Existing user trades on Nadobro.
2. `update_trade_stats()` increments their all-time volume.
3. The Referral Deck calculates earned invite allowance from that volume.
4. If they qualify, the user can generate/share a referral invite code.
5. New user opens the bot with `?start=ref_<code>` or types the code.
6. `redeem_invite_code()` grants private access and inserts a row into `referrals`.
7. As the referred user trades, `record_referred_volume()` updates direct referred volume.

## Telegram UX

Users open the dashboard through:

- Home card button: `Refer Friends`
- Callback route: `refer:view`
- Generate route: `refer:generate`

The Referral Deck shows:

- Direct referrals
- Total referred volume
- Earned/generated/available codes
- Shareable code and deep link
- Top direct referred users by volume
- Warning text when the user has not reached the next volume threshold

## Production Notes

- Self-referrals are blocked.
- One referred Telegram account can only be linked to one referrer.
- Referral volume is direct-only and based on committed trade volume deltas.
- Existing private alpha invite codes remain hashed and are not exposed as public codes.
