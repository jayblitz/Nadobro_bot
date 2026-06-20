# Night HOWL — nightly per-user trade-pattern reports

Night HOWL sweeps each active user's trades over the last 24h (across **all**
strategies), computes their unique trading pattern, replays their live strategy
config over the last-24h candles through the backtester (net of fees/funding),
turns that into concrete recommendations, and delivers a **report at the user's
local 8am**. Every report is saved so users can pull it up later with `/howl`.

It builds on — but is distinct from — the existing `howl_service` (the Alpha-Agent
parameter auto-tuner). Night HOWL is **read-only**: it advises, it never changes
live settings.

## What's in a report

- **Last 24h**: trades, volume, net PnL (after fees/funding), gross realized,
  fees, funding, win rate (W/L), top pairs, busiest hour.
- **24h backtest (net of fees)**: the user's live config vs tweaked variants
  (e.g. wider/tighter grid step), ranked by net-of-fees PnL — the evidence
  behind the advice.
- **Recommendations**: deterministic, plain-English guidance (fee drag, win-rate
  discipline, concentration risk, and "a wider step would have netted more").

## Architecture

`src/nadobro/services/night_howl_service.py`

- **Pure (unit-tested):** `compute_user_pattern`, `derive_recommendations`,
  `compare_configs` (drives the backtester), `night_howl_due` (per-user local-8am
  scheduling check), `render_report_markdown`.
- **Persistence (bot_state KV):** `save_report` / `list_report_dates` /
  `get_report`; an index per user capped at `MAX_SAVED_REPORTS` (30), oldest
  evicted. Plus `mark_sent` / `last_sent_date` for per-day de-dup.
- **Orchestration (DB + venue):** `build_report` ties trades + candles + backtest
  together for one user. Never raises — a per-user failure can't break the sweep.

## Delivery

`scheduler.tick_night_howl` runs **hourly on the hour** (`night_howl_hourly`
cron). For each active user (enumerated from `strategy_bot:*` state) it checks
`night_howl_due(now_utc, tz_offset, last_sent)` — true only when it's that user's
local 8am and today's report hasn't gone out. It builds the report, saves it,
marks the day sent, and sends the Telegram message (a no-activity day is saved
silently, no ping).

Per-user timezone is a UTC offset in hours, stored in settings as
`howl_tz_offset` (default 0 = UTC). Opt-out via `night_howl_enabled` (default on).

## Commands

- `/howl` — latest saved report
- `/howl list` — dates of saved reports
- `/howl YYYY-MM-DD` — a specific saved report

## Tests

`tests/services/test_night_howl.py` — pattern metrics (windowing, win rate, top
pairs), recommendation rules (fee drag, backtest-backed, hold-course), local-8am
scheduling across timezone offsets + de-dup, persistence round-trip + eviction,
and a backtester-backed `compare_configs` ranking.
