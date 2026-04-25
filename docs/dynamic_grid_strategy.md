# Dynamic GRID (DGRID)

Dynamic GRID is Nadobro's adaptive grid mode for Nado perps. It uses the existing GRID/RGRID execution engine, but automatically chooses which phase to run each cycle.

## What It Does

- Runs as `dgrid` in the strategy runtime.
- Uses GRID behavior in ranging regimes.
- Uses Reverse GRID behavior in trend/high-volatility regimes.
- Resizes spread from recent realized movement, capped by configuration.
- Auto-derives soft reset sensitivity from the current spread.

## Regime Logic

Every strategy cycle, DGRID computes a local variance-ratio proxy from recent mid-price history:

- Variance ratio `>= 1.25`: switch to RGRID phase.
- Variance ratio `<= 1.15`: switch to GRID phase.
- Between those values: keep the current phase.

The hysteresis band avoids flip-flopping in mixed regimes.

## Spread Logic

In GRID phase:

- Spread is sized from recent realized movement.
- Spread is bounded by `dgrid_min_spread_bp` and `dgrid_max_spread_bp`.

In RGRID phase:

- Spread is forced to `0bp`, because the directional legs create the offset.

## Soft Reset

DGRID derives reset sensitivity as roughly `4x` the measured spread and snaps it to one of:

- `5bp`
- `12.5bp`
- `25bp`
- `50bp`
- `100bp`

The existing reset machinery consumes this as percent threshold internally.

## Key Settings

- `dgrid_trend_on_variance_ratio`: default `1.25`.
- `dgrid_range_on_variance_ratio`: default `1.15`.
- `dgrid_min_spread_bp`: default `2`.
- `dgrid_max_spread_bp`: default `50`.
- `dgrid_short_window_points`: default `4`.
- `dgrid_long_window_points`: default `12`.

## User-Facing Status

Strategy status exposes:

- `dgrid_phase`
- `dgrid_variance_ratio`
- `dgrid_realized_move_bp`
- `dgrid_dynamic_spread_bp`
- `dgrid_reset_threshold_bp`
- `dgrid_phase_changed`

Telegram renders these in the Dynamic GRID dashboard/status card.
