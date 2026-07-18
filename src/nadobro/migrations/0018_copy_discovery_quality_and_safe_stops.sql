-- Copy discovery quality metrics and retry-safe manual stops.
--
-- Leader fields preserve the public NadoExplorer snapshot attached to a
-- private selection. They are display/ranking data only; mirror sizing and
-- execution continue to read the venue. ``stop_requested`` keeps a mirror in
-- the poller when a user-requested flatten fails, so open copied positions
-- cannot become inactive and unmonitored.
--
-- Idempotent: db.py startup DDL carries the same additions for deployments
-- that have not run the migration separately.

ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_roi NUMERIC(20, 8);
ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_active_days INTEGER;
ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_period_days INTEGER;
ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_last_activity_at TIMESTAMPTZ;
ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_closed_trades INTEGER;
ALTER TABLE copy_traders
  ADD COLUMN IF NOT EXISTS leader_max_drawdown_pct NUMERIC(20, 8);

ALTER TABLE copy_mirrors
  ADD COLUMN IF NOT EXISTS stop_requested BOOLEAN NOT NULL DEFAULT false;
