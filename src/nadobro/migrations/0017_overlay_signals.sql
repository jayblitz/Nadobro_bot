-- Financial-overlay signal log: one row per tick the overlay produced a signal
-- for an MM strategy, plus the bounded action it applied. Powers Night HOWL's
-- signal-grounded analysis and an audit trail of what the overlay did and why.
-- Network-scoped like every other trading surface. Idempotent.

CREATE TABLE IF NOT EXISTS overlay_signals (
  id                 BIGSERIAL PRIMARY KEY,
  user_id            BIGINT NOT NULL,
  network            TEXT NOT NULL,
  strategy           TEXT NOT NULL,
  product_id         INTEGER,
  product_name       TEXT,
  strategy_session_id INTEGER,
  ts                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  bias               DOUBLE PRECISION,
  regime             TEXT,
  confidence         DOUBLE PRECISION,
  entry_ok           BOOLEAN,
  scale              DOUBLE PRECISION,
  spread_mult        DOUBLE PRECISION,
  sl_pct             DOUBLE PRECISION,
  tp_pct             DOUBLE PRECISION,
  action_json        JSONB,
  reasons_json       JSONB,
  risks_json         JSONB
);

CREATE INDEX IF NOT EXISTS idx_overlay_signals_user
  ON overlay_signals (user_id, network, ts DESC);
CREATE INDEX IF NOT EXISTS idx_overlay_signals_session
  ON overlay_signals (strategy_session_id) WHERE strategy_session_id IS NOT NULL;
