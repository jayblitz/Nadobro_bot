-- Engine v2: live per-controller progress, written each tick by the worker
-- process so the main process (/status) can surface cycles-completed and
-- funding-earned cross-process. Keyed by the deterministic controller id.
CREATE TABLE IF NOT EXISTS engine_controller_state (
  controller_id      TEXT PRIMARY KEY,
  user_id            BIGINT NOT NULL,
  strategy           TEXT,
  network            TEXT,
  cycles_completed   INTEGER NOT NULL DEFAULT 0,
  funding_earned_usd NUMERIC(38,18) NOT NULL DEFAULT 0,
  phase              TEXT,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
