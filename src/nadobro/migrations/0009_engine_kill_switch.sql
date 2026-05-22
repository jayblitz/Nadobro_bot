-- Engine v2: persisted risk kill switch (so process restarts honor it).
CREATE TABLE IF NOT EXISTS engine_kill_switch (
  scope      TEXT PRIMARY KEY,
  engaged    BOOLEAN NOT NULL DEFAULT FALSE,
  reason     TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
