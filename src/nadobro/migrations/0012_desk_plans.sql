-- Desk text-to-trade execution plans (TreadFi-parity feature, 2026-06).
-- One row per parsed-and-confirmed plan; lifecycle is
-- draft -> awaiting_trigger -> running -> completed/cancelled/failed,
-- every transition a guarded UPDATE so restarts cannot double-fire.
-- Mirrors the startup DDL in src/nadobro/db.py (init_db).

CREATE TABLE IF NOT EXISTS desk_plans_testnet (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    plan_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'draft',
    plan_json TEXT NOT NULL,
    state_json TEXT,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CHECK (status IN ('draft','awaiting_trigger','running',
                      'completed','cancelled','failed'))
);
CREATE INDEX IF NOT EXISTS idx_desk_plans_testnet_user_status
    ON desk_plans_testnet (user_id, status);
CREATE INDEX IF NOT EXISTS idx_desk_plans_testnet_active
    ON desk_plans_testnet (status)
    WHERE status IN ('awaiting_trigger', 'running');

CREATE TABLE IF NOT EXISTS desk_plans_mainnet (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    plan_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'draft',
    plan_json TEXT NOT NULL,
    state_json TEXT,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CHECK (status IN ('draft','awaiting_trigger','running',
                      'completed','cancelled','failed'))
);
CREATE INDEX IF NOT EXISTS idx_desk_plans_mainnet_user_status
    ON desk_plans_mainnet (user_id, status);
CREATE INDEX IF NOT EXISTS idx_desk_plans_mainnet_active
    ON desk_plans_mainnet (status)
    WHERE status IN ('awaiting_trigger', 'running');
