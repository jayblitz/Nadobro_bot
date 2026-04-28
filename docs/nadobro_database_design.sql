-- Nadobro database design reference.
-- This file documents the product-level schema in plain SQL. The running app
-- applies the same design additively from src/nadobro/db.py so existing bot
-- tables and telegram_id based references keep working.

-- Core identities and access
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    telegram_username TEXT,
    main_address TEXT,
    linked_signer_address TEXT,
    encrypted_linked_signer_pk TEXT,
    salt TEXT,
    language TEXT DEFAULT 'en',
    strategy_settings JSONB DEFAULT '{}',
    network_mode TEXT DEFAULT 'mainnet',
    onboarding_status TEXT DEFAULT 'not_started',
    has_strategy_bot BOOLEAN DEFAULT false,
    private_access_granted BOOLEAN DEFAULT false,
    private_access_code_id BIGINT,
    private_access_granted_at TIMESTAMPTZ,
    private_access_granted_by BIGINT,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_active TIMESTAMPTZ DEFAULT now(),
    last_trade_at TIMESTAMPTZ,
    total_trades INT DEFAULT 0,
    total_volume_usd DOUBLE PRECISION DEFAULT 0
);

CREATE TABLE IF NOT EXISTS invite_codes (
    id BIGSERIAL PRIMARY KEY,
    code_hash TEXT UNIQUE NOT NULL,
    code_prefix TEXT NOT NULL,
    created_by BIGINT NOT NULL,
    created_for_telegram_id BIGINT,
    note TEXT,
    max_redemptions INT NOT NULL DEFAULT 1,
    redemption_count INT NOT NULL DEFAULT 0,
    active BOOLEAN NOT NULL DEFAULT true,
    redeemed_by BIGINT,
    redeemed_username TEXT,
    redeemed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_by BIGINT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    CHECK (max_redemptions > 0),
    CHECK (redemption_count >= 0)
);

-- Strategy configuration and analytics
CREATE TABLE IF NOT EXISTS strategies (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    strategy_type TEXT NOT NULL CHECK (
        strategy_type IN ('grid', 'r_grid', 'd_grid', 'delta_neutral', 'volume_bot', 'bro_mode')
    ),
    name TEXT,
    network TEXT NOT NULL DEFAULT 'mainnet' CHECK (network IN ('testnet', 'mainnet')),
    pair TEXT NOT NULL,
    capital_usd NUMERIC(20, 8),
    leverage NUMERIC(10, 4) NOT NULL DEFAULT 1,
    risk_level TEXT NOT NULL DEFAULT 'medium',
    parameters JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'stopped' CHECK (status IN ('running', 'paused', 'stopped', 'failed')),
    started_at TIMESTAMPTZ,
    stopped_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (
    id BIGSERIAL PRIMARY KEY,
    strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    period TEXT NOT NULL CHECK (period IN ('daily', 'weekly', 'monthly')),
    period_start DATE NOT NULL,
    pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    fees_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    volume_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    trade_count INT NOT NULL DEFAULT 0,
    win_rate NUMERIC(8, 4),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (strategy_id, period, period_start)
);

-- Trading state
CREATE TABLE IF NOT EXISTS positions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    strategy_id BIGINT REFERENCES strategies(id) ON DELETE SET NULL,
    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
    pair TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('long', 'short')),
    size NUMERIC(30, 12) NOT NULL,
    entry_price NUMERIC(30, 12),
    mark_price NUMERIC(30, 12),
    leverage NUMERIC(10, 4) NOT NULL DEFAULT 1,
    unrealized_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    realized_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    opened_at TIMESTAMPTZ DEFAULT now(),
    closed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS open_orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    strategy_id BIGINT REFERENCES strategies(id) ON DELETE SET NULL,
    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
    pair TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL DEFAULT 'limit',
    size NUMERIC(30, 12) NOT NULL,
    price NUMERIC(30, 12),
    leverage NUMERIC(10, 4) DEFAULT 1,
    order_digest TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'open',
    placed_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    metadata JSONB NOT NULL DEFAULT '{}'
);

-- Nado points and bot state
CREATE TABLE IF NOT EXISTS points_snapshots (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    period TEXT NOT NULL CHECK (period IN ('daily', 'weekly', 'monthly', 'all')),
    period_start DATE NOT NULL,
    nado_points NUMERIC(20, 8) NOT NULL DEFAULT 0,
    volume_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
    cost_per_point_usd NUMERIC(20, 8),
    maker_ratio NUMERIC(8, 4),
    taker_ratio NUMERIC(8, 4),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, period, period_start)
);

CREATE TABLE IF NOT EXISTS bot_state (
    id SERIAL PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    value TEXT,
    scope TEXT DEFAULT 'global',
    user_id BIGINT,
    metadata JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Copy trading. These are compatible with the app's existing copy-trading names.
CREATE TABLE IF NOT EXISTS copy_traders (
    id SERIAL PRIMARY KEY,
    wallet_address TEXT UNIQUE NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    is_curated BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS copy_mirrors (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    trader_id INT NOT NULL REFERENCES copy_traders(id),
    network TEXT NOT NULL DEFAULT 'mainnet',
    margin_per_trade DOUBLE PRECISION NOT NULL DEFAULT 50.0,
    max_leverage DOUBLE PRECISION NOT NULL DEFAULT 10.0,
    cumulative_stop_loss_pct DOUBLE PRECISION NOT NULL DEFAULT 50.0,
    cumulative_take_profit_pct DOUBLE PRECISION NOT NULL DEFAULT 100.0,
    total_allocated_usd DOUBLE PRECISION NOT NULL DEFAULT 500.0,
    cumulative_pnl DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    active BOOLEAN DEFAULT true,
    paused BOOLEAN DEFAULT false,
    auto_stopped_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    stopped_at TIMESTAMPTZ,
    UNIQUE (user_id, trader_id, network)
);

CREATE TABLE IF NOT EXISTS copy_trades (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    mirror_id INT NOT NULL REFERENCES copy_mirrors(id),
    hl_fill_tid BIGINT,
    hl_coin TEXT NOT NULL DEFAULT '',
    nado_product_id INT NOT NULL DEFAULT 0,
    side TEXT NOT NULL,
    hl_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    hl_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    nado_size DOUBLE PRECISION,
    nado_price DOUBLE PRECISION,
    nado_trade_id INT,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    filled_at TIMESTAMPTZ
);

ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS total_pnl_usd NUMERIC(20, 8) DEFAULT 0;
ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS total_volume_usd NUMERIC(20, 8) DEFAULT 0;
ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS nado_points NUMERIC(20, 8) DEFAULT 0;
ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS win_rate NUMERIC(8, 4);
ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMPTZ;

ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS budget_cap_usd NUMERIC(20, 8);
ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS risk_multiplier NUMERIC(10, 4) DEFAULT 1;
ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS budget_usd DOUBLE PRECISION;
ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS risk_factor DOUBLE PRECISION DEFAULT 1.0;
ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS last_synced_fill_tid BIGINT;

ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS original_trade_digest TEXT;
ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS copied_order_digest TEXT;
ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS pair TEXT;
ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS pnl_usd NUMERIC(20, 8);
ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS fees_usd NUMERIC(20, 8) DEFAULT 0;

-- Indexes for common reads
CREATE INDEX IF NOT EXISTS idx_users_last_active ON users (last_active DESC);
CREATE INDEX IF NOT EXISTS idx_users_private_access ON users (private_access_granted);
CREATE INDEX IF NOT EXISTS idx_invite_codes_redeemed_by ON invite_codes (redeemed_by);
CREATE INDEX IF NOT EXISTS idx_invite_codes_created_at ON invite_codes (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategies_user_status ON strategies (user_id, status);
CREATE INDEX IF NOT EXISTS idx_strategies_user_type_network_status ON strategies (user_id, strategy_type, network, status);
CREATE INDEX IF NOT EXISTS idx_strategy_perf_user_period ON strategy_performance_snapshots (user_id, period, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_positions_user_status ON positions (user_id, status);
CREATE INDEX IF NOT EXISTS idx_open_orders_user_status ON open_orders (user_id, status);
CREATE INDEX IF NOT EXISTS idx_points_snapshots_user_period ON points_snapshots (user_id, period, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_bot_state_scope_key ON bot_state (scope, key);
CREATE INDEX IF NOT EXISTS idx_copy_traders_leaderboard ON copy_traders (active, total_pnl_usd DESC, total_volume_usd DESC);
CREATE INDEX IF NOT EXISTS idx_copy_trades_original_digest ON copy_trades (original_trade_digest);

-- Sample data
INSERT INTO users (telegram_id, telegram_username, language, main_address, network_mode, onboarding_status, private_access_granted)
VALUES
    (1001, 'alice_nado', 'en', '0xAliceMainWallet', 'mainnet', 'complete', true),
    (1002, 'bob_trader', 'en', '0xBobMainWallet', 'testnet', 'wallet_pending', true)
ON CONFLICT (telegram_id) DO NOTHING;

INSERT INTO invite_codes (code_hash, code_prefix, created_by, max_redemptions, redemption_count, active, note)
VALUES
    ('sha256_hash_of_code_plus_pepper_1', 'ABC', 1001, 1, 0, true, 'launch alpha'),
    ('sha256_hash_of_code_plus_pepper_2', 'XYZ', 1001, 5, 2, true, 'community batch')
ON CONFLICT (code_hash) DO NOTHING;

INSERT INTO strategies (user_id, strategy_type, name, pair, capital_usd, leverage, risk_level, parameters, status)
VALUES (
    1001,
    'grid',
    'BTC Main Grid',
    'BTC-PERP',
    1000,
    3,
    'medium',
    '{"levels": 12, "spacing_bps": 25, "take_profit_bps": 40}'::jsonb,
    'running'
);

-- Query library
-- 1. SELECT * FROM users WHERE telegram_id = $1;
-- 2. SELECT * FROM strategies WHERE user_id = $1 AND status = 'running' ORDER BY created_at DESC;
-- 3. SELECT * FROM trades_mainnet WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50;
-- 4. SELECT * FROM positions WHERE user_id = $1 AND status = 'open' ORDER BY opened_at DESC;
-- 5. SELECT * FROM open_orders WHERE user_id = $1 AND status = 'open' ORDER BY placed_at DESC;
-- 6. SELECT * FROM points_snapshots WHERE user_id = $1 AND period = 'daily' ORDER BY period_start DESC LIMIT 30;
-- 7. SELECT * FROM invite_codes WHERE code_hash = $1 AND active = true AND revoked_at IS NULL AND redemption_count < max_redemptions AND (expires_at IS NULL OR expires_at > now());
-- 8. SELECT m.*, t.wallet_address, t.label FROM copy_mirrors m JOIN copy_traders t ON t.id = m.trader_id WHERE m.user_id = $1 AND m.active = true;
-- 9. SELECT period_start, pnl_usd, fees_usd, volume_usd, trade_count, win_rate FROM strategy_performance_snapshots WHERE strategy_id = $1 AND period = 'daily' ORDER BY period_start ASC;
-- 10. SELECT telegram_id, telegram_username, total_trades, total_volume_usd FROM users WHERE private_access_granted = true ORDER BY last_active DESC;
