-- Store Strategy Studio conditional/trailing order intents evaluated by a watcher.

CREATE TABLE IF NOT EXISTS conditional_orders (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
    studio_session_id BIGINT REFERENCES studio_sessions(id) ON DELETE SET NULL,
    strategy_session_id BIGINT REFERENCES strategy_sessions(id) ON DELETE SET NULL,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,
    order_type TEXT NOT NULL DEFAULT 'conditional',
    intent_json JSONB NOT NULL DEFAULT '{}',
    conditions_json JSONB NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'armed',
    time_limit TIMESTAMPTZ NULL,
    time_limit_source TEXT NULL,
    time_limit_fired_at TIMESTAMPTZ NULL,
    fired_at TIMESTAMPTZ NULL,
    last_evaluated_at TIMESTAMPTZ NULL,
    last_evaluation TEXT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_conditional_orders_armed
    ON conditional_orders (network, status, updated_at)
    WHERE status = 'armed';

CREATE INDEX IF NOT EXISTS idx_conditional_orders_time_limit_due
    ON conditional_orders (network, time_limit)
    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status = 'armed';

CREATE INDEX IF NOT EXISTS idx_conditional_orders_user_status
    ON conditional_orders (telegram_id, network, status);
