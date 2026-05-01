-- Persist natural-language Strategy Studio conversations across restarts.

CREATE TABLE IF NOT EXISTS studio_sessions (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
    state TEXT NOT NULL CHECK (state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING', 'DONE', 'CANCELLED')),
    intent_json JSONB NOT NULL DEFAULT '{}',
    history_json JSONB NOT NULL DEFAULT '[]',
    strategy_session_id BIGINT REFERENCES strategy_sessions(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_studio_sessions_user_active
    ON studio_sessions (telegram_id, network, updated_at DESC)
    WHERE state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING');

CREATE INDEX IF NOT EXISTS idx_studio_sessions_strategy_session
    ON studio_sessions (strategy_session_id)
    WHERE strategy_session_id IS NOT NULL;
