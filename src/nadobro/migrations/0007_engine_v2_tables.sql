-- Engine v2 (Phase 2): portfolio + executor persistence tables.
-- Idempotent Postgres migration. FK-free by design — the engine links by
-- user_id loosely (see ARCHITECTURE.md / ENGINE_V2_BRIEF §5).

CREATE TABLE IF NOT EXISTS engine_executors (
  id               UUID PRIMARY KEY,
  user_id          BIGINT NOT NULL,
  controller_id    TEXT NOT NULL,
  strategy_type    TEXT NOT NULL,            -- order/position/grid/reverse_grid/dca/twap
  trading_pair     TEXT NOT NULL,
  side             TEXT NOT NULL,            -- BUY/SELL
  config_json      JSONB NOT NULL,
  state            TEXT NOT NULL,            -- CREATED/ACTIVE/TERMINATED
  close_type       TEXT,                     -- TP/SL/TIME_LIMIT/TRAILING/EARLY/COMPLETED/FAILED
  net_pnl_quote    NUMERIC(38,18) DEFAULT 0,
  fees_paid_quote  NUMERIC(38,18) DEFAULT 0,
  volume_quote     NUMERIC(38,18) DEFAULT 0,
  duration_seconds INTEGER,
  keep_position    BOOLEAN NOT NULL DEFAULT TRUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  terminated_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_engine_executors_user_ctrl ON engine_executors(user_id, controller_id);
CREATE INDEX IF NOT EXISTS ix_engine_executors_pair ON engine_executors(trading_pair);
CREATE INDEX IF NOT EXISTS ix_engine_executors_state ON engine_executors(state);

CREATE TABLE IF NOT EXISTS engine_position_hold (
  user_id            BIGINT NOT NULL,
  trading_pair       TEXT NOT NULL,
  controller_id      TEXT NOT NULL,
  buy_amount_base    NUMERIC(38,18) NOT NULL DEFAULT 0,
  buy_amount_quote   NUMERIC(38,18) NOT NULL DEFAULT 0,
  sell_amount_base   NUMERIC(38,18) NOT NULL DEFAULT 0,
  sell_amount_quote  NUMERIC(38,18) NOT NULL DEFAULT 0,
  cum_fees_quote     NUMERIC(38,18) NOT NULL DEFAULT 0,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, trading_pair, controller_id)
);

CREATE TABLE IF NOT EXISTS engine_portfolio_history (
  user_id           BIGINT NOT NULL,
  ts                TIMESTAMPTZ NOT NULL,
  total_value_quote NUMERIC(38,18) NOT NULL,
  by_account_json   JSONB NOT NULL,
  by_asset_json     JSONB NOT NULL,
  PRIMARY KEY (user_id, ts)
);
-- Retention worker (daily): 1m granularity for 7d, 1h for 30d, 1d for 1y.

CREATE TABLE IF NOT EXISTS engine_strategy_sessions (
  id            UUID PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  controller_id TEXT NOT NULL,
  session_n     INTEGER NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  ended_at      TIMESTAMPTZ,
  summary       TEXT,
  journal_path  TEXT NOT NULL,
  UNIQUE (user_id, controller_id, session_n)
);
