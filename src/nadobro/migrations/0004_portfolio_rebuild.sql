-- Portfolio rebuild: Nado is the source of truth; Postgres is a write-through cache.
-- Idempotent Postgres migration. Re-running on a healthy DB should be a no-op.

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS isolated BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT 'mainnet',
  ADD COLUMN IF NOT EXISTS product_id INTEGER,
  ADD COLUMN IF NOT EXISTS amount_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS v_quote_balance_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS last_cum_funding_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS est_pnl NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS est_liq_price NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS avg_entry_price NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS notional_value NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS margin_used NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS leverage NUMERIC(20,8),
  ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT,
  ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS close_price NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS close_realized_pnl NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;

DROP INDEX IF EXISTS positions_unique_open;
CREATE UNIQUE INDEX IF NOT EXISTS positions_unique_open
  ON positions (user_id, network, COALESCE(product_id, 0), isolated)
  WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS positions_user_network_open
  ON positions (user_id, network)
  WHERE closed_at IS NULL;

ALTER TABLE open_orders
  ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT 'mainnet',
  ADD COLUMN IF NOT EXISTS isolated BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS product_id INTEGER,
  ADD COLUMN IF NOT EXISTS price_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS amount_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS expiration BIGINT,
  ADD COLUMN IF NOT EXISTS nonce BIGINT,
  ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT,
  ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS open_orders_unique_digest
  ON open_orders (user_id, network, order_digest);

ALTER TABLE trades_mainnet
  ADD COLUMN IF NOT EXISTS submission_idx NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS isolated BOOLEAN,
  ADD COLUMN IF NOT EXISTS realized_pnl_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS fee_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS base_filled_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS quote_filled_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT,
  ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ;

DROP INDEX IF EXISTS trades_mainnet_submission_idx;
CREATE UNIQUE INDEX IF NOT EXISTS trades_mainnet_submission_idx
  ON trades_mainnet (submission_idx)
  WHERE submission_idx IS NOT NULL;

ALTER TABLE trades_testnet
  ADD COLUMN IF NOT EXISTS submission_idx NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS isolated BOOLEAN,
  ADD COLUMN IF NOT EXISTS realized_pnl_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS fee_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS base_filled_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS quote_filled_x18 NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT,
  ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ;

DROP INDEX IF EXISTS trades_testnet_submission_idx;
CREATE UNIQUE INDEX IF NOT EXISTS trades_testnet_submission_idx
  ON trades_testnet (submission_idx)
  WHERE submission_idx IS NOT NULL;

CREATE TABLE IF NOT EXISTS funding_payments_mainnet (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id INTEGER NOT NULL,
  amount_x18 NUMERIC(78,0) NOT NULL,
  balance_amount_x18 NUMERIC(78,0),
  rate_x18 NUMERIC(78,0),
  oracle_price_x18 NUMERIC(78,0),
  paid_at TIMESTAMPTZ NOT NULL,
  synced_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (user_id, product_id, paid_at, amount_x18)
);

CREATE TABLE IF NOT EXISTS funding_payments_testnet (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id INTEGER NOT NULL,
  amount_x18 NUMERIC(78,0) NOT NULL,
  balance_amount_x18 NUMERIC(78,0),
  rate_x18 NUMERIC(78,0),
  oracle_price_x18 NUMERIC(78,0),
  paid_at TIMESTAMPTZ NOT NULL,
  synced_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (user_id, product_id, paid_at, amount_x18)
);

ALTER TABLE funding_payments_mainnet
  DROP CONSTRAINT IF EXISTS funding_payments_mainnet_user_id_product_id_paid_at_key;

CREATE UNIQUE INDEX IF NOT EXISTS funding_payments_mainnet_event_unique
  ON funding_payments_mainnet (user_id, product_id, paid_at, amount_x18);

ALTER TABLE funding_payments_testnet
  DROP CONSTRAINT IF EXISTS funding_payments_testnet_user_id_product_id_paid_at_key;

CREATE UNIQUE INDEX IF NOT EXISTS funding_payments_testnet_event_unique
  ON funding_payments_testnet (user_id, product_id, paid_at, amount_x18);

CREATE TABLE IF NOT EXISTS sync_cursors (
  user_id BIGINT,
  network TEXT,
  matches_idx NUMERIC(78,0),
  funding_idx NUMERIC(78,0),
  ws_last_event_at TIMESTAMPTZ,
  PRIMARY KEY (user_id, network)
);

CREATE TABLE IF NOT EXISTS sync_log (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  network TEXT,
  ran_at TIMESTAMPTZ DEFAULT now(),
  positions_seen INTEGER DEFAULT 0,
  positions_closed INTEGER DEFAULT 0,
  orders_seen INTEGER DEFAULT 0,
  orders_cleared INTEGER DEFAULT 0,
  fills_inserted INTEGER DEFAULT 0,
  funding_inserted INTEGER DEFAULT 0,
  duration_ms INTEGER,
  error TEXT
);

ALTER TABLE strategy_sessions
  ADD COLUMN IF NOT EXISTS strategy_label TEXT,
  ADD COLUMN IF NOT EXISTS mode TEXT,
  ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS fees NUMERIC(38,18) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS funding NUMERIC(38,18) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS volume NUMERIC(38,18) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS trade_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS win_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS loss_count INTEGER DEFAULT 0;
