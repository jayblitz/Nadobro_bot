-- Nadobro Supabase schema (run in Supabase SQL editor or via migration)
-- RLS: use service role key for full access; or add policies as needed.

-- Users: linked signer model (telegram_id PK, main_address, linked_signer, encrypted key, language)
CREATE TABLE IF NOT EXISTS users (
  telegram_id BIGINT PRIMARY KEY,
  telegram_username TEXT,
  main_address TEXT,
  linked_signer_address TEXT,
  encrypted_linked_signer_pk TEXT,
  salt TEXT,
  language TEXT DEFAULT 'en',
  strategy_settings JSONB DEFAULT '{}',
  network_mode TEXT DEFAULT 'mainnet',
  created_at TIMESTAMPTZ DEFAULT now(),
  last_active TIMESTAMPTZ DEFAULT now(),
  last_trade_at TIMESTAMPTZ,
  total_trades INT DEFAULT 0,
  total_volume_usd DOUBLE PRECISION DEFAULT 0
);

-- Bot state: key-value for onboarding, strategy runtime, settings
CREATE TABLE IF NOT EXISTS bot_state (
  id SERIAL PRIMARY KEY,
  key TEXT UNIQUE NOT NULL,
  value TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Trades history
CREATE TABLE IF NOT EXISTS trades (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id INT NOT NULL,
  product_name TEXT NOT NULL,
  order_type TEXT NOT NULL,
  side TEXT NOT NULL,
  size DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION,
  leverage DOUBLE PRECISION DEFAULT 1.0,
  status TEXT DEFAULT 'pending',
  order_digest TEXT,
  pnl DOUBLE PRECISION,
  fees DOUBLE PRECISION DEFAULT 0,
  network TEXT NOT NULL,
  error_message TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  filled_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_trades_user_product ON trades (user_id, product_id);
CREATE INDEX IF NOT EXISTS idx_trades_created ON trades (created_at);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  product_id INT NOT NULL,
  product_name TEXT NOT NULL,
  condition TEXT NOT NULL,
  target_value DOUBLE PRECISION NOT NULL,
  is_active BOOLEAN DEFAULT true,
  triggered_at TIMESTAMPTZ,
  network TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts (user_id, is_active);

-- Admin logs
CREATE TABLE IF NOT EXISTS admin_logs (
  id SERIAL PRIMARY KEY,
  admin_id BIGINT NOT NULL,
  action TEXT NOT NULL,
  details TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
