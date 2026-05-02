-- Concurrency/idempotency hardening introduced during the audit.
-- Idempotent Postgres migration. Re-running on a healthy DB should be a no-op.

ALTER TABLE fill_sync_queue
  ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fill_sync_processing
  ON fill_sync_queue (claimed_at)
  WHERE status = 'processing';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fill_sync_queue_status_check'
  ) THEN
    ALTER TABLE fill_sync_queue
      ADD CONSTRAINT fill_sync_queue_status_check
      CHECK (status IN ('pending', 'processing', 'resolved', 'expired'));
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS order_intents (
  intent_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending',
  value JSONB NOT NULL DEFAULT '{}',
  trade_id BIGINT,
  order_digest TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_order_intents_status_updated
  ON order_intents (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_order_intents_trade_id
  ON order_intents (trade_id)
  WHERE trade_id IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'order_intents_status_check'
  ) THEN
    ALTER TABLE order_intents
      ADD CONSTRAINT order_intents_status_check
      CHECK (status IN ('pending', 'recorded', 'submitted', 'filled', 'failed', 'cancelled', 'expired'));
  END IF;
END $$;

ALTER TABLE copy_positions
  ADD COLUMN IF NOT EXISTS tp_order_digest TEXT,
  ADD COLUMN IF NOT EXISTS sl_order_digest TEXT;

CREATE INDEX IF NOT EXISTS idx_copy_positions_tp_digest
  ON copy_positions (tp_order_digest)
  WHERE tp_order_digest IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_copy_positions_sl_digest
  ON copy_positions (sl_order_digest)
  WHERE sl_order_digest IS NOT NULL;
