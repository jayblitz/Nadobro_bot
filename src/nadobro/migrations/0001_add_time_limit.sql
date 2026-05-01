-- Add optional time-limit auto-close metadata to positions and open orders.

ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit TIMESTAMPTZ NULL;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit_source TEXT NULL;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit_fired_at TIMESTAMPTZ NULL;

ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit TIMESTAMPTZ NULL;
ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit_source TEXT NULL;
ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit_fired_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_positions_time_limit_due
    ON positions (network, time_limit)
    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status = 'open';

CREATE INDEX IF NOT EXISTS idx_open_orders_time_limit_due
    ON open_orders (network, time_limit)
    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status IN ('open', 'pending', 'armed');
