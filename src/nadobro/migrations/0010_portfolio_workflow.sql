-- Portfolio workflow redesign (2026-05).
--
-- 1) strategy_sessions: persist win/loss counters so per-session cards stop
--    reading nonexistent columns (today the performance UI silently zeros).
-- 2) positions: drop the legacy NOT NULL on `leverage`. Nado's
--    calculate_account_summary omits leverage on most cross/isolated rows;
--    the inline sync now derives a fallback from notional/margin and falls
--    back to 1, but accepting NULL keeps the door open for historic rows.

ALTER TABLE strategy_sessions
  ADD COLUMN IF NOT EXISTS win_count INT NOT NULL DEFAULT 0;

ALTER TABLE strategy_sessions
  ADD COLUMN IF NOT EXISTS loss_count INT NOT NULL DEFAULT 0;

ALTER TABLE positions
  ALTER COLUMN leverage DROP NOT NULL;
