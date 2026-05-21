-- Engine v2 (Phase 2): drop orphaned legacy strategy tables.
--
-- Operator confirmed no strategies are running and no production data needs
-- preserving, so no data-copy migration is required. Only tables with ZERO
-- references anywhere in the codebase (outside their db.py CREATE, now also
-- removed) are dropped here:
--   * strategies                    — never queried in kept or legacy code
--   * strategy_performance_snapshots — never queried in kept or legacy code
--
-- NOT dropped: `strategy_sessions` is still read/written by kept features
-- (pnl_card_builder, models/database session tracking), so it is retained.
-- The brief's example tables (strategy_runtime, vol_phase_state) do not exist
-- in this repository.

DROP TABLE IF EXISTS strategy_performance_snapshots CASCADE;
DROP TABLE IF EXISTS strategies CASCADE;
