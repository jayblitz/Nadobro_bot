"""Controller base class — long-running strategy with ``on_start`` /
``on_tick`` / ``on_stop`` hooks, a stable ``controller_id`` used to filter
the Orchestrator's executor pool, and a tick scheduler.

Implemented in Phase 4.
"""
