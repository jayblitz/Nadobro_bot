"""Strategy controllers — long-running classes with ``on_start`` /
``on_tick`` / ``on_stop`` hooks. Controllers spawn executors via the
Orchestrator and never talk to the venue directly.
"""
