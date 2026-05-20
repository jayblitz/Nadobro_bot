"""Executor Orchestrator — single supervisor that owns executor lifecycles.

Supports spawn / stop / list (filtered by controller_id), an event bus,
batched cancel via asyncio.gather, and consults the Risk Engine before each
spawn. Enforces the process-level kill switch.

Implemented in Phase 1.
"""
