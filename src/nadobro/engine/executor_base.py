"""Executor base class — abstract lifecycle (CREATED → ACTIVE → TERMINATED),
standardized metrics, controller_id linkage, and retry policy (3 retries with
exponential backoff; transitions to TERMINATED with close_type=FAILED on max
retries).

Implemented in Phase 1.
"""
