"""Operational journal — file-backed per-session log at
``~/.nadobro/sessions/<user_id>/<controller_id>/session_<n>/journal.md``,
with per-tick ``snapshots/snapshot_<k>.md`` and a cross-session
``learnings.md`` capped at 20 entries. For ops debugging only; NOT an LLM
memory.

Implemented in Phase 1.
"""
