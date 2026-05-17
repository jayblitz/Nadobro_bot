-- Strategy Studio retired (2026-05). Drop the studio_sessions table and the
-- conditional_orders watcher table that depended on it. Idempotent.

DROP TABLE IF EXISTS conditional_orders CASCADE;
DROP TABLE IF EXISTS studio_sessions CASCADE;
