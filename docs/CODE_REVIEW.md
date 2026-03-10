# Nadobro Bot — Complete Dev Code Review

**Date:** 2025-03-08  
**Scope:** Full codebase review for bugs, security, reliability, and best practices.

---

## Executive Summary

| Severity | Count | Status |
|----------|-------|--------|
| Critical | 2 | Must fix before deploy |
| High | 4 | Fix soon |
| Medium | 5 | Plan to fix |
| Low / Info | 8 | Nice to have |

---

## Critical Issues

### 1. Hardcoded Debug Log Path (Production Failure)

**Files:** `src/nadobro/services/trade_service.py`, `src/nadobro/services/nado_client.py`

```python
_DEBUG_LOG_PATH = "/Users/jerry/Nadobro_bot/.cursor/debug-086b41.log"
```

**Problem:**
- Absolute path to a local dev machine; will fail on Fly.io, Docker, or any other deployment
- Writes debug logs to disk in production
- `_debug_log()` is called in hot paths (close_position, execution reports)
- Can cause `FileNotFoundError` or `PermissionError` during normal operation

**Fix:** Remove or gate behind `os.environ.get("NADO_DEBUG_LOG_PATH")` and disable when unset.

---

### 2. Rate Limit Excludes Failed Trades (DoS / Spam)

**File:** `src/nadobro/models/database.py`  
**Function:** `get_last_trade_for_rate_limit()`

```python
"SELECT created_at FROM trades WHERE user_id = %s AND status = 'filled' ORDER BY created_at DESC LIMIT 1"
```

**Problem:**
- Only `filled` trades count toward rate limiting
- Failed attempts (invalid passphrase, insufficient margin, market down) do not rate limit
- User can spam invalid trade requests and overload Nado API, DB, and bot
- No protection against automated abuse

**Fix:** Include `status IN ('filled','failed','pending')` (any trade attempt) or add a separate `trade_attempts` table for stricter limiting.

---

## High Priority Issues

### 3. Unbounded In-Memory Caches

**Files:**
- `src/nadobro/services/user_service.py`: `_user_cache`, `_readonly_cache`
- `src/nadobro/services/nado_client.py`: `_price_cache`, `_CONTRACTS_CACHE`, etc.
- `src/nadobro/services/execution_queue.py`: `_dedupe_seen`

**Problem:**
- No max size or eviction; caches grow unbounded with unique keys
- Under load or long uptime, memory can grow indefinitely
- `_dedupe_seen` has TTL-based cleanup but no cap
**Fix:** Add max size + LRU eviction (e.g. `functools.lru_cache`, `cachetools.TTLCache`) or periodic cleanup.

---

### 4. Passphrase Stored in Session Without Encryption

**File:** `src/nadobro/handlers/messages.py`  
**Variable:** `PENDING_PASSPHRASE_ACTION` in `context.user_data`

**Problem:**
- Passphrase stored in `context.user_data` (in-memory, per-user chat context)
- If bot restarts or context is serialized/persisted, passphrase could leak
- `_manual_session_passphrases` in `bot_runtime.py` has TTL but is still in-memory

**Mitigation:** Session passphrases are temporary and cleared after use. Document that `user_data` should not be persisted to disk with sensitive data. Consider shortening TTL further.

---

### 5. `datetime.utcnow()` Deprecation (Python 3.12+)

**Files:** Throughout (`trade_service.py`, `user_service.py`, `models/database.py`, etc.)

**Problem:**
- `datetime.utcnow()` is deprecated in Python 3.12
- Replace with `datetime.now(timezone.utc)` for future compatibility

**Fix:** Mechanical find/replace + `from datetime import timezone`.

---

### 6. Missing Network Scoping in `find_open_trade`

**File:** `src/nadobro/models/database.py`

```python
def find_open_trade(telegram_id: int, product_id: int, network: str = None) -> Optional[dict]:
    if network:
        return query_one(..., network=network)
    return query_one(...)  # No network filter
```

**Problem:**
- When `network` is `None`, query returns the most recent `filled` trade across both testnet and mainnet
- User could have positions on both; PnL and close logic could use the wrong trade

**Fix:** Always require `network` for consistency, or explicitly document cross-network behavior.

---

## Medium Priority Issues

### 7. Broad `except Exception` Swallowing Errors

**Example locations:** `messages.py`, `callbacks.py`, `intent_handlers.py`, `formatters.py`, etc.

**Problem:**
- Many `except Exception: pass` blocks hide real bugs
- Example: Broad exception swallowing in various handlers can hide real bugs
- Production debugging becomes difficult

**Fix:** Log exceptions before `pass`; use specific exception types where possible; surface user-friendly errors where appropriate.

---

### 8. DB Connection Pool Not Closed on Shutdown

**File:** `src/nadobro/main.py`

**Problem:**
- `main.py` shutdown calls `stop_scheduler`, `stop_runtime`, `stop_workers`, `bot_app.shutdown`
- No explicit `db.close_pool()` or equivalent
- Connection pools may keep connections open until process exit

**Fix:** Add `db.close_pool()` or similar in `finally` if `db.py` exposes it.

---

### 9. Potential Race in Strategy Start/Stop

**File:** `src/nadobro/services/bot_runtime.py`

**Problem:**
- `_tasks` dict and task lifecycle managed with asyncio; no locks
- Concurrent start/stop for same user could lead to duplicate tasks or inconsistent state
- `restore_running_bots` runs at startup; if user also taps Start, could overlap

**Fix:** Use `asyncio.Lock` per user or per task key when modifying `_tasks`.

---

### 10. Lambda Closure in `execution_queue`

**File:** `src/nadobro/services/execution_queue.py`

```python
lambda: _strategy_handler
```

**Status:** OK — captures global by reference, not loop variable. No bug.

---

### 11. Nado API Response Validation

**File:** `src/nadobro/services/nado_client.py`

**Problem:**
- Many API responses parsed with `.get()` and defaults; malformed or unexpected JSON could produce `0` or `None` where a number is expected
- Trade execution could proceed with wrong size/price if API changes format
- `get_balance` returns `{"exists": False, "balances": {}}` on failure; callers assume `balances` is `dict[int, float]`

**Fix:** Add schema validation (e.g. Pydantic) for critical API responses; fail fast on malformed data.

---

## Low / Informational

### 12. `requirements.txt` vs `pyproject.toml` Drift

- `requirements.txt` pins exact versions; `pyproject.toml` uses `>=`  
- Missing deps in `requirements.txt`: `requests`, `PySocks` (used by `cmc_client`, proxies)  
- Prefer single source of truth (`pyproject.toml` + `uv` or `pip install -e .`)

---

### 13. No Structured Logging

- All logging uses `logger.info/warning/error` with `%s` formatting
- No correlation IDs, no JSON logs for production log aggregation
- Consider `structlog` or `python-json-logger` for production

---

### 14. Test Coverage Gaps

- Only 3 test files; no tests for `trade_service`, `user_service`, `nado_client`, handlers
- No integration tests for trade flow, wallet linking
- Add tests for critical paths (execute_market_order, close_position, validate_trade)

---

### 15. `escape_md` and Markdown v2

- `formatters.escape_md` escapes special chars for Telegram MarkdownV2
- Ensure all user-provided content passed to `reply_text(parse_mode=ParseMode.MARKDOWN_V2)` goes through `escape_md`
- Audit: positions, alerts, trade previews, portfolio — most flows use `escape_md`; verify any new flows do too

---

### 16. `execute_returning` and `psycopg2.sql`

- `models/database.py` uses `pgsql.SQL`, `pgsql.Placeholder()`, `pgsql.Identifier`
- `execute_returning(query, vals)` passes Composed + params to `cur.execute()`
- Confirmed: psycopg2 supports Composed objects with params. No change needed, but worth keeping in mind for DB migrations.

---

### 17. Admin User IDs Parsing

**File:** `src/nadobro/config.py`

```python
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip()]
```

- Correctly strips whitespace; invalid integers would crash at import
- Document expected format in README or deploy docs

---

### 18. `LOWIQPTS_BRIDGE_CHAT_ID` Validation

**File:** `main.py`

- `int(bridge_chat_id)` can raise if chat ID is non-numeric
- Wrapped in `try/except (TypeError, ValueError)` — good

---

## Security Checklist

| Check | Status |
|-------|--------|
| No secrets in code | ✓ |
| ENCRYPTION_KEY validated at startup | ✓ |
| Passphrase never logged | ✓ |
| User input escaped for Markdown | ✓ (via `escape_md`) |
| SQL uses parameterized queries | ✓ |
| No raw SQL from user input | ✓ |
| Wallet keys encrypted at rest | ✓ |
| Admin actions gated by ADMIN_USER_IDS | ✓ |

---

## Recommended Next Steps

1. **Immediate:** Remove or make conditional the debug log path; fix rate limit to include failed trades.
2. **Short-term:** Add cache size limits; replace `datetime.utcnow()`; improve error logging.
3. **Medium-term:** Add tests for trade_service, user_service, handlers; introduce structured logging.
4. **Ongoing:** Monitor memory usage in production; add metrics for queue depth and latency.
