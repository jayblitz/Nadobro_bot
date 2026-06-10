# Error-handling policy for money paths

NadoBro trades real funds. A swallowed exception in the wrong place is a
position that exists on the venue but not in our books, or an order that
rests open after the user asked to stop. This policy says where broad
`except Exception` swallows are allowed, and is enforced by
`tests/lint/test_error_policy_money_paths.py`.

## Categories

**A. Money paths — never swallow silently.**
Order placement / cancellation / status, position open/close, balance and
margin mutations, fill booking and PnL accounting, signer-key handling.

A broad `except Exception` here must do at least one of:
1. **Re-raise** (possibly wrapped, e.g. `AdapterError`) so the caller or the
   executor `_guard` retry/terminate machinery sees it;
2. **Log at WARNING or higher**, naming the *consequence*, not just the
   error (e.g. "order may still be resting", "fills may be unbooked") — so
   an operator grepping logs understands the exposure;
3. Carry an explicit `# policy: degrade-ok(<reason>)` marker on the
   `except` line or the line above it, when silent degradation is a
   deliberate, reviewed decision.

**B. Read paths with fallbacks — degrade allowed, log at DEBUG.**
Catalog lookups, caches, market-data refreshes that have a static fallback
or simply retry next tick. Prefer narrow exception types where practical.

**C. Cosmetic / UI — degrade allowed.**
Telegram message edits, keyboard refreshes, banner updates. Failure must
never affect trading state.

## The lint

`tests/lint/test_error_policy_money_paths.py` scans the money modules for
**broad** silent handlers: `except:` / `except Exception:` /
`except BaseException:` whose body is only `pass` / `continue` / `...`.
Narrow typed handlers (e.g. `except asyncio.QueueFull:`) are not flagged —
silently dropping a *specific* expected condition is usually deliberate.

- The **engine** (`src/nadobro/engine/**`) baseline is **zero**. New silent
  swallows there fail CI immediately.
- The **services** money modules carry a frozen per-file baseline (the debt
  at the time this policy landed). The count may go down, never up. When
  you clean a file up, lower its baseline in the test.
- To deliberately keep a silent swallow, tag it:

  ```python
  except Exception:  # policy: degrade-ok(cache warm; next tick retries)
      pass
  ```

## History

Why this exists: the `_FairStrategyQueue.full` TypeError (June 2026) broke
every strategy enqueue in production, and the failure was invisible because
the scheduler caught it broadly and logged at DEBUG. The prior code audit's
#1 critical (silent cancel failures leaking open orders) was the same
pattern. See `audit_patches/AUDIT_REPORT.md` history and the repo audit of
2026-06-10.
