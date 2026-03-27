# NadoBro Review Findings

## Scope

This review focused on the highest-risk user flows, background runtime reliability, and visible command/help copy.

## Issues Found

### User-facing bugs

- Portfolio refresh had multiple implementations across `callbacks`, `messages`, and `home_card`, which meant different code paths could block, fail differently, or show inconsistent data.
- Home-card portfolio and positions views performed synchronous API and database work inside async flows, which could stall the Telegram event loop.
- Portfolio history paging trusted callback payloads and could raise `ValueError` on malformed callback data.
- Markdown edit fallbacks in callback responses were weaker than the existing home-card fallback and could still fail on Telegram parse edge cases.
- Several callback and reply-button deck views were still doing synchronous points and status work from async handlers.

### Process and runtime issues

- Copy-trading polling used synchronous Nado and database calls inside async loops, which could slow unrelated bot activity during polling windows.
- Copy-poll shutdown only cancelled the task reference and did not await task completion.
- Execution queue shutdown cancelled workers without awaiting them, increasing the chance of messy shutdown behavior.

### Copy and command drift

- Telegram menu command descriptions, `/help`, onboarding, and dashboard copy described overlapping features with different wording and stale examples.

## Changes Applied

### Shared user-flow fixes

- Centralized portfolio rendering in `src/nadobro/handlers/home_card.py` via `build_portfolio_view()`.
- Centralized positions rendering in `src/nadobro/handlers/home_card.py` via `build_positions_view()`.
- Updated `callbacks`, `messages`, and home-card routing to reuse the shared view builders.
- Moved home-card module resolution for wallet, portfolio, positions, points, and settings onto `run_blocking(...)` so card flows no longer do synchronous work on the event loop.
- Updated callback markdown fallback to reuse the plain-text fallback used by home cards.
- Hardened portfolio history paging against malformed callback payloads.
- Wrapped callback and command status refresh paths with `run_blocking(...)`.
- Wrapped points dashboard loads in `callbacks` and `messages` with `run_blocking(...)`.

### Process fixes

- Made execution-queue shutdown await worker cancellation in `src/nadobro/services/execution_queue.py`.
- Updated `main.py` to await worker shutdown cleanly.
- Hardened copy-poll lifecycle in `src/nadobro/services/copy_service.py` so stop waits for the background task to finish.
- Moved leader snapshot loading and key copy-sync operations in `copy_service.py` behind `run_blocking(...)` to reduce event-loop blocking.

### Product-copy updates

- Refreshed Telegram menu command descriptions in `main.py`.
- Updated onboarding and dashboard text in `src/nadobro/handlers/commands.py` and `src/nadobro/handlers/callbacks.py`.
- Updated home dashboard summary text in `src/nadobro/handlers/home_card.py`.
- Rewrote `/help` content in `src/nadobro/handlers/formatters.py` to match the actual current modules and supported example commands.

## Remaining Risks

- The runtime and scheduler stack still has room for deeper resilience work, especially around queue backpressure policy and alert-client readiness signaling.
- Copy trading still performs a meaningful amount of synchronous business logic per mirror; the changes here reduce event-loop blocking, but a fuller service-layer refactor could improve throughput further.
- There is still limited automated coverage for whole-app lifecycle behavior compared with the unit-test coverage for parsing and strategy logic.
