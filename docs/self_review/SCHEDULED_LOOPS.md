# Scheduled loops

Recurring streams moved onto time-based loops. Design rule (from Anthropic's
"Getting started with loops"): **deterministic scripts do the checking; Claude
only gets invoked to triage when a script says something is wrong.** Align each
interval with how fast the underlying state actually changes.

## Loop A — nightly attribution reconciliation (the important one)

Attribution/PnL drift is this repo's most recurrent bug class (the venue
reports no per-fill realized PnL, so History/volume rest entirely on our fill
attribution). `scripts/reconcile_attribution.py` is the parameterless drift
detector: 6 checks over a lookback window, **exit 0 clean / 1 drift / 2 error**.
Hard findings (swallowed manual fills, unlinked manual closes, untagged close
digests, session rollup mismatches) fail the run; soft candidates (manual fills
inside a session window, post-stop flatten fills) WARN only unless `--strict`,
because they can be legitimate manual trading.

**Option 0 — Supabase connector (verified live 2026-07-09, no DSN needed).**
The prod DB is the `NadoBro` Supabase project (`gevenrdkwrodvojsszmr`), already
connected to Claude. A scheduled Claude task can run all six checks as one
read-only counts query via the connector's `execute_sql` and only dig into
samples/triage when a HARD count > 0. A live dry-run returned all HARD checks
0 and correctly warn-flagged one genuine manual trade. Ask Claude to "run the
attribution drift check" any time, or schedule it nightly.

**Option 1 — cron + script.** Run it wherever the prod DB is reachable, with a
**read-only** DSN in `NADO_AUDIT_DATABASE_URL` (the script refuses
`DATABASE_URL` and forces the session read-only either way):

```bash
NADO_AUDIT_DATABASE_URL='postgresql://<readonly-role>@.../nadobro' \
  .venv/bin/python scripts/reconcile_attribution.py --network mainnet --hours 24
```

Nightly cron on the dev machine (02:30, after the day's sessions), escalating
to a Claude triage run only on drift:

```cron
30 2 * * * cd $HOME/Nadobro_bot && NADO_AUDIT_DATABASE_URL='<readonly-dsn>' \
  .venv/bin/python scripts/reconcile_attribution.py --network mainnet \
  > /tmp/nadobro_reconcile.txt 2>&1 \
  || claude -p "Attribution drift detected. Follow the triage prompt in docs/self_review/SCHEDULED_LOOPS.md. Report: $(cat /tmp/nadobro_reconcile.txt)"
```

(Or keep it manual-ish at first: run the script from cron, and paste the report
into a Claude session yourself when it exits 1. A Claude Code cloud routine via
`/schedule` also works **only if** that environment carries the read-only DSN —
don't hand prod credentials to a cloud routine casually.)

### Triage prompt

```
Attribution drift was detected by scripts/reconcile_attribution.py — report
below. Triage per docs/self_review/SELF_REVIEW_WORKFLOW.md: re-run each hard
finding's underlying query read-only, classify each [VERIFIED] / [SUSPECTED] /
benign with file:line evidence from the attribution path
(services/order_intents.py::link_digest_intent, nado_sync._back_link_intent,
session rollup in models/database.py). For each [VERIFIED] regression, add a
strict-xfail guardrail per the protocol and propose the fix on a branch. Never
modify production data. Report:
```

First triage session, regardless of findings: also capture a handful of live
funding rows while you're connected and settle **FUNDING-SIGN** (see
GOAL_LOOPS.md — it needs live data, then becomes a normal fix goal).

## Loop B — PR babysitting (`/loop`, only while a PR is live)

```
/loop 5m check PR #<N> with gh: address new review comments, fix failing CI
checks, push, and re-request review. Stop when the PR is merged or closed.
```

Run it in that PR's worktree so the main session stays free. Needs the
permission allowlist to be broad enough that the loop doesn't stall on
prompts (see the settings cleanup — recommendation #5).

## Loop C — alpha-brief prototype (`/schedule`, cloud, no secrets)

Prototype of the "NadoBro proactively scans for edges" product vision (Night
HOWL / AI-assistant roadmap) as a routine BEFORE building it into the bot —
web-only, so it's safe to run in the cloud:

```
/schedule every day at 07:00: scan X, crypto news, and the Nado/Ink docs and
blogs for anything actionable for a perps trader on Nado (new listings,
funding-rate dislocations, incentive programs, points epochs, Ink ecosystem
launches, competitor DEX changes). Write a <300-word brief: 3-5 bullets, each
with source link + why it's tradeable. Skip days with nothing actionable —
say "no edge today" instead of padding.
```

Read the output for a week; whatever proves useful is the spec for the in-bot
version (route it through services/llm_gateway.py like everything else).

## Not scheduled here

- CI already covers per-PR invariants (`.github/workflows/self-review.yml`) and
  the security audit — don't duplicate those as loops.
- Redeploy/resume anything: never scheduled. Redeploys stand down; resuming is
  strictly user-initiated (hard rule).
