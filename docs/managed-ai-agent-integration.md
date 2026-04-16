# Managed AI Agent Integration Guide

This guide enables Nadobro's hybrid Managed AI mode in the existing single Telegram bot.

## What this adds

- Opt-in managed AI conversation mode (`/agent_on`, `/agent_off`, `/agent_status`)
- Bro-style conversational layer ("Your Trading Bro for Life")
- Strategy triggers from natural language mapped to existing strategy runtime
- No bypass of existing wallet safety, linked signer checks, or budget/risk logic
- Existing backend intelligence remains the reasoning and tooling brain

## Environment variables

No mandatory new env vars are required for this first rollout.

Recommended existing vars for full AI quality:

- `XAI_API_KEY` and/or `OPENAI_API_KEY`
- `NADO_AI_PROVIDER` (`auto`, `xai`, `openai`)
- `XAI_SUPPORT_MODEL` / `OPENAI_SUPPORT_MODEL`
- `TELEGRAM_TRANSPORT` (`polling` or `webhook`)

Optional rollout guardrail (recommended to add in your deployment config process):

- `NADO_MANAGED_AGENT_ENABLED` (`true`/`false`, default `true`) - global runtime switch for managed mode

## BotFather setup

1. Open `@BotFather` -> `/mybots` -> select your Nadobro bot.
2. Ensure command menu includes:
   - `/agent_on` - Enable managed AI mode
   - `/agent_off` - Disable managed AI mode
   - `/agent_status` - Check managed AI mode
3. Keep existing commands:
   - `/start`, `/help`, `/status`, `/ops`, `/revoke`, `/stop_all`
4. Update bot description/about text to mention:
   - "Your Trading Bro for Life"
   - AI-assisted strategy routing with safety guardrails

## How managed mode works

When a user runs `/agent_on`:

- Free text remains in Telegram chat as normal.
- Managed mode intercepts text after existing pending-flow and intent handlers.
- Strategy launch/stop requests are routed into existing backend services:
  - Strategy launch -> `start_user_bot(...)`
  - Stop all strategies -> `stop_all_user_bots(...)`
- Non-action Q&A/analysis requests are delegated to existing `knowledge_service` (`answer_nado_question`), preserving tool access and external-source parsing.

When a user runs `/agent_off`:

- The message pipeline returns fully to standard Nadobro behavior.

If `NADO_MANAGED_AGENT_ENABLED=false`:

- `/agent_on` will refuse activation and explain that managed mode is globally disabled.
- Existing users with local opt-in will also bypass managed routing until the flag is turned back on.

## Safety guarantees retained

- Linked signer readiness check is enforced before managed-mode strategy launches.
- Existing strategy runtime and trade services remain execution authority.
- Budget/risk controls continue through existing strategy logic (including Bro/Alpha mode safeguards).
- No direct unmanaged order placement path is introduced by managed mode.

## External source and X parsing

Managed mode delegates analysis to `knowledge_service`, so current capabilities remain available:

- X/Twitter search and Nado/Ink source routing
- Knowledge base retrieval and synthesis
- Existing market/sentiment/external context pipeline

## Manual verification checklist

1. Start bot and run `/agent_status` -> should show `OFF` by default.
2. Run `/agent_on` -> status should switch to `ON`.
3. Send casual text ("gm") -> should reply in bro tone.
4. Ask market question requiring external context -> ensure response returns normally.
5. Ask to launch strategy (examples):
   - "start grid BTC 3x"
   - "run r-grid ETH 4x"
   - "activate alpha agent"
6. Confirm strategy starts through existing runtime status checks (`/status`).
7. Test wallet-not-linked user path -> managed mode should refuse launch with setup guidance.
8. Run `/agent_off` -> message handling should revert to baseline flow.

## Rollback

Immediate rollback options:

- User-level: run `/agent_off`
- Ops-level: set `NADO_MANAGED_AGENT_ENABLED=false` and restart process
- System-level: remove managed-agent commands from command menu and redeploy previous build

No database migration is required for this integration.
