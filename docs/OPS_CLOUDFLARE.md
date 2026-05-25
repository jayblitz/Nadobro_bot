# Cloudflare 403 challenge — operations runbook

This bot talks to three Nado endpoints over HTTPS:

| What                | Host                                | Used by                                            |
| ------------------- | ----------------------------------- | -------------------------------------------------- |
| Gateway REST        | `gateway.{mainnet,testnet}.nado.xyz` | `services.nado_client`, `services.product_catalog` |
| Archive REST        | `archive.{mainnet,testnet}.nado.xyz` | `services.nado_archive`                            |
| Archive v2 symbols  | `archive.{...}.nado.xyz/v2/symbols`  | `services.product_catalog`                         |

When Cloudflare (Nado's edge) decides our traffic looks botty, it answers any
plain JSON request with a 403 Forbidden whose body is an HTML "Just a moment…"
interstitial. Symptoms:

```
WARNING] src.nadobro.services.nado_client: REST returned non-JSON status=403
content_type=text/html; charset=UTF-8 body='<!DOCTYPE html>...Just a moment...'
```

Until the challenge clears, the bot has:

- Empty product catalogs (mainnet menu collapses to "Switch to mainnet"-style
  errors even when the user is already on mainnet).
- Stale portfolio snapshots.
- Strategy cycles that report success but fail to fetch market price.

## How the code defends itself

The defenses live in `services/http_session.py`:

1. **Hardened headers**: every outbound call goes through a shared
   `requests.Session` configured with browser-like `User-Agent`,
   `Accept-Language`, `Sec-Fetch-*`, `Origin: https://app.nado.xyz`, and a
   `Referer`. This is enough for Cloudflare's lightweight bot check on most
   days.
2. **CF-aware retries**: `cf_request` recognises the
   `403 + content-type:text/html` Cloudflare interstitial and retries with
   exponential backoff plus jitter (default `0.5s, 1.0s, 2.0s` ±`0.4s`).
3. **Per-host circuit breaker**: after 8 challenges within a 10s window the
   circuit opens for 30s. Subsequent calls short-circuit to `None`; callers
   serve from `_spot_catalog_cache` / `_catalog_cache` via the stale-TTL
   path (`NADO_PRODUCT_CATALOG_STALE_TTL_SECONDS`, default 900s).
4. **Throttled logs**: only one Cloudflare-warning line per host per minute
   (was: hundreds per second).

## Tunables (env vars)

| Env var                                  | Default | Purpose                                              |
| ---------------------------------------- | ------- | ---------------------------------------------------- |
| `NADO_HTTP_USER_AGENT`                   | Chrome  | Override the UA string if Cloudflare blocks ours.    |
| `NADO_CF_RETRY_MAX`                      | 2       | Retries on Cloudflare 403 before giving up.          |
| `NADO_CF_RETRY_BASE_SECONDS`             | 0.5     | Base backoff before the 2× ramp.                     |
| `NADO_CF_RETRY_JITTER_SECONDS`           | 0.4     | Random jitter added to each retry.                   |
| `NADO_CF_BREAKER_THRESHOLD`              | 8       | Challenges in window before opening the breaker.     |
| `NADO_CF_BREAKER_WINDOW_SECONDS`         | 10      | Sliding window for the threshold above.              |
| `NADO_CF_BREAKER_COOLDOWN_SECONDS`       | 30      | How long the breaker stays open.                     |
| `NADO_CF_LOG_THROTTLE_SECONDS`           | 60      | Cooldown between CF-warning log emissions per host.  |
| `NADO_PRODUCT_CATALOG_STALE_TTL_SECONDS` | 900     | How long the cached catalog stays valid after a 403. |

## What to do when you see the storm in logs

1. Check `services.http_session.breaker_snapshot()` (or grep
   `Cloudflare circuit OPEN`). If the breaker is open, the bot is correctly
   self-throttling; users keep seeing the *previous* catalog instead of an
   empty list. No action required on the bot side.
2. If the breaker is stuck open for >5 minutes, escalate to the Nado team
   using the templated message below.

## Templated message to send to Nado

> Hi Nado team — our Telegram bot is consistently being challenged by
> Cloudflare on `gateway.{mainnet,testnet}.nado.xyz/query` and
> `archive.{mainnet,testnet}.nado.xyz/v2/symbols`. We see HTTP 403 with the
> "Just a moment…" interstitial across reasonable request rates
> (≈1–2 rps per worker, normal browser-like headers).
>
> Could you either (a) whitelist our egress IPs (we will share static ones),
> (b) issue us an API key / bearer token that bypasses the bot-management
> rule for these paths, or (c) raise the bot score threshold for the
> `/query` and `/v2/symbols` endpoints?
>
> Our service ID is **`<fill-in>`**, our egress IPs are
> **`<fill-in / contact ops>`**, and we are happy to add any custom header or
> auth scheme you prefer. Even a 30 rpm shared bucket would unblock the
> per-user portfolio sync that 1000s of our users rely on.
>
> Thanks!

## Why the previous reproducer showed "Switch to mainnet"

The old menu literal was hardcoded. When Cloudflare blocked the v2 symbols
fetch, `list_volume_spot_product_names("mainnet")` collapsed to an empty list
and the menu rendered `"Switch to mainnet"` to a user who was already on
mainnet. This module now serves the stale catalog so the menu keeps working,
and the menu copy now distinguishes "no pairs listed on $network" vs
"catalog temporarily unavailable on $network" so users get accurate guidance.
