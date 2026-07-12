# Nado Vault (NLP) in Nadobro

The Nado Vault is Nadobro's in-Telegram surface for [NLP — the Nado Liquidity Provider vault](https://docs.nado.xyz/nlp). Users deposit USDT0, receive NLP tokens that represent a proportional claim on Nado's MM sub-vaults, and earn a share of the venue's institutional market-making yield. Nadobro turns the entire flow into a few Telegram taps and keeps users on the safe side of the gateway rules.

This page is the canonical reference for how the Vault is wired up inside the bot.

## What users see

The Vault lives on the **Home card** (callback `vault:home`). Tapping it opens the Vault home, which shows:

- Pool **TVL** and **APR** (from archive NLP snapshots).
- **Your Position**: USDT0 NAV, NLP balance, all-time earned, unrealized PnL.
- Idle USDT0 in the trading account.
- Deposit room and venue-authoritative **max mintable** (via `max_nlp_mintable`).
- Lockup countdown (4-day post-mint, see below).
- Opt-in **deposit opening alerts** (`vault:watch:on` / `vault:watch:off`).
- Buttons: **Deposit**, **Withdraw**, **Refresh**, **Back**.

**Deposit picker** offers presets (`$100`, `$500`, `$1,000`, `$5,000`), a `Max` button sized to whichever of (idle USDT0, remaining cap room) is smaller, and a `✍️ Custom amount` prompt that captures a free-text USDT0 amount.

**Withdraw picker** is locked while the 4-day timer is active. Once unlocked, it offers `25% / 50% / 75% / 100%` of the user's NLP balance plus a custom NLP amount. The confirmation card shows the estimated USDT0 out, the gateway withdraw fees, and the approximate net USDT0 the user will receive.

## Mechanics that matter

Source: [docs.nado.xyz/nlp](https://docs.nado.xyz/nlp).

| Rule | Value | Where it's enforced |
| --- | --- | --- |
| Quote asset | USDT0 only | `vault/nlp_vault_service.py:USDT0_PRODUCT_ID` |
| Private Alpha cap per account | 20,000 USDT0 | `PRIVATE_ALPHA_CAP_USDT0` |
| Post-mint lockup before burns are allowed | 4 days | `LOCKUP_SECONDS`, `lockup_remaining_seconds()` |
| Withdraw fee | $1 sequencer + max($1, 10 bps × amount) | `estimate_withdraw_fee_usdt0()` |
| Redemption price | NAV at burn time | Set by gateway; surfaced as "approximate" |
| Borrow protection on mint | `spot_leverage=False` | `NadoClient.mint_nlp(...)` |

If a user tries to burn within the lockup window, Nadobro fails fast with a friendly Telegram error before signing — we don't waste a gateway rejection on it.

## Under the hood

### Service layer

`src/nadobro/vault/nlp_vault_service.py` orchestrates everything:

```text
get_user_vault_snapshot(telegram_id) -> dict
    Returns USDT0 balance, NLP balance, NLP USDT0 value, pool info, and
    lockup countdown. Drives the Vault home card.

deposit_to_vault(telegram_id, usdt0_amount) -> dict
    Sanity-checks balance and cap, then calls NadoClient.mint_nlp(...).

withdraw_from_vault(telegram_id, nlp_amount) -> dict
    Sanity-checks NLP balance + lockup, then calls NadoClient.burn_nlp(...).
```

### SDK wrappers

`src/nadobro/venue/nado_client.py` exposes two new methods that wrap the pinned `nado-protocol==0.3.3` Python SDK:

```python
# Deposit
from nado_protocol.engine_client.types.execute import MintNlpParams
params = MintNlpParams(
    quoteAmount=int(round(usdt0_amount * 1e18)),
    spot_leverage=False,  # never borrow on a vault deposit
)
client.market.mint_nlp(params)

# Withdraw
from nado_protocol.engine_client.types.execute import BurnNlpParams
params = BurnNlpParams(nlpAmount=int(round(nlp_amount * 1e18)))
client.market.burn_nlp(params)
```

`quoteAmount` and `nlpAmount` are integers in the x10^18 representation per Nado's gateway contract; the wrappers do the float-to-int conversion.

Two helpers cover state we read but don't sign:

```python
client.get_nlp_position()  # composed locally from `nlp_locked_balances` + spot balance + oracle price (gateway no longer exposes `nlp_position`)
client.get_nlp_pool_info() # REST `nlp_pool_info` query — TVL / pool stats
```

### Telegram handler

`src/nadobro/handlers/vault_handler.py` implements the callback router for `vault:*` events:

| Callback | Effect |
| --- | --- |
| `vault:home`, `vault:refresh` | Render the Vault home card |
| `vault:deposit` | Show the deposit picker |
| `vault:deposit:preset:<amount>` | Show the deposit confirmation card |
| `vault:deposit:custom` | Prompt for a free-text USDT0 amount |
| `vault:deposit:confirm:<amount>` | Sign + submit `mint_nlp` |
| `vault:withdraw` | Show the withdraw picker (or lockup notice) |
| `vault:withdraw:pct:<pct>` | Show confirmation card for an N% burn |
| `vault:withdraw:custom` | Prompt for a free-text NLP amount |
| `vault:withdraw:confirm:<nlp_amount>` | Sign + submit `burn_nlp` |
| `vault:watch:on` | Opt in to deposit-capacity alerts |
| `vault:watch:off` | Opt out of deposit-capacity alerts |

Custom-amount continuation messages route through `handle_vault_text` in the same module, which is wired into `handlers/messages.py` ahead of the brief-intent gate.

## Deposit opening alerts

Users below the $20k Private Alpha cap can opt in on the vault card. A scheduler job polls `max_nlp_mintable` every 60s (configurable via `NADO_VAULT_DEPOSIT_WATCH_SECONDS`) and sends a Telegram message whenever capacity transitions from closed (≤ `VAULT_DEPOSIT_CLOSED_EPSILON_USDT0`, default $1) to open (≥ `VAULT_DEPOSIT_OPEN_MIN_USDT0`, default $100). Controlled by `NADO_VAULT_DEPOSIT_WATCH=1`.

## Safety guardrails

- `spot_leverage=False` on every deposit so a mint can never silently lever the user's trading account.
- Local lockup gate (`lockup_remaining_seconds`) fails before signing if the user is still inside the 4-day window.
- USDT0 balance pre-check before signing the deposit, with a clear error if they need to fund the account first.
- Private Alpha cap pre-check returns the exact USDT0 of headroom they have so they can size the rest of their deposit accordingly.

## What's deliberately not in scope

- **Auto-compound** — the bot does not auto-mint accumulated NLP yield. Users see updated NAV on each `vault:refresh`.
- **Multi-subvault selection** — the gateway allocates LP capital across MM sub-vaults by predefined weights; Nadobro does not expose per-subvault controls.
- **On-chain withdraw outside the gateway** — burns always route through Nado's gateway execute endpoint via the SDK.

## See also

- [docs.nado.xyz/nlp](https://docs.nado.xyz/nlp) — vault rules and economics.
- [docs.nado.xyz/developer-resources/api/gateway/executes/burn-nlp](https://docs.nado.xyz/developer-resources/api/gateway/executes/burn-nlp) — burn execute spec.
- [docs.nado.xyz/developer-resources/api/gateway/queries/nlp-pool-info](https://docs.nado.xyz/developer-resources/api/gateway/queries/nlp-pool-info) — pool-info query spec.
- [nadohq/nado-typescript-sdk](https://github.com/nadohq/nado-typescript-sdk) — equivalent TypeScript SDK.
