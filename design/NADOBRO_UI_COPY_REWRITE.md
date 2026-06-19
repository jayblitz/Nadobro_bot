# Nadobro UI/UX + Copy Uplift (Draft v1)

Prepared as a content + Telegram bot UX review. Nothing here is wired into code yet. This is the draft to approve before we touch live surfaces.

Voice setting: **smooth and light bro**. Confident, human, a little personality. No cringe, no slang dump, and always serious around money and risk.

Source of truth: the live GitBook docs (nadobro.gitbook.io/docs) reconciled against the actual handlers in `src/nadobro/handlers/`. Every command and feature below was checked against both.

---

## 1. What's wrong right now (the short version)

Three things are dragging the experience down.

**The copy reads like a bot wrote it.** Em dashes everywhere as separators, phrases like "Your trading copilot is online and ready," "Configure and run automated strategies including...," "runtime health." It's clean but it's robotic. None of it sounds like the "Trading Bro for Life" the docs promise.

**The `/help` is out of date and incomplete.** It's missing live features (Nado Vault, Price Alerts, Referrals, Copy Trading, Dual Mode) and three registered commands (`/brief`, `/news`, `/desk`). It still lists "Alpha Agent" as a strategy. Per the docs that's "Bro Mode," and it's coming soon, not live.

**Names don't match across surfaces.** The same module is called three different things depending on where you tap. That quietly tells users the product is held together with tape. Full map in section 6.

---

## 2. Voice and style rules (apply everywhere)

Keep these in your back pocket for every string going forward.

- **Kill the em dash.** No `—` as a separator. Use a colon, a period, or just restructure. This is the single biggest "AI tell" in the current copy.
- **Talk to one person.** "You're in." "Your call." Not "the user" or "users."
- **Short lines win.** Telegram is a phone screen. One idea per line.
- **Personality on the chrome, discipline on the money.** Headers and empty states can have swagger. Trade previews, balances, risk warnings, and errors stay calm and exact.
- **No filler verbs.** Cut "simply," "seamlessly," "leverage" (as a verb), "powerful," "robust," "comprehensive."
- **Bro identity, used sparingly.** "I got you," "let's go," "easy" land when they're rare. Once per screen, max.
- **Active and present.** "Pick a strategy." Not "Strategies can be configured."

Quick gut check: read it out loud. If it sounds like a press release or a settings menu from 2014, rewrite it.

---

## 3. The updated `/help` (headline fix)

This is rebuilt against the current docs and the actual registered command handlers. It adds the missing commands, fixes the strategy list, and folds in the modules that exist today.

> 📖 **Nadobro Guide**
> *Your trading bro, right here in chat.*
>
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> **Commands**
> `/start` Open your home dashboard
> `/help` This guide
> `/status` Bot health, setup, and live strategies
> `/ops` Order flow and runtime diagnostics
> `/desk` Talk a trade out loud, preview it, then confirm
> `/brief` Your full morning market brief
> `/news` Latest market news (add a category to filter)
> `/mm_status` Live market-making dashboard (GRID / RGRID / DGRID / Mid)
> `/mm_fills` Recent fills on your active MM strategy
> `/stop_all` Stop every running strategy and flatten bot exposure
> `/revoke` How to revoke your 1CT signer
> `/agent_on` Turn on managed AI chat
> `/agent_off` Turn off managed AI chat
> `/agent_status` Check AI chat status
>
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> **What's inside**
>
> 💼 **Wallet Vault**
> Link your wallet with the secure 1CT flow, check balances, manage your signer.
>
> 🤖 **Trade Console**
> Market and limit orders. Tap through the guided flow or just type the trade.
>
> 🧠 **Strategy Lab**
> Market making (GRID, Reverse GRID, Dynamic GRID, Mid Mode) plus Volume Bot, Copy Trading, and Delta Neutral. Bro Mode (AI autonomous) is coming soon. Every strategy opens with controls, safety defaults, and a pre-trade readout.
>
> 📁 **Portfolio Deck**
> Open positions, realized and unrealized PnL, trade history, and analytics in one place.
>
> 💰 **Nado Vault**
> One-tap deposit and withdraw on Nado's market-making LP vault. We ping you when a slot opens.
>
> 🔔 **Alerts**
> Set price and funding triggers, get pinged instantly.
>
> 🏆 **Nado Points + Market Radar**
> Track your Season 1 points, volume, and cost-per-point in real time, plus a live market read.
>
> 🎁 **Referrals**
> Claim your code (live now) and bring traders in. Fee commissions are coming soon.
>
> 🌐 **Execution Mode**
> Flip between Testnet and Mainnet anytime. Practice risk-free, then go live.
>
> 🔒 **Security**
> Your 1CT signer can trade but can never withdraw. Never share a private key or seed phrase. Use a dedicated wallet for automation.
>
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> **Just type it**
> `Long BTC 0.01 at 5x`
> `Short ETH 0.05 limit 2400`
> `Show my portfolio`
> `Close all positions`
> `What's unified margin?`
>
> Stuck? Tell me what you tapped and paste the error. I'll sort it.

**Facts corrected vs. the old `/help`:**

- Added `/desk`, `/brief`, `/news` (registered in `main.py`, were missing from help and the Telegram command menu).
- Strategy list fixed: added **Copy Trading**, replaced **"Alpha Agent"** with **Bro Mode (coming soon)**, and labeled **Volume Bot** correctly.
- Added the modules that exist today but weren't in help: **Nado Vault, Alerts, Referrals, Execution Mode**.
- `/stop_all` now matches what it really does (stops strategies *and* flattens bot exposure, per the BotCommand description in code).
- Renamed "Points And Market Radar" to "Nado Points + Market Radar" so it matches the buttons (see section 6).

---

## 4. Rewritten core surfaces

Old copy on the left intent, new copy below it. These map to specific functions in `src/nadobro/handlers/formatters.py` and `commands.py`. Em dashes are gone, escaping is left to your existing `escape_md`/`_loc` helpers.

### Welcome / language picker (`WELCOME_MSG`, commands.py)
Old: "Trade perps on Nado DEX from Telegram with guided execution, portfolio tools, automation, and AI support."

New:
> Welcome to Nadobro 👋
>
> Trade perps on Nado straight from Telegram. Type the trade, tap to confirm, done. Automation, portfolio, and AI are all here too.
>
> Pick your language:

### Terms card (`WELCOME_CARD_MSG`)
Old: "🔥 You're in!"

New:
> 🔥 You're in.
>
> Tapping **"Let's Get It"** means you're good with the Terms of Use and Privacy Policy.
>
> 🔐 How it works:
> We spin up a secure 1CT signing key for your account. Your main wallet keys are never touched. Revoke whenever you want.

### Home command center (`fmt_home_header` + `fmt_home_command_center_card`)
Old header: "🎯 Nadobro Command Center" / "Your trading copilot is online and ready."

New header:
> 🎯 **Nadobro Command Center**
> *Your trading bro's online. Let's get it.*

New toolkit tree (note: em dashes replaced with a clean middot, names aligned to section 6):
> **Your toolkit**
> ├ 💼 **Wallet Vault** · balances and signer
> ├ 🤖 **Trade Console** · place perp orders
> ├ 🧠 **Strategy Lab** · automation
> ├ 📁 **Portfolio Deck** · positions and PnL
> ├ 💰 **Nado Vault** · LP deposit and withdraw
> ├ 🏆 **Nado Points** · rewards and market radar
> ├ 🎁 **Referrals** · your code
> ├ 🔔 **Alerts** · price and funding triggers
> ├ ⚙️ **Settings** · leverage and slippage
> └ 🌐 **Execution Mode** · mainnet or testnet

New footer:
> *Tap a button, or just type. I'll answer questions and place trades in plain English.*

### Quick snapshot block (`_fmt_network_balance_snapshot`)
Keep the data, drop the em dashes:
> **Quick snapshot**
> ├ 🌐 **Mode:** MAINNET
> ├ 💵 **USDT:** $1,240.00

### Strategy Lab intro (`fmt_strategy_hub_intro`)
Old: "Open any strategy cockpit dashboard / Edit parameters with safety defaults / Launch with pre-trade analytics"

New:
> 🧠 **Strategy Lab**
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> Pick a strategy and I'll open its cockpit.
> ├ Tune the parameters (safe defaults are already in)
> └ Launch with a pre-trade readout so nothing's a surprise
>
> *Pick one below.*

### Execution Mode (`fmt_mode_view`)
Old: "Choose where Nadobro should trade and read account state."

New:
> 🌐 **Execution Mode**
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> **Current mode:** 🌐 MAINNET
>
> *Mainnet trades real funds. Testnet is your sandbox. Flip anytime.*

### Settings / Control Panel (`fmt_settings`)
Naming note: pick one name. Recommend **Settings** everywhere (see section 6).
> ⚙️ **Settings**
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> **Right now**
> ├ Risk profile: BALANCED
> ├ Default leverage: 5x
> └ Slippage: 1%
>
> *Buttons below change language, leverage, slippage, and risk preset.*

### Trade preview (`fmt_trade_preview`)
This is money copy. Keep it tight and exact. Only the closing line changes.
Old close: "Confirm to execute this trade."
New close:
> *Look good? Hit confirm.*

### Trade executed (`fmt_trade_result`, success header)
Old: "✅ Trade Executed!"
New: "✅ **Filled.**" (and for limits: "✅ **Limit order's in.**")

### Close all confirm (`fmt_close_all_confirm`)
Old: "This will try to close every open position for the active mode. / Only continue if you want a full exit."

New:
> ⚠️ **Close everything?**
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> This closes every open position in your current mode.
>
> *Only tap continue if you want all the way out.*

### Stop all (`fmt_stop_all_result`, success)
Title "Automation stopped" stays. Hint line:
New: *Strategies are down and bot exposure is flat. Restart whenever you're ready.*

### Managed AI on (`fmt_managed_agent_enabled`)
Old: "Hey boss — talk naturally in chat. I route analysis to the backend brain and strategies through the normal safety checks."

New:
> 🧠 **Managed AI** is **ON**
> ━━━━━━━━━━━━━━━━━━━━━━━━
>
> Just talk to me normally. I send analysis to the brain and run any strategy through the usual safety checks first.
>
> *Check anytime with `/agent_status`.*

### Revoke 1CT (`fmt_revoke_card` / `fmt_wallet_revoke_steps_card`)
Steps are correct, just warm the closer.
Old: "Your main wallet stays safe. Re-link anytime via Wallet."
New: *Your main wallet never moves. Re-link anytime from Wallet Vault.*

### Referral deck (`fmt_referral_dashboard`)
Empty state old: "No direct referrals yet. Share your code with traders to start tracking their volume here."
New: *No referrals yet. Drop your code in the group chats and watch the volume roll in here.*

### Common errors / empty states
- Wallet not initialized: *Wallet's not set up yet. Hit /start to link it.*
- Positions unavailable: *Can't pull positions right now. Give it a sec and tap again.*
- Balance fetch failed: *Couldn't grab your balance just now. Try again in a moment.*
- Refreshing placeholder: *Refreshing… tap again in a sec.*

These small ones matter. Error copy is where most bots sound the most robotic.

---

## 5. Telegram command menu (`BotCommand` list in `main.py`)

The slash-menu Telegram shows is also missing the three commands and has stiff descriptions. Suggested set:

| Command | Description |
|---|---|
| start | Open your home dashboard |
| help | Guide and examples |
| desk | Talk out a trade, then confirm |
| status | Bot and strategy status |
| ops | Order flow and diagnostics |
| brief | Your morning market brief |
| news | Latest market news |
| mm_status | Live market-making dashboard |
| mm_fills | Recent MM fills |
| stop_all | Stop strategies and flatten bot exposure |
| revoke | Revoke your 1CT signer |
| agent_on | Turn on managed AI |
| agent_off | Turn off managed AI |
| agent_status | Check managed AI |

---

## 6. Naming + button consistency (the quiet credibility killer)

Same module, different name and emoji depending on where you are. Here's the drift I found in code, and the one name to standardize on.

| Module | Names found in code | Emojis found | Standardize on |
|---|---|---|---|
| Trade entry | "Trading Console" (help), "Trade Console" (home), "Trade" (button) | 🤖 / 📊 | **🤖 Trade Console** |
| Points/Radar | "Points And Market Radar" (help), "Nado Points" (home/button), "Market Radar" (button) | 🏆 / 📡 | **🏆 Nado Points** (subtitle: "+ Market Radar") |
| Wallet | "Wallet Vault" / "Wallet" | 💼 / 👛 | **💼 Wallet Vault** |
| Settings | "Control Panel" (formatter), "Settings" (button) | ⚙️ | **⚙️ Settings** |
| Mode | "Execution Mode" (home), "Dual Mode" (docs) | 🌐 / 🔄 | **🌐 Execution Mode** (keep "Dual Mode" as a docs concept only) |
| Portfolio | "Portfolio Deck" / "Portfolio" | 📁 | **📁 Portfolio Deck** |
| AI chat | "Ask NadoBro AI" (help, 🧠), Strategy Lab also 🧠 | 🧠 collision | Give AI its own mark, e.g. **💬 Ask Nadobro**, free up 🧠 for Strategy Lab |

Rule going forward: **one module = one name + one emoji, used identically on the button, the card header, and in `/help`.** When the button says one thing and the screen it opens says another, users feel it even if they can't name it.

Also lock the brand spelling. Docs mix "Nadobro" and "NadoBro." Pick one (recommend **Nadobro**) and use it everywhere.

---

## 7. Workflow + button interconnectivity

UX notes from walking the navigation graph in `keyboards.py`.

**The good:** nearly every card has a 🏠 Home button, the persistent reply keyboard is a smart anchor, and trade cards keep a consistent Home/Back. Solid base.

**Fixes worth making:**

1. **"Home shortcut enabled." is leaking.** In `home_card.py` the persistent keyboard is installed by sending then deleting a throwaway message with that text. On slow clients users can see it flash. Either make it silent or make the text intentional ("Keyboard's ready 👇").

2. **Dead-end empty states.** When positions or portfolio are empty, give the next action, not just a notice. Empty Portfolio should offer **Trade Console** and **Strategy Lab** buttons right there. Every empty state should point to the obvious next tap.

3. **Back vs Home.** Several inline cards only offer Home, so a user one level deep gets bounced all the way out. Add a **◀ Back** next to **🏠 Home** on second-level cards (Strategy cockpits, Settings sub-menus, Alerts setup).

4. **Confirm/Cancel placement.** On money actions (trade preview, close-all, stop-all) keep **Cancel on the left, Confirm on the right**, every time. Mixed order on destructive actions causes fat-finger fills.

5. **First-run path.** After TOS accept, the strongest first move is a guided 3-step: link wallet → fund → first trade. Right now `/start` drops users at the full toolkit, which is a lot at once. A short "new here?" rail that disappears after the first trade would lift activation.

6. **Surface `/desk` and `/brief`.** Two genuinely good features that are basically hidden. Add a **💬 Ask Nadobro** entry on the home card that routes to the desk flow, and consider a daily brief opt-in toggle in Settings.

7. **Strategy Lab depth.** Lots of granular config buttons (spread, levels, drift, bias, etc.). Group them under labeled sub-sections with a one-line "what this does" on each cockpit so new users aren't guessing. The pre-trade readout in the new intro copy should always show before launch.

---

## 8. Suggested rollout order

1. Ship the new `/help` and the `BotCommand` menu. Pure accuracy fix, lowest risk, highest "is this maintained" signal.
2. Standardize names and emojis (section 6). Mechanical, high trust payoff.
3. Roll the rewritten copy surface by surface, starting with the home card, welcome, and error states.
4. Then the UX/workflow changes (back buttons, empty states, first-run rail).

---

## Appendix: where each surface lives

- `/help` text: `fmt_help()` in `src/nadobro/handlers/formatters.py` (line ~1406)
- Home card: `fmt_home_header`, `fmt_home_command_center_card`, `_fmt_home_toolkit_tree` (formatters.py ~1076-1131)
- Welcome / TOS: `WELCOME_MSG`, `WELCOME_CARD_MSG` in `commands.py` (~72)
- Strategy Lab: `fmt_strategy_hub_intro` (~1240)
- Mode: `fmt_mode_view` (~699)
- Settings: `fmt_settings` (~1322)
- Trade preview/result: `fmt_trade_preview` (~386), `fmt_trade_result` (~456)
- Confirms: `fmt_close_all_confirm` (~710), `fmt_stop_all_result` (~1258)
- AI agent: `fmt_managed_agent_*` (~1282-1319)
- Buttons + labels: `keyboards.py`
- Command registration + Telegram menu: `main.py` (~254 and ~445)

All MarkdownV2 escaping is handled by the existing `escape_md` / `_loc` helpers, so the rewritten strings drop in the same way the current ones do.
