# NadoBro Design System

NadoBro is a **Telegram-native trading platform for perpetual futures on
Nado** — the high-performance CLOB DEX running on Ink (the L2 backed by
Kraken). The product lives almost entirely inside the Telegram chat
window: users talk to `@Nadbro_bot` to inspect markets, ask Trading-Bro
questions, link a wallet, place perp trades by typing things like
`long BTC 0.01 5x market`, run automated strategies (Market Making,
Grid, Dynamic Grid, Delta Neutral, Volume, Alpha Agent), share PnL cards,
and refer friends.

The brand persona is **"your trading buddy who happens to be a quant"** —
warm, fun, cool-guy vibe; crypto-fluent without being try-hard. The bot
calls itself *Nadobro / NadoBro / Bro* interchangeably and signs off
casual replies with *"Anytime, bro."*

## Sources used to build this system

- **Codebase (read-only mount):** `Nadobro_bot/` — Python Telegram bot.
  Key files referenced:
  - `src/nadobro/handlers/formatters.py` — chat-card markup, headers, rules
  - `src/nadobro/handlers/keyboards.py` — inline + reply keyboard layouts
  - `src/nadobro/handlers/home_card.py` — Home command-center card
  - `src/nadobro/services/knowledge_service.py` — persona + tone prompts
    (`CASUAL_SYSTEM_PROMPT`, `SYNTHESIZER_SYSTEM_PROMPT`)
  - `src/nadobro/services/pnl_card.py` — exact pixel coords + colour
    constants for the share card. Most of the brand colour tokens in
    `colors_and_type.css` come from this file.
- **Brand assets** (copied into `assets/`): the canonical NB glyph
  (`nadobro_glyph.png`, `nadobro_nb_logo.png`), full logo with wordmark
  (`nadobro_logo_full.png`), Nado mark (`nado.png`), the PnL share card
  master (`pnl_card_master.png`), the in-bot QR (`nadobro_qr.jpeg`),
  Bro hero illustration (`card_bg.png`), and the strategy session card
  template (`session_card_template.png`).
- **Public docs:** [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs)
  (referenced; not fetched at build time)

---

## Index

- `README.md` — this file
- `SKILL.md` — Agent-Skills cross-compatible entry point
- `colors_and_type.css` — CSS tokens for colour, type, spacing, radius,
  shadow. Import this in any HTML you build for the brand.
- `assets/` — logos, the PnL share-card master, hero illustrations, QR
- `preview/` — small cards rendered in the Design System tab
- `ui_kits/telegram_bot/` — interactive recreation of the Telegram-bot
  surface (Home command center, Trade Console flow, Portfolio Deck,
  Trading Bro answer card, share PnL card)

---

## Content fundamentals

NadoBro's voice is the load-bearing part of the brand. Get this right and
the rest follows. Direct lifts from `CASUAL_SYSTEM_PROMPT`:

> "You are Nadobro — a cool trading buddy on Telegram who happens to be
> sharp at crypto, markets, and automation. Think: helpful friend, calm
> trader, quick wit."

**Vibe.** Friendly, relaxed, confident. The buddy who trades *with* the
user, not a corporate support bot. Hype good setups; never roast or dunk
on the user. Chill on the surface, precise when it matters.

**Person.** Second-person *you*, first-person singular when reacting
("I'd sit this one out"). The bot refers to itself as **Nadobro** in
prompts, **Trading Bro** when answering market questions, and **Bro** in
casual chat.

**Length.** Casual replies are SHORT — 1–3 sentences. Strategy/build/debug
answers can stretch to 2–6 short sections. No filler.

**Casing.**
- Headlines / share-card wordmark: `NADOBRO` (ALL CAPS, display font).
- Card headers: Title Case with leading emoji — `📊 Morning Brief`,
  `📋 Open Positions`, `💰 Wallet Vault Balance`.
- Buttons: Title Case with leading emoji — `🤖 Trade Console`,
  `📁 Portfolio Deck`, `✅ Confirm Trade`.
- Body: sentence case.
- Tickers / sides: ALL CAPS — `BTC-PERP`, `LONG`, `SHORT`.

**Slang.** Used naturally, never forced: *WAGMI, LFG, ser, fren, based,
rekt, copium, ngmi, alpha, chad move, sit this one out.* If it doesn't
land, drop it.

**Closing rules.** When an answer touches markets/prices/news, end with
a single line:

> 🎯 **Actionable Insight:** _one concrete step on Nado, or "sit out"._

For data-backed answers, follow with a compact `Sources: CMC, Nado, X`
line. Casual chat takes neither.

**Emoji vocabulary** (pick from the menu, do not stack):
`📊` market snapshot · `🗞` news · `🎯` actionable insight ·
`🟢` bullish / Long · `🔴` bearish / Short · `⚠️` risk · `⚡` key info ·
`💡` tip · `💰` money/profits · `🏆` rankings/points · `📈📉` direction ·
`🔥` hot take · `🤖` bot/trade · `📁` portfolio · `💼` wallet · `🧠` strategy ·
`🔔` alerts · `⚙️` settings · `🌐` mode · `🚀` Alpha Agent · `✅` confirm · `❌` cancel.

**Microcopy examples** (lifted / adapted):
- Greeting: *"What's up, fren — markets warming up. ☀️"*
- Thanks: *"Anytime, bro. That's what I'm here for."*
- Risk flag: *"Setup looks decent but funding's wild — I'd size small or sit it out."*
- No data: *"No points drop yet this week, ser. They usually hit on Fridays."*
- Bot self-ref: *"You could try: `long BTC 0.01 5x market`."*

---

## Visual foundations

The brand reads as **dark-mode trader-desk crossed with arcade neon**:
deep navy/ink surfaces, cyan circuit-trace outlines, electric-green
nodes for go/profit/Bro, monospaced data, and one human illustration of
"Bro" himself for share cards.

### Colour

Three families. Always-dark UI.

- **Ink surfaces** — `--nb-ink-900` (#07111A) page → `--nb-ink-800`
  (#0C1A28) cards → `--nb-ink-700` (#122439) raised pills/inputs. The
  Telegram dark theme `#17212B` sits between 900 and 800 and is what
  the bot is actually rendered on.
- **Cyan** (`--nb-cyan-500` #29D7E6) — circuit traces, hairline borders
  on cards, "Strategy Session" template, info chips, links.
- **Green** (`--nb-green-500` #44EE94) — the *Bro pill* colour, profit,
  Long, primary CTA, "On Nado" wordmark on the share card. Use sparingly
  so it stays loud.
- **Sell red** (#FF4C4C) — Short, liquidation, destructive.
- **Text:** white #FFF for values, #BCC4D0 for labels, #7C8AA0 for meta.

There are **no purple gradients**, no pastel; do not invent them.

### Type

- **Display:** Space Grotesk 700, ALL CAPS, ~+2% tracking — used for the
  `NADOBRO` wordmark on share cards and any hero moment. **Locked in** after
  reviewing the Nadobro docs voice and the geometric circuit-glyph: Space
  Grotesk's slightly squared, friendly geometry mirrors the NB monogram
  forms while staying readable for headlines. Pairs warm with Inter body.
- **Body:** Inter 400/600/700 — every chat card, button, label, and the
  big PnL value (`Inter-Bold 80px` per `pnl_card.py`).
- **Mono:** JetBrains Mono — wallet addresses, referral codes, the
  `long BTC 0.01 5x market` trade syntax, anywhere data needs to align.
  Telegram inline-code spans render in the user's system mono; we mirror
  with JetBrains Mono in HTML mocks.

### Backgrounds

- Flat dark ink for chat surfaces and product UI. **No gradients on
  product chrome.**
- Two illustrated brand backgrounds exist and are used *only* for hero
  moments and share assets:
  - `assets/card_bg.png` — circuit-board background with the Bro
    character holding a coffee. Used behind PnL share cards.
  - `assets/pnl_card_master.png` — the composed share card including
    the Bro illustration and a darkened gradient panel on the left for
    text. The bot paints user values directly onto this PNG.
- No repeating patterns or grain. The "texture" comes from the circuit-
  trace inside the logo, not the background.

### Animation & motion

The bot itself is server-rendered Markdown — *no animation in the chat
surface*. In our HTML mocks, keep motion subtle:

- **Easing:** `cubic-bezier(0.2, 0.8, 0.2, 1)` (decelerate)
- **Durations:** 120–200ms for hover / press; 240–320ms for sheet slides
- **Hover:** brighten by 6–10% (`filter: brightness(1.08)`) or shift to
  the next-lighter ink token; never colour-shift.
- **Press:** scale `0.97`, opacity `0.9`; for Bro-pill CTAs add a brief
  green glow (`--shadow-glow-green`).
- **Loading:** a single inline rule of pulsing `─` characters or a 3-dot
  typing indicator — both match how the bot streams answers.
- No bounces, no parallax.

### Borders, shadows, transparency

- **Hairline cyan border** on cards: `1px solid rgba(110,226,240,0.18)`,
  upgraded to `0.35` when active/focused.
- **Card shadow:** soft drop, no glow:
  `0 12px 32px -12px rgba(0,0,0,0.6)` plus a 1px inner highlight.
- **Glow shadows** are reserved for the Bro pill and the strategy
  "session" frame (cyan glow). Use them, do not over-use them.
- Transparency / blur appears in **only one place**: the dark gradient
  panel that sits on top of the Bro illustration on the PnL share card.
  Don't add blur to chat UI.

### Corners

- Chat bubbles: `12–18px` (Telegram default — we don't redraw this).
- Cards / panels: `16–20px` (`--r-lg` / `--r-xl`).
- Pills (Alpha Agent, mode toggle, referral pill): `999px` (`--r-pill`).
- Share-card pills: `28px` (`--r-2xl`) — measured from the master PNG.
- Avatars / app icon: `22%` of size (Telegram-style squircle).

### Card pattern (chat-side)

Every Bro card in chat follows the same skeleton:

```
{leading-emoji} *Title*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{section header}
├ {row}
├ {row}
└ {row}
…
🎯 *Actionable Insight:* …
_Sources: …_
```

The `━` rule is a literal box-drawing character produced by
`md2_rule()` in `formatters.py`. Tree characters `├ └` mark sub-rows
inside a section. Replicate this in any card we generate.

### Layout rules

- Chat is single-column; everything stacks. Inline keyboards are 1–3
  buttons per row, max 4 rows.
- Reply keyboards: 2 columns of primary actions, 3 columns for compact
  pickers (size presets, leverage).
- Share card is fixed `1366×768` (per `pnl_card.py` CANVAS_W/H).
- Buttons / hit targets: minimum 44px tall in HTML mocks.

---

## Iconography

NadoBro is **emoji-first** — there is no custom icon font and no SVG
icon set in the codebase. Emoji are how the bot communicates affordance
on every button, every card header, every list row. They function as
icons, not decoration.

- **Source:** the user's Telegram client renders emoji using their
  platform's native font (Apple Color Emoji / Noto Color Emoji /
  Twemoji). In HTML mocks, render with the system emoji stack via
  `font-family: 'Apple Color Emoji', 'Segoe UI Emoji', 'Noto Color Emoji', sans-serif`.
- **Allowed set:** the controlled vocabulary listed under "Emoji
  vocabulary" above. Don't introduce new ones casually. Prefer the
  existing token if any one of them fits.
- **No custom SVG icons.** Don't draw your own.
- **Logo / brand marks** are the only raster assets:
  - **`assets/nadobro_glyph_transparent.png`** — primary NB monogram, transparent
    background. **Default choice** for avatar, app icon, share-card stamp,
    over imagery — works on any surface.
  - `assets/nadobro_glyph.png` — same monogram, deep-ink square background.
    Use only when transparency is unsupported (e.g. some Telegram contexts).
  - `assets/nadobro_nb_logo.png` — legacy variant, retained for parity with
    the bot codebase.
  - `assets/nadobro_logo_full.png` / `nadobro_wordmark.png` — full
    "NADOBRO" lockup. Use on share cards and marketing.
  - `assets/nado.png` — the underlying Nado DEX mark. Used when the bot
    cites Nado as a source ("On Nado").
- **Unicode characters as icons:** the chat cards use `━` (U+2501) as a
  rule, `├ └` (U+251C / U+2514) as tree connectors, and `•` for inline
  list bullets in casual answers. These are part of the system; treat
  them like icons.
- **No CDN icon set** is referenced. If a future surface (web app /
  marketing site) needs a stroke icon set, the closest match in stroke
  weight + corner feel is **Lucide** (1.5px, rounded). **This would be
  a substitution — flag it.**

---

## Files at a glance

```
.
├── README.md                  # ← you are here
├── SKILL.md                   # Agent-Skills entry point
├── colors_and_type.css        # tokens (import everywhere)
├── assets/
│   ├── nadobro_glyph.png
│   ├── nadobro_nb_logo.png
│   ├── nadobro_logo_full.png
│   ├── nadobro_wordmark.png
│   ├── nadobro_logo_sq.png
│   ├── nadobro_logo_s.png
│   ├── nb_name.png
│   ├── nado.png
│   ├── pnl_card_master.png
│   ├── pnl_background.jpg
│   ├── card_bg.png
│   ├── nadobro_qr.jpeg
│   └── session_card_template.png
├── preview/                   # design-system tab cards
│   └── *.html
└── ui_kits/
    └── telegram_bot/
        ├── README.md
        ├── index.html         # interactive recreation
        ├── components.jsx     # bubble, card, keyboard, pill, etc.
        └── screens.jsx        # home / trade / portfolio / answer
```

---

## Open items / substitutions to flag

- **Display font:** Space Grotesk — **locked in** as the official display
  face after a docs/brand review. Geometric, slightly squared forms echo
  the NB circuit-glyph; warm enough to pair with the "trading buddy"
  voice. Loaded via Google Fonts.
- **Body font:** the bot uses Inter (per `pnl_card.py`). Loaded via
  Google Fonts; swap to a local TTF if you want pixel-parity with
  server-rendered share cards.
- **Stroke icon set** is *not* defined yet — the bot is emoji-first.
  If/when a marketing site needs line icons, propose Lucide and re-flag.
