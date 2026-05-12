---
name: nadobro-design
description: Use this skill to generate well-branded interfaces and assets for NadoBro — the Telegram-native trading platform for perp futures on Nado DEX (Ink L2). Contains essential design guidelines, colors, type, fonts, brand assets, and a Telegram-bot UI kit for prototyping. The brand voice is "your trading buddy who happens to be a quant" — warm, fun, crypto-fluent, never corporate.
user-invocable: true
---

# NadoBro design skill

Read `README.md` in this skill first — it has the brand context, content fundamentals (voice / tone / emoji vocabulary / casing rules), and visual foundations (colors / type / spacing / shadows / card pattern). Then explore the other files:

- `colors_and_type.css` — drop-in CSS tokens. Always import this in any HTML you generate.
- `assets/` — logos, the PnL share-card master, hero illustrations, QR.
- `preview/` — small atomic specimens (one concept per file): colours, type, spacing, radii, shadows, buttons, pills, chat bubbles, the Bro card pattern, inline + reply keyboards.
- `ui_kits/telegram_bot/` — high-fi recreation of the Telegram-bot surface. `components.jsx` exports `BroCard`, `Bubble`, `InlineKeyboard`, `ReplyKeyboard`, `Pill`, `BroPill`, `PositionRow`, `ShareCard`, etc. Reuse them — don't redraw chat chrome from scratch.

## When working

If the user is creating a **visual artifact** (slide, mock, throwaway prototype, share asset), copy the assets you need out of `assets/`, write a static HTML file, import `colors_and_type.css`, and lean on the chat-card pattern from `preview/bro-card.html`.

If the user is working on **production code**, read the rules in `README.md` and apply the tokens directly.

If the user **invokes this skill with no further guidance**, ask them what they want to design or build, ask a couple of focused questions (audience, surface, variations they want), and act as the NadoBro design expert — outputting either HTML artifacts or production code as appropriate.

## Non-negotiables

- Voice: warm trading-buddy. Crypto slang lands naturally; never forced. Casual replies stay short (1–3 sentences). Sign off market answers with `🎯 **Actionable Insight:** …`.
- Colour: dark ink surfaces only; cyan and green accents from the logo; red only for short / liq / destructive. **Never** invent purple gradients.
- Iconography: emoji-first, controlled vocabulary in `README.md`. **Do not draw your own SVG icons.** If you genuinely need stroke icons (e.g. marketing site), substitute Lucide and flag the swap.
- Cards in chat follow the header-rule-section-tree skeleton. Use the `━` (U+2501) rule and `├ └` tree connectors.
- Type: Space Grotesk for the NADOBRO wordmark / display; Inter for everything else; JetBrains Mono for addresses, prices, trade syntax.
