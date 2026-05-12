# NadoBro Telegram Bot — UI Kit

Interactive, click-thru recreation of the NadoBro Telegram surface. The bot lives inside Telegram's dark theme; we mirror that with the `--tg-bg` token from `colors_and_type.css`. Components are cosmetic recreations — no real wallets / orders are placed.

## Files
- `index.html` — full prototype: chat history, persistent reply keyboard, inline-keyboard cards. Click anywhere through the flow.
- `components.jsx` — reusable parts: `ChatShell`, `Bubble`, `BroCard`, `InlineKeyboard`, `ReplyKeyboard`, `Pill`, `BroPill`, `ShareCard`.
- `screens.jsx` — composed flows: Onboarding → Home → Trade Console → Portfolio Deck → Bro Answer → Share PnL.

## Mapped from codebase
- Card markup pattern → `src/nadobro/handlers/formatters.py` (`md2_rule`, `_ui_header`, `_ui_section`).
- Inline / reply keyboards → `src/nadobro/handlers/keyboards.py` (`home_card_kb`, `trade_direction_kb`, leverage row).
- Share card → `src/nadobro/services/pnl_card.py` (uses `pnl_card_master.png` as the painted base).
- Persona / tone → `services/knowledge_service.py` (`CASUAL_SYSTEM_PROMPT`).
