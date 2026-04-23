export const siteMeta = {
  name: "NadoBro",
  tagline: "Your Trading Bro for Life, built on Nado, powered by InkOnchain",
  docsUrl: "https://nadobro.gitbook.io/docs",
  xUrl: "https://x.com/NBdotbot",
  telegramUrl: "https://t.me/NadoBro_bot",
};

export const keyMetrics = [
  { label: "Cumulative DEX volume", value: "$48B+" },
  { label: "Trading interface", value: "Telegram-native" },
  { label: "Execution venue", value: "Nado CLOB on Ink" },
];

export const keyFeatures = [
  {
    title: "Text-to-Trade",
    description:
      "Type trades in natural language like 'long 0.5 BTC 10x' and execute directly from chat.",
  },
  {
    title: "AI Agent (Bro Chat)",
    description:
      "Grok-powered assistance for live prices, sentiment analysis, Fear & Greed, and X search.",
  },
  {
    title: "Five Automated Strategies",
    description:
      "Bro Mode, Copy Trading, Volume Bot, Delta Neutral, and Market Maker / Grid Bot.",
  },
  {
    title: "Points Tracker",
    description:
      "Monitor Nado Season 1 points, trading volume, and cost-per-point efficiency in real time.",
  },
  {
    title: "Self-Custody by Design",
    description:
      "Linked Signer (1CT) model keeps private keys on your device for full user control.",
  },
  {
    title: "Dual Mode Trading",
    description:
      "Switch between Testnet and Mainnet instantly to test and iterate before going live.",
  },
];

export const strategies = [
  {
    name: "Bro Mode",
    description:
      "An assisted mode designed for conversational execution with quick reactions to market setup changes.",
  },
  {
    name: "Copy Trading",
    description:
      "Follow and mirror proven trader behavior while preserving your own risk controls.",
  },
  {
    name: "Volume Bot",
    description:
      "Automate volume generation patterns with configurable pacing for campaign and points goals.",
  },
  {
    name: "Delta Neutral",
    description:
      "Balance directional exposure while seeking opportunities through market structure and carry conditions.",
  },
  {
    name: "Market Maker / Grid Bot",
    description:
      "Systematic quote placement and grid logic to operate around volatility and microstructure movement.",
  },
];

/**
 * Richer strategy view used on the landing page. Each row has:
 *   - short tag for the chip ("Assisted", "Social", etc.)
 *   - bullet list of defining features
 *   - difficulty/auto classification for clarity
 */
export const strategyShowcase = [
  {
    name: "Bro Mode",
    tag: "Assisted",
    tagline: "Conversational execution with quick reactions.",
    bullets: [
      "Natural-language trade intents",
      "Inline risk prompts & leverage guardrails",
      "One-tap reversal and scale-in",
    ],
    accent: "from-cyan-300/20 to-cyan-300/5",
  },
  {
    name: "Copy Trading",
    tag: "Social",
    tagline: "Mirror proven traders, keep your own risk limits.",
    bullets: [
      "Follow curated on-chain leaders",
      "Per-trade size & leverage caps",
      "Kill-switch + drawdown stop",
    ],
    accent: "from-purple-400/20 to-cyan-300/5",
  },
  {
    name: "Volume Bot",
    tag: "Automation",
    tagline: "Pace volume for campaigns and points goals.",
    bullets: [
      "Spot & perp recycling loops",
      "Configurable cycle interval",
      "Points-per-dollar telemetry",
    ],
    accent: "from-emerald-300/20 to-cyan-300/5",
  },
  {
    name: "Delta Neutral",
    tag: "Carry",
    tagline: "Balanced exposure, harvest structure & funding.",
    bullets: [
      "Auto-hedged spot/perp legs",
      "Funding-rate triggered rebalancing",
      "Exposure drift alerts",
    ],
    accent: "from-sky-400/20 to-emerald-300/5",
  },
  {
    name: "Market Maker / Grid Bot",
    tag: "Pro",
    tagline: "Systematic quoting around the mid.",
    bullets: [
      "GRID + reverse-grid (RGRID) modes",
      "EMA crossover momentum bias",
      "Post-only with widening retry ladder",
    ],
    accent: "from-cyan-300/30 to-emerald-300/10",
  },
];

export const philosophy = [
  {
    title: "Security First",
    description: "Zero private key exposure with a Linked Signer-only architecture.",
  },
  {
    title: "Simplicity & Fun",
    description: "Natural language trading flow with reactive keyboard and buttons.",
  },
  {
    title: "Intelligence",
    description:
      "Grok-powered AI plus a continuously improving Alpha Agent for better decision support.",
  },
];

export const howItWorks = [
  "Connect with NadoBro in Telegram and choose your trading mode.",
  "Describe your trade in plain English with size, direction, and leverage.",
  "NadoBro interprets, validates, and submits the order to Nado DEX.",
  "Track execution, positions, and points directly inside your chat workflow.",
  "Switch between Testnet and Mainnet whenever you want to test safely.",
];

/**
 * Landing-page version: 4 steps with short titles + longer descriptions for
 * the visual stepper. Keeps the ordinal arc "connect → tell → execute → track"
 * that matches the reference docs.
 */
export const howItWorksSteps = [
  {
    title: "Open the bot",
    body: "Start NadoBro in Telegram and pick Testnet or Mainnet. Link your signer — keys stay on your device.",
  },
  {
    title: "Describe the trade",
    body: "Type what you want in plain English. \"Long 0.5 BTC 10x, stop 3%, tp 7%\" is a valid order.",
  },
  {
    title: "NadoBro routes it to Nado",
    body: "The bot parses, validates, sizes, and submits to the Nado CLOB on Ink L2 in one flow.",
  },
  {
    title: "Track & iterate",
    body: "Positions, PnL, funding, and Season 1 points stream back into your chat. Adjust or automate.",
  },
];

export const faq = [
  {
    question: "Do I need a separate web terminal to trade?",
    answer:
      "No. NadoBro is designed to execute directly inside Telegram so you can trade from chat.",
  },
  {
    question: "Where are my keys stored?",
    answer:
      "NadoBro follows a self-custody model with Linked Signer (1CT), so your private keys remain on your device.",
  },
  {
    question: "Can I test strategies before using real funds?",
    answer:
      "Yes. Dual Mode support allows quick switching between Testnet and Mainnet.",
  },
  {
    question: "What kinds of strategy automation are available?",
    answer:
      "You can run Bro Mode, Copy Trading, Volume Bot, Delta Neutral, and Market Maker / Grid Bot.",
  },
];

/** FAQ subset used on the landing page (keeps the first four). */
export const landingFaq = faq.slice(0, 4);

export const infraStack = [
  {
    title: "Telegram",
    description: "The interface. Chat, buttons, keyboards — zero new UI to learn.",
  },
  {
    title: "Nado CLOB",
    description: "On-chain central limit order book. Real matching, real fills.",
  },
  {
    title: "Ink L2",
    description: "High-throughput, low-fee L2 that keeps fills fast and cheap.",
  },
  {
    title: "Self-Custody",
    description: "Linked Signer (1CT) keeps your private key on your device.",
  },
];

// (chatDemoScript removed — real screen recording replaces the mockup.)
