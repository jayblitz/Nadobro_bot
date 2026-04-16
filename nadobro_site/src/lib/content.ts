export const siteMeta = {
  name: "NadoBro",
  tagline: "Your Trading Bro for Life, built on Nado, powered by InkOnchain",
  docsUrl: "https://nadobro.gitbook.io/docs",
  xUrl: "https://x.com/NBdotbot",
};

export const keyMetrics = [
  { label: "Cumulative DEX volume", value: "$48B+" },
  { label: "Trading interface", value: "Telegram-native" },
  { label: "Execution venue", value: "Nado CLOB DEX on Ink" },
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
