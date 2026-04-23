export const siteMeta = {
  name: "NadoBro",
  tagline: "Telegram-native perpetuals on Nado.",
  docsUrl: "https://nadobro.gitbook.io/docs",
  xUrl: "https://x.com/NBdotbot",
  telegramUrl: "https://t.me/NadoBro_bot",
};

export const stats = [
  { label: "Nado cumulative volume", value: "$48B+" },
  { label: "Trading interface", value: "Telegram" },
  { label: "Settlement", value: "Nado CLOB · Ink L2" },
];

export const features = [
  {
    title: "Text-to-Trade",
    description:
      "Type “long 0.5 BTC 10x, stop 3%” and NadoBro parses, validates, and routes the order to the Nado CLOB.",
  },
  {
    title: "Five automated strategies",
    description:
      "Bro Mode, Copy Trading, Volume, Delta Neutral, and Grid / Market Maker — each tuned for a different style.",
  },
  {
    title: "AI co-pilot",
    description:
      "Grok-powered chat for live prices, sentiment, funding rates, and Fear & Greed — inline with your orders.",
  },
  {
    title: "Self-custody by default",
    description:
      "Linked Signer (1CT) model keeps your private key on your device. NadoBro never sees it.",
  },
];

export const steps = [
  {
    title: "Open in Telegram",
    body: "Start NadoBro and link a signer. Keys stay on your device.",
  },
  {
    title: "Describe the trade",
    body: "Plain English, inline confirmation, one-tap approve.",
  },
  {
    title: "Settle on Nado",
    body: "Orders hit the Nado CLOB on Ink. Real matching, real fills.",
  },
];
