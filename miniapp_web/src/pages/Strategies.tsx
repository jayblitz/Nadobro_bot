import { clsx } from "clsx";

interface StrategyCard {
  type: string;
  name: string;
  description: string;
  color: string;
}

const STRATEGIES: StrategyCard[] = [
  {
    type: "grid",
    name: "Grid Bot",
    description: "Symmetric maker grid around mid price. Profits from mean reversion.",
    color: "from-blue-500/20 to-transparent",
  },
  {
    type: "rgrid",
    name: "Reverse Grid",
    description: "Reverse-grid quoting with exposure anchoring and PnL controls.",
    color: "from-purple-500/20 to-transparent",
  },
  {
    type: "dn",
    name: "Delta Neutral",
    description: "Funding rate farming — spot long + perp short hedge.",
    color: "from-teal-500/20 to-transparent",
  },
  {
    type: "volume",
    name: "Volume Bot",
    description: "Alternating long/short flips to hit a target volume.",
    color: "from-orange-500/20 to-transparent",
  },
  {
    type: "bro",
    name: "Bro Mode",
    description: "AI-driven autonomous trading powered by Grok-3.",
    color: "from-pink-500/20 to-transparent",
  },
];

export default function Strategies() {
  return (
    <div className="flex-1 overflow-y-auto hide-scrollbar px-4 pt-4 pb-4">
      <h1 className="text-lg font-bold text-white mb-1">Strategy Hub</h1>
      <p className="text-xs text-tg-hint mb-4">
        Configure and run automated trading strategies.
      </p>

      <div className="flex flex-col gap-3">
        {STRATEGIES.map((s) => (
          <button
            key={s.type}
            className={clsx(
              "w-full text-left rounded-xl p-4 bg-gradient-to-r",
              s.color,
              "bg-white/5 active:scale-[0.99] transition-transform",
            )}
          >
            <div className="font-semibold text-white mb-1">{s.name}</div>
            <div className="text-xs text-tg-hint leading-relaxed">{s.description}</div>
            <div className="mt-3 text-[11px] text-tg-button font-medium">Configure &rarr;</div>
          </button>
        ))}
      </div>
    </div>
  );
}
