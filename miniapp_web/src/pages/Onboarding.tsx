import { useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { hapticImpact, hapticSuccess } from "@/lib/haptics";
import { api } from "@/api/client";
import { useAuthStore } from "@/store/auth";

const DOCS_URL = "https://nadobro.gitbook.io/docs";

interface Slide {
  title: string;
  subtitle: string;
  gradient: string;
  graphic: React.ReactNode;
}

const SLIDES: Slide[] = [
  {
    title: "Welcome to NadoBro",
    subtitle:
      "Your Trading Bro for Life — built on Nado, powered by Ink. Trade perpetual futures from Telegram with natural language and pro-grade execution.",
    gradient: "from-slate-950 via-[#0d1520] to-tg-bg",
    graphic: (
      <div className="flex flex-col items-center justify-center gap-4">
        <img
          src="/nadobro-logo.png"
          alt="NadoBro"
          className="w-40 h-40 object-contain drop-shadow-[0_0_28px_rgba(34,211,238,0.45)]"
        />
        <p className="text-[11px] text-center text-tg-hint max-w-xs leading-relaxed">
          Nado is a high-performance CLOB DEX on Ink L2 — Nadobro is purpose-built to trade there from chat.
        </p>
      </div>
    ),
  },
  {
    title: "Text-to-trade & AI",
    subtitle:
      "Type what you want in plain English — or use Speak with Bro. Get live prices, sentiment, and strategy automation: Bro Mode, grids, delta neutral, volume, and more (see docs).",
    gradient: "from-emerald-950/50 via-[#0d1520] to-tg-bg",
    graphic: (
      <div className="flex justify-center gap-2 flex-wrap max-w-sm mx-auto">
        {["Text-to-trade", "Grok AI", "5 strategies", "Points"].map((label) => (
          <span
            key={label}
            className="px-3 py-1.5 rounded-full text-xs font-medium bg-nb-green/15 text-nb-green border border-nb-green/30"
          >
            {label}
          </span>
        ))}
      </div>
    ),
  },
  {
    title: "Think the price will rise?\nOpen a long",
    subtitle: "As spot rises, your PnL can rise — always size for your risk.",
    gradient: "from-green-950/40 to-tg-bg",
    graphic: (
      <div className="flex items-end justify-center gap-1 h-40">
        {[40, 35, 50, 45, 60, 55, 70, 65, 80, 90].map((h, i) => (
          <div
            key={i}
            className="w-5 rounded-sm"
            style={{
              height: `${h}%`,
              background: h > 50 ? "#4ade80" : "#475569",
            }}
          />
        ))}
      </div>
    ),
  },
  {
    title: "Think the price will fall?\nOpen a short",
    subtitle: "If the market moves your way, short perps can pay — leverage cuts both ways.",
    gradient: "from-red-950/40 to-tg-bg",
    graphic: (
      <div className="flex items-end justify-center gap-1 h-40">
        {[90, 80, 70, 65, 55, 50, 45, 40, 35, 30].map((h, i) => (
          <div
            key={i}
            className="w-5 rounded-sm"
            style={{
              height: `${h}%`,
              background: h > 50 ? "#475569" : "#ef4444",
            }}
          />
        ))}
      </div>
    ),
  },
  {
    title: "Leverage & liquidation",
    subtitle:
      "Higher leverage means larger notionals and faster liquidations. Use what you understand — start small on testnet with Dual Mode.",
    gradient: "from-indigo-950/40 to-tg-bg",
    graphic: (
      <div className="text-center">
        <div className="text-5xl font-black text-white mb-6">10x</div>
        <div className="flex items-end justify-center gap-6">
          {[
            { h: "30%", color: "#64748b", label: "Price\nmove" },
            { h: "75%", color: "#4ade80", label: "Profit\npotential" },
            { h: "75%", color: "#ef4444", label: "Loss\npotential" },
          ].map((bar, i) => (
            <div key={i} className="flex flex-col items-center gap-2">
              <div className="h-28 w-12 flex items-end">
                <div
                  className="w-full rounded-lg"
                  style={{ height: bar.h, background: bar.color }}
                />
              </div>
              <span className="text-[10px] text-tg-hint whitespace-pre-line text-center leading-tight">
                {bar.label}
              </span>
            </div>
          ))}
        </div>
      </div>
    ),
  },
  {
    title: "Self-custody & Dual Mode",
    subtitle:
      "Linked Signer (1CT) keeps keys off our servers. Switch between Testnet and Mainnet instantly — practice before you go live.",
    gradient: "from-cyan-950/30 to-tg-bg",
    graphic: (
      <div className="text-center space-y-4">
        <img
          src="/nadobro-wordmark.png"
          alt=""
          className="h-10 object-contain mx-auto opacity-90"
        />
        <div className="inline-flex items-center gap-2 bg-nb-cyan/10 border border-nb-cyan/25 rounded-xl px-4 py-2">
          <div className="w-2 h-2 rounded-full bg-nb-cyan animate-pulse" />
          <span className="text-xs font-medium text-nb-cyan">Security first · Simplicity · Intelligence</span>
        </div>
      </div>
    ),
  },
  {
    title: "Important risks",
    subtitle:
      "Perpetuals are high risk. You can lose your entire margin quickly due to leverage and volatility. By continuing you acknowledge you have read the documentation.",
    gradient: "from-slate-900/80 to-tg-bg",
    graphic: (
      <div className="text-center">
        <a
          href={DOCS_URL}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-2 text-nb-cyan text-sm font-medium"
        >
          Read the NadoBro docs
          <span aria-hidden>→</span>
        </a>
      </div>
    ),
  },
];

interface OnboardingProps {
  onComplete: () => void;
}

export default function Onboarding({ onComplete }: OnboardingProps) {
  const [currentSlide, setCurrentSlide] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fetchUser = useAuthStore((s) => s.fetchUser);
  const isLast = currentSlide === SLIDES.length - 1;

  const next = useCallback(async () => {
    hapticImpact("light");
    if (isLast) {
      setSubmitting(true);
      setError(null);
      try {
        await api.patch("/api/me/settings", { tos_accepted: true });
        hapticSuccess();
        await fetchUser();
        onComplete();
      } catch {
        setError("Failed to save. Please try again.");
        hapticImpact("heavy");
      } finally {
        setSubmitting(false);
      }
    } else {
      setCurrentSlide((s) => s + 1);
    }
  }, [isLast, onComplete, fetchUser]);

  const slide = SLIDES[currentSlide]!;

  return (
    <div className="flex-1 flex flex-col bg-tg-bg relative overflow-hidden">
      <div className="flex gap-1 px-4 pt-3">
        {SLIDES.map((_, i) => (
          <div
            key={i}
            className="flex-1 h-[3px] rounded-full transition-colors duration-300"
            style={{
              background: i <= currentSlide ? "var(--nb-cyan, #22d3ee)" : "rgba(255,255,255,0.12)",
              boxShadow: i === currentSlide ? "0 0 12px rgba(34,211,238,0.45)" : undefined,
            }}
          />
        ))}
      </div>

      <AnimatePresence mode="wait">
        <motion.div
          key={currentSlide}
          initial={{ opacity: 0, x: 40 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -40 }}
          transition={{ duration: 0.25 }}
          className={`flex-1 flex flex-col justify-center px-6 bg-gradient-to-b ${slide.gradient}`}
        >
          <div className="mb-8">{slide.graphic}</div>
          <h1 className="text-2xl font-bold text-white whitespace-pre-line leading-tight mb-3">
            {slide.title}
          </h1>
          <p className="text-tg-hint text-[15px] leading-relaxed">{slide.subtitle}</p>
        </motion.div>
      </AnimatePresence>

      <div className="px-6 pb-8 pt-4 safe-bottom">
        {error && <p className="text-short text-sm text-center mb-2">{error}</p>}
        <button
          type="button"
          onClick={next}
          disabled={submitting}
          className="w-full py-4 rounded-2xl font-semibold text-[17px] active:scale-[0.98] transition-transform disabled:opacity-50 bg-gradient-to-r from-[#22d3ee] to-[#06b6d4] text-black shadow-[0_0_24px_rgba(34,211,238,0.25)]"
        >
          {submitting ? "Saving..." : isLast ? "Accept & continue" : "Continue"}
        </button>
      </div>
    </div>
  );
}
