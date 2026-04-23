"use client";

import { motion } from "framer-motion";
import {
  MessagesSquare,
  Users2,
  Gauge,
  Scale,
  LineChart,
  Check,
  type LucideIcon,
} from "lucide-react";
import { strategyShowcase } from "@/lib/content";

const icons: LucideIcon[] = [MessagesSquare, Users2, Gauge, Scale, LineChart];

export function StrategyShowcase() {
  return (
    <div className="grid gap-5 md:grid-cols-2 lg:grid-cols-3">
      {strategyShowcase.map((strategy, i) => {
        const Icon = icons[i % icons.length];
        return (
          <motion.article
            key={strategy.name}
            initial={{ opacity: 0, y: 22 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, amount: 0.25 }}
            transition={{
              delay: i * 0.06,
              duration: 0.7,
              ease: [0.16, 1, 0.3, 1],
            }}
            className="group relative overflow-hidden rounded-2xl border border-white/10 bg-gradient-to-b from-[#091831] to-[#04101f] p-6 transition hover:-translate-y-1 hover:border-cyan-200/30"
          >
            {/* Accent glow */}
            <div
              className={`pointer-events-none absolute -right-12 -top-12 h-44 w-44 rounded-full bg-gradient-to-br ${strategy.accent} opacity-60 blur-2xl transition-opacity group-hover:opacity-100`}
            />

            <div className="relative flex items-center justify-between gap-3">
              <div className="flex h-11 w-11 items-center justify-center rounded-xl border border-cyan-200/25 bg-cyan-300/10 text-cyan-200">
                <Icon size={18} />
              </div>
              <span className="rounded-full border border-cyan-200/20 bg-cyan-300/5 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-cyan-200/80">
                {strategy.tag}
              </span>
            </div>

            <h3 className="relative mt-5 text-lg font-semibold text-white">
              {strategy.name}
            </h3>
            <p className="relative mt-2 text-sm leading-relaxed text-slate-300">
              {strategy.tagline}
            </p>

            <ul className="relative mt-4 space-y-2">
              {strategy.bullets.map((b) => (
                <li
                  key={b}
                  className="flex items-start gap-2 text-sm text-slate-200"
                >
                  <Check
                    size={14}
                    className="mt-1 flex-shrink-0 text-emerald-300"
                  />
                  <span className="leading-relaxed">{b}</span>
                </li>
              ))}
            </ul>
          </motion.article>
        );
      })}
    </div>
  );
}
