"use client";

import { motion } from "framer-motion";
import {
  MessageSquare,
  Wand2,
  Zap,
  Activity,
  type LucideIcon,
} from "lucide-react";
import { howItWorksSteps } from "@/lib/content";

const icons: LucideIcon[] = [MessageSquare, Wand2, Zap, Activity];

export function HowItWorksSteps() {
  return (
    <div className="relative">
      {/* Vertical rail (desktop only) */}
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 top-0 hidden h-full -translate-x-1/2 md:block"
      >
        <div className="h-full w-px bg-gradient-to-b from-transparent via-cyan-300/40 to-transparent" />
      </div>

      <ol className="grid gap-6 md:grid-cols-2">
        {howItWorksSteps.map((step, i) => {
          const Icon = icons[i % icons.length];
          const isRight = i % 2 === 1;
          return (
            <motion.li
              key={step.title}
              initial={{ opacity: 0, y: 24 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.3 }}
              transition={{
                delay: i * 0.08,
                duration: 0.8,
                ease: [0.16, 1, 0.3, 1],
              }}
              className={`relative ${isRight ? "md:mt-16" : ""}`}
            >
              <div className="glass relative overflow-hidden rounded-2xl p-6">
                <div className="flex items-center gap-4">
                  <div className="relative flex h-12 w-12 items-center justify-center rounded-2xl border border-cyan-200/30 bg-cyan-300/10 text-cyan-200">
                    <Icon size={20} />
                    <span className="absolute -right-2 -top-2 inline-flex h-6 min-w-6 items-center justify-center rounded-full bg-gradient-to-br from-cyan-400 to-emerald-300 px-1.5 text-[11px] font-semibold text-[#032232] shadow-[0_4px_15px_rgba(0,255,178,0.3)]">
                      {String(i + 1).padStart(2, "0")}
                    </span>
                  </div>
                  <h3 className="text-lg font-semibold text-white">
                    {step.title}
                  </h3>
                </div>
                <p className="mt-4 text-sm leading-relaxed text-slate-300">
                  {step.body}
                </p>
                <div className="pointer-events-none absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-cyan-300/40 to-transparent" />
              </div>
            </motion.li>
          );
        })}
      </ol>
    </div>
  );
}
