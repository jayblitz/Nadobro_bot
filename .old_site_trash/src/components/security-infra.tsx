"use client";

import { motion } from "framer-motion";
import {
  ShieldCheck,
  KeyRound,
  Eye,
  Network,
  Send,
  Lock,
  type LucideIcon,
} from "lucide-react";
import { infraStack } from "@/lib/content";

const pillars = [
  {
    title: "Keys stay on device",
    body: "Linked Signer (1CT) model — NadoBro never sees, stores, or transmits your private key.",
    Icon: KeyRound as LucideIcon,
  },
  {
    title: "Transparent execution",
    body: "Every order hits the Nado on-chain CLOB. Fills, cancels, and funding are verifiable on Ink.",
    Icon: Eye as LucideIcon,
  },
  {
    title: "Scoped permissions",
    body: "Granular trading scopes — size caps, leverage caps, and instant kill-switch from chat.",
    Icon: ShieldCheck as LucideIcon,
  },
];

const stackIcons: Record<string, LucideIcon> = {
  Telegram: Send,
  "Nado CLOB": Network,
  "Ink L2": Network,
  "Self-Custody": Lock,
};

export function SecurityInfra() {
  return (
    <div className="grid gap-12">
      {/* Pillars */}
      <div className="grid gap-5 md:grid-cols-3">
        {pillars.map((p, i) => (
          <motion.article
            key={p.title}
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, amount: 0.3 }}
            transition={{
              delay: i * 0.08,
              duration: 0.75,
              ease: [0.16, 1, 0.3, 1],
            }}
            className="glass rounded-2xl p-6"
          >
            <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl border border-emerald-200/30 bg-emerald-300/10 text-emerald-200">
              <p.Icon size={18} />
            </div>
            <h3 className="text-lg font-semibold text-white">{p.title}</h3>
            <p className="mt-3 text-sm leading-relaxed text-slate-300">{p.body}</p>
          </motion.article>
        ))}
      </div>

      {/* Infra stack row */}
      <div className="rounded-2xl border border-white/10 bg-[#040e1e]/80 p-6 md:p-8">
        <div className="flex flex-col gap-1 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.22em] text-cyan-300/80">
              Stack
            </p>
            <h3 className="mt-2 text-2xl font-semibold text-white">
              Four layers. No middleman.
            </h3>
          </div>
          <p className="max-w-md text-sm text-slate-400">
            Interface, matching, settlement, and custody each stay in their own
            tightly-scoped layer — so nothing else has to be trusted.
          </p>
        </div>

        <div className="mt-8 grid gap-3 md:grid-cols-4">
          {infraStack.map((item, i) => {
            const Icon = stackIcons[item.title] ?? Network;
            return (
              <motion.div
                key={item.title}
                initial={{ opacity: 0, y: 18 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, amount: 0.25 }}
                transition={{
                  delay: i * 0.08,
                  duration: 0.6,
                  ease: [0.16, 1, 0.3, 1],
                }}
                className="group relative rounded-xl border border-white/10 bg-[#06132a] p-5 transition hover:border-cyan-200/30"
              >
                <div className="flex items-center gap-3">
                  <div className="flex h-9 w-9 items-center justify-center rounded-lg border border-cyan-200/25 bg-cyan-300/10 text-cyan-200">
                    <Icon size={16} />
                  </div>
                  <p className="text-sm font-semibold text-white">
                    {item.title}
                  </p>
                </div>
                <p className="mt-3 text-xs leading-relaxed text-slate-400">
                  {item.description}
                </p>
              </motion.div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
