"use client";

import Image from "next/image";
import { motion } from "framer-motion";
import { ArrowUpRight, Sparkles, BookOpen } from "lucide-react";
import { AuroraBackground } from "./aurora-background";
import { ShinyText } from "./shiny-text";
import { siteMeta } from "@/lib/content";

const fadeUp = {
  hidden: { opacity: 0, y: 24 },
  show: (i: number = 0) => ({
    opacity: 1,
    y: 0,
    transition: {
      delay: 0.15 + i * 0.08,
      duration: 0.9,
      ease: [0.16, 1, 0.3, 1],
    },
  }),
};

export function HeroSection() {
  return (
    <section className="relative isolate flex min-h-[100svh] w-full items-center overflow-hidden pt-28">
      <AuroraBackground />

      <div className="relative z-10 mx-auto grid w-full max-w-6xl grid-cols-1 items-center gap-12 px-6 pb-24 md:grid-cols-[1.25fr_1fr] md:pb-32">
        <div className="text-center md:text-left">
          <motion.div
            initial="hidden"
            animate="show"
            custom={0}
            variants={fadeUp}
            className="mb-6 inline-flex items-center gap-2 rounded-full border border-cyan-300/30 bg-cyan-300/10 px-4 py-1.5 text-xs font-medium uppercase tracking-[0.18em] text-cyan-200"
          >
            <Sparkles size={14} className="text-cyan-300" />
            Telegram-native perps · Live on Nado DEX
          </motion.div>

          <motion.h1
            initial="hidden"
            animate="show"
            custom={1}
            variants={fadeUp}
            className="text-balance text-5xl font-semibold leading-[1.04] tracking-tight text-white md:text-7xl"
          >
            Trade perps at{" "}
            <ShinyText duration={5}>the speed of chat.</ShinyText>
          </motion.h1>

          <motion.p
            initial="hidden"
            animate="show"
            custom={2}
            variants={fadeUp}
            className="mx-auto mt-6 max-w-xl text-lg leading-relaxed text-slate-300 md:mx-0"
          >
            {siteMeta.tagline}. Type what you want, NadoBro executes —
            institutional‑grade order flow on Nado CLOB, wrapped in a bot that
            feels like texting a pro.
          </motion.p>

          <motion.div
            initial="hidden"
            animate="show"
            custom={3}
            variants={fadeUp}
            className="mt-10 flex flex-col items-center gap-3 sm:flex-row sm:gap-4 md:items-start md:justify-start"
          >
            <a
              href={siteMeta.xUrl}
              target="_blank"
              rel="noreferrer"
              className="cta-btn group inline-flex items-center gap-2 rounded-full px-7 py-3.5 text-base font-semibold text-[#032232]"
            >
              Launch on Telegram
              <ArrowUpRight
                size={18}
                className="transition-transform group-hover:-translate-y-0.5 group-hover:translate-x-0.5"
              />
            </a>
            <a
              href={siteMeta.docsUrl}
              target="_blank"
              rel="noreferrer"
              className="group inline-flex items-center gap-2 rounded-full border border-cyan-200/40 bg-white/5 px-6 py-3.5 text-base font-medium text-cyan-100 backdrop-blur transition hover:border-cyan-200/70 hover:bg-cyan-300/10"
            >
              <BookOpen size={18} />
              Read the Docs
            </a>
          </motion.div>

          <motion.div
            initial="hidden"
            animate="show"
            custom={4}
            variants={fadeUp}
            className="mt-12 flex flex-wrap items-center gap-x-8 gap-y-3 text-sm text-slate-400 md:justify-start"
          >
            <span className="flex items-center gap-2">
              <span className="inline-block h-2 w-2 rounded-full bg-emerald-400 shadow-[0_0_10px_rgba(0,255,178,0.8)]" />
              Live on Ink mainnet
            </span>
            <span>Self-custody · 1CT Linked Signer</span>
            <span>$48B+ DEX volume</span>
          </motion.div>
        </div>

        {/* Logo orb */}
        <motion.div
          initial={{ opacity: 0, scale: 0.85 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ delay: 0.25, duration: 1.1, ease: [0.16, 1, 0.3, 1] }}
          className="relative mx-auto hidden aspect-square w-full max-w-md md:block"
        >
          {/* Rotating ring */}
          <motion.div
            className="absolute inset-6 rounded-full border border-cyan-200/20"
            animate={{ rotate: 360 }}
            transition={{ duration: 40, repeat: Infinity, ease: "linear" }}
          >
            <div className="absolute left-1/2 top-0 h-2 w-2 -translate-x-1/2 rounded-full bg-cyan-300 shadow-[0_0_15px_rgba(105,227,255,0.9)]" />
            <div className="absolute bottom-0 left-1/2 h-1.5 w-1.5 -translate-x-1/2 rounded-full bg-emerald-300 shadow-[0_0_12px_rgba(0,255,178,0.9)]" />
          </motion.div>

          <motion.div
            className="absolute inset-0 rounded-full"
            style={{
              background:
                "radial-gradient(circle, rgba(105,227,255,0.35) 0%, rgba(0,255,178,0.12) 45%, transparent 72%)",
              filter: "blur(2px)",
            }}
            animate={{ opacity: [0.6, 1, 0.6] }}
            transition={{ duration: 4, repeat: Infinity, ease: "easeInOut" }}
          />

          <div className="relative flex h-full w-full items-center justify-center">
            <Image
              src="/nadobro-logo.png"
              alt="NadoBro mark"
              width={520}
              height={520}
              priority
              className="relative h-auto w-[82%] drop-shadow-[0_0_45px_rgba(105,227,255,0.45)]"
            />
          </div>
        </motion.div>
      </div>

      {/* Scroll hint */}
      <motion.div
        initial={{ opacity: 0, y: -4 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 1.4, duration: 0.8 }}
        className="absolute inset-x-0 bottom-6 z-10 flex justify-center text-[11px] uppercase tracking-[0.35em] text-cyan-200/60"
      >
        <span className="flex items-center gap-2">
          <span className="h-[1px] w-8 bg-cyan-200/40" />
          Scroll to explore
          <span className="h-[1px] w-8 bg-cyan-200/40" />
        </span>
      </motion.div>
    </section>
  );
}
