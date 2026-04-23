import Image from "next/image";
import { ArrowUpRight } from "lucide-react";
import { SiteHeader } from "@/components/site-header";
import { SiteFooter } from "@/components/site-footer";
import { features, siteMeta, stats, steps } from "@/lib/content";

export default function Home() {
  return (
    <div className="relative">
      <SiteHeader />

      {/* ============================================================= */}
      {/* HERO — minimal dark, huge type, centered, one accent line.     */}
      {/* ============================================================= */}
      <section className="relative overflow-hidden">
        <div className="mx-auto w-full max-w-5xl px-6 pt-28 pb-20 text-center md:pt-40 md:pb-28">
          <p className="mb-8 inline-flex items-center gap-2 rounded-full border border-[var(--border-strong)] bg-white/[0.02] px-3.5 py-1.5 text-[12px] uppercase tracking-[0.15em] text-[var(--muted)]">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-[var(--accent)]" />
            Live on Ink mainnet
          </p>

          <h1 className="font-display text-balance text-[44px] leading-[0.98] text-white md:text-[88px]">
            Trade perps from{" "}
            <span className="text-[var(--accent)]">Telegram.</span>
          </h1>

          <p className="mx-auto mt-8 max-w-xl text-balance text-[17px] leading-[1.55] text-[var(--muted)] md:text-[19px]">
            NadoBro is a chat-native trading bot for the Nado CLOB. Type the
            trade — we parse, validate, and route it on-chain.
          </p>

          <div className="mt-10 flex flex-wrap justify-center gap-3">
            <a
              href={siteMeta.telegramUrl}
              target="_blank"
              rel="noreferrer"
              className="btn-primary"
            >
              Launch on Telegram
              <ArrowUpRight size={16} />
            </a>
            <a
              href={siteMeta.docsUrl}
              target="_blank"
              rel="noreferrer"
              className="btn-secondary"
            >
              Read the docs
            </a>
          </div>
        </div>

        {/* ============================================================= */}
        {/* PRODUCT SHOT — centered monochrome mockup, no frame chrome.    */}
        {/* Single visual tells the whole story. Anchored-style.           */}
        {/* ============================================================= */}
        <div className="mx-auto w-full max-w-5xl px-6 pb-24">
          <div className="relative overflow-hidden rounded-2xl border border-[var(--border-strong)] bg-gradient-to-b from-[#111114] to-[#0a0a0b]">
            <ProductMock />
          </div>
        </div>
      </section>

      <div className="section-divider" />

      {/* ============================================================= */}
      {/* STATS — three rows, left-aligned numbers.                      */}
      {/* ============================================================= */}
      <section className="mx-auto w-full max-w-6xl px-6 py-20">
        <div className="grid grid-cols-1 gap-10 md:grid-cols-3">
          {stats.map((s) => (
            <div key={s.label}>
              <p className="text-[12px] uppercase tracking-[0.15em] text-[var(--muted)]">
                {s.label}
              </p>
              <p className="mt-3 font-display text-[40px] leading-none text-white md:text-[48px]">
                {s.value}
              </p>
            </div>
          ))}
        </div>
      </section>

      <div className="section-divider" />

      {/* ============================================================= */}
      {/* FEATURES — four minimal blocks, no icons, no glass.            */}
      {/* ============================================================= */}
      <section id="features" className="mx-auto w-full max-w-6xl px-6 py-24">
        <div className="mb-16 max-w-2xl">
          <p className="text-[12px] uppercase tracking-[0.15em] text-[var(--muted)]">
            What&apos;s inside
          </p>
          <h2 className="font-display mt-4 text-[36px] leading-[1.05] text-white md:text-[52px]">
            Everything you need to trade from chat.
          </h2>
        </div>

        <div className="grid gap-x-12 gap-y-14 md:grid-cols-2">
          {features.map((f, i) => (
            <div key={f.title} className="border-t border-[var(--border)] pt-6">
              <div className="flex items-baseline justify-between gap-4">
                <h3 className="text-[20px] font-medium text-white">
                  {f.title}
                </h3>
                <span className="text-[12px] tabular-nums text-[var(--muted-2)]">
                  {String(i + 1).padStart(2, "0")}
                </span>
              </div>
              <p className="mt-4 text-[15px] leading-[1.65] text-[var(--muted)]">
                {f.description}
              </p>
            </div>
          ))}
        </div>
      </section>

      <div className="section-divider" />

      {/* ============================================================= */}
      {/* HOW IT WORKS — three numbered rows.                            */}
      {/* ============================================================= */}
      <section id="how" className="mx-auto w-full max-w-6xl px-6 py-24">
        <div className="grid gap-16 md:grid-cols-[1fr_1.4fr]">
          <div>
            <p className="text-[12px] uppercase tracking-[0.15em] text-[var(--muted)]">
              Flow
            </p>
            <h2 className="font-display mt-4 text-[36px] leading-[1.05] text-white md:text-[48px]">
              Three steps. Chat to execution.
            </h2>
          </div>
          <ol className="space-y-10">
            {steps.map((step, i) => (
              <li key={step.title} className="flex gap-6">
                <span className="flex-shrink-0 font-display text-[28px] tabular-nums text-[var(--muted-2)]">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <div className="flex-1 border-t border-[var(--border)] pt-3">
                  <h3 className="text-[20px] font-medium text-white">
                    {step.title}
                  </h3>
                  <p className="mt-3 text-[15px] leading-[1.65] text-[var(--muted)]">
                    {step.body}
                  </p>
                </div>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <div className="section-divider" />

      {/* ============================================================= */}
      {/* CLOSING CTA — one line, one button.                            */}
      {/* ============================================================= */}
      <section className="mx-auto w-full max-w-5xl px-6 py-28 text-center">
        <h2 className="font-display text-[40px] leading-[1.02] text-white md:text-[72px]">
          Ready to trade?
        </h2>
        <p className="mx-auto mt-6 max-w-md text-[17px] leading-[1.55] text-[var(--muted)]">
          Open the bot, link your signer, run your first trade in under a
          minute.
        </p>
        <div className="mt-10 flex justify-center">
          <a
            href={siteMeta.telegramUrl}
            target="_blank"
            rel="noreferrer"
            className="btn-primary"
          >
            Launch on Telegram
            <ArrowUpRight size={16} />
          </a>
        </div>
      </section>

      <SiteFooter />
    </div>
  );
}

/* ----------------------------------------------------------------------
 * ProductMock — a centered, restrained product visual.
 *
 * No video (user opted out). No skeuomorphic phone frame. Just a clean
 * monochrome composition of NadoBro's Command Center that reads as a
 * product still. All SVG + Tailwind — no external assets beyond the
 * saved logo.
 * -------------------------------------------------------------------- */
function ProductMock() {
  const modules = [
    "Trade Console",
    "Portfolio Deck",
    "Wallet Vault",
    "Nado Points",
    "Strategy Lab",
    "Alert Engine",
    "Control Panel",
    "Execution Mode",
  ];

  return (
    <div className="grid gap-0 md:grid-cols-[1fr_1.3fr]">
      {/* LEFT — Telegram chat pane */}
      <div className="border-b border-[var(--border)] p-8 md:border-b-0 md:border-r">
        <div className="flex items-center gap-3 border-b border-[var(--border)] pb-4">
          <Image
            src="/nadobro-logo-symbol.png"
            alt=""
            width={28}
            height={28}
            className="rounded-sm"
          />
          <div className="flex-1 leading-tight">
            <p className="text-[14px] font-semibold text-white">NadoBro</p>
            <p className="text-[11px] text-[var(--muted)]">bot · online</p>
          </div>
          <span className="inline-flex h-2 w-2 rounded-full bg-[var(--accent)]" />
        </div>

        <p className="mt-5 text-[11px] uppercase tracking-[0.15em] text-[var(--muted-2)]">
          Command Center
        </p>

        <div className="mt-3 grid grid-cols-2 gap-1.5">
          {modules.map((m) => (
            <div
              key={m}
              className="rounded-md border border-[var(--border)] bg-white/[0.015] px-3 py-2.5 text-[12px] text-[var(--foreground)]"
            >
              {m}
            </div>
          ))}
        </div>

        <div className="mt-5 rounded-lg border border-[var(--border)] bg-white/[0.015] p-3">
          <p className="text-[11px] uppercase tracking-[0.15em] text-[var(--muted-2)]">
            Last intent
          </p>
          <p className="mt-2 text-[13px] leading-relaxed text-white">
            long 0.5 BTC 10x, stop 3%, tp 7%
          </p>
          <p className="mt-2 text-[11px] text-[var(--accent)]">
            ✓ Filled on Nado CLOB · $67,842.30
          </p>
        </div>
      </div>

      {/* RIGHT — abstract CLOB / chart pane */}
      <div className="p-8">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <p className="font-display text-[18px] text-white">
              BTC
              <span className="ml-2 text-[var(--muted)]">·Perp</span>
            </p>
            <span className="rounded-full border border-[var(--border-strong)] px-2 py-0.5 text-[10px] uppercase tracking-[0.15em] text-[var(--muted)]">
              Nado
            </span>
          </div>
          <div className="text-right">
            <p className="font-display text-[22px] tabular-nums text-white">
              67,842.30
            </p>
            <p className="text-[11px] tabular-nums text-[var(--accent)]">
              +1.43% · 24h
            </p>
          </div>
        </div>

        {/* Minimal SVG chart line */}
        <div className="mt-6 overflow-hidden rounded-lg border border-[var(--border)] bg-white/[0.015] p-4">
          <svg
            viewBox="0 0 600 180"
            className="h-32 w-full"
            preserveAspectRatio="none"
          >
            <defs>
              <linearGradient id="chartFill" x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor="#69e3ff" stopOpacity="0.25" />
                <stop offset="100%" stopColor="#69e3ff" stopOpacity="0" />
              </linearGradient>
            </defs>
            <path
              d="M0 130 L60 110 L120 125 L180 95 L240 100 L300 75 L360 85 L420 60 L480 68 L540 45 L600 50 L600 180 L0 180 Z"
              fill="url(#chartFill)"
            />
            <path
              d="M0 130 L60 110 L120 125 L180 95 L240 100 L300 75 L360 85 L420 60 L480 68 L540 45 L600 50"
              fill="none"
              stroke="#69e3ff"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>

          {/* Order-book rows */}
          <div className="mt-4 grid grid-cols-3 gap-3 text-[11px] tabular-nums">
            {[
              ["Bid", "67,841.90", "+0.24"],
              ["Mid", "67,842.30", ""],
              ["Ask", "67,842.70", "-0.18"],
            ].map(([label, price, delta]) => (
              <div
                key={label}
                className="rounded-md border border-[var(--border)] bg-white/[0.015] px-3 py-2"
              >
                <p className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted-2)]">
                  {label}
                </p>
                <p className="mt-1 text-white">{price}</p>
                {delta && (
                  <p className="text-[10px] text-[var(--muted)]">{delta}</p>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Footer strip */}
        <div className="mt-6 flex items-center justify-between text-[11px] text-[var(--muted)]">
          <span>app.nado.xyz/perpetuals?market=BTC</span>
          <span className="tabular-nums">Ink L2 · block #9,204,117</span>
        </div>
      </div>
    </div>
  );
}
