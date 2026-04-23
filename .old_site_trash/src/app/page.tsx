import Link from "next/link";
import {
  ArrowRight,
  ShieldCheck,
  Sparkles,
  Cpu,
  LineChart,
  KeyRound,
  Layers,
} from "lucide-react";
import { HeroSection } from "@/components/hero-section";
import { ChatDemo } from "@/components/chat-demo";
import { HowItWorksSteps } from "@/components/how-it-works-steps";
import { StrategyShowcase } from "@/components/strategy-showcase";
import { SecurityInfra } from "@/components/security-infra";
import { FaqAccordion } from "@/components/faq-accordion";
import { keyFeatures, keyMetrics, siteMeta } from "@/lib/content";

const featureIcons = [Sparkles, Cpu, LineChart, Layers, KeyRound, ShieldCheck];

export default function Home() {
  return (
    <div className="relative overflow-hidden">
      <HeroSection />

      {/* --- Metrics strip --- */}
      <section className="relative border-y border-white/5 bg-[#040e1e]/60">
        <div className="mx-auto grid w-full max-w-6xl grid-cols-1 gap-8 px-6 py-14 md:grid-cols-3">
          {keyMetrics.map((metric) => (
            <div key={metric.label} className="text-center md:text-left">
              <p className="text-xs uppercase tracking-[0.22em] text-cyan-200/80">
                {metric.label}
              </p>
              <p className="mt-2 text-3xl font-semibold text-white md:text-4xl">
                {metric.value}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* --- Text-to-Trade demo --- */}
      <section className="mx-auto w-full max-w-6xl px-6 py-24">
        <div className="mx-auto mb-14 max-w-3xl text-center">
          <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
            Text-to-Trade · Live demo
          </p>
          <h2 className="mt-4 text-balance text-4xl font-semibold leading-tight tracking-tight text-white md:text-5xl">
            Chat the intent. Nado does the rest.
          </h2>
          <p className="mx-auto mt-5 max-w-2xl text-lg leading-relaxed text-slate-300">
            One tap opens the Command Center — Trade Console, Portfolio Deck,
            Strategy Lab, Alert Engine. Every action fires straight into the
            Nado CLOB. This is a real recording, not a mockup.
          </p>
        </div>

        <ChatDemo />

        <div className="mx-auto mt-10 grid max-w-4xl gap-4 md:grid-cols-3">
          {[
            {
              title: "Command Center",
              body: "Trade Console, Portfolio Deck, Wallet Vault, Nado Points, Strategy Lab, Alert Engine — all a tap away.",
            },
            {
              title: "On-chain venue",
              body: "Orders route to app.nado.xyz — the Nado CLOB on Ink L2. Real matching, real fills.",
            },
            {
              title: "Chat-first UX",
              body: "Natural-language intents, reactive keyboards, inline confirmations. Zero new UI to learn.",
            },
          ].map((card) => (
            <div
              key={card.title}
              className="glass rounded-2xl p-5"
            >
              <h3 className="text-sm font-semibold text-white">{card.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-slate-300">
                {card.body}
              </p>
            </div>
          ))}
        </div>

        <div className="mt-10 flex flex-wrap justify-center gap-3">
          <a
            href={siteMeta.telegramUrl}
            target="_blank"
            rel="noreferrer"
            className="cta-btn inline-flex items-center gap-2 rounded-full px-6 py-3 text-sm font-semibold text-[#032232]"
          >
            Try it on Telegram
            <ArrowRight size={16} />
          </a>
          <Link
            href="/how-it-works"
            className="inline-flex items-center gap-2 rounded-full border border-cyan-200/40 bg-white/5 px-6 py-3 text-sm font-medium text-cyan-100 transition hover:border-cyan-200/70 hover:bg-cyan-300/10"
          >
            See the flow
          </Link>
        </div>
      </section>

      {/* --- Features --- */}
      <section className="relative border-y border-white/5 bg-[#040e1e]/50">
        <div className="mx-auto w-full max-w-6xl px-6 py-24">
          <div className="mb-10 flex items-end justify-between gap-6">
            <div>
              <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
                Capabilities
              </p>
              <h2 className="mt-3 text-4xl font-semibold tracking-tight text-white md:text-5xl">
                Built for fast decisions.
              </h2>
            </div>
            <Link
              href="/features"
              className="group hidden items-center gap-1 text-sm text-cyan-300 transition hover:text-cyan-200 md:inline-flex"
            >
              View all
              <ArrowRight
                size={14}
                className="transition-transform group-hover:translate-x-0.5"
              />
            </Link>
          </div>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {keyFeatures.map((feature, i) => {
              const Icon = featureIcons[i % featureIcons.length];
              return (
                <article
                  key={feature.title}
                  className="glass group relative overflow-hidden rounded-2xl p-6 transition hover:-translate-y-1"
                >
                  <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl border border-cyan-200/30 bg-cyan-300/10 text-cyan-200">
                    <Icon size={18} />
                  </div>
                  <h3 className="text-lg font-semibold text-white">
                    {feature.title}
                  </h3>
                  <p className="mt-3 text-sm leading-relaxed text-slate-300">
                    {feature.description}
                  </p>
                  <div className="pointer-events-none absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-cyan-300/40 to-transparent opacity-0 transition-opacity group-hover:opacity-100" />
                </article>
              );
            })}
          </div>
        </div>
      </section>

      {/* --- How it works --- */}
      <section className="mx-auto w-full max-w-6xl px-6 py-24">
        <div className="mb-12 max-w-3xl">
          <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
            How it works
          </p>
          <h2 className="mt-3 text-4xl font-semibold tracking-tight text-white md:text-5xl">
            Four steps. Chat to execution.
          </h2>
          <p className="mt-5 text-lg leading-relaxed text-slate-300">
            Every trade on NadoBro goes through the same clean path — from your
            intent in Telegram to a fill on the Nado CLOB.
          </p>
        </div>

        <HowItWorksSteps />
      </section>

      {/* --- Strategy showcase --- */}
      <section className="relative border-y border-white/5 bg-[#040e1e]/50">
        <div className="mx-auto w-full max-w-6xl px-6 py-24">
          <div className="mb-12 flex flex-col items-start justify-between gap-6 md:flex-row md:items-end">
            <div>
              <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
                Strategy automation
              </p>
              <h2 className="mt-3 text-4xl font-semibold tracking-tight text-white md:text-5xl">
                Five bots. One chat.
              </h2>
              <p className="mt-5 max-w-2xl text-lg leading-relaxed text-slate-300">
                Pick a path that matches your style — from conversational trades
                to fully-automated market making.
              </p>
            </div>
            <Link
              href="/strategies"
              className="group inline-flex items-center gap-1 text-sm text-cyan-300 transition hover:text-cyan-200"
            >
              Explore strategies
              <ArrowRight
                size={14}
                className="transition-transform group-hover:translate-x-0.5"
              />
            </Link>
          </div>

          <StrategyShowcase />
        </div>
      </section>

      {/* --- Security + infra --- */}
      <section className="mx-auto w-full max-w-6xl px-6 py-24">
        <div className="mb-12 max-w-3xl">
          <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
            Security &amp; infra
          </p>
          <h2 className="mt-3 text-4xl font-semibold tracking-tight text-white md:text-5xl">
            Self-custody, by architecture.
          </h2>
          <p className="mt-5 text-lg leading-relaxed text-slate-300">
            NadoBro never touches your private key. Everything that sizes,
            risks, and books orders runs between your signer and the on-chain
            CLOB.
          </p>
        </div>

        <SecurityInfra />

        <div className="mt-10 rounded-2xl border border-cyan-200/20 bg-gradient-to-r from-cyan-300/5 to-emerald-300/5 p-6 md:p-8">
          <div className="flex items-start gap-4">
            <div className="hidden flex-shrink-0 rounded-xl border border-cyan-200/30 bg-cyan-300/10 p-3 text-cyan-200 md:block">
              <KeyRound size={20} />
            </div>
            <div>
              <h3 className="text-lg font-semibold text-white">
                Linked Signer (1CT)
              </h3>
              <p className="mt-2 text-sm leading-relaxed text-slate-300">
                A linked signer on your device co-signs orders for NadoBro —
                so the bot can execute fast without ever seeing your key. Revoke
                with one command at any time.
              </p>
              <Link
                href="/security"
                className="mt-3 inline-flex items-center gap-1 text-sm text-cyan-300 transition hover:text-cyan-200"
              >
                Read the security model
                <ArrowRight size={14} />
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* --- FAQ --- */}
      <section className="relative border-t border-white/5 bg-[#040e1e]/50">
        <div className="mx-auto grid w-full max-w-6xl grid-cols-1 gap-12 px-6 py-24 md:grid-cols-[1fr_1.4fr]">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
              FAQ
            </p>
            <h2 className="mt-3 text-4xl font-semibold tracking-tight text-white md:text-5xl">
              Quick answers.
            </h2>
            <p className="mt-5 text-lg leading-relaxed text-slate-300">
              Custody, workflow, testing, automation. If you don&apos;t see
              your question, the full list lives on the FAQ page.
            </p>
            <Link
              href="/faq"
              className="mt-6 inline-flex items-center gap-1 text-sm text-cyan-300 transition hover:text-cyan-200"
            >
              All FAQ
              <ArrowRight size={14} />
            </Link>
          </div>
          <FaqAccordion />
        </div>
      </section>

      {/* --- Final CTA --- */}
      <section className="relative overflow-hidden border-t border-white/10 bg-[#04122a]/60">
        <div
          aria-hidden
          className="pointer-events-none absolute -left-40 top-1/2 h-[30rem] w-[30rem] -translate-y-1/2 rounded-full bg-[radial-gradient(circle,rgba(25,222,255,0.35),transparent_70%)] blur-3xl"
        />
        <div
          aria-hidden
          className="pointer-events-none absolute -right-40 top-1/2 h-[30rem] w-[30rem] -translate-y-1/2 rounded-full bg-[radial-gradient(circle,rgba(0,255,176,0.28),transparent_70%)] blur-3xl"
        />
        <div className="relative mx-auto w-full max-w-5xl px-6 py-28 text-center">
          <h2 className="text-4xl font-semibold tracking-tight text-white md:text-6xl">
            Ready to trade from chat?
          </h2>
          <p className="mx-auto mt-5 max-w-2xl text-lg leading-relaxed text-slate-300">
            Open the Telegram bot, link your signer, and run your first trade
            on Nado in under two minutes.
          </p>
          <div className="mt-10 flex flex-wrap justify-center gap-3">
            <a
              href={siteMeta.telegramUrl}
              target="_blank"
              rel="noreferrer"
              className="cta-btn inline-flex items-center gap-2 rounded-full px-8 py-4 text-base font-semibold text-[#032232]"
            >
              Launch on Telegram
              <ArrowRight size={18} />
            </a>
            <a
              href={siteMeta.docsUrl}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-2 rounded-full border border-cyan-200/40 bg-white/5 px-7 py-4 text-base font-medium text-cyan-100 transition hover:border-cyan-200/70 hover:bg-cyan-300/10"
            >
              Read the Docs
            </a>
          </div>
        </div>
      </section>
    </div>
  );
}
