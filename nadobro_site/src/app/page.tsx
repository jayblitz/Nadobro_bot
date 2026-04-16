import Image from "next/image";
import Link from "next/link";
import {
  keyFeatures,
  keyMetrics,
  philosophy,
  siteMeta,
  strategies,
} from "@/lib/content";

export default function Home() {
  return (
    <div className="relative overflow-hidden">
      <section className="relative overflow-hidden border-b border-white/10">
        <div className="hero-blur hero-blur-left" />
        <div className="hero-blur hero-blur-right" />
        <div className="hero-grid absolute inset-0" />
        <div className="scan-line" />

        <div className="relative mx-auto grid w-full max-w-6xl items-center gap-12 px-6 py-20 md:grid-cols-2 md:py-28">
          <div>
            <p className="mb-4 inline-block rounded-full border border-cyan-300/30 bg-cyan-300/10 px-4 py-1 text-sm text-cyan-200">
              Built on Ink L2, trading on Nado CLOB DEX
            </p>
            <h1 className="text-5xl font-semibold leading-tight tracking-tight text-white md:text-6xl">
              Perpetual trading from chat, engineered for speed.
            </h1>
            <p className="mt-6 max-w-xl text-lg leading-relaxed text-slate-300">
              {siteMeta.tagline}. Type what you want, NadoBro executes with
              low-friction precision directly inside Telegram.
            </p>
            <div className="mt-8 flex flex-wrap items-center gap-4">
              <a
                href={siteMeta.xUrl}
                target="_blank"
                rel="noreferrer"
                className="rounded-full bg-gradient-to-r from-cyan-400 to-emerald-300 px-6 py-3 font-medium text-[#032232] transition hover:scale-[1.02]"
              >
                Follow on X
              </a>
              <a
                href={siteMeta.docsUrl}
                target="_blank"
                rel="noreferrer"
                className="rounded-full border border-cyan-200/40 px-6 py-3 font-medium text-cyan-100 transition hover:bg-cyan-200/10"
              >
                Read Docs
              </a>
            </div>
          </div>

          <div className="relative">
            <div className="glass float-card rounded-3xl p-6">
              <Image
                src="/nadobro-logo.png"
                alt="NadoBro"
                width={480}
                height={480}
                className="mx-auto h-auto w-full max-w-sm drop-shadow-[0_0_30px_rgba(27,237,255,0.35)]"
                priority
              />
            </div>
            <div className="glass float-card float-card-delay absolute -bottom-8 -left-8 hidden rounded-2xl p-4 md:block">
              <p className="text-xs uppercase tracking-[0.2em] text-cyan-200/80">
                Cumulative Volume
              </p>
              <p className="mt-1 text-2xl font-semibold text-white">$48B+</p>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <h2 className="text-3xl font-semibold text-white">Overview</h2>
        <p className="mt-4 max-w-4xl text-slate-300">
          NadoBro is a sophisticated Telegram-native platform for perpetual
          futures on Nado, giving users institutional-grade execution without
          leaving chat.
        </p>
        <div className="mt-8 grid gap-4 md:grid-cols-3">
          {keyMetrics.map((metric) => (
            <div
              key={metric.label}
              className="glass rounded-2xl p-5 transition hover:-translate-y-1"
            >
              <p className="text-sm text-cyan-200">{metric.label}</p>
              <p className="mt-2 text-xl font-semibold text-white">
                {metric.value}
              </p>
            </div>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6 pb-16">
        <div className="mb-8 flex items-end justify-between gap-6">
          <h2 className="text-3xl font-semibold text-white">Key Features</h2>
          <Link href="/features" className="text-cyan-300 hover:text-cyan-200">
            View all
          </Link>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {keyFeatures.map((feature) => (
            <article key={feature.title} className="glass rounded-2xl p-6">
              <h3 className="text-lg font-semibold text-white">{feature.title}</h3>
              <p className="mt-3 text-sm leading-relaxed text-slate-300">
                {feature.description}
              </p>
            </article>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6 pb-16">
        <div className="mb-8 flex items-end justify-between gap-6">
          <h2 className="text-3xl font-semibold text-white">
            Strategy Automation
          </h2>
          <Link
            href="/strategies"
            className="text-cyan-300 transition hover:text-cyan-200"
          >
            Explore strategies
          </Link>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          {strategies.map((strategy) => (
            <article key={strategy.name} className="glass rounded-2xl p-6">
              <h3 className="text-lg font-semibold text-white">{strategy.name}</h3>
              <p className="mt-3 text-sm leading-relaxed text-slate-300">
                {strategy.description}
              </p>
            </article>
          ))}
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6 pb-20">
        <h2 className="text-3xl font-semibold text-white">
          Core Philosophy & Differentiation
        </h2>
        <div className="mt-8 grid gap-4 md:grid-cols-3">
          {philosophy.map((pillar) => (
            <article key={pillar.title} className="glass rounded-2xl p-6">
              <h3 className="text-lg font-semibold text-white">{pillar.title}</h3>
              <p className="mt-3 text-sm leading-relaxed text-slate-300">
                {pillar.description}
              </p>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}
