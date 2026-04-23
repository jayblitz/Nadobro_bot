import { PageHero } from "@/components/page-hero";
import { philosophy } from "@/lib/content";

export default function SecurityPage() {
  return (
    <div>
      <PageHero
        title="Security-First by Architecture"
        subtitle="NadoBro is designed around self-custody and operational clarity, so users can execute quickly without giving up control of private keys."
      />
      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <div className="grid gap-5 md:grid-cols-3">
          {philosophy.map((pillar) => (
            <article key={pillar.title} className="glass rounded-2xl p-6">
              <h2 className="text-xl font-semibold text-white">{pillar.title}</h2>
              <p className="mt-3 leading-relaxed text-slate-300">
                {pillar.description}
              </p>
            </article>
          ))}
        </div>
        <div className="glass mt-8 rounded-2xl p-6">
          <h3 className="text-lg font-semibold text-white">Linked Signer (1CT)</h3>
          <p className="mt-3 leading-relaxed text-slate-300">
            The platform follows a linked signer approach where key ownership
            remains user-side. This keeps NadoBro aligned with a self-custodial
            model while preserving fast Telegram execution flow.
          </p>
        </div>
      </section>
    </div>
  );
}
