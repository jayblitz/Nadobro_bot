import { PageHero } from "@/components/page-hero";
import { strategies } from "@/lib/content";

export default function StrategiesPage() {
  return (
    <div>
      <PageHero
        title="5 Strategies, One Chat Interface"
        subtitle="NadoBro provides five built-in automation paths so traders can align execution with their style and market thesis."
      />
      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <div className="grid gap-5 md:grid-cols-2">
          {strategies.map((strategy) => (
            <article key={strategy.name} className="glass rounded-2xl p-6">
              <h2 className="text-xl font-semibold text-white">{strategy.name}</h2>
              <p className="mt-3 leading-relaxed text-slate-300">
                {strategy.description}
              </p>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}
