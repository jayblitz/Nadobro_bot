import { PageHero } from "@/components/page-hero";
import { keyFeatures } from "@/lib/content";

export default function FeaturesPage() {
  return (
    <div>
      <PageHero
        title="Feature Stack Built for Fast Decisions"
        subtitle="NadoBro combines natural language execution, market intelligence, automation, and portfolio visibility inside a Telegram-native workflow."
      />
      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <div className="grid gap-5 md:grid-cols-2">
          {keyFeatures.map((feature) => (
            <article key={feature.title} className="glass rounded-2xl p-6">
              <h2 className="text-xl font-semibold text-white">{feature.title}</h2>
              <p className="mt-3 leading-relaxed text-slate-300">
                {feature.description}
              </p>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}
