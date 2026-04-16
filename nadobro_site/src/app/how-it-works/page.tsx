import { PageHero } from "@/components/page-hero";
import { howItWorks } from "@/lib/content";

export default function HowItWorksPage() {
  return (
    <div>
      <PageHero
        title="How NadoBro Works"
        subtitle="A streamlined flow from prompt to execution so traders can move from intent to position in seconds."
      />
      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <ol className="grid gap-5 md:grid-cols-2">
          {howItWorks.map((step, index) => (
            <li key={step} className="glass rounded-2xl p-6">
              <p className="text-sm font-medium text-cyan-200">
                Step {index + 1}
              </p>
              <p className="mt-3 text-lg text-white">{step}</p>
            </li>
          ))}
        </ol>
      </section>
    </div>
  );
}
