import { PageHero } from "@/components/page-hero";
import { faq } from "@/lib/content";

export default function FaqPage() {
  return (
    <div>
      <PageHero
        title="Frequently Asked Questions"
        subtitle="Fast answers about custody, workflows, strategy coverage, and testing modes."
      />
      <section className="mx-auto w-full max-w-6xl px-6 py-16">
        <div className="grid gap-5">
          {faq.map((item) => (
            <article key={item.question} className="glass rounded-2xl p-6">
              <h2 className="text-xl font-semibold text-white">
                {item.question}
              </h2>
              <p className="mt-3 leading-relaxed text-slate-300">
                {item.answer}
              </p>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}
