type PageHeroProps = {
  title: string;
  subtitle: string;
};

export function PageHero({ title, subtitle }: PageHeroProps) {
  return (
    <section className="relative overflow-hidden border-b border-white/10 bg-[#050f20]">
      <div className="hero-blur hero-blur-left" />
      <div className="hero-blur hero-blur-right" />
      <div className="mx-auto w-full max-w-6xl px-6 py-20">
        <h1 className="text-4xl font-semibold tracking-tight text-white md:text-5xl">
          {title}
        </h1>
        <p className="mt-5 max-w-3xl text-lg leading-relaxed text-slate-300">
          {subtitle}
        </p>
      </div>
    </section>
  );
}
