import Image from "next/image";
import Link from "next/link";
import { siteMeta } from "@/lib/content";

const navLinks = [
  { href: "/", label: "Home" },
  { href: "/features", label: "Features" },
  { href: "/strategies", label: "Strategies" },
  { href: "/security", label: "Security" },
  { href: "/how-it-works", label: "How It Works" },
  { href: "/faq", label: "FAQ" },
];

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-40 border-b border-white/10 bg-[#061021]/70 backdrop-blur-xl">
      <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
        <Link href="/" className="group flex items-center gap-3">
          <Image
            src="/nadobro-logo-symbol.png"
            alt="NadoBro logo"
            width={36}
            height={36}
            className="rounded-md shadow-[0_0_25px_rgba(0,255,214,0.45)]"
            priority
          />
          <span className="text-lg font-semibold tracking-wide text-white transition group-hover:text-[#8efef5]">
            {siteMeta.name}
          </span>
        </Link>
        <nav className="hidden gap-6 md:flex">
          {navLinks.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="text-sm text-slate-300 transition hover:text-cyan-300"
            >
              {link.label}
            </Link>
          ))}
        </nav>
        <a
          href={siteMeta.xUrl}
          target="_blank"
          rel="noreferrer"
          className="rounded-full border border-cyan-400/40 px-4 py-2 text-sm text-cyan-200 transition hover:border-cyan-300 hover:text-white"
        >
          Follow on X
        </a>
      </div>
    </header>
  );
}
