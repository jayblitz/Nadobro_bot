import Image from "next/image";
import Link from "next/link";
import { BookOpen, Send, Twitter } from "lucide-react";
import { siteMeta } from "@/lib/content";

const productLinks = [
  { href: "/features", label: "Features" },
  { href: "/strategies", label: "Strategies" },
  { href: "/how-it-works", label: "How It Works" },
  { href: "/security", label: "Security" },
];

const resourceLinks = [
  { href: siteMeta.docsUrl, label: "Documentation", external: true },
  { href: "/faq", label: "FAQ", external: false },
];

const socialLinks = [
  { href: siteMeta.telegramUrl, label: "Telegram", Icon: Send },
  { href: siteMeta.xUrl, label: "X / Twitter", Icon: Twitter },
  { href: siteMeta.docsUrl, label: "Docs", Icon: BookOpen },
];

export function SiteFooter() {
  return (
    <footer className="relative border-t border-white/10 bg-[#040b17]">
      {/* Top gradient line */}
      <div
        aria-hidden
        className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan-400/40 to-transparent"
      />

      <div className="mx-auto w-full max-w-6xl px-6 py-16">
        <div className="grid gap-10 md:grid-cols-[1.5fr_1fr_1fr_1fr]">
          <div>
            <Link
              href="/"
              className="inline-flex items-center gap-3"
              aria-label={`${siteMeta.name} home`}
            >
              <Image
                src="/nadobro-logo-symbol.png"
                alt=""
                width={36}
                height={36}
                className="rounded-md shadow-[0_0_25px_rgba(105,227,255,0.4)]"
              />
              <span className="text-lg font-semibold text-white">
                {siteMeta.name}
              </span>
            </Link>
            <p className="mt-4 max-w-sm text-sm leading-relaxed text-slate-400">
              {siteMeta.tagline}. Perpetuals on Nado, executed from Telegram.
            </p>

            <div className="mt-6 flex items-center gap-3">
              {socialLinks.map(({ href, label, Icon }) => (
                <a
                  key={label}
                  href={href}
                  target="_blank"
                  rel="noreferrer"
                  aria-label={label}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-200 transition hover:border-cyan-300/50 hover:bg-cyan-300/10 hover:text-cyan-100"
                >
                  <Icon size={16} />
                </a>
              ))}
            </div>
          </div>

          <FooterColumn title="Product">
            {productLinks.map((link) => (
              <li key={link.href}>
                <Link
                  href={link.href}
                  className="text-sm text-slate-400 transition hover:text-cyan-200"
                >
                  {link.label}
                </Link>
              </li>
            ))}
          </FooterColumn>

          <FooterColumn title="Resources">
            {resourceLinks.map((link) =>
              link.external ? (
                <li key={link.href}>
                  <a
                    href={link.href}
                    target="_blank"
                    rel="noreferrer"
                    className="text-sm text-slate-400 transition hover:text-cyan-200"
                  >
                    {link.label}
                  </a>
                </li>
              ) : (
                <li key={link.href}>
                  <Link
                    href={link.href}
                    className="text-sm text-slate-400 transition hover:text-cyan-200"
                  >
                    {link.label}
                  </Link>
                </li>
              ),
            )}
          </FooterColumn>

          <FooterColumn title="Status">
            <li className="flex items-center gap-2 text-sm text-slate-400">
              <span className="inline-block h-2 w-2 rounded-full bg-emerald-400 shadow-[0_0_10px_rgba(0,255,178,0.8)]" />
              Ink mainnet · live
            </li>
            <li className="text-sm text-slate-400">Nado CLOB · online</li>
            <li className="text-sm text-slate-400">Testnet · open</li>
          </FooterColumn>
        </div>

        <div className="mt-14 flex flex-col gap-3 border-t border-white/5 pt-6 md:flex-row md:items-center md:justify-between">
          <p className="text-xs text-slate-500">
            © {new Date().getFullYear()} {siteMeta.name}. Trade responsibly —
            leveraged products carry risk.
          </p>
          <p className="text-xs text-slate-500">
            Built on Nado · Powered by InkOnchain
          </p>
        </div>
      </div>
    </footer>
  );
}

function FooterColumn({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-200/80">
        {title}
      </p>
      <ul className="mt-4 space-y-2.5">{children}</ul>
    </div>
  );
}
