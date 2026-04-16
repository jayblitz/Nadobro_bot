import Link from "next/link";
import { siteMeta } from "@/lib/content";

export function SiteFooter() {
  return (
    <footer className="border-t border-white/10 bg-[#040b17]">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4 px-6 py-10 md:flex-row md:items-center md:justify-between">
        <p className="text-sm text-slate-400">
          {siteMeta.name} - Telegram-native perpetual futures on Nado.
        </p>
        <div className="flex items-center gap-5 text-sm text-slate-300">
          <a
            href={siteMeta.docsUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-cyan-300"
          >
            Docs
          </a>
          <a
            href={siteMeta.xUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-cyan-300"
          >
            X
          </a>
          <Link href="/faq" className="transition hover:text-cyan-300">
            FAQ
          </Link>
        </div>
      </div>
    </footer>
  );
}
