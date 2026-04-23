import Image from "next/image";
import { siteMeta } from "@/lib/content";

export function SiteFooter() {
  return (
    <footer className="mt-24 border-t border-[var(--border)]">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-8 px-6 py-12 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-2.5">
          <Image
            src="/nadobro-logo-symbol.png"
            alt=""
            width={24}
            height={24}
            className="rounded-sm"
          />
          <span className="text-[14px] font-semibold tracking-tight">
            {siteMeta.name}
          </span>
          <span className="ml-3 text-[13px] text-[var(--muted)]">
            © {new Date().getFullYear()} — built on Nado
          </span>
        </div>

        <div className="flex items-center gap-6 text-[13px] text-[var(--muted)]">
          <a
            href={siteMeta.telegramUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-white"
          >
            Telegram
          </a>
          <a
            href={siteMeta.xUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-white"
          >
            X
          </a>
          <a
            href={siteMeta.docsUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-white"
          >
            Docs
          </a>
        </div>
      </div>
    </footer>
  );
}
