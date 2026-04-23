import Image from "next/image";
import Link from "next/link";
import { siteMeta } from "@/lib/content";

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-40 border-b border-[var(--border)] bg-[var(--background)]/80 backdrop-blur-md">
      <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
        <Link href="/" className="flex items-center gap-2.5">
          <Image
            src="/nadobro-logo-symbol.png"
            alt=""
            width={28}
            height={28}
            className="rounded-sm"
            priority
          />
          <span className="text-[15px] font-semibold tracking-tight text-[var(--foreground)]">
            {siteMeta.name}
          </span>
        </Link>

        <nav className="hidden items-center gap-8 text-[14px] text-[var(--muted)] md:flex">
          <a href="#features" className="transition hover:text-white">
            Features
          </a>
          <a href="#how" className="transition hover:text-white">
            How it works
          </a>
          <a
            href={siteMeta.docsUrl}
            target="_blank"
            rel="noreferrer"
            className="transition hover:text-white"
          >
            Docs
          </a>
        </nav>

        <a
          href={siteMeta.telegramUrl}
          target="_blank"
          rel="noreferrer"
          className="btn-primary !py-2 !text-[14px]"
        >
          Launch
        </a>
      </div>
    </header>
  );
}
