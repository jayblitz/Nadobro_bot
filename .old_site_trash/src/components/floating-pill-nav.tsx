"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "framer-motion";
import { useEffect, useState } from "react";
import { Menu, X } from "lucide-react";
import { siteMeta } from "@/lib/content";

const navLinks = [
  { href: "/", label: "Home" },
  { href: "/features", label: "Features" },
  { href: "/strategies", label: "Strategies" },
  { href: "/how-it-works", label: "How It Works" },
  { href: "/security", label: "Security" },
  { href: "/faq", label: "FAQ" },
];

export function FloatingPillNav() {
  const pathname = usePathname();
  const [scrolled, setScrolled] = useState(false);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 24);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <motion.header
      initial={{ y: -24, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.8, ease: [0.16, 1, 0.3, 1] }}
      className="fixed inset-x-0 top-4 z-50 flex justify-center px-4"
    >
      <div
        className={`pill-nav-shell flex w-full max-w-5xl items-center justify-between gap-3 rounded-full border px-3 py-2 transition-[background,border,box-shadow] duration-500 ${
          scrolled
            ? "border-white/15 bg-[#061021]/85 shadow-[0_12px_40px_rgba(0,12,26,0.55)] backdrop-blur-xl"
            : "border-white/10 bg-[#061021]/55 backdrop-blur-lg"
        }`}
      >
        <Link
          href="/"
          className="group flex items-center gap-2 rounded-full px-2 py-1"
          aria-label={`${siteMeta.name} home`}
        >
          <Image
            src="/nadobro-logo-symbol.png"
            alt=""
            width={28}
            height={28}
            className="rounded-md shadow-[0_0_20px_rgba(105,227,255,0.5)]"
            priority
          />
          <span className="text-sm font-semibold tracking-wide text-white transition group-hover:text-[#8efef5]">
            {siteMeta.name}
          </span>
        </Link>

        <nav className="hidden items-center gap-1 md:flex">
          {navLinks.map((link) => {
            const active =
              link.href === "/"
                ? pathname === "/"
                : pathname?.startsWith(link.href);
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`relative rounded-full px-4 py-2 text-sm transition ${
                  active
                    ? "text-white"
                    : "text-slate-300 hover:text-white"
                }`}
              >
                {active && (
                  <motion.span
                    layoutId="pill-nav-active"
                    className="absolute inset-0 rounded-full bg-white/10 ring-1 ring-inset ring-white/15"
                    transition={{ type: "spring", stiffness: 380, damping: 32 }}
                  />
                )}
                <span className="relative">{link.label}</span>
              </Link>
            );
          })}
        </nav>

        <div className="flex items-center gap-2">
          <a
            href={siteMeta.xUrl}
            target="_blank"
            rel="noreferrer"
            className="cta-btn hidden items-center gap-2 rounded-full px-4 py-2 text-sm font-medium text-[#032232] md:inline-flex"
          >
            Launch on Telegram
          </a>
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/15 text-slate-200 md:hidden"
            aria-label={open ? "Close menu" : "Open menu"}
            aria-expanded={open}
          >
            {open ? <X size={18} /> : <Menu size={18} />}
          </button>
        </div>
      </div>

      {/* Mobile drawer */}
      {open && (
        <motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          className="absolute left-4 right-4 top-20 rounded-2xl border border-white/10 bg-[#061021]/95 p-3 backdrop-blur-xl md:hidden"
        >
          <div className="flex flex-col">
            {navLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                onClick={() => setOpen(false)}
                className="rounded-xl px-4 py-3 text-sm text-slate-200 hover:bg-white/5 hover:text-white"
              >
                {link.label}
              </Link>
            ))}
            <a
              href={siteMeta.xUrl}
              target="_blank"
              rel="noreferrer"
              className="cta-btn mt-2 inline-flex items-center justify-center rounded-full px-4 py-3 text-sm font-medium text-[#032232]"
            >
              Launch on Telegram
            </a>
          </div>
        </motion.div>
      )}
    </motion.header>
  );
}
