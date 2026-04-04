import { useLocation, useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { hapticSelection } from "@/lib/haptics";

interface Tab {
  path: string;
  label: string;
  icon: React.ReactNode;
}

const TABS: Tab[] = [
  {
    path: "/",
    label: "Home",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
        <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z" strokeLinecap="round" strokeLinejoin="round" />
        <polyline points="9 22 9 12 15 12 15 22" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
  },
  {
    path: "/portfolio",
    label: "Portfolio",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
      </svg>
    ),
  },
  {
    path: "/strategies",
    label: "Bots",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
        <circle cx="12" cy="12" r="3" />
        <path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
  },
  {
    path: "/ai",
    label: "Bro",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
        <path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z" />
        <path d="M19 10v2a7 7 0 01-14 0v-2" strokeLinecap="round" />
        <line x1="12" y1="19" x2="12" y2="23" strokeLinecap="round" />
        <line x1="8" y1="23" x2="16" y2="23" strokeLinecap="round" />
      </svg>
    ),
  },
  {
    path: "/settings",
    label: "More",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
        <circle cx="12" cy="12" r="1" />
        <circle cx="19" cy="12" r="1" />
        <circle cx="5" cy="12" r="1" />
      </svg>
    ),
  },
];

export default function BottomTabs() {
  const location = useLocation();
  const navigate = useNavigate();

  // Hide tabs on product detail page
  if (location.pathname.startsWith("/product/")) return null;

  return (
    <nav className="flex items-center justify-around bg-tg-secondary-bg border-t border-white/5 safe-bottom pt-2 pb-1">
      {TABS.map((tab) => {
        const active =
          tab.path === "/"
            ? location.pathname === "/"
            : location.pathname.startsWith(tab.path);
        return (
          <button
            key={tab.path}
            onClick={() => {
              hapticSelection();
              navigate(tab.path);
            }}
            aria-label={tab.label}
            aria-current={active ? "page" : undefined}
            className={clsx(
              "flex flex-col items-center gap-0.5 px-3 py-1 text-[11px] transition-colors focus-visible:ring-2 focus-visible:ring-tg-button focus-visible:ring-offset-1 focus-visible:ring-offset-tg-secondary-bg rounded-lg",
              active ? "text-tg-button" : "text-tg-hint",
            )}
          >
            {tab.icon}
            <span>{tab.label}</span>
          </button>
        );
      })}
    </nav>
  );
}
