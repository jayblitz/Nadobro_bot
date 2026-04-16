import { useState } from "react";
import { useAuthStore } from "@/store/auth";
import { api, getApiErrorMessage } from "@/api/client";
import { hapticSuccess, hapticError } from "@/lib/haptics";
import { clsx } from "clsx";

const LANGUAGES = [
  { code: "en", label: "English" },
  { code: "zh", label: "Chinese" },
  { code: "fr", label: "French" },
  { code: "ar", label: "Arabic" },
  { code: "ru", label: "Russian" },
  { code: "ko", label: "Korean" },
];

export default function Settings() {
  const { user, fetchUser } = useAuthStore();
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const switchNetwork = async (network: string) => {
    setBusy("network");
    setError(null);
    try {
      await api.post("/api/me/network", { network });
      hapticSuccess();
      await fetchUser();
    } catch (err) {
      hapticError();
      setError(getApiErrorMessage(err));
    } finally {
      setBusy(null);
    }
  };

  const changeLanguage = async (lang: string) => {
    setBusy("language");
    setError(null);
    try {
      await api.patch("/api/me/settings", { language: lang });
      hapticSuccess();
      await fetchUser();
    } catch (err) {
      hapticError();
      setError(getApiErrorMessage(err));
    } finally {
      setBusy(null);
    }
  };

  if (!user) return null;

  return (
    <div className="flex-1 overflow-y-auto hide-scrollbar px-4 pt-4 pb-4">
      <h1 className="text-lg font-bold text-white mb-4">Settings</h1>

      {error && (
        <div className="bg-short/10 text-short text-sm rounded-xl px-4 py-2 mb-4">
          {error}
        </div>
      )}

      <section className="bg-white/5 rounded-xl p-4 mb-4">
        <div className="text-[11px] text-tg-hint mb-1">Wallet</div>
        <div className="text-sm text-white font-mono break-all">
          {user.main_address ?? "Not linked"}
        </div>
      </section>

      <section className="bg-white/5 rounded-xl p-4 mb-4">
        <div className="text-[11px] text-tg-hint mb-3">Network</div>
        <div className="flex rounded-xl overflow-hidden bg-black/30">
          {(["testnet", "mainnet"] as const).map((n) => (
            <button
              key={n}
              onClick={() => switchNetwork(n)}
              disabled={busy === "network" || user.network === n}
              className={clsx(
                "flex-1 py-2.5 text-sm font-medium transition-colors disabled:opacity-60",
                user.network === n
                  ? "bg-tg-button text-tg-button-text"
                  : "text-tg-hint",
              )}
            >
              {busy === "network" && user.network !== n
                ? "..."
                : n.charAt(0).toUpperCase() + n.slice(1)}
            </button>
          ))}
        </div>
      </section>

      <section className="bg-white/5 rounded-xl p-4 mb-4">
        <div className="text-[11px] text-tg-hint mb-3">Language</div>
        <div className="grid grid-cols-3 gap-2">
          {LANGUAGES.map((l) => (
            <button
              key={l.code}
              onClick={() => changeLanguage(l.code)}
              disabled={busy === "language" || user.language === l.code}
              className={clsx(
                "py-2 rounded-lg text-xs font-medium transition-colors disabled:opacity-60",
                user.language === l.code
                  ? "bg-tg-button text-tg-button-text"
                  : "bg-black/20 text-tg-hint",
              )}
            >
              {l.label}
            </button>
          ))}
        </div>
      </section>

      <section className="bg-white/5 rounded-xl p-4">
        <div className="text-[11px] text-tg-hint mb-2">Account Stats</div>
        <div className="flex justify-between text-sm">
          <span className="text-tg-hint">Total Trades</span>
          <span className="text-white font-medium">{user.total_trades}</span>
        </div>
        <div className="flex justify-between text-sm mt-1">
          <span className="text-tg-hint">Total Volume</span>
          <span className="text-white font-medium">
            ${user.total_volume_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </span>
        </div>
      </section>
    </div>
  );
}
