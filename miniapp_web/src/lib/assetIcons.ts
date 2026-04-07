/** Remote asset icons (crypto) + fallback colors for initials. */

const CRYPTO_ICON_CDN =
  "https://cdn.jsdelivr.net/npm/cryptocurrency-icons@0.18.1/svg/color";

/** Fallback circle colors when an icon URL is unavailable. */
export const ASSET_FALLBACK_COLORS: Record<string, string> = {
  BTC: "#F7931A",
  ETH: "#627EEA",
  SOL: "#9945FF",
  XRP: "#23292F",
  BNB: "#F3BA2F",
  LINK: "#2A5ADA",
  DOGE: "#C2A633",
  HYPE: "#8B5CF6",
  WTI: "#6B7280",
  SPY: "#166534",
  QQQ: "#1D4ED8",
  XAG: "#94A3B8",
  XAUT: "#EAB308",
  XPL: "#64748B",
};

const _noRemoteIcon = new Set([
  "WTI",
  "SPY",
  "QQQ",
  "XAG",
  "XPL",
]);

/**
 * Return a URL for the asset logo, or null to use initials + ASSET_FALLBACK_COLORS.
 */
export function getAssetIconUrl(symbol: string): string | null {
  const s = (symbol || "").toUpperCase().trim();
  if (!s) return null;
  if (_noRemoteIcon.has(s)) return null;
  return `${CRYPTO_ICON_CDN}/${s.toLowerCase()}.svg`;
}

export function getAssetFallbackColor(symbol: string): string {
  return ASSET_FALLBACK_COLORS[symbol.toUpperCase()] ?? "#5288c1";
}
