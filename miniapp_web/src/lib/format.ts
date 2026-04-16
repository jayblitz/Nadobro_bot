/** Shared number/price formatting for the mini app UI. */

/** USD amounts with fixed decimal places (balances, margin, portfolio). */
export function formatUsdFixed(n: number, digits = 2): string {
  return `$${n.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })}`;
}

/**
 * Price for display. When `product` is omitted, uses magnitude-based decimals (home list).
 * When `product` is set, uses per-asset rules (detail / trade).
 */
export function formatPrice(n: number | null | undefined, product?: string): string {
  if (n == null) return "--";
  if (product) {
    const u = product.toUpperCase();
    const decimals = u === "DOGE" || u === "XRP" ? 4 : 2;
    return n >= 1000
      ? `$${n.toLocaleString(undefined, { maximumFractionDigits: decimals })}`
      : `$${n.toFixed(decimals)}`;
  }
  if (n >= 1000) return `$${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  if (n >= 1) return `$${n.toFixed(2)}`;
  return `$${n.toFixed(4)}`;
}

/** Signed percent with two decimals (e.g. 24h row when value is known). */
export function formatSignedPct2(n: number): string {
  return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
}

/** Perp funding rate from API as decimal → percent string. */
export function formatFundingRate(n: number | null | undefined): string {
  if (n == null) return "--";
  return `${(n * 100).toFixed(4)}%`;
}

export function formatTradeFillSummary(res: {
  side?: string;
  product?: string;
  fill_price?: number | null;
}): string {
  const px = res.fill_price != null ? res.fill_price.toLocaleString() : "--";
  return `Filled ${res.side ?? "?"} ${res.product ?? "?"} @ $${px}`;
}
