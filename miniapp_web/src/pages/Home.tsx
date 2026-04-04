import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { api } from "@/api/client";
import { hapticImpact } from "@/lib/haptics";
import { useMarketStore } from "@/store/market";
import { useAccountStore } from "@/store/account";
import type {
  ProductInfo,
  PortfolioSummary,
  QuotesResponse,
  CryptoQuote,
  AllPricesResponse,
} from "@/api/types";

/** Crypto token color map for avatar backgrounds */
const TOKEN_COLORS: Record<string, string> = {
  BTC: "#F7931A",
  ETH: "#627EEA",
  SOL: "#9945FF",
  XRP: "#23292F",
  BNB: "#F3BA2F",
  LINK: "#2A5ADA",
  DOGE: "#C2A633",
  AVAX: "#E84142",
};

function formatPrice(n: number | null | undefined): string {
  if (n == null) return "--";
  if (n >= 1000) return `$${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  if (n >= 1) return `$${n.toFixed(2)}`;
  return `$${n.toFixed(4)}`;
}

function formatChange(n: number | null | undefined): string {
  if (n == null) return "--";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function formatFunding(n: number | null | undefined): string {
  if (n == null) return "--";
  return `${(n * 100).toFixed(4)}%`;
}

export default function Home() {
  const navigate = useNavigate();
  const { products, prices, setProducts, updatePrices, selectProduct } = useMarketStore();
  const { portfolio, setPortfolio } = useAccountStore();

  // Fetch products
  const { data: productsData } = useQuery({
    queryKey: ["products"],
    queryFn: () => api.get<ProductInfo[]>("/api/products"),
    staleTime: 60_000,
  });

  useEffect(() => {
    if (productsData) setProducts(productsData);
  }, [productsData, setProducts]);

  // Fetch prices every 5s
  const { data: pricesData } = useQuery({
    queryKey: ["prices"],
    queryFn: () => api.get<AllPricesResponse>("/api/prices"),
    refetchInterval: 5000,
  });

  useEffect(() => {
    if (pricesData?.prices) updatePrices(pricesData.prices);
  }, [pricesData, updatePrices]);

  // Fetch 24h changes + funding rates from Nado Indexer
  const { data: quotesData } = useQuery({
    queryKey: ["quotes"],
    queryFn: () => api.get<QuotesResponse>("/api/quotes"),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  // Fetch portfolio
  const { data: portfolioData } = useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.get<PortfolioSummary>("/api/portfolio"),
    refetchInterval: 10_000,
  });

  useEffect(() => {
    if (portfolioData) setPortfolio(portfolioData);
  }, [portfolioData, setPortfolio]);

  const quotes = quotesData?.quotes ?? {};
  const positions = portfolio?.positions ?? [];

  const openProduct = (name: string) => {
    hapticImpact("light");
    selectProduct(name);
    navigate(`/product/${name}`);
  };

  return (
    <div className="flex-1 flex flex-col overflow-y-auto hide-scrollbar pb-2">
      {/* Header */}
      <div className="px-4 pt-4 pb-2">
        <h1 className="text-xl font-bold text-white">Perpetuals</h1>
      </div>

      {/* Open positions card */}
      {positions.length > 0 && (
        <div className="mx-4 mb-4 bg-white/5 rounded-2xl overflow-hidden">
          <div className="flex items-center justify-between px-4 pt-3 pb-2">
            <span className="text-xs font-medium text-tg-hint uppercase tracking-wide">Open Positions</span>
            <button
              onClick={() => navigate("/portfolio")}
              className="text-xs text-tg-link"
            >
              View All
            </button>
          </div>
          <div className="divide-y divide-white/5">
            {positions.slice(0, 3).map((pos) => {
              const pnl = pos.unrealized_pnl;
              const isLong = pos.side === "LONG";
              return (
                <button
                  key={`${pos.product_id}-${pos.side}`}
                  onClick={() => openProduct(pos.product_name)}
                  className="w-full flex items-center justify-between px-4 py-3 active:bg-white/5 transition-colors"
                >
                  <div className="flex items-center gap-3">
                    <div className="flex flex-col items-start">
                      <div className="flex items-center gap-1.5">
                        <span className="text-sm font-semibold text-white">
                          {pos.product_name}
                        </span>
                        <span
                          className={clsx(
                            "text-[10px] font-bold px-1.5 py-0.5 rounded",
                            isLong ? "bg-long/20 text-long" : "bg-short/20 text-short",
                          )}
                        >
                          {isLong ? "LONG" : "SHORT"}
                        </span>
                        {pos.leverage && (
                          <span className="text-[10px] text-tg-hint">
                            {pos.leverage}x
                          </span>
                        )}
                      </div>
                      <span className="text-xs text-tg-hint mt-0.5">
                        {pos.size.toFixed(4)} @ {formatPrice(pos.entry_price)}
                      </span>
                    </div>
                  </div>
                  <div className="text-right">
                    <span
                      className={clsx(
                        "text-sm font-semibold",
                        pnl == null
                          ? "text-tg-hint"
                          : pnl >= 0
                            ? "text-long"
                            : "text-short",
                      )}
                    >
                      {pnl != null ? `${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}` : "--"}
                    </span>
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Portfolio summary bar */}
      {portfolio && (
        <div className="flex gap-3 mx-4 mb-4">
          <div className="flex-1 bg-white/5 rounded-xl px-3 py-2.5">
            <div className="text-[10px] text-tg-hint uppercase tracking-wide">Equity</div>
            <div className="text-sm font-bold text-white mt-0.5">
              ${portfolio.equity.toFixed(2)}
            </div>
          </div>
          <div className="flex-1 bg-white/5 rounded-xl px-3 py-2.5">
            <div className="text-[10px] text-tg-hint uppercase tracking-wide">Unrealized PnL</div>
            <div
              className={clsx(
                "text-sm font-bold mt-0.5",
                portfolio.total_unrealized_pnl >= 0 ? "text-long" : "text-short",
              )}
            >
              {portfolio.total_unrealized_pnl >= 0 ? "+" : ""}
              ${portfolio.total_unrealized_pnl.toFixed(2)}
            </div>
          </div>
          <div className="flex-1 bg-white/5 rounded-xl px-3 py-2.5">
            <div className="text-[10px] text-tg-hint uppercase tracking-wide">Available</div>
            <div className="text-sm font-bold text-white mt-0.5">
              ${portfolio.available_balance.toFixed(2)}
            </div>
          </div>
        </div>
      )}

      {/* Trending Perps grid */}
      <div className="px-4 mb-2">
        <h2 className="text-sm font-semibold text-tg-hint uppercase tracking-wide">
          Trending Perps
        </h2>
      </div>

      <div className="grid grid-cols-2 gap-3 px-4">
        {(products.length > 0 ? products : []).map((p) => {
          const quote: CryptoQuote | undefined = quotes[p.name];
          const priceData = prices[p.name];
          const mid = priceData?.mid ?? quote?.price;
          const change = quote?.change_24h;
          const bgColor = TOKEN_COLORS[p.name] ?? "#5288c1";

          return (
            <button
              key={p.name}
              onClick={() => openProduct(p.name)}
              className="bg-white/5 rounded-2xl p-4 text-left active:scale-[0.98] transition-transform"
            >
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div
                    className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white"
                    style={{ backgroundColor: bgColor }}
                  >
                    {p.name.slice(0, 2)}
                  </div>
                  <div>
                    <div className="text-sm font-semibold text-white">{p.name}</div>
                    <div className="text-[10px] text-tg-hint">{p.name}-PERP</div>
                  </div>
                </div>
                <span className="text-[10px] font-medium text-tg-hint bg-white/10 px-1.5 py-0.5 rounded">
                  {p.max_leverage}x
                </span>
              </div>

              <div className="text-lg font-bold text-white tabular-nums">
                {formatPrice(mid)}
              </div>
              <div className="flex items-center gap-2 mt-0.5">
                <span
                  className={clsx(
                    "text-xs font-medium",
                    change == null
                      ? "text-tg-hint"
                      : change >= 0
                        ? "text-long"
                        : "text-short",
                  )}
                >
                  {formatChange(change)}
                </span>
                {quote?.funding_rate != null && (
                  <span className="text-[10px] text-tg-hint">
                    F: {formatFunding(quote.funding_rate)}
                  </span>
                )}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
