import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { api } from "@/api/client";
import { hapticImpact } from "@/lib/haptics";
import { useMarketStore } from "@/store/market";
import { useAccountStore } from "@/store/account";
import { useFavoritePerps } from "@/hooks/useFavoritePerps";
import AssetAvatar from "@/components/common/AssetAvatar";
import type {
  ProductInfo,
  PortfolioSummary,
  QuotesResponse,
  CryptoQuote,
  AllPricesResponse,
  HomeCategoryTabId,
} from "@/api/types";

const CATEGORY_TABS: { id: HomeCategoryTabId; label: string }[] = [
  { id: "all", label: "All assets" },
  { id: "perps", label: "Perps" },
  { id: "spot", label: "Spot" },
  { id: "memes", label: "Memes" },
  { id: "defi", label: "DeFi" },
  { id: "chains", label: "Chains" },
  { id: "commodities", label: "Commodities" },
  { id: "favorites", label: "Favorites" },
];

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

function formatFundingRate(n: number | null | undefined): string {
  if (n == null) return "--";
  return `${(n * 100).toFixed(4)}%`;
}

function filterByCategory(
  products: ProductInfo[],
  tab: HomeCategoryTabId,
  favorites: Set<string>,
): ProductInfo[] {
  if (tab === "all") return products;
  if (tab === "favorites") {
    return products.filter((p) => favorites.has(p.name.toUpperCase()));
  }
  if (tab === "spot") {
    return products.filter((p) => (p.category ?? "perps") === "spot");
  }
  return products.filter((p) => (p.category ?? "perps") === tab);
}

export default function Home() {
  const navigate = useNavigate();
  const { products, prices, setProducts, updatePrices, selectProduct } = useMarketStore();
  const { portfolio, setPortfolio } = useAccountStore();
  const { favorites, toggle, isFavorite } = useFavoritePerps();
  const [categoryTab, setCategoryTab] = useState<HomeCategoryTabId>("all");

  const { data: productsData, isPending: productsLoading } = useQuery({
    queryKey: ["products"],
    queryFn: () => api.get<ProductInfo[]>("/api/products"),
    staleTime: 60_000,
  });

  useEffect(() => {
    if (productsData) setProducts(productsData);
  }, [productsData, setProducts]);

  const { data: pricesData } = useQuery({
    queryKey: ["prices"],
    queryFn: () => api.get<AllPricesResponse>("/api/prices"),
    refetchInterval: 5000,
  });

  useEffect(() => {
    if (pricesData?.prices) updatePrices(pricesData.prices);
  }, [pricesData, updatePrices]);

  const { data: quotesData } = useQuery({
    queryKey: ["quotes"],
    queryFn: () => api.get<QuotesResponse>("/api/quotes"),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

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

  const filteredProducts = useMemo(
    () => filterByCategory(products, categoryTab, favorites),
    [products, categoryTab, favorites],
  );

  const openProduct = (name: string) => {
    hapticImpact("light");
    selectProduct(name);
    navigate(`/product/${name}`);
  };

  const emptySpot =
    categoryTab === "spot" &&
    !productsLoading &&
    products.every((p) => (p.category ?? "perps") !== "spot");

  const emptyFavorites =
    categoryTab === "favorites" && !productsLoading && filteredProducts.length === 0;

  return (
    <div className="flex-1 flex flex-col overflow-y-auto hide-scrollbar pb-2">
      <div className="px-4 pt-4 pb-2">
        <h1 className="text-xl font-bold text-white">Perpetuals</h1>
      </div>

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
              const isLong = pos.side?.toLowerCase() === "long";
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

      <div className="mb-2 pl-4">
        <div className="flex gap-2 overflow-x-auto hide-scrollbar pb-1 pr-4">
          {CATEGORY_TABS.map((tab) => {
            const active = categoryTab === tab.id;
            return (
              <button
                key={tab.id}
                type="button"
                onClick={() => {
                  hapticImpact("light");
                  setCategoryTab(tab.id);
                }}
                className={clsx(
                  "shrink-0 px-3 py-1.5 rounded-full text-xs font-medium transition-colors",
                  active
                    ? "bg-white/15 text-white"
                    : "bg-white/5 text-tg-hint active:bg-white/10",
                )}
              >
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 px-4">
        {productsLoading && products.length === 0 && (
          <>
            {Array.from({ length: 6 }).map((_, i) => (
              <div
                key={`sk-${i}`}
                className="bg-white/5 rounded-2xl p-4 h-[120px] animate-pulse"
              />
            ))}
          </>
        )}

        {!productsLoading && emptySpot && (
          <div className="col-span-2 text-center py-8 text-sm text-tg-hint">
            Spot markets are not available in Nadobro yet. Trade perps from the other tabs.
          </div>
        )}

        {!productsLoading && emptyFavorites && (
          <div className="col-span-2 text-center py-8 text-sm text-tg-hint">
            Tap the star on a market to add favorites.
          </div>
        )}

        {!productsLoading &&
          !emptySpot &&
          filteredProducts.map((p) => {
            const quote: CryptoQuote | undefined = quotes[p.name];
            const priceData = prices[p.name];
            const mid = priceData?.mid ?? quote?.price;
            const change = quote?.change_24h;
            const fav = isFavorite(p.name);

            return (
              <div
                key={p.name}
                className="bg-white/5 rounded-2xl p-4 text-left active:scale-[0.98] transition-transform cursor-pointer"
                onClick={() => openProduct(p.name)}
              >
                <div className="flex items-start justify-between gap-2 mb-3">
                  <div className="flex items-center gap-1.5 min-w-0 flex-1">
                    <button
                      type="button"
                      aria-label={fav ? "Remove from favorites" : "Add to favorites"}
                      className={clsx(
                        "shrink-0 text-lg leading-none px-0.5 rounded active:opacity-70",
                        fav ? "text-amber-400" : "text-tg-hint/70",
                      )}
                      onClick={(e) => {
                        e.stopPropagation();
                        hapticImpact("light");
                        toggle(p.name);
                      }}
                    >
                      {fav ? "★" : "☆"}
                    </button>
                    <AssetAvatar symbol={p.name} size={32} />
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-white truncate">{p.name}</div>
                      <div className="text-[10px] text-tg-hint truncate">{p.name}-PERP</div>
                    </div>
                  </div>
                  <span className="text-[10px] font-medium text-tg-hint bg-white/10 px-1.5 py-0.5 rounded shrink-0">
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
                      F: {formatFundingRate(quote.funding_rate)}
                    </span>
                  )}
                </div>
              </div>
            );
          })}
      </div>
    </div>
  );
}
