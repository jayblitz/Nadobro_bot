import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { clsx } from "clsx";
import { api } from "@/api/client";
import type { PortfolioSummary } from "@/api/types";
import { useAccountStore } from "@/store/account";

export default function Portfolio() {
  const setPortfolio = useAccountStore((s) => s.setPortfolio);

  const { data, isLoading, isError } = useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.get<PortfolioSummary>("/api/portfolio"),
    refetchInterval: 5_000,
  });

  // Sync to store outside of queryFn to avoid side effects during render.
  useEffect(() => {
    if (data) setPortfolio(data);
  }, [data, setPortfolio]);

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-tg-button border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="flex-1 flex items-center justify-center px-6">
        <div className="text-center">
          <p className="text-tg-text font-semibold mb-1">Failed to load portfolio</p>
          <p className="text-tg-hint text-sm">Check your connection and try again.</p>
        </div>
      </div>
    );
  }

  const pnlColor =
    data.total_unrealized_pnl > 0
      ? "text-long"
      : data.total_unrealized_pnl < 0
        ? "text-short"
        : "text-tg-hint";

  return (
    <div className="flex-1 overflow-y-auto hide-scrollbar px-4 pt-4 pb-4">
      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-3 mb-6">
        <div className="bg-white/5 rounded-xl p-4">
          <div className="text-[11px] text-tg-hint mb-1">Equity</div>
          <div className="text-xl font-bold text-white">
            ${data.equity.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
        </div>
        <div className="bg-white/5 rounded-xl p-4">
          <div className="text-[11px] text-tg-hint mb-1">Unrealized PnL</div>
          <div className={clsx("text-xl font-bold", pnlColor)}>
            {data.total_unrealized_pnl >= 0 ? "+" : ""}$
            {Math.abs(data.total_unrealized_pnl).toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
        </div>
        <div className="bg-white/5 rounded-xl p-4">
          <div className="text-[11px] text-tg-hint mb-1">Available</div>
          <div className="text-lg font-semibold text-white">
            ${data.available_balance.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
        </div>
        <div className="bg-white/5 rounded-xl p-4">
          <div className="text-[11px] text-tg-hint mb-1">Margin Used</div>
          <div className="text-lg font-semibold text-white">
            ${data.total_margin_used.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
        </div>
      </div>

      {/* Positions */}
      <h2 className="text-sm font-semibold text-tg-hint mb-3">
        Open Positions ({data.positions.length})
      </h2>

      {data.positions.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16">
          <div className="w-16 h-16 rounded-full bg-white/5 flex items-center justify-center mb-3">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} className="w-8 h-8 text-tg-hint">
              <path d="M3 17l6-6 4 4 8-8" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <p className="text-tg-hint text-sm">No open positions</p>
          <p className="text-tg-hint/50 text-xs mt-1">Open a trade to get started</p>
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {data.positions.map((pos, idx) => {
            const pnl = pos.unrealized_pnl ?? 0;
            const posColor = pnl > 0 ? "text-long" : pnl < 0 ? "text-short" : "text-tg-hint";
            return (
              <div
                key={`${pos.product_id}-${pos.side}-${idx}`}
                className="bg-white/5 rounded-xl p-4"
              >
                <div className="flex justify-between items-start mb-2">
                  <div>
                    <span className="font-semibold text-white">{pos.product_name}</span>
                    <span
                      className={clsx(
                        "ml-2 text-xs font-medium px-1.5 py-0.5 rounded",
                        pos.side === "long" ? "bg-long/20 text-long" : "bg-short/20 text-short",
                      )}
                    >
                      {pos.side.toUpperCase()}
                    </span>
                  </div>
                  <div className={clsx("font-semibold", posColor)}>
                    {pnl >= 0 ? "+" : ""}${Math.abs(pnl).toFixed(2)}
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-2 text-xs text-tg-hint">
                  <div>
                    <div className="mb-0.5">Size</div>
                    <div className="text-white">{pos.size.toFixed(4)}</div>
                  </div>
                  <div>
                    <div className="mb-0.5">Entry</div>
                    <div className="text-white">${pos.entry_price.toLocaleString()}</div>
                  </div>
                  <div>
                    <div className="mb-0.5">Leverage</div>
                    <div className="text-white">{pos.leverage ?? "-"}x</div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
