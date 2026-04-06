import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { clsx } from "clsx";
import { api } from "@/api/client";
import type { PortfolioSummary } from "@/api/types";
import { useAccountStore } from "@/store/account";
import { useAuthStore } from "@/store/auth";

type OverviewTab = "overview" | "margin" | "history";
type AccountSubTab = "account" | "pnl" | "volume";
type Timeframe = "24h" | "7d" | "30d";

function fmtUsd(n: number, digits = 2) {
  return `$${n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits })}`;
}

/** Decorative gradient area (no historical equity API yet). */
function EquitySparkline() {
  return (
    <div className="relative h-36 w-full rounded-xl overflow-hidden bg-[#0a0a0a] border border-white/[0.06]">
      <div
        className="absolute inset-0 opacity-30"
        style={{
          backgroundImage: "radial-gradient(circle, rgba(255,255,255,0.06) 1px, transparent 1px)",
          backgroundSize: "12px 12px",
        }}
      />
      <svg className="absolute inset-0 w-full h-full" preserveAspectRatio="none" viewBox="0 0 400 120">
        <defs>
          <linearGradient id="eqFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#22c55e" stopOpacity="0.45" />
            <stop offset="100%" stopColor="#22c55e" stopOpacity="0" />
          </linearGradient>
        </defs>
        <path
          d="M0,80 Q100,70 200,55 T400,50 L400,120 L0,120 Z"
          fill="url(#eqFill)"
        />
        <path
          d="M0,80 Q100,70 200,55 T400,50"
          fill="none"
          stroke="#22c55e"
          strokeWidth="2"
          strokeOpacity="0.9"
        />
      </svg>
      <p className="absolute bottom-2 left-3 text-[10px] text-white/35">
        Illustrative — live equity history coming soon
      </p>
    </div>
  );
}

export default function Portfolio() {
  const setPortfolio = useAccountStore((s) => s.setPortfolio);
  const user = useAuthStore((s) => s.user);
  const [overviewTab, setOverviewTab] = useState<OverviewTab>("overview");
  const [accountTab, setAccountTab] = useState<AccountSubTab>("account");
  const [timeframe, setTimeframe] = useState<Timeframe>("24h");

  const { data, isLoading, isError } = useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.get<PortfolioSummary>("/api/portfolio"),
    refetchInterval: 5_000,
  });

  useEffect(() => {
    if (data) setPortfolio(data);
  }, [data, setPortfolio]);

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center bg-black">
        <div className="w-8 h-8 border-2 border-emerald-500/80 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="flex-1 flex items-center justify-center px-6 bg-black">
        <div className="text-center">
          <p className="text-white font-semibold mb-1">Failed to load portfolio</p>
          <p className="text-white/45 text-sm">Check your connection and try again.</p>
        </div>
      </div>
    );
  }

  const pnlColor =
    data.total_unrealized_pnl > 0
      ? "text-emerald-400"
      : data.total_unrealized_pnl < 0
        ? "text-red-400"
        : "text-white/45";

  const spotPnlColor =
    data.unrealized_spot_pnl > 0
      ? "text-emerald-400"
      : data.unrealized_spot_pnl < 0
        ? "text-red-400"
        : "text-white/45";

  const util = data.margin_utilization ?? 0;
  const utilPct = Math.round(util * 1000) / 10;

  return (
    <div className="flex-1 overflow-y-auto hide-scrollbar bg-black text-white">
      {/* Header */}
      <div className="px-4 pt-4 pb-2 border-b border-white/[0.06]">
        <h1 className="text-lg font-semibold tracking-tight">Portfolio</h1>
        <p className="text-[11px] text-white/45 mt-0.5">
          {user?.username ? `${user.username} · ` : ""}
          Cross margin overview
        </p>
      </div>

      {/* Overview tabs */}
      <div className="flex gap-1 px-4 pt-3 pb-2">
        {(
          [
            ["overview", "Overview"],
            ["margin", "Margin"],
            ["history", "History"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => setOverviewTab(id)}
            className={clsx(
              "px-3 py-1.5 rounded-lg text-xs font-medium transition-colors",
              overviewTab === id ? "bg-white/10 text-white" : "text-white/40",
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {overviewTab === "overview" && (
        <>
          {/* Key metric cards — Nado-style three column */}
          <div className="grid grid-cols-1 gap-2 px-4 sm:grid-cols-3">
            <div className="rounded-xl bg-white/[0.04] border border-white/[0.06] p-4">
              <div className="text-[11px] text-white/45 uppercase tracking-wide">Total equity</div>
              <div className="text-2xl font-semibold tabular-nums mt-1">{fmtUsd(data.equity)}</div>
              <div className="text-[11px] text-white/35 mt-1">24h PnL —</div>
            </div>
            <div className="rounded-xl bg-white/[0.04] border border-white/[0.06] p-4">
              <div className="text-[11px] text-white/45 uppercase tracking-wide">Volume</div>
              <div className="text-xl font-semibold tabular-nums mt-1">
                {fmtUsd(data.total_volume_usd, 0)}
              </div>
              <div className="text-[11px] text-white/35 mt-1">
                Fee tier {data.fee_tier_display}
              </div>
            </div>
            <div className="rounded-xl bg-white/[0.04] border border-white/[0.06] p-4">
              <div className="text-[11px] text-white/45 uppercase tracking-wide">NLP balance</div>
              <div className="text-xl font-semibold tabular-nums mt-1">
                {fmtUsd(data.nlp_balance_usd)}
              </div>
              <div className="text-[11px] text-white/35 mt-1">APR —</div>
            </div>
          </div>

          {/* Account / PnL / Volume + timeframe */}
          <div className="flex flex-wrap items-center justify-between gap-2 px-4 pt-5 pb-2">
            <div className="flex gap-1">
              {(
                [
                  ["account", "Account"],
                  ["pnl", "PnL"],
                  ["volume", "Volume"],
                ] as const
              ).map(([id, label]) => (
                <button
                  key={id}
                  type="button"
                  onClick={() => setAccountTab(id)}
                  className={clsx(
                    "px-3 py-1 rounded-lg text-xs font-medium",
                    accountTab === id ? "bg-white/10 text-white" : "text-white/40",
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
            <select
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value as Timeframe)}
              className="bg-white/5 border border-white/10 rounded-lg text-[11px] text-white/70 px-2 py-1 outline-none"
            >
              <option value="24h">24h</option>
              <option value="7d">7d</option>
              <option value="30d">30d</option>
            </select>
          </div>

          <div className="px-4 pb-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Left: stats */}
            <div className="space-y-0 rounded-xl border border-white/[0.06] overflow-hidden bg-white/[0.02]">
              <div className="flex justify-between px-4 py-3 border-b border-white/[0.06]">
                <span className="text-[13px] text-white/50">Balance</span>
                <span className="text-sm font-medium tabular-nums">{fmtUsd(data.balance_usd)}</span>
              </div>
              <div className="flex justify-between px-4 py-3 border-b border-white/[0.06]">
                <span className="text-[13px] text-white/50">Unrealized perp PnL</span>
                <span className={clsx("text-sm font-medium tabular-nums", pnlColor)}>
                  {data.total_unrealized_pnl >= 0 ? "+" : ""}
                  {fmtUsd(data.total_unrealized_pnl)}
                </span>
              </div>
              <div className="flex justify-between px-4 py-3 border-b border-white/[0.06]">
                <span className="text-[13px] text-white/50">Unrealized spot PnL</span>
                <span className={clsx("text-sm font-medium tabular-nums", spotPnlColor)}>
                  {data.unrealized_spot_pnl >= 0 ? "+" : ""}
                  {fmtUsd(data.unrealized_spot_pnl)}
                </span>
              </div>
              <div className="flex justify-between px-4 py-3 border-b border-white/[0.06]">
                <span className="text-[13px] text-white/50">Available margin</span>
                <span className="text-sm font-medium tabular-nums">{fmtUsd(data.available_balance)}</span>
              </div>
              <div className="px-4 py-3">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-[13px] text-white/50">Margin utilization</span>
                  <span
                    className={clsx(
                      "text-xs font-medium tabular-nums",
                      utilPct >= 80 ? "text-red-400" : utilPct >= 50 ? "text-amber-300" : "text-emerald-400",
                    )}
                  >
                    {utilPct.toFixed(1)}%
                  </span>
                </div>
                <div className="h-2 rounded-full bg-white/[0.08] overflow-hidden">
                  <div
                    className="h-full rounded-full bg-gradient-to-r from-emerald-500 via-amber-400 to-red-500"
                    style={{ width: `${Math.min(100, utilPct)}%` }}
                  />
                </div>
                <p className="text-[10px] text-white/30 mt-2">
                  Est. from open positions and recorded leverage. Open orders: {data.open_orders_count}
                </p>
              </div>
            </div>

            {/* Right: chart placeholder */}
            <div className="flex flex-col gap-2">
              <EquitySparkline />
              <p className="text-[10px] text-white/35 text-center">
                Timeframe {timeframe} · {accountTab === "account" ? "Account snapshot" : "Chart preview"}
              </p>
            </div>
          </div>
        </>
      )}

      {overviewTab === "margin" && (
        <div className="px-4 py-4 space-y-3">
          <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="text-[11px] text-white/45 uppercase">Margin used (est.)</div>
            <div className="text-2xl font-semibold mt-1 tabular-nums">{fmtUsd(data.total_margin_used)}</div>
          </div>
          <p className="text-xs text-white/40 leading-relaxed">
            Margin is estimated from position notionals and leverage stored when your opening order was recorded
            in NadoBro. If you opened on another interface, leverage may show as an estimate.
          </p>
        </div>
      )}

      {overviewTab === "history" && (
        <div className="px-4 py-8 text-center text-sm text-white/45">
          Use the bot or future mini-app history for full fills and funding — this tab is reserved for a
          detailed ledger.
        </div>
      )}

      {/* Positions list */}
      <div className="px-4 pb-8 pt-2 border-t border-white/[0.06]">
        <h2 className="text-xs font-semibold text-white/45 uppercase tracking-wide mb-3">
          Open positions ({data.positions.length})
        </h2>

        {data.positions.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-14 rounded-xl border border-dashed border-white/10">
            <p className="text-white/45 text-sm">No open positions</p>
            <p className="text-white/25 text-xs mt-1">Open a trade to get started</p>
          </div>
        ) : (
          <div className="flex flex-col gap-2">
            {data.positions.map((pos, idx) => {
              const pnl = pos.unrealized_pnl;
              const posColor =
                pnl == null
                  ? "text-white/45"
                  : pnl > 0
                    ? "text-emerald-400"
                    : pnl < 0
                      ? "text-red-400"
                      : "text-white/45";
              return (
                <div
                  key={`${pos.product_id}-${pos.side}-${idx}`}
                  className="rounded-xl border border-white/[0.06] bg-white/[0.03] p-4"
                >
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <span className="font-semibold text-white">{pos.product_name}</span>
                      <span
                        className={clsx(
                          "ml-2 text-xs font-medium px-1.5 py-0.5 rounded",
                          pos.side === "long" ? "bg-emerald-500/15 text-emerald-400" : "bg-red-500/15 text-red-400",
                        )}
                      >
                        {pos.side.toUpperCase()}
                      </span>
                    </div>
                    <div className={clsx("font-semibold tabular-nums text-sm", posColor)}>
                      {pnl == null
                        ? "—"
                        : `${pnl >= 0 ? "+" : ""}$${Math.abs(pnl).toLocaleString(undefined, { maximumFractionDigits: 2 })}`}
                    </div>
                  </div>
                  <div className="grid grid-cols-3 gap-2 text-xs text-white/45">
                    <div>
                      <div className="mb-0.5">Size</div>
                      <div className="text-white tabular-nums">{pos.size.toFixed(4)}</div>
                    </div>
                    <div>
                      <div className="mb-0.5">Entry</div>
                      <div className="text-white tabular-nums">${pos.entry_price.toLocaleString()}</div>
                    </div>
                    <div>
                      <div className="mb-0.5">Mark</div>
                      <div className="text-white tabular-nums">
                        {pos.mark_price != null ? `$${pos.mark_price.toLocaleString()}` : "—"}
                      </div>
                    </div>
                    <div>
                      <div className="mb-0.5">Leverage</div>
                      <div className="text-white">{pos.leverage != null ? `${pos.leverage}x` : "—"}</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
