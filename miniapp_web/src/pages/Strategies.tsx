import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { clsx } from "clsx";
import { api, ApiError } from "@/api/client";
import type { ProductInfo, StrategyActionResponse, StrategyBotStatus } from "@/api/types";
import { hapticImpact } from "@/lib/haptics";

type StrategyId = "grid" | "rgrid" | "dn" | "vol" | "bro";

interface StrategyCard {
  type: StrategyId;
  name: string;
  description: string;
  color: string;
}

const STRATEGIES: StrategyCard[] = [
  {
    type: "grid",
    name: "Grid Bot",
    description: "Symmetric maker grid around mid price. Profits from mean reversion.",
    color: "from-blue-500/20 to-transparent",
  },
  {
    type: "rgrid",
    name: "Reverse Grid",
    description: "Reverse-grid quoting with exposure anchoring and PnL controls.",
    color: "from-purple-500/20 to-transparent",
  },
  {
    type: "dn",
    name: "Delta Neutral",
    description: "Funding rate farming — spot long + perp short hedge.",
    color: "from-teal-500/20 to-transparent",
  },
  {
    type: "vol",
    name: "Volume Bot",
    description: "Alternating long/short flips to hit a target volume.",
    color: "from-orange-500/20 to-transparent",
  },
  {
    type: "bro",
    name: "Bro Mode",
    description: "AI-driven autonomous trading powered by Grok-3.",
    color: "from-pink-500/20 to-transparent",
  },
];

export default function Strategies() {
  const qc = useQueryClient();
  const [selected, setSelected] = useState<StrategyId>("grid");
  const [product, setProduct] = useState("BTC");
  const [leverage, setLeverage] = useState(3);
  const [slippage, setSlippage] = useState(1);
  const [formError, setFormError] = useState<string | null>(null);

  const { data: products } = useQuery({
    queryKey: ["products"],
    queryFn: () => api.get<ProductInfo[]>("/api/products"),
    staleTime: 60_000,
  });

  const perpNames = useMemo(() => {
    const list = products?.filter((p) => p.type === "perp").map((p) => p.name) ?? [];
    return list.length ? list : ["BTC", "ETH", "SOL"];
  }, [products]);

  useEffect(() => {
    if (perpNames.length && !perpNames.includes(product)) {
      setProduct(perpNames[0]!);
    }
  }, [perpNames, product]);

  const {
    data: status,
    isLoading: statusLoading,
    error: statusError,
    refetch: refetchStatus,
  } = useQuery({
    queryKey: ["strategies", "status"],
    queryFn: () => api.get<StrategyBotStatus>("/api/strategies/status"),
    refetchInterval: 15_000,
  });

  const startMutation = useMutation({
    mutationFn: () =>
      api.post<StrategyActionResponse>("/api/strategies/start", {
        strategy: selected,
        product: selected === "bro" ? "MULTI" : product,
        leverage,
        slippage_pct: slippage,
      }),
    onSuccess: () => {
      setFormError(null);
      qc.invalidateQueries({ queryKey: ["strategies", "status"] });
      qc.invalidateQueries({ queryKey: ["portfolio"] });
    },
    onError: (e: unknown) => {
      const msg =
        e instanceof ApiError && e.body && typeof e.body === "object" && "detail" in e.body
          ? String((e.body as { detail: string }).detail)
          : e instanceof Error
            ? e.message
            : "Start failed";
      setFormError(msg);
    },
  });

  const stopMutation = useMutation({
    mutationFn: () => api.post<StrategyActionResponse>("/api/strategies/stop"),
    onSuccess: () => {
      setFormError(null);
      qc.invalidateQueries({ queryKey: ["strategies", "status"] });
      qc.invalidateQueries({ queryKey: ["portfolio"] });
    },
    onError: (e: unknown) => {
      const msg =
        e instanceof ApiError && e.body && typeof e.body === "object" && "detail" in e.body
          ? String((e.body as { detail: string }).detail)
          : e instanceof Error
            ? e.message
            : "Stop failed";
      setFormError(msg);
    },
  });

  const running = status?.running === true;
  const activeStrategy = status?.strategy as string | undefined;
  const activeProduct = status?.product as string | undefined;

  const maxLev = useMemo(() => {
    const p = products?.find((x) => x.name === product);
    return p?.max_leverage ?? 50;
  }, [products, product]);

  return (
    <div className="flex-1 overflow-y-auto hide-scrollbar px-4 pt-4 pb-4">
      <h1 className="text-lg font-bold text-white mb-1">Strategy Lab</h1>
      <p className="text-xs text-tg-hint mb-4">
        Start and monitor the same automated strategies as in Telegram.
      </p>

      {statusLoading && (
        <p className="text-xs text-tg-hint mb-3">Loading strategy status…</p>
      )}
      {statusError && (
        <p className="text-xs text-red-400 mb-3">
          Could not load status. Check your connection and wallet setup.
        </p>
      )}

      {status && !statusLoading && (
        <div
          className={clsx(
            "rounded-xl p-3 mb-4 border",
            running ? "border-emerald-500/40 bg-emerald-500/10" : "border-white/10 bg-white/5",
          )}
        >
          <div className="text-[11px] uppercase tracking-wide text-tg-hint mb-1">Runtime</div>
          <div className="text-sm text-white font-medium">
            {running ? "Running" : "Stopped"}
            {running && activeStrategy && (
              <span className="text-tg-hint font-normal">
                {" "}
                · {activeStrategy}
                {activeProduct && activeProduct !== "MULTI" ? ` · ${activeProduct}` : ""}
              </span>
            )}
          </div>
          {status.global_pause_active && (
            <p className="text-xs text-amber-300 mt-2">Global trading pause is active.</p>
          )}
          {status.last_error && (
            <p className="text-xs text-red-300 mt-2 break-words">Last error: {String(status.last_error)}</p>
          )}
          <div className="flex gap-2 mt-3">
            <button
              type="button"
              disabled={!running || stopMutation.isPending}
              onClick={() => {
                hapticImpact("medium");
                stopMutation.mutate();
              }}
              className="flex-1 py-2 rounded-lg bg-red-500/20 text-red-200 text-sm font-medium disabled:opacity-40"
            >
              {stopMutation.isPending ? "Stopping…" : "Stop bot"}
            </button>
            <button
              type="button"
              onClick={() => {
                hapticImpact("light");
                refetchStatus();
              }}
              className="px-3 py-2 rounded-lg bg-white/10 text-white text-sm"
            >
              Refresh
            </button>
          </div>
        </div>
      )}

      {formError && (
        <div className="rounded-lg bg-red-500/15 border border-red-500/30 text-red-200 text-xs p-3 mb-4">
          {formError}
        </div>
      )}

      <div className="rounded-xl border border-white/10 bg-white/5 p-3 mb-4 space-y-3">
        <div>
          <label className="text-[11px] text-tg-hint block mb-1">Strategy</label>
          <select
            value={selected}
            onChange={(e) => {
              setSelected(e.target.value as StrategyId);
              hapticImpact("light");
            }}
            className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-sm text-white"
          >
            {STRATEGIES.map((s) => (
              <option key={s.type} value={s.type}>
                {s.name}
              </option>
            ))}
          </select>
        </div>

        {selected !== "bro" && (
          <div>
            <label className="text-[11px] text-tg-hint block mb-1">Product</label>
            <select
              value={product}
              onChange={(e) => {
                setProduct(e.target.value);
                hapticImpact("light");
              }}
              className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-sm text-white"
            >
              {perpNames.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>
        )}

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-[11px] text-tg-hint block mb-1">Leverage</label>
            <input
              type="number"
              min={1}
              max={maxLev}
              value={leverage}
              onChange={(e) => setLeverage(Number(e.target.value))}
              className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-sm text-white"
            />
          </div>
          <div>
            <label className="text-[11px] text-tg-hint block mb-1">Slippage %</label>
            <input
              type="number"
              min={0}
              max={100}
              step={0.1}
              value={slippage}
              onChange={(e) => setSlippage(Number(e.target.value))}
              className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-sm text-white"
            />
          </div>
        </div>

        <button
          type="button"
          disabled={running || startMutation.isPending}
          onClick={() => {
            hapticImpact("medium");
            startMutation.mutate();
          }}
          className="w-full py-2.5 rounded-lg bg-tg-button text-tg-button-text text-sm font-semibold disabled:opacity-40"
        >
          {startMutation.isPending ? "Starting…" : "Start strategy"}
        </button>
      </div>

      <div className="flex flex-col gap-3">
        {STRATEGIES.map((s) => (
          <div
            key={s.type}
            className={clsx(
              "w-full text-left rounded-xl p-4 bg-gradient-to-r",
              s.color,
              "bg-white/5",
              selected === s.type && "ring-1 ring-tg-button/50",
            )}
          >
            <div className="font-semibold text-white mb-1">{s.name}</div>
            <div className="text-xs text-tg-hint leading-relaxed">{s.description}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
