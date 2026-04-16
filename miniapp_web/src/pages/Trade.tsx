import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { clsx } from "clsx";
import { api } from "@/api/client";
import { hapticImpact, hapticSuccess, hapticError } from "@/lib/haptics";
import type { OrderSide, ProductInfo, TradeResponse } from "@/api/types";
import { useMarketStore } from "@/store/market";
import { Link } from "react-router-dom";
import TpSlFields, { type TpSlInputMode } from "@/components/trade/TpSlFields";
import { formatPrice, formatTradeFillSummary, formatUsdFixed } from "@/lib/format";

export default function Trade() {
  const { selectedProduct, selectProduct, prices } = useMarketStore();
  const [side, setSide] = useState<OrderSide>("long");
  const [sizeUsd, setSizeUsd] = useState("");
  const [leverage, setLeverage] = useState(5);
  const [orderType, setOrderType] = useState<"market" | "limit">("market");
  const [limitPrice, setLimitPrice] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [lastResult, setLastResult] = useState<TradeResponse | null>(null);
  const [tpSlMode, setTpSlMode] = useState<TpSlInputMode>("pct");
  const [tpPct, setTpPct] = useState("");
  const [slPct, setSlPct] = useState("");
  const [tpPrice, setTpPrice] = useState("");
  const [slPrice, setSlPrice] = useState("");
  const dismissTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    if (lastResult) {
      clearTimeout(dismissTimer.current);
      dismissTimer.current = setTimeout(() => setLastResult(null), 4000);
    }
    return () => clearTimeout(dismissTimer.current);
  }, [lastResult]);

  const { data: products, isError: productsError } = useQuery({
    queryKey: ["products"],
    queryFn: () => api.get<ProductInfo[]>("/api/products"),
  });

  const currentPrice = prices[selectedProduct]?.mid;
  const selectedInfo = products?.find((p) => p.name === selectedProduct);
  const maxLev = selectedInfo?.max_leverage ?? 20;

  const submit = async () => {
    if (!sizeUsd || Number(sizeUsd) <= 0) return;
    setSubmitting(true);
    hapticImpact("medium");

    try {
      const path = orderType === "market" ? "/api/trade/market" : "/api/trade/limit";
      const body: Record<string, unknown> = {
        product: selectedProduct,
        side,
        size_usd: Number(sizeUsd),
        leverage,
      };
      if (orderType === "limit" && limitPrice) {
        body.price = Number(limitPrice);
      }
      if (orderType === "market") {
        if (tpSlMode === "pct") {
          if (tpPct.trim()) body.take_profit_pct = Number(tpPct);
          if (slPct.trim()) body.stop_loss_pct = Number(slPct);
        } else {
          if (tpPrice.trim()) body.take_profit_price = Number(tpPrice);
          if (slPrice.trim()) body.stop_loss_price = Number(slPrice);
        }
      }
      const res = await api.post<TradeResponse>(path, body);
      setLastResult(res);
      if (res.ok) {
        hapticSuccess();
        setSizeUsd("");
        setTpPct("");
        setSlPct("");
        setTpPrice("");
        setSlPrice("");
      } else {
        hapticError();
      }
    } catch {
      hapticError();
      setLastResult({ ok: false, error: "Network error" });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="flex-1 flex flex-col overflow-y-auto hide-scrollbar">
      <div className="flex gap-2 px-4 py-3 overflow-x-auto hide-scrollbar">
        {(products ?? []).map((p) => (
          <button
            key={p.name}
            onClick={() => selectProduct(p.name)}
            className={clsx(
              "px-3 py-1.5 rounded-full text-xs font-medium whitespace-nowrap transition-colors",
              p.name === selectedProduct
                ? "bg-tg-button text-tg-button-text"
                : "bg-white/5 text-tg-hint",
            )}
          >
            {p.name}
          </button>
        ))}
      </div>

      <div className="px-4 pb-3">
        <div className="text-3xl font-bold text-white tabular-nums">
          {formatPrice(currentPrice, selectedProduct)}
        </div>
        <div className="text-xs text-tg-hint mt-0.5">{selectedProduct}-PERP</div>
      </div>

      <Link
        to={`/product/${selectedProduct}`}
        className="mx-4 h-48 rounded-xl bg-gradient-to-b from-white/[0.07] to-white/[0.02] border border-nb-cyan/20 flex flex-col items-center justify-center mb-4 active:scale-[0.99] transition-transform"
      >
        <span className="text-nb-cyan text-sm font-semibold">Live chart & depth</span>
        <span className="text-tg-hint text-xs mt-1">Tap for full screen trading</span>
      </Link>

      {productsError && (
        <div className="mx-4 mb-3 px-4 py-2 rounded-xl bg-short/10 text-short text-sm">
          Failed to load products. Check your connection.
        </div>
      )}

      <div className="px-4 flex-1 flex flex-col gap-3">
        <div className="flex rounded-xl overflow-hidden bg-white/5">
          {(["long", "short"] as const).map((s) => (
            <button
              key={s}
              onClick={() => setSide(s)}
              className={clsx(
                "flex-1 py-3 text-sm font-semibold transition-colors",
                side === s
                  ? s === "long"
                    ? "bg-long text-white"
                    : "bg-short text-white"
                  : "text-tg-hint",
              )}
            >
              {s === "long" ? "Long" : "Short"}
            </button>
          ))}
        </div>

        <div className="flex gap-2">
          {(["market", "limit"] as const).map((t) => (
            <button
              key={t}
              onClick={() => setOrderType(t)}
              className={clsx(
                "px-4 py-1.5 rounded-lg text-xs font-medium transition-colors",
                orderType === t ? "bg-white/10 text-white" : "text-tg-hint",
              )}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>

        <div className="bg-white/5 rounded-xl px-4 py-3">
          <label className="text-[11px] text-tg-hint block mb-1">Size (USD)</label>
          <input
            type="number"
            inputMode="decimal"
            placeholder="0.00"
            min="1"
            step="any"
            value={sizeUsd}
            onChange={(e) => {
              const v = e.target.value;
              if (v === "" || Number(v) >= 0) setSizeUsd(v);
            }}
            className="w-full bg-transparent text-white text-lg font-medium outline-none placeholder:text-white/20"
          />
        </div>

        {orderType === "limit" && (
          <div className="bg-white/5 rounded-xl px-4 py-3">
            <label className="text-[11px] text-tg-hint block mb-1">Limit Price</label>
            <input
              type="number"
              inputMode="decimal"
              placeholder="0.00"
              value={limitPrice}
              onChange={(e) => setLimitPrice(e.target.value)}
              className="w-full bg-transparent text-white text-lg font-medium outline-none placeholder:text-white/20"
            />
          </div>
        )}

        <div className="bg-white/5 rounded-xl px-4 py-3">
          <div className="flex justify-between mb-2">
            <span className="text-[11px] text-tg-hint">Leverage</span>
            <span className="text-sm font-bold text-white">{leverage}x</span>
          </div>
          <input
            type="range"
            min={1}
            max={maxLev}
            step={1}
            value={leverage}
            onChange={(e) => setLeverage(Number(e.target.value))}
            className="w-full accent-tg-button"
          />
          <div className="flex justify-between mt-1">
            <span className="text-[10px] text-tg-hint">1x</span>
            <span className="text-[10px] text-tg-hint">{maxLev}x</span>
          </div>
        </div>

        {orderType === "market" && (
          <TpSlFields
            mode={tpSlMode}
            onModeChange={setTpSlMode}
            tpPct={tpPct}
            slPct={slPct}
            tpPrice={tpPrice}
            slPrice={slPrice}
            onTpPct={setTpPct}
            onSlPct={setSlPct}
            onTpPrice={setTpPrice}
            onSlPrice={setSlPrice}
          />
        )}

        {sizeUsd && Number(sizeUsd) > 0 && (
          <div className="flex justify-between text-xs text-tg-hint px-1">
            <span>Est. margin</span>
            <span className="text-white">
              {formatUsdFixed(Number(sizeUsd) / leverage, 2)}
            </span>
          </div>
        )}

        {lastResult && (
          <div
            className={clsx(
              "rounded-xl px-4 py-2 text-sm",
              lastResult.ok ? "bg-long/20 text-long" : "bg-short/20 text-short",
            )}
          >
            {lastResult.ok ? formatTradeFillSummary(lastResult) : lastResult.error}
          </div>
        )}
      </div>

      <div className="px-4 pb-4 pt-3">
        <button
          onClick={submit}
          disabled={submitting || !sizeUsd || Number(sizeUsd) <= 0}
          className={clsx(
            "w-full py-4 rounded-2xl font-semibold text-[16px] transition-all active:scale-[0.98]",
            side === "long" ? "bg-long" : "bg-short",
            "text-white disabled:opacity-40",
          )}
        >
          {submitting
            ? "Placing..."
            : `${side === "long" ? "Long" : "Short"} ${selectedProduct}`}
        </button>
      </div>
    </div>
  );
}
