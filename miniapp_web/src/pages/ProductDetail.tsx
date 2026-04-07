import { useState, useEffect, useRef, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { clsx } from "clsx";
import { createChart, type IChartApi, type ISeriesApi, type UTCTimestamp } from "lightweight-charts";
import { api } from "@/api/client";
import { hapticImpact, hapticSuccess, hapticError } from "@/lib/haptics";
import { useMarketStore } from "@/store/market";
import type {
  CandleResponse,
  PriceResponse,
  ProductInfo,
  TradeResponse,
  QuotesResponse,
} from "@/api/types";
import TpSlFields, { type TpSlInputMode } from "@/components/trade/TpSlFields";
import AssetAvatar from "@/components/common/AssetAvatar";

const INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"] as const;
type Interval = (typeof INTERVALS)[number];

function formatPrice(n: number | null | undefined, product?: string): string {
  if (n == null) return "--";
  const decimals = product === "DOGE" || product === "XRP" ? 4 : 2;
  return n >= 1000
    ? `$${n.toLocaleString(undefined, { maximumFractionDigits: decimals })}`
    : `$${n.toFixed(decimals)}`;
}

export default function ProductDetail() {
  const { product } = useParams<{ product: string }>();
  const navigate = useNavigate();
  const { prices, products } = useMarketStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  const [interval, setInterval_] = useState<Interval>("1h");
  const [side, setSide] = useState<"long" | "short">("long");
  const [orderType, setOrderType] = useState<"market" | "limit">("market");
  const [sizeUsd, setSizeUsd] = useState("");
  const [limitPrice, setLimitPrice] = useState("");
  const [leverage, setLeverage] = useState(5);
  const [submitting, setSubmitting] = useState(false);
  const [lastResult, setLastResult] = useState<TradeResponse | null>(null);
  const [showOrderForm, setShowOrderForm] = useState(false);
  const [tpSlMode, setTpSlMode] = useState<TpSlInputMode>("pct");
  const [tpPct, setTpPct] = useState("");
  const [slPct, setSlPct] = useState("");
  const [tpPrice, setTpPrice] = useState("");
  const [slPrice, setSlPrice] = useState("");
  const dismissTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const productName = product?.toUpperCase() ?? "BTC";
  const priceData: PriceResponse | undefined = prices[productName];
  const currentPrice = priceData?.mid;
  const productInfo: ProductInfo | undefined = products.find(
    (p) => p.name === productName,
  );
  const maxLev = productInfo?.max_leverage ?? 20;

  // 24h change
  const { data: quotesData } = useQuery({
    queryKey: ["quotes"],
    queryFn: () => api.get<QuotesResponse>("/api/quotes"),
    staleTime: 60_000,
  });
  const quoteInfo = quotesData?.quotes?.[productName];
  const change24h = quoteInfo?.change_24h;
  const fundingRate = quoteInfo?.funding_rate;

  // Auto-dismiss trade result
  useEffect(() => {
    if (lastResult) {
      clearTimeout(dismissTimer.current);
      dismissTimer.current = setTimeout(() => setLastResult(null), 4000);
    }
    return () => clearTimeout(dismissTimer.current);
  }, [lastResult]);

  // Fetch candles
  const { data: candleData } = useQuery({
    queryKey: ["candles", productName, interval],
    queryFn: () =>
      api.get<CandleResponse>(
        `/api/products/${productName}/candles?interval=${interval}&limit=300`,
      ),
    staleTime: interval === "1m" ? 15_000 : 60_000,
    refetchInterval: interval === "1m" ? 30_000 : 120_000,
  });

  // Create chart
  useEffect(() => {
    if (!chartContainerRef.current) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
    }

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 280,
      layout: {
        background: { color: "transparent" },
        textColor: "#708499",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      crosshair: {
        mode: 0,
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.1)",
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.1)",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    const series = chart.addCandlestickSeries({
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderUpColor: "#22c55e",
      borderDownColor: "#ef4444",
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });

    chartRef.current = chart;
    candleSeriesRef.current = series;

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };
    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
    };
  }, [productName]);

  // Update candle data
  useEffect(() => {
    if (candleData?.candles && candleSeriesRef.current) {
      candleSeriesRef.current.setData(
        candleData.candles.map((c) => ({
          ...c,
          time: c.time as UTCTimestamp,
        })),
      );
      chartRef.current?.timeScale().fitContent();
    }
  }, [candleData]);

  // Submit order
  const submit = useCallback(async () => {
    if (!sizeUsd || Number(sizeUsd) <= 0) return;
    setSubmitting(true);
    hapticImpact("medium");

    try {
      const path =
        orderType === "market" ? "/api/trade/market" : "/api/trade/limit";
      const body: Record<string, unknown> = {
        product: productName,
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
        setShowOrderForm(false);
      } else {
        hapticError();
      }
    } catch {
      hapticError();
      setLastResult({ ok: false, error: "Network error" });
    } finally {
      setSubmitting(false);
    }
  }, [
    sizeUsd,
    orderType,
    productName,
    side,
    leverage,
    limitPrice,
    tpSlMode,
    tpPct,
    slPct,
    tpPrice,
    slPrice,
  ]);

  return (
    <div className="flex-1 flex flex-col overflow-y-auto hide-scrollbar">
      {/* Top bar */}
      <div className="flex items-center gap-3 px-4 pt-3 pb-2">
        <button
          onClick={() => navigate(-1)}
          className="w-8 h-8 flex items-center justify-center rounded-lg bg-white/5"
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth={2}
            className="w-5 h-5 text-white"
          >
            <path d="M15 18l-6-6 6-6" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
        <div className="flex items-center gap-2">
          <AssetAvatar symbol={productName} size={28} textClassName="text-[10px]" />
          <div>
            <span className="text-base font-bold text-white">{productName}-PERP</span>
            {productInfo && (
              <span className="ml-2 text-[10px] text-tg-hint bg-white/10 px-1.5 py-0.5 rounded">
                {productInfo.max_leverage}x
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Price + change */}
      <div className="px-4 pb-2">
        <div className="text-3xl font-bold text-white tabular-nums">
          {formatPrice(currentPrice, productName)}
        </div>
        <span
          className={clsx(
            "text-sm font-medium",
            change24h == null
              ? "text-tg-hint"
              : change24h >= 0
                ? "text-long"
                : "text-short",
          )}
        >
          {change24h != null
            ? `${change24h >= 0 ? "+" : ""}${change24h.toFixed(2)}%`
            : ""}
          <span className="text-tg-hint ml-1 text-xs">24h</span>
        </span>
        {fundingRate != null && (
          <span className="text-xs text-tg-hint ml-3">
            Funding: <span className={clsx(fundingRate >= 0 ? "text-long" : "text-short")}>{(fundingRate * 100).toFixed(4)}%</span>
          </span>
        )}
      </div>

      {/* Interval selector */}
      <div className="flex gap-1 px-4 mb-2">
        {INTERVALS.map((iv) => (
          <button
            key={iv}
            onClick={() => setInterval_(iv)}
            className={clsx(
              "px-3 py-1 rounded-lg text-xs font-medium transition-colors",
              interval === iv
                ? "bg-white/15 text-white"
                : "text-tg-hint",
            )}
          >
            {iv}
          </button>
        ))}
      </div>

      {/* Chart */}
      <div ref={chartContainerRef} className="mx-4 rounded-xl overflow-hidden bg-white/[0.02] mb-4" />

      {/* Result toast */}
      {lastResult && (
        <div
          className={clsx(
            "mx-4 mb-3 rounded-xl px-4 py-2 text-sm",
            lastResult.ok ? "bg-long/20 text-long" : "bg-short/20 text-short",
          )}
        >
          {lastResult.ok
            ? `Filled ${lastResult.side} ${lastResult.product} @ $${lastResult.fill_price?.toLocaleString() ?? "--"}`
            : lastResult.error}
        </div>
      )}

      {/* Order form toggle */}
      {!showOrderForm ? (
        <div className="flex gap-3 px-4 mt-auto pb-4">
          <button
            onClick={() => {
              hapticImpact("light");
              setSide("long");
              setShowOrderForm(true);
            }}
            className="flex-1 py-4 rounded-2xl bg-long text-white font-semibold text-[16px] active:scale-[0.98] transition-transform"
          >
            Long
          </button>
          <button
            onClick={() => {
              hapticImpact("light");
              setSide("short");
              setShowOrderForm(true);
            }}
            className="flex-1 py-4 rounded-2xl bg-short text-white font-semibold text-[16px] active:scale-[0.98] transition-transform"
          >
            Short
          </button>
        </div>
      ) : (
        /* Expanded order form */
        <div className="px-4 pb-4 flex flex-col gap-3">
          {/* Side toggle */}
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

          {/* Order type tabs */}
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

          {/* Size input */}
          <div className="bg-white/5 rounded-xl px-4 py-3">
            <label className="text-[11px] text-tg-hint block mb-1">
              Size (USD)
            </label>
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

          {/* Limit price */}
          {orderType === "limit" && (
            <div className="bg-white/5 rounded-xl px-4 py-3">
              <label className="text-[11px] text-tg-hint block mb-1">
                Limit Price
              </label>
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

          {/* Leverage slider */}
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

          {/* Est. margin */}
          {sizeUsd && Number(sizeUsd) > 0 && (
            <div className="flex justify-between text-xs text-tg-hint px-1">
              <span>Est. margin</span>
              <span className="text-white">
                ${(Number(sizeUsd) / leverage).toFixed(2)}
              </span>
            </div>
          )}

          {/* Buttons */}
          <div className="flex gap-3">
            <button
              onClick={() => setShowOrderForm(false)}
              className="flex-1 py-3 rounded-2xl bg-white/5 text-tg-hint font-semibold text-sm"
            >
              Cancel
            </button>
            <button
              onClick={submit}
              disabled={submitting || !sizeUsd || Number(sizeUsd) <= 0}
              className={clsx(
                "flex-1 py-3 rounded-2xl font-semibold text-sm transition-all active:scale-[0.98]",
                side === "long" ? "bg-long" : "bg-short",
                "text-white disabled:opacity-40",
              )}
            >
              {submitting
                ? "Placing..."
                : `${side === "long" ? "Long" : "Short"} ${productName}`}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
