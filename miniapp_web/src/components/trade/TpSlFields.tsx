import { clsx } from "clsx";

export type TpSlInputMode = "pct" | "price";

interface TpSlFieldsProps {
  mode: TpSlInputMode;
  onModeChange: (m: TpSlInputMode) => void;
  tpPct: string;
  slPct: string;
  tpPrice: string;
  slPrice: string;
  onTpPct: (v: string) => void;
  onSlPct: (v: string) => void;
  onTpPrice: (v: string) => void;
  onSlPrice: (v: string) => void;
  /** Only market orders use TP/SL in the API today */
  disabled?: boolean;
}

export default function TpSlFields({
  mode,
  onModeChange,
  tpPct,
  slPct,
  tpPrice,
  slPrice,
  onTpPct,
  onSlPct,
  onTpPrice,
  onSlPrice,
  disabled,
}: TpSlFieldsProps) {
  return (
    <div className={clsx("space-y-2", disabled && "opacity-40 pointer-events-none")}>
      <div className="flex justify-between items-center gap-2">
        <span className="text-[11px] text-tg-hint">Take profit / Stop loss</span>
        <div className="flex rounded-lg overflow-hidden bg-white/5 p-0.5 shrink-0">
          <button
            type="button"
            onClick={() => onModeChange("pct")}
            className={clsx(
              "px-2.5 py-1 rounded-md text-[10px] font-semibold transition-colors",
              mode === "pct" ? "bg-white/15 text-white" : "text-tg-hint",
            )}
          >
            %
          </button>
          <button
            type="button"
            onClick={() => onModeChange("price")}
            className={clsx(
              "px-2.5 py-1 rounded-md text-[10px] font-semibold transition-colors",
              mode === "price" ? "bg-white/15 text-white" : "text-tg-hint",
            )}
          >
            Price
          </button>
        </div>
      </div>
      {mode === "pct" ? (
        <div className="grid grid-cols-2 gap-2">
          <div className="bg-white/5 rounded-xl px-3 py-2.5">
            <label className="text-[10px] text-tg-hint block mb-0.5">TP %</label>
            <input
              type="number"
              inputMode="decimal"
              placeholder="—"
              value={tpPct}
              onChange={(e) => onTpPct(e.target.value)}
              className="w-full bg-transparent text-white text-sm font-medium outline-none placeholder:text-white/20"
            />
          </div>
          <div className="bg-white/5 rounded-xl px-3 py-2.5">
            <label className="text-[10px] text-tg-hint block mb-0.5">SL %</label>
            <input
              type="number"
              inputMode="decimal"
              placeholder="—"
              value={slPct}
              onChange={(e) => onSlPct(e.target.value)}
              className="w-full bg-transparent text-white text-sm font-medium outline-none placeholder:text-white/20"
            />
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-2">
          <div className="bg-white/5 rounded-xl px-3 py-2.5">
            <label className="text-[10px] text-tg-hint block mb-0.5">TP price</label>
            <input
              type="number"
              inputMode="decimal"
              placeholder="—"
              value={tpPrice}
              onChange={(e) => onTpPrice(e.target.value)}
              className="w-full bg-transparent text-white text-sm font-medium outline-none placeholder:text-white/20"
            />
          </div>
          <div className="bg-white/5 rounded-xl px-3 py-2.5">
            <label className="text-[10px] text-tg-hint block mb-0.5">SL price</label>
            <input
              type="number"
              inputMode="decimal"
              placeholder="—"
              value={slPrice}
              onChange={(e) => onSlPrice(e.target.value)}
              className="w-full bg-transparent text-white text-sm font-medium outline-none placeholder:text-white/20"
            />
          </div>
        </div>
      )}
    </div>
  );
}
