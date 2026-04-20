/** API response types — mirrors miniapp_api/models/schemas.py */

/** Perp order / strategy direction — matches `^(long|short)$` on order and strategy requests in schemas.py */
export type OrderSide = "long" | "short";

/** Runtime strategy ids — matches `StrategyStartRequest.strategy` in schemas.py */
const STRATEGY_IDS = ["grid", "rgrid", "dn", "vol", "bro"] as const;
export type StrategyId = (typeof STRATEGY_IDS)[number];

/** Intervals accepted by `GET /api/products/{product}/candles` — see `miniapp_api/candle_intervals.py` */
const CANDLE_INTERVALS = ["1m", "5m", "15m", "1h", "2h", "4h", "1d", "1w"] as const;
type CandleInterval = (typeof CANDLE_INTERVALS)[number];

/** Chart selector on product detail; subset of candle API intervals */
export const PRODUCT_CHART_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"] as const satisfies ReadonlyArray<CandleInterval>;
export type ProductChartInterval = (typeof PRODUCT_CHART_INTERVALS)[number];

/** Tab/filter labels for `ProductInfo.category` (API may return other strings). */
type ProductCategoryFilter =
  | "perps"
  | "spot"
  | "memes"
  | "defi"
  | "chains"
  | "commodities";

/** Home asset list tabs — API categories plus UI-only `all` / `favorites` */
export type HomeCategoryTabId = "all" | "favorites" | ProductCategoryFilter;

export interface UserResponse {
  telegram_id: number;
  username: string;
  language: string;
  network: string;
  main_address: string | null;
  tos_accepted: boolean;
  is_new: boolean;
  total_trades: number;
  total_volume_usd: number;
}

export interface ProductInfo {
  id: number;
  name: string;
  symbol: string;
  type: string;
  max_leverage: number;
  isolated_only: boolean;
  dn_eligible: boolean;
  /** Nado-style tab: perps | spot | memes | defi | chains | commodities */
  category: string;
}

export interface PriceResponse {
  product: string;
  bid: number | null;
  ask: number | null;
  mid: number | null;
  timestamp: number;
}

export interface AllPricesResponse {
  prices: Record<string, PriceResponse>;
}

export interface TradeResponse {
  ok: boolean;
  trade_id?: number;
  digest?: string;
  fill_price?: number;
  size?: number;
  side?: string;
  product?: string;
  error?: string;
}

export interface PositionResponse {
  product_id: number;
  product_name: string;
  side: string;
  size: number;
  entry_price: number;
  mark_price: number | null;
  unrealized_pnl: number | null;
  leverage: number | null;
  liquidation_price: number | null;
  margin: number | null;
}

export interface PortfolioSummary {
  equity: number;
  balance_usd: number;
  available_balance: number;
  total_unrealized_pnl: number;
  unrealized_spot_pnl: number;
  total_margin_used: number;
  margin_utilization: number | null;
  total_volume_usd: number;
  fee_tier_display: string;
  nlp_balance_usd: number;
  positions: PositionResponse[];
  open_orders_count: number;
}

interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface CandleResponse {
  candles: Candle[];
  error?: string;
}

export interface CryptoQuote {
  price: number | null;
  change_24h: number | null;
  volume_24h: number | null;
  mark_price: number | null;
  index_price: number | null;
  funding_rate: number | null;
  open_interest: number | null;
  open_interest_usd: number | null;
  max_leverage: number | null;
}

export interface QuotesResponse {
  quotes: Record<string, CryptoQuote>;
  error?: string;
}

export interface StrategyBotStatus {
  network: string;
  running: boolean;
  global_pause_active?: boolean;
  strategy?: string | null;
  product?: string | null;
  runs?: number;
  last_error?: string | null;
  started_at?: string | null;
  tp_pct?: number | null;
  sl_pct?: number | null;
  interval_seconds?: number;
  next_cycle_in?: number;
  [key: string]: unknown;
}

export interface StrategyActionResponse {
  ok: boolean;
  message?: string;
}

export interface VoiceTradeToolArgs {
  side?: string;
  product?: string;
  size_usd?: number;
}

export interface VoiceTradeToolResult {
  status?: string;
  fill_price?: number;
  error?: unknown;
}

export interface VoiceServerMessage {
  type: "auth_ok" | "audio" | "text" | "function_call" | "turn_complete" | "error";
  username?: string;
  data?: string;
  mime_type?: string;
  text?: string;
  name?: string;
  args?: Record<string, unknown>;
  result?: Record<string, unknown>;
  message?: string;
  /** Server error code, e.g. invalid_gemini_key */
  code?: string;
}
