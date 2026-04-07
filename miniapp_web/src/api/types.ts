/** API response types — mirrors miniapp_api/models/schemas.py */

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

export interface TradeHistoryItem {
  id: number;
  product_name: string;
  side: string;
  size: number;
  price: number | null;
  leverage: number;
  status: string;
  pnl: number | null;
  fees: number;
  created_at: string | null;
  filled_at: string | null;
}

export interface ParseIntentResponse {
  intent: string | null;
  product: string | null;
  side: string | null;
  size_usd: number | null;
  price: number | null;
  leverage: number | null;
  raw: Record<string, unknown> | null;
}

export interface OkResponse {
  ok: boolean;
  message: string;
}

// --- Candles ---

export interface Candle {
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

// --- Nado Market Quotes ---

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

// --- Strategies (bot runtime) ---

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

// --- Voice ---

export interface VoiceMessage {
  type: "auth" | "audio" | "text" | "end";
  init_data?: string;
  data?: string;
  text?: string;
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
