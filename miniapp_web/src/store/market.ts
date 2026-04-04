import { create } from "zustand";
import type { PriceResponse, ProductInfo } from "@/api/types";

interface MarketState {
  products: ProductInfo[];
  prices: Record<string, PriceResponse>;
  selectedProduct: string;
  setProducts: (products: ProductInfo[]) => void;
  updatePrices: (prices: Record<string, PriceResponse>) => void;
  selectProduct: (name: string) => void;
}

export const useMarketStore = create<MarketState>((set) => ({
  products: [],
  prices: {},
  selectedProduct: "BTC",

  setProducts: (products) => set({ products }),

  updatePrices: (prices) =>
    set((state) => ({
      prices: { ...state.prices, ...prices },
    })),

  selectProduct: (name) => set({ selectedProduct: name }),
}));
