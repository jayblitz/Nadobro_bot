import { create } from "zustand";
import type { PositionResponse, PortfolioSummary } from "@/api/types";

interface AccountState {
  portfolio: PortfolioSummary | null;
  positions: PositionResponse[];
  loading: boolean;
  setPortfolio: (p: PortfolioSummary) => void;
  setPositions: (p: PositionResponse[]) => void;
  setLoading: (l: boolean) => void;
}

export const useAccountStore = create<AccountState>((set) => ({
  portfolio: null,
  positions: [],
  loading: false,

  setPortfolio: (portfolio) =>
    set({ portfolio, positions: portfolio.positions }),

  setPositions: (positions) => set({ positions }),

  setLoading: (loading) => set({ loading }),
}));
