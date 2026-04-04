import { create } from "zustand";
import { api } from "@/api/client";
import type { UserResponse } from "@/api/types";

interface AuthState {
  user: UserResponse | null;
  loading: boolean;
  error: string | null;
  fetchUser: () => Promise<void>;
  setUser: (user: UserResponse) => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  loading: true,
  error: null,

  fetchUser: async () => {
    set({ loading: true, error: null });
    try {
      const user = await api.get<UserResponse>("/api/me");
      set({ user, loading: false });
    } catch (err) {
      set({
        loading: false,
        error: err instanceof Error ? err.message : "Auth failed",
      });
    }
  },

  setUser: (user) => set({ user }),
}));
