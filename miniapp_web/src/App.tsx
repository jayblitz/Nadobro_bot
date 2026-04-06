import { useEffect, useState, useRef } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { signalReady } from "@/lib/telegram";
import { useViewportHeight } from "@/hooks/useTelegram";
import { useAuthStore } from "@/store/auth";
import BottomTabs from "@/components/common/BottomTabs";
import LoadingScreen from "@/components/common/LoadingScreen";
import Onboarding from "@/pages/Onboarding";
import Home from "@/pages/Home";
import ProductDetail from "@/pages/ProductDetail";
import Trade from "@/pages/Trade";
import Portfolio from "@/pages/Portfolio";
import Strategies from "@/pages/Strategies";
import AI from "@/pages/AI";
import Settings from "@/pages/Settings";
import ErrorBoundary from "@/components/common/ErrorBoundary";

export default function App() {
  const { user, loading, error, fetchUser } = useAuthStore();
  const [onboarded, setOnboarded] = useState(false);
  const didInit = useRef(false);

  useViewportHeight();

  // Signal readiness to Telegram and fetch the user once on mount.
  useEffect(() => {
    if (didInit.current) return;
    didInit.current = true;
    signalReady();
    fetchUser();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Determine onboarding state once user loads.
  useEffect(() => {
    if (user) {
      setOnboarded(user.tos_accepted);
    }
  }, [user]);

  if (loading) {
    return <LoadingScreen />;
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center bg-tg-bg px-6">
        <div className="text-center">
          <p className="text-tg-text text-lg font-semibold mb-2">Connection failed</p>
          <p className="text-tg-hint text-sm mb-4">{error}</p>
          <button
            onClick={() => fetchUser()}
            className="px-6 py-2 rounded-xl bg-tg-button text-tg-button-text text-sm font-medium"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!onboarded) {
    return <Onboarding onComplete={() => setOnboarded(true)} />;
  }

  return (
    <div className="flex flex-col h-full">
      <main className="flex-1 flex flex-col overflow-hidden">
        <ErrorBoundary>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/product/:product" element={<ProductDetail />} />
            <Route path="/trade" element={<Trade />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/strategies" element={<Strategies />} />
            <Route path="/ai" element={<AI />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </ErrorBoundary>
      </main>
      <BottomTabs />
    </div>
  );
}
