import { useEffect, useCallback } from "react";
import { getWebApp } from "@/lib/telegram";

/**
 * Hook providing the Telegram WebApp instance and common helpers.
 * Returns `null` for `webApp` when running outside Telegram.
 */
export function useTelegram() {
  const webApp = getWebApp();

  const showBackButton = useCallback(
    (onBack: () => void) => {
      if (!webApp) return;
      webApp.BackButton.show();
      webApp.BackButton.onClick(onBack);
      return () => {
        webApp.BackButton.offClick(onBack);
        webApp.BackButton.hide();
      };
    },
    [webApp],
  );

  return { webApp, showBackButton };
}

/** Track Telegram viewport height and update the CSS variable. */
export function useViewportHeight() {
  useEffect(() => {
    const wa = getWebApp();
    if (!wa) return;

    const update = () => {
      document.documentElement.style.setProperty(
        "--tg-viewport-height",
        `${wa.viewportStableHeight}px`,
      );
    };

    update();

    // The SDK fires a viewportChanged event; listen via resize as fallback.
    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, []);
}
