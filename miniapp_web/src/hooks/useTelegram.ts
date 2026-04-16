import { useEffect } from "react";
import { getWebApp } from "@/lib/telegram";

/** Syncs `--tg-viewport-height` from `viewportStableHeight`. */
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

    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, []);
}
