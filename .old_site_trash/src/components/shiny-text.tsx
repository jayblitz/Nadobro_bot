"use client";

import { motion } from "framer-motion";
import type { ReactNode } from "react";

type ShinyTextProps = {
  children: ReactNode;
  className?: string;
  /**
   * Animation duration in seconds. Defaults to 4s (one full shine pass).
   */
  duration?: number;
};

/**
 * Cinematic headline text that renders a slow-moving specular highlight across a
 * base brand-tinted gradient. Pure CSS animation driven by `background-position`
 * so it doesn't repaint React on every frame.
 */
export function ShinyText({
  children,
  className = "",
  duration = 4,
}: ShinyTextProps) {
  return (
    <motion.span
      initial={{ opacity: 0, y: 18 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.9, ease: [0.16, 1, 0.3, 1] }}
      className={`shiny-text inline-block bg-clip-text text-transparent ${className}`}
      style={{
        // Base color + moving white highlight. Base is the NadoBro cyan
        // (#64CEFB-adjacent); highlight is pure white for the sheen pass.
        backgroundImage:
          "linear-gradient(110deg, #64CEFB 0%, #64CEFB 40%, #ffffff 50%, #64CEFB 60%, #64CEFB 100%)",
        backgroundSize: "220% 100%",
        animationDuration: `${duration}s`,
      }}
    >
      {children}
    </motion.span>
  );
}
