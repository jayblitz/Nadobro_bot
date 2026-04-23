"use client";

import { motion } from "framer-motion";

/**
 * A multi-layer animated background that stands in for the reference site's
 * looping video. Built from three slow-drifting radial gradients (cyan, mint,
 * deep blue), plus a subtle animated grid and a slow scan line — tuned to the
 * NadoBro palette defined in globals.css.
 *
 * Everything is pointer-events:none so content above remains interactive.
 */
export function AuroraBackground() {
  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-0 overflow-hidden"
    >
      {/* Deep space base */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,#0b1f3a_0%,#030a15_55%,#01060d_100%)]" />

      {/* Animated aurora blobs */}
      <motion.div
        className="absolute -left-40 -top-32 h-[40rem] w-[40rem] rounded-full"
        style={{
          background:
            "radial-gradient(circle, rgba(25,222,255,0.55), transparent 65%)",
          filter: "blur(60px)",
        }}
        animate={{
          x: [0, 40, -20, 0],
          y: [0, -20, 30, 0],
        }}
        transition={{ duration: 18, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        className="absolute -right-40 top-40 h-[34rem] w-[34rem] rounded-full"
        style={{
          background:
            "radial-gradient(circle, rgba(0,255,176,0.42), transparent 65%)",
          filter: "blur(70px)",
        }}
        animate={{
          x: [0, -30, 20, 0],
          y: [0, 40, 10, 0],
        }}
        transition={{ duration: 22, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        className="absolute bottom-[-10rem] left-1/3 h-[30rem] w-[30rem] rounded-full"
        style={{
          background:
            "radial-gradient(circle, rgba(138,93,255,0.35), transparent 70%)",
          filter: "blur(80px)",
        }}
        animate={{
          x: [0, 30, -40, 0],
          y: [0, -20, 20, 0],
        }}
        transition={{ duration: 26, repeat: Infinity, ease: "easeInOut" }}
      />

      {/* Subtle circuit grid, masked to fade at edges */}
      <div className="hero-grid absolute inset-0 opacity-70" />

      {/* Scan line — slow vertical sweep */}
      <div className="scan-line" />

      {/* Top/bottom vignette so content above stays legible */}
      <div className="absolute inset-x-0 top-0 h-48 bg-gradient-to-b from-[#030a15] to-transparent" />
      <div className="absolute inset-x-0 bottom-0 h-48 bg-gradient-to-t from-[#030a15] to-transparent" />
    </div>
  );
}
