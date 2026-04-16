import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        tg: {
          bg: "var(--tg-theme-bg-color, #0f1923)",
          "secondary-bg": "var(--tg-theme-secondary-bg-color, #17212b)",
          text: "var(--tg-theme-text-color, #ffffff)",
          hint: "var(--tg-theme-hint-color, #708499)",
          link: "var(--tg-theme-link-color, #6ab3f3)",
          button: "var(--tg-theme-button-color, #5288c1)",
          "button-text": "var(--tg-theme-button-text-color, #ffffff)",
        },
        nb: {
          cyan: "var(--nb-cyan, #22d3ee)",
          green: "var(--nb-green, #4ade80)",
          deep: "var(--nb-bg-deep, #0a0e12)",
        },
        long: "#22c55e",
        short: "#ef4444",
        "long-dim": "#22c55e33",
        "short-dim": "#ef444433",
      },
      fontFamily: {
        sans: [
          "-apple-system",
          "BlinkMacSystemFont",
          "Segoe UI",
          "Roboto",
          "Helvetica Neue",
          "Arial",
          "sans-serif",
        ],
      },
    },
  },
  plugins: [],
} satisfies Config;
