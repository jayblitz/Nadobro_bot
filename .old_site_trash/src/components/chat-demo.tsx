"use client";

import { useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";
import { Pause, Play, Volume2, VolumeX } from "lucide-react";

/**
 * Real product demo — a looping, muted-by-default video of the NadoBro
 * Telegram Command Center driving live order flow on the Nado CLOB
 * (app.nado.xyz). Wrapped in a glass browser-style bezel so it feels
 * in-product rather than a raw <video> tag.
 *
 * Assets (transcoded from the uploaded screen recording):
 *   /demo/text-to-trade.mp4          — h264, 960x592, ~575 KB, 24 fps
 *   /demo/text-to-trade-poster.jpg   — first-frame poster for fast LCP
 */
export function ChatDemo() {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const [playing, setPlaying] = useState(true);
  const [muted, setMuted] = useState(true);

  // Respect prefers-reduced-motion: don't autoplay, show poster.
  useEffect(() => {
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    if (mq.matches && videoRef.current) {
      videoRef.current.pause();
      setPlaying(false);
    }
  }, []);

  const togglePlay = () => {
    const v = videoRef.current;
    if (!v) return;
    if (v.paused) {
      v.play().catch(() => {
        /* ignore autoplay policies */
      });
      setPlaying(true);
    } else {
      v.pause();
      setPlaying(false);
    }
  };

  const toggleMute = () => {
    const v = videoRef.current;
    if (!v) return;
    v.muted = !v.muted;
    setMuted(v.muted);
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 30 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, amount: 0.35 }}
      transition={{ duration: 0.9, ease: [0.16, 1, 0.3, 1] }}
      className="relative w-full"
    >
      {/* Soft cyan halo behind the player */}
      <div
        aria-hidden
        className="pointer-events-none absolute -inset-8 -z-10 rounded-[3rem] bg-[radial-gradient(circle_at_center,rgba(105,227,255,0.28),transparent_65%)] blur-2xl"
      />

      <div className="overflow-hidden rounded-2xl border border-white/10 bg-gradient-to-b from-[#0a1a32] to-[#04101f] shadow-[0_30px_80px_rgba(0,10,25,0.7)]">
        {/* Browser-style chrome */}
        <div className="flex items-center gap-2 border-b border-white/5 bg-[#081425]/80 px-4 py-3">
          <span className="flex gap-1.5" aria-hidden>
            <span className="h-2.5 w-2.5 rounded-full bg-red-400/80" />
            <span className="h-2.5 w-2.5 rounded-full bg-amber-300/80" />
            <span className="h-2.5 w-2.5 rounded-full bg-emerald-400/80" />
          </span>
          <div className="ml-3 flex-1 truncate rounded-md border border-white/10 bg-[#0a1a32] px-3 py-1 text-xs text-slate-400">
            t.me/NadoBro_bot · app.nado.xyz
          </div>
          <div className="ml-3 flex items-center gap-1 text-[10px] uppercase tracking-[0.2em] text-cyan-200/70">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-400 shadow-[0_0_8px_rgba(0,255,178,0.9)]" />
            LIVE
          </div>
        </div>

        {/* Video */}
        <div className="relative aspect-[960/592] w-full bg-black">
          <video
            ref={videoRef}
            className="absolute inset-0 h-full w-full object-cover"
            src="/demo/text-to-trade.mp4"
            poster="/demo/text-to-trade-poster.jpg"
            autoPlay
            muted
            loop
            playsInline
            preload="metadata"
            aria-label="NadoBro Telegram bot executing a trade on the Nado CLOB"
          />

          {/* Controls overlay — fades in on hover, always on for keyboard nav */}
          <div className="pointer-events-none absolute inset-0 flex items-end justify-between gap-3 bg-gradient-to-t from-black/55 via-transparent to-transparent p-3 opacity-0 transition-opacity duration-200 hover:opacity-100 focus-within:opacity-100">
            <button
              type="button"
              onClick={togglePlay}
              aria-label={playing ? "Pause demo" : "Play demo"}
              className="pointer-events-auto inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/20 bg-black/50 text-white backdrop-blur transition hover:border-cyan-300/50 hover:bg-cyan-300/10"
            >
              {playing ? <Pause size={14} /> : <Play size={14} />}
            </button>
            <button
              type="button"
              onClick={toggleMute}
              aria-label={muted ? "Unmute demo" : "Mute demo"}
              className="pointer-events-auto inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/20 bg-black/50 text-white backdrop-blur transition hover:border-cyan-300/50 hover:bg-cyan-300/10"
            >
              {muted ? <VolumeX size={14} /> : <Volume2 size={14} />}
            </button>
          </div>
        </div>

        {/* Caption strip */}
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/5 bg-[#081425]/80 px-4 py-3 text-[11px] uppercase tracking-[0.18em] text-cyan-200/70">
          <span>Command Center · Trade Console · Strategy Lab</span>
          <span className="text-slate-400 normal-case tracking-normal">
            Recorded on Nado mainnet
          </span>
        </div>
      </div>
    </motion.div>
  );
}
