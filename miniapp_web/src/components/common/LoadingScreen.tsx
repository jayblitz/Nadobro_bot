export default function LoadingScreen() {
  return (
    <div className="flex-1 flex items-center justify-center bg-tg-bg">
      <div className="flex flex-col items-center gap-5">
        <img
          src="/nadobro-logo.png"
          alt="NadoBro"
          className="w-24 h-24 object-contain drop-shadow-[0_0_20px_rgba(34,211,238,0.4)] animate-pulse"
        />
        <div className="w-10 h-10 border-2 border-nb-cyan border-t-transparent rounded-full animate-spin" />
        <span className="text-tg-hint text-sm">Loading…</span>
      </div>
    </div>
  );
}
