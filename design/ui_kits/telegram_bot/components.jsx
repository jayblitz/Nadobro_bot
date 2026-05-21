/* global React */
const { useState } = React;

/* ────────────────────────────────────────────────────────────────────
   NadoBro Telegram UI Kit — components
   Tokens come from ../../colors_and_type.css
   ──────────────────────────────────────────────────────────────────── */

const RULE = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
const tgBg = "#17212B";
const inBubble = "#182533";
const outBubble = "#2B5278";

/* Telegram chrome ─────────────────────────────────────────────────── */
function TelegramChrome({ children }) {
  return (
    <div className="tg-frame">
      <div className="tg-status">
        <span>9:41</span>
        <span className="tg-icons">●●● 5G ▮▮▮</span>
      </div>
      <div className="tg-header">
        <button className="tg-back">←</button>
        <img className="tg-avatar" src="../../assets/nadobro_glyph_transparent.png" alt="" />
        <div className="tg-title">
          <div className="tg-name">NadoBro</div>
          <div className="tg-sub">bot · always online</div>
        </div>
        <button className="tg-menu">⋮</button>
      </div>
      <div className="tg-body">{children}</div>
    </div>
  );
}

/* Chat bubbles ────────────────────────────────────────────────────── */
function Bubble({ from = "in", time = "14:02", children }) {
  return (
    <div className={`b-row b-${from}`}>
      <div className={`bubble b-${from}`}>
        <div className="bubble-body">{children}</div>
        <div className="bubble-time">{time}</div>
      </div>
    </div>
  );
}

/* Bro card — header + rule + sections + tree rows ─────────────────── */
function BroCard({ icon = "📊", title, children, footer }) {
  return (
    <div className="bro-card">
      <div className="bro-hd">{icon} <b>{title}</b></div>
      <div className="bro-rule">{RULE}</div>
      <div className="bro-body">{children}</div>
      {footer ? <div className="bro-footer">{footer}</div> : null}
    </div>
  );
}
function BroSection({ title, children }) {
  return (<div className="bro-section">
    <div className="bro-sec-title">{title}</div>
    <div className="bro-sec-body">{children}</div>
  </div>);
}
function TreeRow({ last = false, children }) {
  return <div className="tree-row"><span className="tree-tick">{last ? "└" : "├"}</span><span>{children}</span></div>;
}
function Insight({ children }) {
  return <div className="bro-insight">🎯 <b>Actionable Insight:</b> {children}</div>;
}
function Sources({ items }) {
  return <div className="bro-sources"><i>Sources: {items.join(", ")}</i></div>;
}

/* Inline keyboard (under a card) ──────────────────────────────────── */
function InlineKeyboard({ rows, onTap }) {
  return (
    <div className="ikb">
      {rows.map((row, i) => (
        <div key={i} className="ikb-row" style={{ gridTemplateColumns: `repeat(${row.length}, 1fr)` }}>
          {row.map((b, j) => (
            <button key={j} className="ikb-btn" onClick={() => onTap && onTap(b.cb)}>{b.label}</button>
          ))}
        </div>
      ))}
    </div>
  );
}

/* Reply keyboard (persistent, below composer) ─────────────────────── */
function ReplyKeyboard({ rows, onTap }) {
  return (
    <div className="rkb">
      {rows.map((row, i) => (
        <div key={i} className="rkb-row">
          {row.map((label, j) => (
            <button key={j} className="rkb-btn" onClick={() => onTap && onTap(label)}>{label}</button>
          ))}
        </div>
      ))}
    </div>
  );
}

/* Composer ────────────────────────────────────────────────────────── */
function Composer({ onSend }) {
  const [v, setV] = useState("");
  return (
    <div className="composer">
      <button className="cmp-btn">📎</button>
      <input className="cmp-input" placeholder="Message" value={v} onChange={(e) => setV(e.target.value)}
        onKeyDown={(e) => { if (e.key === "Enter" && v.trim()) { onSend(v); setV(""); } }} />
      <button className="cmp-btn">🎤</button>
    </div>
  );
}

/* Pills ───────────────────────────────────────────────────────────── */
function BroPill({ children = "🚀 Alpha Agent" }) {
  return <span className="pill bro-pill">{children}</span>;
}
function Pill({ tone = "info", children }) {
  return <span className={`pill pill-${tone}`}>{children}</span>;
}

/* Position row ────────────────────────────────────────────────────── */
function PositionRow({ side, product, size, upnl, entry, mark, liq }) {
  const long = side === "LONG";
  return (
    <div className="pos-row">
      <div className="pos-hd">
        <span className={long ? "pos-side long" : "pos-side short"}>{long ? "🟢" : "🔴"} <b>{side}</b></span>
        <span className="pos-prod">{product}</span>
        <span className={`pos-pnl ${upnl.startsWith("+") ? "pos" : "neg"}`}>{upnl}</span>
      </div>
      <div className="pos-meta">
        <span>Size <b className="mono">{size}</b></span>
        <span>Entry <b className="mono">{entry}</b></span>
        <span>Mark <b className="mono">{mark}</b></span>
        <span>Liq <b className="mono">{liq}</b></span>
      </div>
    </div>
  );
}

/* Share PnL card (uses master PNG) ────────────────────────────────── */
function ShareCard({ symbol = "USDC", strategy = "Alpha Agent", volume = "$12,480", pnl = "+$1,284.50", fees = "$184.20", referral = "K5CJBTEN" }) {
  return (
    <div className="share-card">
      <img className="share-bg" src="../../assets/pnl_card_master.png" alt="" />
      <div className="share-overlay">
        <div className="share-pill-row">
          <span className="share-symbol">{symbol}</span>
          <span className="share-bro">🚀 {strategy}</span>
        </div>
        <div className="share-vol">
          <div className="share-lbl">Volume</div>
          <div className="share-val">{volume}</div>
          <div className="share-onnado">✈ On Nado</div>
        </div>
        <div className="share-row">
          <div><div className="share-lbl">Net Fees</div><div className="share-num pos">{fees}</div></div>
          <div><div className="share-lbl">PnL</div><div className="share-num pos">{pnl}</div></div>
        </div>
        <div className="share-ref">Referral Code: {referral}</div>
      </div>
    </div>
  );
}

Object.assign(window, {
  TelegramChrome, Bubble, BroCard, BroSection, TreeRow, Insight, Sources,
  InlineKeyboard, ReplyKeyboard, Composer, BroPill, Pill, PositionRow, ShareCard,
});
