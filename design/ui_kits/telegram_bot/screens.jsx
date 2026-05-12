/* global React, TelegramChrome, Bubble, BroCard, BroSection, TreeRow, Insight, Sources,
   InlineKeyboard, ReplyKeyboard, Composer, BroPill, Pill, PositionRow, ShareCard */
const { useState } = React;

/* Reply keyboard — persistent menu (from keyboards.py persistent_menu_kb) */
const HOME_REPLY_KB = [
  ["🤖 Trade Console", "📁 Portfolio Deck"],
  ["💼 Wallet Vault", "🏆 Nado Points"],
  ["🧠 Strategy Lab", "🎁 Refer Friends"],
  ["🔔 Alert Engine", "⚙️ Control Panel"],
  ["🌐 Execution Mode"],
];

/* Inline keyboards (callback id, label) */
const HOME_IKB = [
  [{ label: "🤖 Trade Console", cb: "trade" }, { label: "📁 Portfolio Deck", cb: "portfolio" }],
  [{ label: "💼 Wallet Vault", cb: "wallet" },  { label: "🏆 Nado Points", cb: "points" }],
  [{ label: "🎁 Refer Friends", cb: "refer" },   { label: "🔔 Alert Engine", cb: "alert" }],
  [{ label: "🧠 Strategy Lab", cb: "strategy" }, { label: "⚙️ Control Panel", cb: "settings" }],
  [{ label: "🌐 Execution Mode", cb: "mode" }],
];
const ANSWER_IKB = [
  [{ label: "🧠 Strategy Lab", cb: "strategy" }, { label: "📁 Portfolio", cb: "portfolio" }],
  [{ label: "🏠 Home", cb: "home" }],
];
const PORTFOLIO_IKB = [
  [{ label: "📌 Open Positions", cb: "pos" }, { label: "📜 Trade History", cb: "history" }],
  [{ label: "📊 Performance", cb: "perf" },   { label: "🔄 Refresh", cb: "refresh" }],
  [{ label: "🏠 Home", cb: "home" }],
];
const TRADE_DIR_RKB = [["🟢 Long", "🔴 Short"], ["◀ Home"]];
const TRADE_LEV_RKB = [["1x","2x","3x"],["5x","10x","20x"],["✏️ Custom","◀ Back","◀ Home"]];

/* ─────────────────────────────────────────────────────────────────── */
function App() {
  const [stage, setStage] = useState("home"); // home | trade-dir | trade-lev | portfolio | answer | share
  const [history, setHistory] = useState([]);
  const replyKb = (() => {
    if (stage === "trade-dir") return TRADE_DIR_RKB;
    if (stage === "trade-lev") return TRADE_LEV_RKB;
    return HOME_REPLY_KB;
  })();

  function go(next, userMsg, broNode) {
    setHistory((h) => [
      ...h,
      ...(userMsg ? [{ kind: "out", body: userMsg }] : []),
      ...(broNode ? [{ kind: "card", body: broNode }] : []),
    ]);
    setStage(next);
  }

  function onReplyTap(label) {
    if (label === "🤖 Trade Console") {
      go("trade-dir", label, <BroCard icon="🤖" title="Trade Console">
        <p>Pick a side, fren. I'll walk you through size + leverage.</p>
      </BroCard>);
    } else if (label === "🟢 Long" || label === "🔴 Short") {
      const side = label.includes("Long") ? "LONG" : "SHORT";
      go("trade-lev", label, <BroCard icon="📈" title={`${side} BTC-PERP`}>
        <BroSection title="Setup">
          <TreeRow>Side: <b>{side}</b></TreeRow>
          <TreeRow>Product: <b>BTC-PERP</b></TreeRow>
          <TreeRow last>Size: <span className="mono">0.01 BTC</span></TreeRow>
        </BroSection>
        <p style={{ marginTop: 8 }}>Now pick leverage 👇</p>
      </BroCard>);
    } else if (/^[0-9]+x$/.test(label)) {
      const lev = label;
      go("home", label, <BroCard icon="✅" title="Trade Preview" footer={<>
        <Insight>Funding's chill — 0.01 BTC at {lev} is a sane size. Tight stop under 107.8k.</Insight>
        <Sources items={["Nado", "CMC"]} />
      </>}>
        <BroSection title="Order">
          <TreeRow>Side: 🟢 <b>LONG</b> · BTC-PERP</TreeRow>
          <TreeRow>Size: <span className="mono">0.01 BTC</span> @ <span className="mono">~$108,450</span></TreeRow>
          <TreeRow>Leverage: <b>{lev}</b></TreeRow>
          <TreeRow last>Est. margin: <span className="mono">$216.90</span></TreeRow>
        </BroSection>
      </BroCard>);
    } else if (label === "📁 Portfolio Deck") {
      go("portfolio", label, <BroCard icon="📁" title="Portfolio Deck" footer={
        <InlineKeyboard rows={PORTFOLIO_IKB} onTap={(c) => onIkbTap(c)} />
      }>
        <BroSection title="Snapshot">
          <TreeRow>🌐 Mode: <b>Mainnet</b></TreeRow>
          <TreeRow>💵 Equity: <span className="mono">$2,418.30</span></TreeRow>
          <TreeRow last>📈 Day PnL: <b className="pos">+$184.50</b> · <b className="pos">+8.2%</b></TreeRow>
        </BroSection>
        <BroSection title="Positions">
          <PositionRow side="LONG"  product="BTC-PERP" size="0.0500 BTC" upnl="+$284.30" entry="$107,402" mark="$108,450" liq="$92,180" />
          <PositionRow side="SHORT" product="ETH-PERP" size="0.5000 ETH" upnl="-$42.10"  entry="$3,920"   mark="$3,962"   liq="$4,180"  />
        </BroSection>
      </BroCard>);
    } else if (label === "🏠 Home" || label === "◀ Home" || label === "◀ Back") {
      go("home", label, null);
    } else if (label === "🌐 Execution Mode") {
      go("home", label, <BroCard icon="🌐" title="Execution Mode">
        <p>You're on <Pill tone="success">🌐 Mainnet</Pill>. Switch to <Pill tone="warn">🧪 Testnet</Pill> for paper trades.</p>
      </BroCard>);
    } else {
      go(stage, label, <BroCard icon="🤖" title="Trading Bro">
        <p>Tap one of the menu buttons below — or type a market question, fren.</p>
      </BroCard>);
    }
  }

  function onIkbTap(cb) {
    if (cb === "home") go("home", null, null);
    else if (cb === "share") go("share", null, null);
    else if (cb === "portfolio") onReplyTap("📁 Portfolio Deck");
  }

  function onSend(text) {
    const t = text.toLowerCase();
    if (t.includes("long") || t.includes("short")) {
      const side = t.includes("long") ? "LONG" : "SHORT";
      go("home", text, <BroCard icon="✅" title="Trade Preview" footer={<Insight>Sized small, stop tight. LFG.</Insight>}>
        <BroSection title="Order">
          <TreeRow>Side: {side === "LONG" ? "🟢" : "🔴"} <b>{side}</b></TreeRow>
          <TreeRow last>Parsed from: <span className="mono">{text}</span></TreeRow>
        </BroSection>
      </BroCard>);
    } else if (t.includes("share") || t.includes("pnl") || t.includes("card")) {
      go("share", text, <BroCard icon="🎉" title="Share PnL"><p>Hot session, fren. Drop this on the timeline. 🔥</p></BroCard>);
    } else {
      go("home", text, <BroCard icon="🧠" title="Trading Bro" footer={<>
        <Insight>BTC bid stack's healthy — small long with stop &lt; 107.8k.</Insight>
        <Sources items={["CMC", "Nado", "X"]} />
        <InlineKeyboard rows={ANSWER_IKB} onTap={onIkbTap} />
      </>}>
        <p><b>BTC</b> is holding $108,450. Funding chill, OI rising — bullish lean but not heroic.</p>
      </BroCard>);
    }
  }

  return (
    <TelegramChrome>
      <div className="chat-stream">
        {/* Greeting */}
        <Bubble from="in" time="14:00">
          What's up, fren — markets warming up. ☀️ Tap a button or just type something like <span className="mono">long BTC 0.01 5x market</span>.
        </Bubble>

        {/* Home command-center card (initial) */}
        {stage === "home" && history.length === 0 && (
          <div className="card-row">
            <BroCard icon="🤖" title="Home · Command Center" footer={
              <InlineKeyboard rows={HOME_IKB} onTap={onIkbTap} />
            }>
              <BroSection title="Account">
                <TreeRow>🌐 Mode: <Pill tone="success">Mainnet</Pill></TreeRow>
                <TreeRow last>💵 Balance: <span className="mono">$2,418.30</span></TreeRow>
              </BroSection>
            </BroCard>
          </div>
        )}

        {history.map((h, i) => (
          h.kind === "out"
            ? <Bubble key={i} from="out" time="14:01">{h.body}</Bubble>
            : <div key={i} className="card-row">{h.body}</div>
        ))}

        {stage === "share" && (
          <div className="card-row">
            <ShareCard />
          </div>
        )}

        {/* tap-hints */}
        {stage === "trade-dir" && <div className="hint">↓ Pick a side from the keyboard</div>}
        {stage === "trade-lev" && <div className="hint">↓ Pick leverage from the keyboard</div>}
      </div>

      <Composer onSend={onSend} />
      <ReplyKeyboard rows={replyKb} onTap={onReplyTap} />
    </TelegramChrome>
  );
}

window.App = App;
