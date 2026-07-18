# The Trading Bot That Does Not Need to Predict the Market

*How Nadobro uses a delta-neutral strategy on Nado to target funding and spread instead of market direction.*

Directional trading has a simple weakness: every position still depends on being right about what the market does next.

A trader can win several times in a row, increase size, and lose those gains when the next move goes against them. The problem is not always the quality of the trade idea. It is the repeated exposure to raw market direction, combined with leverage, emotion, and inconsistent risk control.

Nadobro's delta-neutral strategy takes a different approach. Instead of choosing long or short, it opens two offsetting positions: a spot long and a short perpetual position in the same market. The goal is to keep net price exposure close to zero and target the economics between the two legs.

## A Different Source of Return

In a typical delta-neutral trade, the spot position gains when the asset rises while the short perpetual position loses a similar amount. When the asset falls, the reverse happens. If the hedge remains balanced, most of the directional price movement should offset.

The result can be expressed simply:

`Net PnL = spot PnL + perp PnL + funding earned - fees - slippage - close costs`

The opportunity usually comes from funding and the spread between spot and perpetual prices. When funding is positive, long perpetual traders pay short traders, allowing the short leg to earn funding while it hedges the spot position.

A live Nado snapshot from a Nadobro run illustrates the mechanics at a small size. With approximately $100 on each leg, the spot position showed about $0.37 in profit while the perpetual leg showed about $0.11 in loss. At that moment, the combined net PnL was approximately $0.26 before any later change in funding, price, or closing costs.

The amount is modest, but the source matters. The result came from a hedged position rather than a prediction that BTC would rise or fall. It is a point-in-time example, not a profit forecast.

*[Insert screenshot: Nado BTC spread view showing the $100-per-leg example, with spot PnL, perp PnL, funding, and net PnL.]*

## Why Nadobro Is Built Differently

Opening two positions is easy. Keeping them balanced is the real work.

Nadobro opens the spot leg first, then sizes the perpetual short from the actual filled spot quantity. This matters because partial fills and price differences can leave a strategy exposed if the second leg is based only on an estimate.

The short leg uses 1x leverage by design. It is intended to function as a hedge, not as a hidden directional trade. This reduces leverage risk, although it does not remove collateral, liquidation, execution, or liquidity risk.

The strategy also monitors the conditions that support the trade. If funding turns unfavorable and remains there after confirmation, Nadobro can close both legs early. If the hedge drifts beyond its allowed range, the bot attempts to flatten the position instead of leaving one leg exposed. Close retries help reduce the chance that a failed order leaves a broken hedge behind.

Most importantly, the accounting is visible. Nadobro surfaces funding rates in Telegram and shows whether the short side earns or pays. Users can monitor the spot leg, perp leg, funding, and combined result as one strategy rather than trying to reconcile separate positions manually.

*[Insert screenshot: Nadobro funding panel showing positive and negative daily funding rates for the short leg.]*

## Why Build It on Nado

Delta-neutral trading works best when the venue makes both sides of the position easy to inspect.

Nado displays the spot position, perpetual position, spread, funding history, and net PnL in one interface. That visibility is important because delta-neutral trading is not passive yield. It is an active hedge whose value depends on funding, execution quality, liquidity, and costs.

Nado also supports a broader set of eligible markets than the usual BTC and ETH examples, including supported stock-linked markets. Nadobro can rank available funding rates and show when the short side would be charged rather than paid. That helps users avoid treating every market as a valid opportunity.

## How to Start a Delta-Neutral Run

Before starting, complete Nadobro's onboarding, connect an active trading wallet, and fund the Nado account with enough collateral for both legs and their fees. A delta-neutral run places real orders, so the first run should be small enough to observe comfortably.

1. **Open the strategy.** From Nadobro's Home screen, select **Strategy Lab**, then **Delta Neutral**.

2. **Check the funding rates.** Select **Funding Rates** to view eligible markets ranked by daily funding. A positive rate means the short perpetual leg is currently being paid. A negative rate means the short is paying. Funding can change after entry, so treat the ranking as a live input rather than a guarantee.

3. **Choose the market.** Tap an eligible market to return to the Delta Neutral dashboard with that asset selected. Confirm that both the spot and perpetual products are available.

4. **Set the core parameters.** Open **Advanced**, then **Core**. Choose the size per leg, hold period, and number of cycles. The current presets include $50, $100, and $250 per leg; hold periods of 1, 6, or 24 hours; and 1, 5, or 10 cycles. For a first run, use one cycle and a small size that meets the venue's minimum order requirements. Remember that the displayed amount applies to each leg, not to the combined position.

5. **Review the safety settings.** Under **Advanced**, review the hedge drift limit and maintenance auto-close setting. The perpetual short remains fixed at 1x. Unless there is a clear reason to change them, the default safety settings are the sensible starting point.

6. **Review and start.** Return to the dashboard and check the selected spot and perpetual pair, funding direction, size per leg, hold period, estimated fees, and available balance. Then select **Start DN**. Nadobro buys the spot asset first and sizes the perpetual short from the actual filled spot quantity.

7. **Monitor the complete position.** Use **Strategy snapshot** or `/status` to follow the two legs, current funding, funding earned, completed cycles, fees, and combined PnL. Spot and perpetual PnL should always be read together.

8. **Confirm the exit.** Nadobro closes both legs at the selected hold time, or earlier if funding turns unfavorable, the hedge drifts beyond its limit, or a safety condition is triggered. A user can also select **Stop Strategy** to request an early exit. After the run, confirm that both legs are flat and review the final net result after fees before increasing size.

## The Limits Matter

Delta-neutral strategies do not scale with conviction. They scale with liquidity.

Larger positions can face more slippage, weaker fills, and reduced returns after costs. Positive funding alone is not enough. The expected funding and spread must still exceed fees, slippage, and the cost of closing both legs.

The strategy can lose money when funding is too small, funding reverses after entry, one leg fills poorly, the hedge drifts, or liquidity weakens during the exit. A 1x hedge lowers some risks, but it does not make the trade risk-free.

This is why Nadobro treats risk controls as part of the strategy itself. Position sizing, funding direction, leg health, hedge drift, and net PnL all matter. Sometimes the correct decision is not to open the trade.

## A Trading Bot Focused on Process

Nadobro's delta-neutral strategy is designed for traders who want part of their portfolio to depend less on predicting the next candle.

It replaces a directional question, "Will the market go up or down?", with a more disciplined set of questions: Is funding attractive after costs? Is the hedge balanced? Is liquidity sufficient? Is the position still worth holding?

That is the difference between a bot that simply places orders and one that manages a strategy. Nadobro uses Nado's market structure to make the trade visible, measurable, and repeatable.

The goal is not to remove risk. It is to make risk easier to understand and manage.

*Disclaimer: This article is educational and explains product mechanics. It is not financial advice and does not promise profit. Delta-neutral strategies can lose money when fees, slippage, hedge drift, funding changes, execution failures, or liquidity conditions move against the position.*
