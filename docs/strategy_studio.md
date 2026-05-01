# Strategy Studio and Time Limits

Strategy Studio is the feature-flagged replacement for the legacy autonomous Bro Mode loop. It is enabled with `NADO_FEATURE_STUDIO=true`; time-limit auto-close is enabled by default with `NADO_FEATURE_TIME_LIMIT=true`.

Users do not get new slash commands. They describe a trade or strategy in chat, confirm the generated card, optionally backtest first, and then Studio routes execution through the existing Nado client, APScheduler, PostgreSQL, and execution queue.

## Natural-Language Entry Points

- `Long ETH 0.1 at 10x`
- `Buy BTC when RSI(14) on 1h drops below 30, TP +5%, SL -2%, close by Friday 17:00 UTC`
- `Short ETH 0.5 at 10x with a trailing 1% stop. Auto-close in 6h`
- `Extend the BTC auto-close to 8pm UTC`

The extractor reuses `services.bro_llm` for provider selection and credentials. Missing fields remain null and are resolved one question at a time by the clarifier.

## Architecture

```text
Telegram free text
  -> handlers/studio_handler.py
  -> studio/extractor.py + services/bro_llm.py
  -> studio/clarifier.py or studio/confirmation.py
  -> studio/execution_bridge.py
  -> trade_service / nado_client / conditional_orders
  -> condition_watcher + time_limit_watcher
  -> execution_queue workers
```

## Indicators

The v1 implementation uses pandas-based indicator formulas directly and declares `ta` as the chosen pure-Python indicator dependency for future expansion. This avoids `pandas-ta` deployment risk on the slim Docker image while keeping indicator behavior testable.

## Backtesting

The confirmation card includes `Backtest first`. V1 walks historical candles with next-bar fills, estimated fees from config, and a fixed basic slippage model. It intentionally does not model order-book depth or partial fills.

## Status

`/status` preserves the existing bot summary and appends Strategy Studio cards for active sessions and armed conditional orders. Cards include strategy id, network, state, trade direction, quantity, auto-close state, TP/SL, and latest condition evaluation. Pagination uses inline `Prev` / `Next` buttons when needed.
