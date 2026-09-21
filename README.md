# GALAXI — Autonomous Trader Engine

GALAXI is an autonomous **PAPER** trading engine for Binance USDⓈ-M Futures market data.

## What changed
- Dynamic ranking of the available USDT-margined Futures universe.
- 20s/1m/3m/5m short-term context from live ticks.
- Real Binance klines for 1m/5m/15m on the highest-volume subset, refreshed independently of the 20s decision loop.
- EMA, RSI, ATR, volatility, Bollinger width, volume ratio, taker-buy ratio, breakout and multi-timeframe structure are evidence for the decision engine, not universal entry gates.
- Dynamic LONG/SHORT/HOLD context and thesis-change exits.
- Diversity/concentration penalty so the engine does not repeatedly select the same correlated style of opportunity.
- Up to 12 PAPER positions with independent margin sizing, fees, TP, SL, timeout, trailing logic and thesis-flip exit.
- STOP and RESUME controls.
- Runtime state and dashboard synchronized through JSON.

## Important
This delivery is **PAPER only**. It does not contain Binance API credentials and does not place live orders.
Do not switch to LIVE by merely changing an environment variable. A real execution layer still needs authenticated Binance order placement, order/position reconciliation, idempotency, exchange filters, reduce-only exits, error/rate-limit handling, and an authenticated user-data stream.

Binance recommends using WebSocket user-data updates for order and position state because REST responses can be delayed during volatile markets.


## V21 HOTFIX — datos de mercado
Si el WebSocket de Binance no entrega datos desde el proveedor de hosting, GALAXI usa automáticamente REST `/fapi/v1/ticker/24hr` como respaldo y continúa construyendo series y análisis. El WebSocket sigue siendo preferente cuando está disponible.
