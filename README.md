# GALAXI V22

Autonomous Binance USD-M Futures engine with an OpenAI decision layer.

## Safety defaults
- `TRADING_MODE=PAPER`
- `LIVE_ARMED=false`
- API secrets belong only in Railway Variables.

## LIVE
Set both `TRADING_MODE=LIVE` and `LIVE_ARMED=true` only when intentionally enabling real orders. V22 places entry orders and exchange-side protective STOP_MARKET / TAKE_PROFIT_MARKET reduce-only orders, reconciles account state, respects exchange filters, and has a file kill switch.

## Important
The AI decides among OPEN_LONG, OPEN_SHORT, CLOSE and HOLD. It does not control hard risk limits. No strategy guarantees profit.
