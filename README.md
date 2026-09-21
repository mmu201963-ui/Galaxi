# GALAXI V22

Autonomous Binance USD-M Futures engine with OpenAI decision layer.

## Railway variables
- `TRADING_MODE=PAPER` (first validation) or `LIVE`
- `LIVE_ARMED=true` only when intentionally enabling real orders
- `OPENAI_API_KEY` secret
- `OPENAI_MODEL` model available in the OpenAI account
- `BINANCE_API_KEY` secret
- `BINANCE_API_SECRET` secret

Risk defaults: 12 positions maximum, 30% total margin, 2% margin per position, leverage 5x, 20s scan, 5% daily loss stop, 10% peak drawdown stop.

Do not put API secrets in source control or chat.

## Railway
Railway must deploy this directory as the project root. The repository root must contain `package.json`, `server.js`, `index.js` and `public/index.html`. Start command: `npm start`.
