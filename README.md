# GALAXI V22 FINAL

Motor autónomo para Binance USD-M con decisiones LONG/SHORT/CLOSE/HOLD mediante OpenAI Responses API.

## Railway Variables

- `TRADING_MODE=PAPER` para validar primero sin dinero real.
- `OPENAI_API_KEY` = tu clave de OpenAI (Railway Secret).
- `OPENAI_MODEL=gpt-5.6-luna` (o un modelo disponible en tu cuenta).
- `BINANCE_API_KEY` y `BINANCE_API_SECRET` sólo como Railway Secrets si vas a usar LIVE.
- `LIVE_ARMED=false` mientras se valida PAPER.
- `MAX_POSITIONS=12`
- `LEVERAGE=5`
- `SCAN_INTERVAL_MS=20000`

Para LIVE, cambia deliberadamente:
- `TRADING_MODE=LIVE`
- `LIVE_ARMED=true`

No pegues claves en el chat.

## Correcciones de esta versión

1. Las claves de Binance sólo se envían en endpoints firmados; las llamadas públicas no llevan `X-MBX-APIKEY`.
2. Las claves se validan antes de crear headers para evitar el error `ByteString`.
3. Modelo por defecto: `gpt-5.6-luna`.
4. El wrapper web permanece disponible aunque el motor hijo se reinicie.
5. `/api/health`, `/api/status`, `/api/start` y `/api/stop`.
