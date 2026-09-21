# GALAXI — Autonomous Trader Engine

Base preparada para desplegar en Railway.

## Principio
GALAXI analiza continuamente el universo de Binance Futures mediante WebSocket público y toma decisiones autónomas en PAPER: LONG, SHORT, mantener o cerrar posiciones según el estado del mercado.

Los indicadores son evidencia para el motor; no son reglas rígidas de compra/venta.

## Estado actual
- `TRADING_MODE=PAPER` por defecto.
- Hasta 12 posiciones PAPER.
- LONG + SHORT.
- Selección dinámica de símbolos.
- Análisis 20s / 1m / 3m / 5m.
- Cierre automático por TP, SL o tiempo máximo.
- Control de margen total y por posición.
- Cooldown para evitar reaperturas inmediatas.
- WebSocket público de Binance Futures.
- Dashboard y API de estado.
- STOP mediante `galaxi-control.json`.

## Importante
Esta entrega NO contiene claves de Binance y NO ejecuta órdenes reales. No cambies a LIVE ni añadas claves hasta validar completamente el motor, la gestión de órdenes y los cierres en un entorno de prueba.

## Railway
`npm install`
`npm start`

Variables: ver `.env.example`.
