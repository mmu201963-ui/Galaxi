GALAXI V38 · ANOMALY ENGINE
============================

BASE
----
GALAXI V37 MISPRICING + FAST EDGE.

NUEVO EN V38
------------
1. BTCUSDT es una referencia rápida común para todo el mercado.
2. Cada mercado calcula un residual relativo:
      movimiento de la moneda - movimiento de BTC
3. También calcula velocidad relativa frente a BTC.
4. Se genera ANOMALY SCORE con residual + velocidad relativa + aceleración.
5. Una anomalía no fuerza una dirección. LONG/SHORT necesita confirmación con
   velocidad, volumen, OI, funding, basis, estructura y expectativa neta.
6. Las señales rápidas llevan timestamp y expiran para evitar usar anomalías viejas.
7. El FAST SCANNER continúa vigilando el universo completo mediante WebSocket.
8. Las anomalías frescas tienen prioridad para entrar al análisis profundo.
9. Se mantiene la cartera de hasta 12 posiciones: máximo 6 LONG y 6 SHORT.
10. Se mantienen los controles de pérdida de V37: pérdida diaria, pérdida por ciclo,
    TP/SL y gestión de salida.

ARQUITECTURA
------------
MERCADO COMPLETO
      -> FAST SCANNER (~1s)
      -> BTC RELATIVE ANOMALY ENGINE
      -> FUTURES/SPOT + MARK/INDEX + FUNDING
      -> MOMENTUM/VOLUMEN/OI/ESTRUCTURA
      -> TOP TRADER AGREGADO
      -> IA
      -> VALIDACIÓN DE RIESGO
      -> EJECUCIÓN
      -> GESTIÓN DE SALIDA
      -> MEMORIA DE RESULTADOS

IMPORTANTE
----------
El residual frente a BTC identifica una desviación relativa; no demuestra por sí solo
que el precio vaya a revertir ni constituye arbitraje sin riesgo. V38 usa la anomalía
como una capa adicional de selección y confirmación.

PAPER primero. LIVE requiere TRADING_MODE=LIVE y LIVE_ARMED=true. Para LIVE de producción
se recomienda añadir órdenes STOP_MARKET/TAKE_PROFIT_MARKET nativas en Binance y un
User Data Stream para que una caída del proceso no deje una posición sin protección.

VARIABLES NUEVAS
----------------
BTC_REFERENCE_SYMBOL=BTCUSDT
ANOMALY_MIN_RESIDUAL_PCT=0.10
ANOMALY_STRONG_RESIDUAL_PCT=0.25
ANOMALY_STALE_MS=3000
ANOMALY_SCORE_WEIGHT=0.55


V38.1 MOVEMENT ONLY
- MOVEMENT_ONLY=true makes the fast scanner the decision engine.
- No anomaly score, expected-net, confidence, learning, funding, OI, or Top-Trader filter is required for OPEN decisions.
- Direction is taken directly from short-window Binance price velocity: positive = LONG, negative = SHORT.
- Scanner refreshes every ~1s and the dashboard refreshes every 1s.
- Portfolio/risk limits, cooldowns, TP/SL, hard-loss, timeout and daily/cycle risk stops remain active.
- PAPER remains the default.
