# GALAXI V32 · BEHAVIORAL ENGINE

Esta versión sustituye el motor de selección repetitiva por una arquitectura de
descubrimiento + comportamiento + memoria de resultados.

## Qué incorpora

1. MERCADO COMPLETO
   - Consulta el ticker 24h de todo el universo USDT-PERP de Binance USDⓈ-M.
   - La oportunidad se ordena principalmente por porcentaje de movimiento,
     rango y liquidez, no por precio nominal.
   - Una parte del análisis profundo rota por todo el universo para evitar que
     las mismas monedas grandes dominen siempre la selección.

2. ANÁLISIS PROFUNDO
   - 1m y 5m.
   - Momentum 1m/5m/15m/30m.
   - EMA 9/21/50.
   - RSI.
   - ATR/volatilidad.
   - volumen relativo.
   - rupturas.
   - categoría NORMAL / MEME / NEW / NEW_MEME cuando Binance proporciona
     la información necesaria.

3. COMPORTAMIENTO TOP TRADER
   - Consulta los endpoints públicos de Binance para Top Trader Long/Short
     Account Ratio y Top Trader Long/Short Position Ratio.
   - Usa la consistencia y la dirección del comportamiento agregado.
   - NO intenta identificar ni copiar wallets individuales.
   - El resultado es una señal de comportamiento colectivo.

4. OPEN INTEREST
   - Añade el open interest actual de los candidatos que llegan al cerebro IA.

5. MEMORIA DE GALAXI
   - Guarda operaciones cerradas en galaxi-learning.json.
   - Aprende estadísticas por patrón: dirección, tendencia, comportamiento
     Top Trader, régimen, volatilidad y categoría.
   - Un patrón sólo influye después de tener suficientes muestras.
   - No modifica el código automáticamente.

6. BALANCE LONG / SHORT
   - Máximo 6 LONG.
   - Máximo 6 SHORT.
   - Máximo 12 posiciones.
   - El objetivo es 50/50 cuando existan oportunidades válidas.
   - Nunca abre una operación sólo para rellenar el 50/50.

7. SALIDAS
   - TP determinista.
   - SL determinista.
   - Máximo 20 minutos por defecto.
   - La IA también puede cerrar.
   - CLOSE se procesa antes de nuevas entradas.
   - PAPER descuenta comisión estimada de entrada y salida.

8. RIESGO
   - límite de margen total.
   - límite por posición.
   - límite de pérdida diaria.
   - límite de drawdown.
   - cooldown.
   - separación mínima entre órdenes.

## Variables importantes

TRADING_MODE=PAPER
LIVE_ARMED=false
OPENAI_API_KEY=...
OPENAI_MODEL=...
BINANCE_API_KEY=...
BINANCE_API_SECRET=...

MAX_POSITIONS=12
MAX_LONG_POSITIONS=6
MAX_SHORT_POSITIONS=6
LEVERAGE=5

DEEP_SCAN_SYMBOLS=36
AI_TOP_SYMBOLS=24
TRADER_TOP_SYMBOLS=24
ROTATION_SYMBOLS=18
SCAN_INTERVAL_MS=20000

MIN_EXPECTED_NET_PCT=0.08
ESTIMATED_FEE_RATE=0.0004

PAPER_TP_PCT=0.90
PAPER_SL_PCT=0.65
PAPER_MAX_HOLD_MS=1200000

## Instalación

Reemplaza el `index.js` del proyecto GALAXI por el incluido en este paquete.

No coloques claves API dentro del código. En Railway usa Variables/Environment
Variables.

Dependencia necesaria:
- ws

El resto usa APIs nativas de Node 18+.

## Orden recomendado

1. Mantener PAPER.
2. Deploy.
3. Confirmar:
   - WS_CONNECTED=1
   - universe > 0
   - deep > 0
   - behaviorCoverage > 0
   - AI calls aumentando
   - posiciones LONG y SHORT cuando existan señales
   - cierres TP/SL/TIME o CLOSE IA
   - galaxi-learning.json creciendo después de cierres
4. Sólo después de comprobar el ciclo PAPER, decidir si se arma LIVE.

## Nota sobre rentabilidad

Esta arquitectura está diseñada para buscar expectativa neta positiva y descartar
señales con expectativa insuficiente. No puede garantizar que cada operación gane;
ningún modelo puede garantizarlo. La memoria se utiliza como evidencia estadística,
no como promesa de resultados futuros.
