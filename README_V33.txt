GALAXI V33 · EDGE TERMINAL / BEHAVIORAL ENGINE

Esta versión aplica al motor GALAXI el concepto del terminal mostrado por el usuario,
adaptándolo a Binance Futures sin inventar datos de wallets individuales.

CAPAS NUEVAS
- EDGE SCANNER: ranking relativo de oportunidades LONG y SHORT.
- MISPRICING CRYPTO: basis entre mark/index + funding rate como señales de crowding,
  no como arbitraje garantizado.
- BEHAVIOR TRACKER: Top Trader agregado de Binance (20% superior por margen), con
  ratio de cuentas, ratio de posiciones, consistencia y cambio 5m.
- OI FLOW: cambio de Open Interest para confirmar o debilitar el movimiento.
- COPY-BEHAVIOR: la señal agregada puede confirmar una operación, pero GALAXI no
  copia wallets individuales ni inventa identidades.
- EDGE LEADERBOARD: muestra las mejores oportunidades y separa Edge LONG/SHORT.
- LEARNING: mantiene la memoria de operaciones cerradas y patrones propios.
- 50/50 estructural: máximo 6 LONG + 6 SHORT, sin forzar una entrada si no hay edge.

SCANNER
1. Descubre todo el universo USD-M USDT perpetual.
2. Hace deep scan rotativo.
3. Enriquece candidatos con Top Trader + OI + funding/basis.
4. Calcula Edge LONG y Edge SHORT.
5. Ordena por edge, no por popularidad ni precio nominal.
6. La IA recibe el leaderboard y decide OPEN/CLOSE/HOLD.

SEGURIDAD
- PAPER es el modo predeterminado.
- LIVE requiere TRADING_MODE=LIVE y LIVE_ARMED=true.
- No hay garantía de ganancias ni de que cada operación sea positiva.
- Antes de LIVE se recomienda añadir órdenes STOP_MARKET/TAKE_PROFIT_MARKET nativas
  en Binance para que una caída de Railway no deje posiciones sin protección.

RAILWAY
- Servidor HTTP en 0.0.0.0:$PORT.
- /health
- /state
- Dashboard en /
