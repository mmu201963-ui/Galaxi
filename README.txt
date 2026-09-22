GALAXI V25 AI FIXED

Esta versión parte de GALAXI V23 MULTIUNIVERSE y conserva:
- PAPER por defecto
- Binance USD-M Futures
- universo normal + memecoins + nuevos listados
- LONG y SHORT
- filtro de expectativa neta positiva
- máximo 12 posiciones
- protección de riesgo

CORRECCIÓN PRINCIPAL:
- diagnóstico explícito de OpenAI
- comprueba al arrancar que OPENAI_API_KEY y el modelo estén disponibles
- registra intentos, llamadas exitosas, errores, estado, latencia y último error
- si OpenAI falla, el panel lo muestra en lugar de aparentar que el motor está trabajando
- modelo por defecto: gpt-5.6-luna (puede sobrescribirse con OPENAI_MODEL)
- dashboard V24 para no confundirlo con V22

NO activar LIVE. Mantener TRADING_MODE=PAPER mientras se verifica que la IA realiza llamadas y genera decisiones.
Las claves de Binance/OpenAI deben permanecer únicamente como variables de entorno de Railway.


FIX V25: define cleanJsonText() before askAI so successful OpenAI responses can be parsed.

The default OpenAI API model is gpt-5.6-luna.
