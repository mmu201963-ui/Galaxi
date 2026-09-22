GALAXI V38.2 — MOVEMENT ONLY + 5% PORTFOLIO RESET

Cambios principales:
- Se elimina el STOP por pérdida de ciclo. Una pérdida durante un ciclo ya no congela el scanner ni el proceso.
- El FAST SCANNER continúa trabajando aunque las entradas nuevas estén pausadas por otros límites de riesgo.
- Se añade objetivo global de cartera: PORTFOLIO_PROFIT_TARGET_PCT=5.
- Cuando la equity alcanza +5% respecto al baseline actual, GALAXI cierra todas las posiciones y reinicia el ciclo de objetivo.
- El siguiente objetivo se calcula desde la nueva equity, permitiendo crecimiento compuesto por ciclos.
- El objetivo se comprueba antes de abrir nuevas posiciones después de actualizar las posiciones existentes.
- PAPER y LIVE (si LIVE_ARMED=true) soportan el cierre global mediante la ruta de cierre existente.

Importante:
- +5% es un objetivo operativo, no una garantía de rentabilidad.
- Los límites de drawdown/pérdida diaria siguen funcionando como bloqueos de NUEVAS entradas, pero ya no apagan el scanner.
- El modo recomendado para probar esta modificación sigue siendo PAPER.
