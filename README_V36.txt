GALAXI V36 — AUDITED PAPER RISK / PNL

Objetivo: corregir el comportamiento observado en V35 antes de cualquier LIVE.

Cambios principales:
- TP determinista: +$8 netos por posición.
- Hard loss determinista: -$12 netos por posición.
- Máximo 12 posiciones: 6 LONG y 6 SHORT.
- No se obliga a llenar 12; cada símbolo/dirección compite de forma independiente.
- Límite de pérdida realizada por defecto: 2% del capital.
- Límite de drawdown por defecto: 3% del capital.
- Los límites se evalúan inmediatamente después de cierres PAPER, antes de nuevas entradas.
- Auditoría explícita de PnL: capital inicial, realizado, no realizado, equity y discrepancia.
- Corregido aprendizaje para no descontar comisiones dos veces.
- Mantiene entrada temprana y gestión activa de beneficios.
- PAPER por defecto. No habilitar LIVE hasta validar varias operaciones.

Importante: un hard loss de -$12 es un tope determinista en PAPER, no una garantía sobre ejecución LIVE.
