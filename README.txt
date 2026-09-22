GALAXI V29 POSITIONS DIRECT FIX

Corrige el problema observado en V28: la IA generaba OPEN_LONG/OPEN_SHORT correctamente, pero la capa PAPER no podía completar la entrada porque faltaban las funciones de control de margen/riesgo usadas por paperOpen().

V29 restaura riskAllowsOpen() y marginFor(), y muestra el motivo si una entrada PAPER es omitida.
Mantiene PAPER, Binance USD-M, IA, multiuniverso, LONG/SHORT, memecoins/nuevas monedas y gestión de posiciones.
No activar LIVE para esta prueba.
