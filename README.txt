GALAXI V22 - MOTOR DE UTILIDAD / GESTION ACTIVA

Reemplaza el index.js del proyecto por index.js de esta carpeta.

Cambios principales:
- Objetivo explícito: maximizar PnL neto esperado y proteger equity.
- La IA recibe PnL, mark, porcentaje no realizado y antigüedad de cada posición.
- La IA puede decidir CLOSE en cada ciclo.
- CLOSE se prioriza antes de nuevas entradas.
- Las nuevas entradas requieren expected_net_pct >= MIN_EXPECTED_NET_PCT (default 0.20%).
- HOLD solo se considera válido cuando la expectativa de mantener sigue siendo favorable.
- No se obliga a llenar las posiciones.
- Se mantiene PAPER por defecto; no activar LIVE hasta validar el comportamiento.

No incluye ni solicita API keys. Las claves permanecen en Railway.
