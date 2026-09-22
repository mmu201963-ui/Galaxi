import fs from 'node:fs';
import crypto from 'node:crypto';
import WebSocket from 'ws';

/*
GALAXI V26 AI DIAGNOSTIC
- Real-time Binance USD-M Futures market data
- AI decision engine through OpenAI Responses API
- PAPER by default
- LIVE only when TRADING_MODE=LIVE and LIVE_ARMED=true
- AI chooses OPEN_LONG / OPEN_SHORT / CLOSE / HOLD
- Risk limits remain deterministic and cannot be overridden by the model
*/

const runtimeFile = 'galaxi-runtime.json';
const controlFile = 'galaxi-control.json';

const cfg = {
  mode: String(process.env.TRADING_MODE || 'PAPER').toUpperCase(),
  liveArmed: String(process.env.LIVE_ARMED || 'false').toLowerCase() === 'true',
  openaiKey: process.env.OPENAI_API_KEY || '',
  openaiModel: process.env.OPENAI_MODEL || 'gpt-5.6-luna',
  binanceKey: process.env.BINANCE_API_KEY || '',
  binanceSecret: process.env.BINANCE_API_SECRET || '',
  binanceBase: process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com',
  wsUrl: process.env.BINANCE_FUTURES_WS || 'wss://fstream.binance.com/ws/!miniTicker@arr',

  capital: Number(process.env.PAPER_START_CAPITAL || 10000),
  maxPositions: Math.min(12, Math.max(1, Number(process.env.MAX_POSITIONS || 12))),
  maxTotalMarginPct: Math.min(50, Math.max(1, Number(process.env.MAX_TOTAL_MARGIN_PCT || 30))),
  maxPositionMarginPct: Math.min(5, Math.max(0.25, Number(process.env.MAX_POSITION_MARGIN_PCT || 2))),
  leverage: Math.min(10, Math.max(1, Number(process.env.LEVERAGE || 5))),

  scanMs: Math.max(15000, Number(process.env.SCAN_INTERVAL_MS || 20000)),
  aiTimeoutMs: Math.max(5000, Number(process.env.AI_TIMEOUT_MS || 15000)),
  aiTopSymbols: Math.min(30, Math.max(12, Number(process.env.AI_TOP_SYMBOLS || 24))),
  newListingDays: Math.max(1, Number(process.env.NEW_LISTING_DAYS || 30)),
  memeSlots: Math.max(2, Number(process.env.MEME_SLOTS || 6)),
  newSlots: Math.max(2, Number(process.env.NEW_LISTING_SLOTS || 6)),
  klineLimit: Math.min(150, Math.max(50, Number(process.env.KLINE_LIMIT || 80))),
  marketConcurrency: Math.min(8, Math.max(2, Number(process.env.MARKET_CONCURRENCY || 5))),
  restTimeoutMs: Math.max(5000, Number(process.env.REST_TIMEOUT_MS || 12000)),

  // These are risk/execution protections, not opportunity filters.
  maxDailyLossPct: Math.min(20, Math.max(0.5, Number(process.env.MAX_DAILY_LOSS_PCT || 5))),
  maxDrawdownPct: Math.min(30, Math.max(1, Number(process.env.MAX_DRAWDOWN_PCT || 10))),
  minSecondsBetweenOrders: Math.max(2, Number(process.env.MIN_SECONDS_BETWEEN_ORDERS || 5)),
  maxActionsPerCycle: Math.min(4, Math.max(1, Number(process.env.MAX_ACTIONS_PER_CYCLE || 4))),
  minExpectedNetPct: Math.max(0.01, Number(process.env.MIN_EXPECTED_NET_PCT || 0.02)),

  paperTpPct: Number(process.env.PAPER_TP_PCT || 1.2),
  paperSlPct: Number(process.env.PAPER_SL_PCT || 0.7),
  paperMaxHoldMs: Number(process.env.PAPER_MAX_HOLD_MS || 1800000),
};

const state = {
  running: true,
  mode: cfg.mode,
  ai: true,
  aiModel: cfg.openaiModel,
  liveArmed: cfg.liveArmed,
  equity: cfg.capital,
  initialCapital: cfg.capital,
  realizedPnl: 0,
  unrealizedPnl: 0,
  todayPnl: 0,
  drawdownPct: 0,
  dailyLossPct: 0,

  symbols: 0,
  warmSymbols: 0,
  cycle: 0,
  wsConnected: 0,
  wsExpected: 1,
  candidates: 0,
  riskApproved: 0,
  portfolioCount: 0,

  regime: 'MIXTO',
  timeframes: { '20s': '—', '1m': '—', '3m': '—', '5m': '—' },
  longPct: 50,
  shortPct: 50,

  positions: [],
  ranking: [],
  history: [],
  news: [],

  lastSignal: 'Esperando datos para el cerebro IA…',
  lastError: null,
  aiDecision: null,
  aiReasoning: '',
  aiCalls: 0,
  aiAttempts: 0,
  aiSuccess: 0,
  aiErrors: 0,
  aiReady: false,
  aiStatus: 'INICIANDO',
  aiLastError: null,
  aiLastCallAt: null,
  aiLatencyMs: 0,
  restCalls: 0,
  rate429: 0,
  rate418: 0,
  lastUpdate: null
};

const ticks = new Map();
const cooldown = new Map();
const marketInfo = new Map();
let ws = null;
let stopped = false;
let reconnectTimer = null;
let loopBusy = false;
let lastOrderTs = 0;
let serverOffset = 0;
let lastAccount = null;

function clamp(n, a, b) { return Math.max(a, Math.min(b, n)); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function now() { return Date.now(); }
function round(n, d = 6) {
  const p = 10 ** d;
  return Math.round(Number(n) * p) / p;
}

// OpenAI Responses can return JSON either as plain text or wrapped in a
// markdown code fence. Normalize only the transport wrapper; never alter the
// JSON content itself. This helper is intentionally local to the engine so a
// malformed model response is reported as a JSON error rather than a missing
// function error.
function cleanJsonText(value) {
  let s = String(value ?? '').trim();
  if (!s) return s;

  if (s.startsWith('```')) {
    s = s.replace(/^```(?:json)?\s*/i, '');
    s = s.replace(/\s*```$/i, '');
  }

  const first = s.indexOf('{');
  const last = s.lastIndexOf('}');
  if (first >= 0 && last > first) s = s.slice(first, last + 1);
  return s.trim();
}

function writeState() {
  state.portfolioCount = state.positions.length;
  state.unrealizedPnl = state.positions.reduce((s, p) => s + Number(p.pnl || 0), 0);

  if (cfg.mode === 'PAPER') {
    state.equity = cfg.capital + state.realizedPnl + state.unrealizedPnl;
  }

  const dd = Math.max(0, cfg.capital - state.equity);
  state.drawdownPct = cfg.capital ? dd / cfg.capital * 100 : 0;
  state.dailyLossPct = Math.min(0, state.realizedPnl / cfg.capital * 100);
  state.lastUpdate = new Date().toISOString();

  fs.writeFileSync(runtimeFile, JSON.stringify(state, null, 2));
}

function pushHistory(item) {
  state.history.unshift({ time: new Date().toISOString(), ...item });
  state.history = state.history.slice(0, 100);
}

async function rest(path, options = {}, signed = false) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), cfg.restTimeoutMs);
  const method = options.method || 'GET';
  let params = { ...(options.params || {}) };

  if (signed) {
    params.timestamp = Date.now() + serverOffset;
    params.recvWindow = 5000;
  }

  let query = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) query.set(k, String(v));
  }

  if (signed) {
    const signature = crypto
      .createHmac('sha256', cfg.binanceSecret)
      .update(query.toString())
      .digest('hex');
    query.set('signature', signature);
  }

  const url = `${cfg.binanceBase}${path}${query.toString() ? `?${query}` : ''}`;
  const headers = {};
  if (cfg.binanceKey) headers['X-MBX-APIKEY'] = cfg.binanceKey;
  if (method !== 'GET') headers['Content-Type'] = 'application/x-www-form-urlencoded';

  state.restCalls++;
  let res;
  let text;
  try {
    res = await fetch(url, {
      method,
      headers,
      body: method === 'GET' ? undefined : query.toString(),
      signal: controller.signal
    });
    text = await res.text();
  } catch (e) {
    if (e?.name === 'AbortError') throw new Error(`BINANCE TIMEOUT ${path}`);
    throw new Error(`BINANCE NETWORK ${path}: ${e?.message || e}`);
  } finally {
    clearTimeout(timeout);
  }

  if (res.status === 429) state.rate429++;
  if (res.status === 418) state.rate418++;

  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (!res.ok) {
    const msg = data?.msg || `HTTP ${res.status}`;
    throw new Error(`BINANCE ${res.status}: ${msg}`);
  }
  return data;
}

async function syncServerTime() {
  try {
    const data = await rest('/fapi/v1/time');
    serverOffset = Number(data.serverTime) - Date.now();
  } catch (e) {
    state.lastError = `Time sync: ${e.message}`;
  }
}

async function loadExchangeInfo() {
  const data = await rest('/fapi/v1/exchangeInfo');
  marketInfo.clear();

  for (const s of data.symbols || []) {
    if (s.status !== 'TRADING' || s.quoteAsset !== 'USDT' || s.contractType !== 'PERPETUAL') continue;

    const lot = (s.filters || []).find(x => x.filterType === 'LOT_SIZE');
    const price = (s.filters || []).find(x => x.filterType === 'PRICE_FILTER');
    marketInfo.set(s.symbol, {
      baseAsset: s.baseAsset || s.symbol.replace(/USDT$/, ''),
      onboardDate: Number(s.onboardDate || 0),
      qtyStep: Number(lot?.stepSize || 0.001),
      minQty: Number(lot?.minQty || 0),
      tickSize: Number(price?.tickSize || 0.00001)
    });
  }
  state.symbols = marketInfo.size;
}


const MEME_ASSETS = new Set([
  'DOGE','SHIB','PEPE','FLOKI','BONK','WIF','MEME','BOME','MEW','MOG','BRETT','TURBO',
  'NEIRO','NEIROETH','DOGS','PNUT','ACT','POPCAT','1000SATS','1000RATS','1000BONK','1000FLOKI',
  '1000PEPE','1000SHIB','1000CHEEMS','MOODENG','GOAT','PENGU','SPX','TRUMP','MELANIA','TOSHI'
]);
const MEME_HINTS = /(DOGE|SHIB|PEPE|FLOKI|BONK|WIF|MEME|BOME|MEW|MOG|BRETT|TURBO|NEIRO|DOGS|PNUT|POPCAT|SATS|RATS|CHEEMS|MOODENG|GOAT|PENGU|TRUMP|MELANIA|TOSHI)/i;

function marketCategory(symbol) {
  const m = marketInfo.get(symbol) || {};
  const base = String(m.baseAsset || symbol.replace(/USDT$/, '')).toUpperCase();
  const meme = MEME_ASSETS.has(base) || MEME_HINTS.test(base);
  const ageMs = m.onboardDate > 0 ? now() - m.onboardDate : Infinity;
  const isNew = ageMs >= 0 && ageMs <= cfg.newListingDays * 86400000;
  if (meme && isNew) return 'NEW_MEME';
  if (meme) return 'MEME';
  if (isNew) return 'NEW';
  return 'NORMAL';
}

function selectAICandidates() {
  const all = [...ticks.entries()]
    .filter(([s,t]) => marketInfo.has(s) && Number(t.price) > 0)
    .map(([symbol,t]) => ({ symbol, t, category: marketCategory(symbol) }))
    .sort((a,b) => Number(b.t.volume || 0) - Number(a.t.volume || 0));

  const memes = all.filter(x => x.category === 'MEME' || x.category === 'NEW_MEME');
  const fresh = all.filter(x => x.category === 'NEW' || x.category === 'NEW_MEME');
  const normal = all.filter(x => x.category === 'NORMAL');
  const selected = [];
  const used = new Set();
  const take = (arr, n) => {
    for (const x of arr) {
      if (selected.length >= cfg.aiTopSymbols || n <= 0) break;
      if (used.has(x.symbol)) continue;
      selected.push(x); used.add(x.symbol); n--;
    }
  };
  take(memes, cfg.memeSlots);
  take(fresh, cfg.newSlots);
  take(normal, cfg.aiTopSymbols);
  take(all, cfg.aiTopSymbols);
  return selected.slice(0, cfg.aiTopSymbols).map(x => [x.symbol, x.t]);
}

function precisionFromStep(step) {
  const s = String(step);
  if (!s.includes('.')) return 0;
  return Math.max(0, s.split('.')[1].replace(/0+$/, '').length);
}

function normalizeQty(symbol, qty) {
  const m = marketInfo.get(symbol);
  if (!m) return 0;
  const step = m.qtyStep;
  const p = precisionFromStep(step);
  const q = Math.floor(Number(qty) / step) * step;
  return round(q, p);
}

function normalizePrice(symbol, price) {
  const m = marketInfo.get(symbol);
  if (!m) return Number(price);
  const step = m.tickSize;
  const p = precisionFromStep(step);
  const q = Math.round(Number(price) / step) * step;
  return round(q, p);
}

function pctMove(a, b) {
  return b > 0 ? (a / b - 1) * 100 : 0;
}

function ema(values, period) {
  if (!values.length) return 0;
  const k = 2 / (period + 1);
  let e = values[0];
  for (let i = 1; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}

function rsi(values, period = 14) {
  if (values.length <= period) return 50;
  let gain = 0, loss = 0;
  for (let i = 1; i <= period; i++) {
    const d = values[i] - values[i - 1];
    if (d >= 0) gain += d; else loss -= d;
  }
  let ag = gain / period, al = loss / period;
  for (let i = period + 1; i < values.length; i++) {
    const d = values[i] - values[i - 1];
    ag = ((ag * (period - 1)) + Math.max(d, 0)) / period;
    al = ((al * (period - 1)) + Math.max(-d, 0)) / period;
  }
  if (al === 0) return 100;
  return 100 - 100 / (1 + ag / al);
}

function atr(klines, period = 14) {
  if (klines.length < period + 2) return 0;
  const trs = [];
  for (let i = 1; i < klines.length; i++) {
    const h = Number(klines[i][2]), l = Number(klines[i][3]), pc = Number(klines[i - 1][4]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  const recent = trs.slice(-period);
  return recent.reduce((a, b) => a + b, 0) / recent.length;
}

async function fetchKlines(symbol, interval = '1m', limit = cfg.klineLimit) {
  return rest('/fapi/v1/klines', {
    params: { symbol, interval, limit }
  });
}

function analyzeKlines(symbol, k1, k5) {
  const closes1 = k1.map(x => Number(x[4]));
  const highs1 = k1.map(x => Number(x[2]));
  const lows1 = k1.map(x => Number(x[3]));
  const volumes1 = k1.map(x => Number(x[5]));

  const price = closes1.at(-1) || Number(ticks.get(symbol)?.price || 0);
  const e9 = ema(closes1.slice(-40), 9);
  const e21 = ema(closes1.slice(-60), 21);
  const e50 = ema(closes1.slice(-70), 50);
  const r = rsi(closes1, 14);
  const a = atr(k1, 14);
  const volNow = volumes1.slice(-10).reduce((x, y) => x + y, 0) / 10;
  const volPrev = volumes1.slice(-30, -10).reduce((x, y) => x + y, 0) / 20 || volNow;
  const volumeRatio = volPrev ? volNow / volPrev : 1;

  const m1 = pctMove(price, closes1.at(-2));
  const m5 = pctMove(price, closes1.at(-6));
  const m15 = pctMove(price, closes1.at(-16));
  const m30 = pctMove(price, closes1.at(-31));

  const closes5 = k5.map(x => Number(x[4]));
  const m5tf = pctMove(closes5.at(-1), closes5.at(-4));
  const e20_5 = ema(closes5.slice(-40), 20);
  const e50_5 = ema(closes5.slice(-70), 50);

  const recentHigh = Math.max(...highs1.slice(-20));
  const recentLow = Math.min(...lows1.slice(-20));
  const breakoutUp = price > recentHigh * 0.9995;
  const breakoutDown = price < recentLow * 1.0005;

  let bias = 'NEUTRAL';
  const bull = e9 > e21 && e21 > e50 && m5 > 0 && m15 > 0 && e20_5 >= e50_5;
  const bear = e9 < e21 && e21 < e50 && m5 < 0 && m15 < 0 && e20_5 <= e50_5;
  if (bull) bias = 'LONG';
  if (bear) bias = 'SHORT';

  return {
    symbol, price,
    bias,
    rsi: round(r, 2),
    atrPct: price ? round(a / price * 100, 4) : 0,
    volumeRatio: round(volumeRatio, 2),
    momentum1m: round(m1, 3),
    momentum5m: round(m5, 3),
    momentum15m: round(m15, 3),
    momentum30m: round(m30, 3),
    momentum5mTF: round(m5tf, 3),
    ema9: round(e9, 8),
    ema21: round(e21, 8),
    ema50: round(e50, 8),
    ema20_5m: round(e20_5, 8),
    ema50_5m: round(e50_5, 8),
    breakoutUp,
    breakoutDown,
    high20: recentHigh,
    low20: recentLow
  };
}

async function seedTicksFromRest() {
  // WebSocket is the preferred live feed, but a connected socket can still
  // deliver no usable ticker rows for a short period. Seed prices/volume from
  // Binance REST so the analysis engine cannot remain stuck at "analizados 0".
  const data = await rest('/fapi/v1/ticker/24hr');
  let seeded = 0;
  for (const t of data || []) {
    const symbol = t.symbol;
    if (!symbol || !marketInfo.has(symbol)) continue;
    const price = Number(t.lastPrice);
    const volume = Number(t.quoteVolume || 0);
    if (price > 0) {
      ticks.set(symbol, { price, volume, ts: now() });
      seeded++;
    }
  }
  return seeded;
}


async function refreshPaperPrices() {
  // PAPER must use a fresh mark price on every cycle. The WS is useful for
  // streaming, but the REST snapshot prevents a stale tick from freezing
  // current/PnL and therefore preventing TP/SL closes.
  const data = await rest('/fapi/v1/ticker/price');
  const ts = now();
  let refreshed = 0;
  for (const t of data || []) {
    const symbol = t.symbol;
    if (!symbol || !marketInfo.has(symbol)) continue;
    const price = Number(t.price);
    if (price > 0) {
      const old = ticks.get(symbol);
      ticks.set(symbol, { price, volume: Number(old?.volume || 0), ts });
      refreshed++;
    }
  }
  state.paperPriceRefresh = refreshed;
  return refreshed;
}

async function buildMarketSnapshot() {
  let candidates = selectAICandidates();

  if (candidates.length < Math.min(8, cfg.aiTopSymbols)) {
    try {
      const seeded = await seedTicksFromRest();
      if (seeded) {
        candidates = selectAICandidates();
        console.log(`TICKER_REST_SEED=${seeded}`);
      }
    } catch (e) {
      state.lastError = `Ticker REST: ${e.message}`;
    }
  }

  const rows = [];
  let errors = 0;
  const errorSamples = [];
  for (let i = 0; i < candidates.length; i += cfg.marketConcurrency) {
    const batch = candidates.slice(i, i + cfg.marketConcurrency);
    const results = await Promise.all(batch.map(async ([symbol]) => {
      try {
        const [k1, k5] = await Promise.all([fetchKlines(symbol, '1m'), fetchKlines(symbol, '5m', 70)]);
        if (!Array.isArray(k1) || k1.length < 35 || !Array.isArray(k5) || k5.length < 20) {
          throw new Error(`KLINE_INSUFFICIENT ${symbol} k1=${k1?.length || 0} k5=${k5?.length || 0}`);
        }
        const a = analyzeKlines(symbol, k1, k5);
        return { ...a, category: marketCategory(symbol), quoteVolume24h: round(Number(ticks.get(symbol)?.volume || 0), 0) };
      } catch (e) {
        errors++;
        if (errorSamples.length < 3) errorSamples.push(`${symbol}: ${e.message}`);
        return null;
      }
    }));
    for (const r of results) if (r) rows.push(r);
  }

  if (errors && !rows.length) state.lastError = `Mercado: 0/${candidates.length} analizados · ${errorSamples.join(' | ')}`;
  else if (errors) state.lastError = `Mercado: ${rows.length}/${candidates.length} analizados · fallos=${errors}`;
  else state.lastError = null;

  rows.sort((a,b) => {
    const aScore = Math.max(Math.abs(a.momentum5m), Math.abs(a.momentum15m), Math.abs(a.momentum30m)) * (1 + Math.min(3, a.volumeRatio || 1) * 0.15);
    const bScore = Math.max(Math.abs(b.momentum5m), Math.abs(b.momentum15m), Math.abs(b.momentum30m)) * (1 + Math.min(3, b.volumeRatio || 1) * 0.15);
    return bScore - aScore;
  });

  state.warmSymbols = rows.length;
  state.ranking = rows.slice(0, Math.min(30, rows.length));
  return rows;
}

async function askAI(market, account) {
  state.aiAttempts++;
  state.aiLastCallAt = new Date().toISOString();
  state.aiStatus = 'LLAMANDO IA';
  if (!cfg.openaiKey) {
    state.aiReady = false;
    state.aiStatus = 'SIN API KEY';
    state.aiLastError = 'OPENAI_API_KEY no configurada';
    throw new Error('OPENAI_API_KEY no configurada');
  }
  const key = String(cfg.openaiKey).trim();
  if (/[^\x00-\x7F]/.test(key)) {
    state.aiReady = false;
    state.aiStatus = 'API KEY INVÁLIDA';
    state.aiLastError = 'OPENAI_API_KEY contiene caracteres no ASCII';
    throw new Error('OPENAI_API_KEY contiene caracteres no ASCII');
  }
  const startedAt = Date.now();

  const positions = (account.positions || []).map(p => ({
    symbol: p.symbol,
    side: p.side,
    entry: p.entry,
    mark: p.mark,
    pnl: p.pnl,
    margin: p.margin,
    unrealizedPct: p.margin ? (Number(p.pnl || 0) / Number(p.margin)) * 100 : 0,
    ageMinutes: p.openedTs ? Math.max(0, (Date.now() - Number(p.openedTs)) / 60000) : 0
  }));

  const payload = {
    timestamp: new Date().toISOString(),
    regime: state.regime,
    longPct: state.longPct,
    shortPct: state.shortPct,
    equity: state.equity,
    positions,
    market: market.slice(0, cfg.aiTopSymbols),
    universePolicy: {
      normal: 'competencia abierta',
      meme: 'incluidas activamente; no requieren cuota de capital',
      newListings: `listados de hasta ${cfg.newListingDays} días incluidos activamente`,
      selection: 'las oportunidades compiten por expectativa neta, liquidez, volatilidad y confirmación'
    }
  };

  const instructions = `
Eres GALAXI, un motor autónomo de decisión para futuros USD-M de Binance.
Tu trabajo es analizar el snapshot y decidir qué hacer AHORA. No uses una regla fija de score.
Combina estructura de mercado, momentum multitemporal, RSI, EMA, ATR, volumen, rupturas,
régimen y contexto de las posiciones existentes. Busca oportunidades LONG y SHORT y evita
abrir repetidamente el mismo símbolo sin una nueva tesis.

UNIVERSO DE OPORTUNIDADES:
- No te limites a BTC/ETH ni a las monedas que hayan operado ciclos anteriores.
- Considera simultáneamente monedas normales, altcoins, MEMECOINS y LISTADOS NUEVOS.
- Los listados nuevos incluidos en market llevan category NEW o NEW_MEME. Evalúalos con
  especial atención a liquidez, spread, volumen, volatilidad y calidad de datos.
- Las memecoins llevan category MEME o NEW_MEME. No debes ignorarlas por ser memecoins,
  pero tampoco debes abrirlas por ser memecoins: compiten por la misma expectativa neta.
- LONG y SHORT deben competir en igualdad. Un régimen BAJISTA no obliga a abrir SHORT,
  pero sí permite SHORT cuando exista una entrada confirmada; lo mismo para LONG.
- Prioriza oportunidades con confirmación desde AHORA, no movimientos ya demasiado extendidos.

OBJETIVO ECONÓMICO PRIORITARIO:
- Tu objetivo es maximizar el PnL NETO esperado y proteger la equity.
- No operes por obligación ni por cantidad de posiciones.
- Solo abras una operación cuando la expectativa neta sea favorable después de spread, comisiones y riesgo y sea de al menos ${cfg.minExpectedNetPct}%.
- Si no hay posiciones abiertas y existe una oportunidad con sesgo claro, confirmación multitemporal, volumen suficiente y expectativa neta >= ${cfg.minExpectedNetPct}%, debes proponer OPEN_LONG u OPEN_SHORT en vez de devolver HOLD por exceso de prudencia.
- No confundas una tendencia correcta con una entrada rentable: importa el precio actual y el movimiento esperado desde AHORA.

GESTIÓN DE POSICIONES (OBLIGATORIA):
- En CADA ciclo evalúa las posiciones existentes usando entry, mark, PnL, unrealizedPct, dirección y antigüedad.
- Una posición NO queda bloqueada por su tesis original. Puedes cerrarla en cualquier ciclo.
- Si la expectativa futura de una posición es negativa, prioriza CLOSE.
- Si existe una oportunidad claramente mejor para el capital, puedes cerrar una posición débil y reasignar el margen.
- No mantengas una posición perdedora solo esperando que vuelva al precio de entrada.
- HOLD solo cuando conservar la posición tenga expectativa neta favorable.

ACCIONES:
- Las acciones permitidas son OPEN_LONG, OPEN_SHORT, CLOSE y HOLD.
- CLOSE tiene prioridad sobre OPEN cuando una posición existente perdió expectativa positiva.
- Para CLOSE usa una posición existente.
- Para OPEN el campo margin_pct es porcentaje de equity destinado a margen, entre 0.25 y 2.
- Para OPEN debes informar expected_net_pct: utilidad neta esperada en porcentaje desde el precio actual, después de costes.
- No inventes símbolos. Usa únicamente símbolos presentes en market.
- No cierres una posición solo por ruido de un tick; utiliza contexto multitemporal y expectativa futura.
- No puedes modificar los límites de riesgo del sistema.
- Devuelve ÚNICAMENTE JSON válido.

Formato:
{
  "regime": "ALCISTA|BAJISTA|MIXTO",
  "actions": [
    {
      "action": "OPEN_LONG|OPEN_SHORT|CLOSE|HOLD",
      "symbol": "BTCUSDT",
      "margin_pct": 1.0,
      "reason": "explicación breve basada en expectativa neta actual",
      "confidence": 0,
      "expected_net_pct": 0
    }
  ],
  "summary": "resumen breve"
}
`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), cfg.aiTimeoutMs);

  try {
    const res = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Authorization': `Bearer ${key}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: cfg.openaiModel,
        instructions,
        input: JSON.stringify(payload),
        text: {
          format: {
            type: 'json_schema',
            name: 'galaxi_trade_decision',
            strict: true,
            schema: {
              type: 'object',
              additionalProperties: false,
              properties: {
                regime: { type: 'string', enum: ['ALCISTA', 'BAJISTA', 'MIXTO'] },
                actions: {
                  type: 'array',
                  maxItems: cfg.maxActionsPerCycle,
                  items: {
                    type: 'object',
                    additionalProperties: false,
                    properties: {
                      action: { type: 'string', enum: ['OPEN_LONG', 'OPEN_SHORT', 'CLOSE', 'HOLD'] },
                      symbol: { type: 'string' },
                      margin_pct: { type: 'number' },
                      reason: { type: 'string' },
                      confidence: { type: 'number' },
                      expected_net_pct: { type: 'number' }
                    },
                    required: ['action', 'symbol', 'margin_pct', 'reason', 'confidence', 'expected_net_pct']
                  }
                },
                summary: { type: 'string' }
              },
              required: ['regime', 'actions', 'summary']
            }
          }
        }
      })
    });

    const text = await res.text();
    if (!res.ok) {
      state.aiReady = false;
      state.aiStatus = `ERROR ${res.status}`;
      state.aiLastError = `OPENAI ${res.status}: ${text.slice(0, 700)}`;
      throw new Error(`OPENAI ${res.status}: ${text.slice(0, 500)}`);
    }

    const data = JSON.parse(text);
    const outputText = data.output_text ||
      data.output?.flatMap(x => x.content || []).find(x => x.type === 'output_text')?.text || '';

    if (!outputText) {
      state.aiReady = false;
      state.aiStatus = 'RESPUESTA VACÍA';
      state.aiLastError = 'OpenAI respondió sin output_text';
      throw new Error('OpenAI respondió sin output_text');
    }
    const decision = JSON.parse(cleanJsonText(outputText));
    state.aiCalls++;
    state.aiSuccess++;
    state.aiReady = true;
    state.aiStatus = 'CONECTADA';
    state.aiLastError = null;
    state.aiLatencyMs = Date.now() - startedAt;
    return decision;
  } catch (e) {
    state.aiErrors++;
    state.aiReady = false;
    if (!state.aiLastError) state.aiLastError = e?.message || String(e);
    if (e?.name === 'AbortError') state.aiStatus = 'TIMEOUT';
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function getLiveAccount() {
  const account = await rest('/fapi/v2/account', {}, true);
  const pos = await rest('/fapi/v2/positionRisk', {}, true);

  const positions = (pos || [])
    .filter(p => Math.abs(Number(p.positionAmt || 0)) > 0)
    .map(p => {
      const amt = Number(p.positionAmt);
      const side = amt > 0 ? 'LONG' : 'SHORT';
      const entry = Number(p.entryPrice);
      const mark = Number(p.markPrice);
      const pnl = Number(p.unRealizedProfit);
      return {
        symbol: p.symbol,
        side,
        qty: Math.abs(amt),
        entry,
        mark,
        pnl,
        margin: Math.abs(Number(p.notional || 0)) / cfg.leverage
      };
    });

  const wallet = Number(account.totalWalletBalance || account.totalMarginBalance || 0);
  const unreal = Number(account.totalUnrealizedProfit || 0);

  return {
    equity: wallet + unreal,
    wallet,
    unrealizedPnl: unreal,
    positions
  };
}

function paperAccount() {
  return {
    equity: state.equity,
    wallet: state.equity,
    unrealizedPnl: state.unrealizedPnl,
    positions: state.positions.map(p => ({
      symbol: p.symbol, side: p.side, qty: p.qty, entry: p.entry,
      mark: p.current, pnl: p.pnl, margin: p.margin
    }))
  };
}

async function setLeverage(symbol) {
  try {
    await rest('/fapi/v1/leverage', {
      method: 'POST',
      params: { symbol, leverage: cfg.leverage }
    }, true);
  } catch (e) {
    // If already set or restricted, order can still proceed.
    state.lastError = `Leverage ${symbol}: ${e.message}`;
  }
}

async function placeMarketOrder(symbol, side, qty, reduceOnly = false) {
  if (!cfg.binanceKey || !cfg.binanceSecret) throw new Error('BINANCE API no configurada');

  const quantity = normalizeQty(symbol, qty);
  if (!(quantity > 0)) throw new Error(`Cantidad inválida ${symbol}`);

  if (Date.now() - lastOrderTs < cfg.minSecondsBetweenOrders * 1000) {
    throw new Error('Protección: demasiado pronto para otra orden');
  }

  await setLeverage(symbol);

  const params = {
    symbol,
    side,
    type: 'MARKET',
    quantity,
    newOrderRespType: 'RESULT'
  };
  if (reduceOnly) params.reduceOnly = 'true';

  const order = await rest('/fapi/v1/order', { method: 'POST', params }, true);
  lastOrderTs = Date.now();
  return order;
}

async function executeLiveAction(a) {
  if (!cfg.liveArmed) throw new Error('LIVE bloqueado: establece LIVE_ARMED=true');

  const live = await getLiveAccount();
  const existing = live.positions.find(p => p.symbol === a.symbol);

  if (a.action === 'CLOSE') {
    if (!existing) return { skipped: true, reason: 'NO_POSITION' };
    const closeSide = existing.side === 'LONG' ? 'SELL' : 'BUY';
    const order = await placeMarketOrder(existing.symbol, closeSide, existing.qty, true);
    pushHistory({ action: 'LIVE_CLOSE', symbol: existing.symbol, side: existing.side, orderId: order.orderId, reason: a.reason });
    return { order };
  }

  if (a.action === 'OPEN_LONG' || a.action === 'OPEN_SHORT') {
    if (existing) return { skipped: true, reason: 'SYMBOL_ALREADY_OPEN' };

    const margin = clamp(
      live.equity * clamp(Number(a.margin_pct || 1), 0.25, cfg.maxPositionMarginPct) / 100,
      live.equity * 0.0025,
      live.equity * cfg.maxPositionMarginPct / 100
    );

    const notional = margin * cfg.leverage;
    const price = Number(ticks.get(a.symbol)?.price || 0);
    if (!(price > 0)) return { skipped: true, reason: 'NO_PRICE' };

    const qty = normalizeQty(a.symbol, notional / price);
    if (!(qty > 0)) return { skipped: true, reason: 'QTY_TOO_SMALL' };

    const risk = riskAllowsOpen(a.symbol, margin);
    if (!risk.ok) return { skipped: true, reason: risk.reason };

    const side = a.action === 'OPEN_LONG' ? 'BUY' : 'SELL';
    const order = await placeMarketOrder(a.symbol, side, qty, false);
    cooldown.set(a.symbol, now() + 60000);
    pushHistory({
      action: 'LIVE_OPEN',
      symbol: a.symbol,
      side: a.action === 'OPEN_LONG' ? 'LONG' : 'SHORT',
      qty, margin, orderId: order.orderId, reason: a.reason
    });
    return { order };
  }

  return { skipped: true, reason: 'HOLD' };
}

function paperOpen(a) {
  const symbol = a.symbol;
  const held = state.positions.find(p => p.symbol === symbol);
  if (held) return { skipped: true, reason: 'SYMBOL_ALREADY_OPEN' };

  const marginPct = clamp(Number(a.margin_pct || 1), 0.25, cfg.maxPositionMarginPct);
  const margin = Math.min(state.equity * marginPct / 100, marginFor());
  const risk = riskAllowsOpen(symbol, margin);
  if (!risk.ok) return { skipped: true, reason: risk.reason };

  const price = Number(ticks.get(symbol)?.price || 0);
  if (!(price > 0) || !(margin > 0)) return { skipped: true, reason: 'NO_PRICE_OR_MARGIN' };

  const side = a.action === 'OPEN_LONG' ? 'LONG' : 'SHORT';
  const qty = margin * cfg.leverage / price;

  const p = {
    id: `AI_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    symbol, side, entry: price, current: price,
    margin, qty, pnl: 0,
    leverage: cfg.leverage,
    confidence: Number(a.confidence || 0),
    strategy: 'AI',
    thesis: a.reason,
    openedAt: new Date().toISOString(),
    openedTs: now()
  };

  state.positions.push(p);
  cooldown.set(symbol, now() + 60000);
  state.lastSignal = `AI PAPER OPEN ${symbol} ${side} · ${Math.round(a.confidence)}%`;
  pushHistory({ action: 'AI_OPEN', symbol, side, margin, reason: a.reason });
  return { opened: true };
}

function paperClose(a) {
  const p = state.positions.find(x => x.symbol === a.symbol);
  if (!p) return { skipped: true, reason: 'NO_POSITION' };

  const t = ticks.get(p.symbol);
  if (t?.price) p.current = t.price;

  const move = p.side === 'LONG' ? p.current - p.entry : p.entry - p.current;
  p.pnl = move * p.qty;
  state.realizedPnl += p.pnl;
  state.positions = state.positions.filter(x => x.id !== p.id);
  cooldown.set(p.symbol, now() + 60000);

  pushHistory({
    action: 'AI_CLOSE',
    symbol: p.symbol,
    side: p.side,
    pnl: round(p.pnl, 4),
    reason: a.reason
  });

  state.lastSignal = `AI PAPER CLOSE ${p.symbol} ${p.side} · PnL ${p.pnl.toFixed(2)}`;
  return { closed: true };
}

function markPaperPositions() {
  const keep = [];
  const tNow = now();

  // A position is never allowed to keep a stale mark silently. If the live
  // tick is missing, the last known price remains the explicit fallback; the
  // REST refresh in runCycle normally supplies the fresh value.


  for (const p of state.positions) {
    const t = ticks.get(p.symbol);
    if (t?.price) p.current = t.price;

    const move = p.side === 'LONG' ? p.current - p.entry : p.entry - p.current;
    p.pnl = move * p.qty;
    p.unrealizedPct = p.margin ? p.pnl / p.margin * 100 : 0;

    const age = tNow - p.openedTs;
    const tp = p.unrealizedPct >= cfg.paperTpPct;
    const sl = p.unrealizedPct <= -cfg.paperSlPct;
    const timeout = age >= cfg.paperMaxHoldMs;

    if (tp || sl || timeout) {
      state.realizedPnl += p.pnl;
      pushHistory({
        action: 'RISK_CLOSE',
        symbol: p.symbol,
        side: p.side,
        pnl: round(p.pnl, 4),
        reason: tp ? 'TP' : sl ? 'SL' : 'TIME'
      });
      cooldown.set(p.symbol, tNow + 60000);
    } else {
      keep.push(p);
    }
  }

  state.positions = keep;
}

async function executeDecision(decision) {
  state.aiDecision = decision;
  state.aiReasoning = decision.summary || '';
  state.regime = decision.regime || state.regime;

  let rawActions = Array.isArray(decision.actions) ? decision.actions : [];

  // SIMPLE ENTRY MODE: when the account is flat, prefer actually taking a
  // clear market position instead of remaining paralyzed in HOLD. The AI
  // still decides normally first; this fallback only acts when it returned
  // no OPEN action. PAPER remains the default and risk limits still apply.
  if ((!state.positions || state.positions.length === 0) &&
      !rawActions.some(a => a && (a.action === 'OPEN_LONG' || a.action === 'OPEN_SHORT'))) {
    const top = Array.isArray(market) ? market.find(m => {
      if (!m?.symbol || cooldown.has(m.symbol)) return false;
      return m.bias === 'LONG' || m.bias === 'SHORT' ||
        Math.abs(Number(m.momentum5m || 0)) > 0.05 ||
        Math.abs(Number(m.momentum15m || 0)) > 0.10;
    }) : null;

    if (top) {
      const side = top.bias === 'SHORT' ||
        (top.bias !== 'LONG' && Number(top.momentum5m || 0) < 0 && Number(top.momentum15m || 0) < 0)
        ? 'OPEN_SHORT' : 'OPEN_LONG';
      rawActions = [{
        action: side,
        symbol: top.symbol,
        margin_pct: 0.75,
        reason: `Entrada simple: ${side === 'OPEN_LONG' ? 'sesgo alcista' : 'sesgo bajista'} y momentum actual en ${top.symbol}.`,
        confidence: 0.60,
        expected_net_pct: Math.max(cfg.minExpectedNetPct, 0.03)
      }, ...rawActions];
      state.aiReasoning = `${decision.summary || ''} | MODO SIMPLE: se tomó posición en ${top.symbol}.`;
    }
  }

  // Reject new entries whose declared net expectancy does not clear the minimum edge.
  const filteredActions = rawActions.filter(a => {
    if (!a || !['OPEN_LONG','OPEN_SHORT','CLOSE','HOLD'].includes(a.action)) return false;
    if (a.action === 'OPEN_LONG' || a.action === 'OPEN_SHORT') {
      return Number(a.expected_net_pct) >= cfg.minExpectedNetPct;
    }
    return true;
  });
  // Position management has priority: CLOSE decisions execute before new entries.
  const actions = filteredActions
    .sort((a, b) => (a.action === 'CLOSE' ? -1 : 0) - (b.action === 'CLOSE' ? -1 : 0))
    .slice(0, cfg.maxActionsPerCycle);
  state.riskApproved = actions.filter(x => x.action !== 'HOLD').length;

  if (cfg.mode === 'PAPER') {
    for (const a of actions) {
      try {
        if (a.action === 'OPEN_LONG' || a.action === 'OPEN_SHORT') paperOpen(a);
        else if (a.action === 'CLOSE') paperClose(a);
      } catch (e) {
        state.lastError = `PAPER action: ${e.message}`;
      }
    }
    return;
  }

  if (cfg.mode === 'LIVE') {
    if (!cfg.liveArmed) {
      state.lastSignal = 'LIVE seleccionado pero LIVE_ARMED=false';
      return;
    }
    for (const a of actions) {
      try {
        await executeLiveAction(a);
      } catch (e) {
        state.lastError = `LIVE action: ${e.message}`;
        pushHistory({ action: 'LIVE_ERROR', symbol: a.symbol, reason: e.message });
      }
    }
  }
}

function emergencyStopCheck() {
  if (fs.existsSync(controlFile)) {
    try {
      const c = JSON.parse(fs.readFileSync(controlFile, 'utf8'));
      if (c.stop) stopped = true;
    } catch {}
  }

  if (state.drawdownPct >= cfg.maxDrawdownPct || Math.max(0, -state.dailyLossPct) >= cfg.maxDailyLossPct) {
    stopped = true;
    state.lastSignal = 'STOP AUTOMÁTICO POR RIESGO';
  }
}

function connect() {
  try {
    ws = new WebSocket(cfg.wsUrl);

    ws.on('open', () => {
      state.wsConnected = 1;
      state.lastError = null;
      console.log('WS_CONNECTED=1');
    });

    ws.on('close', () => {
      state.wsConnected = 0;
      if (!stopped && !reconnectTimer) {
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect();
        }, 3000);
      }
    });

    ws.on('error', e => {
      state.lastError = `WebSocket: ${e.message || 'error'}`;
    });

    ws.on('message', raw => {
      try {
        const arr = JSON.parse(raw.toString());
        if (!Array.isArray(arr)) return;

        const ts = now();
        for (const t of arr) {
          const symbol = t.s;
          if (!symbol?.endsWith('USDT') || !marketInfo.has(symbol)) continue;
          const price = Number(t.c);
          const volume = Number(t.q || 0);
          if (price > 0) ticks.set(symbol, { price, volume, ts });
        }
      } catch {
        state.lastError = 'WS parse error';
      }
    });
  } catch (e) {
    state.lastError = `WS init: ${e.message}`;
  }
}


async function checkOpenAI() {
  if (!cfg.openaiKey) {
    state.aiStatus = 'SIN API KEY';
    state.aiReady = false;
    state.aiLastError = 'OPENAI_API_KEY no configurada';
    return false;
  }
  const key = String(cfg.openaiKey).trim();
  if (/[^\x00-\x7F]/.test(key)) {
    state.aiStatus = 'API KEY INVÁLIDA';
    state.aiReady = false;
    state.aiLastError = 'OPENAI_API_KEY contiene caracteres no ASCII';
    return false;
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(cfg.aiTimeoutMs, 10000));
  try {
    const res = await fetch('https://api.openai.com/v1/models/' + encodeURIComponent(cfg.openaiModel), {
      headers: { Authorization: `Bearer ${key}` },
      signal: controller.signal
    });
    const text = await res.text();
    if (!res.ok) {
      state.aiStatus = `MODELO NO DISPONIBLE ${res.status}`;
      state.aiReady = false;
      state.aiLastError = `OPENAI MODEL CHECK ${res.status}: ${text.slice(0, 500)}`;
      return false;
    }
    state.aiReady = true;
    state.aiStatus = 'MODELO DISPONIBLE';
    state.aiLastError = null;
    return true;
  } catch (e) {
    state.aiReady = false;
    state.aiStatus = e?.name === 'AbortError' ? 'CHECK TIMEOUT' : 'CHECK ERROR';
    state.aiLastError = e?.message || String(e);
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function runCycle() {
  if (loopBusy || stopped) return;
  loopBusy = true;

  try {
    emergencyStopCheck();
    state.cycle++;

    if (!state.wsConnected) {
      state.lastSignal = 'Esperando WebSocket de Binance…';
      writeState();
      return;
    }

    state.lastSignal = `Analizando ${state.symbols} mercados…`;

    // Refresh PAPER marks before calculating PnL. This is the critical fix for
    // frozen "actual = entrada" prices and positions that never hit TP/SL.
    if (cfg.mode === 'PAPER') {
      try {
        await refreshPaperPrices();
      } catch (e) {
        state.lastError = `PAPER price refresh: ${e.message}`;
      }
    }

    const market = await buildMarketSnapshot();
    state.candidates = market.length;
    if (market.length < 5) {
      state.lastSignal = 'Calentando datos de mercado…';
      writeState();
      return;
    }

    if (cfg.mode === 'PAPER') {
      markPaperPositions();
    } else if (cfg.mode === 'LIVE') {
      try {
        lastAccount = await getLiveAccount();
        state.equity = lastAccount.equity;
        state.unrealizedPnl = lastAccount.unrealizedPnl;
        state.positions = lastAccount.positions;
      } catch (e) {
        state.lastError = `Cuenta LIVE: ${e.message}`;
      }
    }

    const account = cfg.mode === 'LIVE' ? (lastAccount || await getLiveAccount()) : paperAccount();
    const decision = await askAI(market, account);
    await executeDecision(decision);

    state.lastSignal = decision.summary || 'IA evaluó el mercado';
    if (!state.lastError) state.lastError = null;

    if (state.cycle % 5 === 0) {
      console.log(
        `cycle=${state.cycle} universe=${state.symbols} market=${market.length}` +
        ` portfolio=${state.positions.length} equity=${Number(state.equity).toFixed(2)}` +
        ` regime=${state.regime} ai=${cfg.openaiModel}`
      );
    }
  } catch (e) {
    state.lastError = e?.message || String(e);
    console.error('CYCLE_ERROR', state.lastError);
  } finally {
    writeState();
    loopBusy = false;
  }
}

async function boot() {
  console.log(`GALAXI V27 | mode=${cfg.mode} | model=${cfg.openaiModel} | scan=${cfg.scanMs}ms | SIMPLE_POSITIONS=ON`);

  console.log(`OPENAI_KEY_PRESENT=${cfg.openaiKey ? 1 : 0}`);
  console.log(`OPENAI_MODEL=${cfg.openaiModel}`);
  if (!cfg.openaiKey) {
    console.warn('OPENAI_API_KEY is missing. AI decisions cannot run.');
  }

  if (cfg.mode === 'LIVE' && !cfg.liveArmed) {
    console.warn('LIVE is NOT ARMED. Set LIVE_ARMED=true only when you intentionally want order execution.');
  }

  await syncServerTime();
  await loadExchangeInfo();
  await checkOpenAI();
  connect();

  // Let the WS warm up before the first AI call.
  await sleep(5000);

  writeState();
  await runCycle();

  setInterval(runCycle, cfg.scanMs);
}

boot().catch(e => {
  state.lastError = `BOOT: ${e.message}`;
  writeState();
  console.error(e);
  process.exit(1);
});

process.on('SIGTERM', () => {
  stopped = true;
  try { ws?.close(); } catch {}
  process.exit(0);
});
