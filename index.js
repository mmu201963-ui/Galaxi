import fs from 'node:fs';
import crypto from 'node:crypto';
import http from 'node:http';
import WebSocket from 'ws';

/*
 GALAXI V38 · ANOMALY ENGINE · 12 POSITIONS · 6 LONG / 6 SHORT

 Objective:
 - Scan the complete Binance USDⓈ-M perpetual USDT universe.
 - Rotate deep analysis across the whole market instead of repeatedly selecting
   only the largest/most familiar coins.
 - Combine technical structure + momentum + volume + volatility + OI +
   Binance Top-Trader aggregated behavior.
 - Compare current conditions with GALAXI's own closed-trade pattern memory.
 - Use up to 12 total positions with a strict 6 LONG / 6 SHORT portfolio capacity; each symbol is still evaluated independently.
 - Manage exits deterministically and through the AI.
 - PAPER is the default. LIVE requires TRADING_MODE=LIVE and LIVE_ARMED=true.

 Important:
 Binance's "Top Trader" endpoints expose aggregated behavior of the top 20%
 by margin balance, not identifiable individual wallets. GALAXI therefore
 treats this as a behavioral market signal, not as wallet-copy trading.
*/

const runtimeFile = 'galaxi-runtime.json';
const learningFile = 'galaxi-learning.json';
const controlFile = 'galaxi-control.json';

const cfg = {
  mode: String(process.env.TRADING_MODE || 'PAPER').toUpperCase(),
  liveArmed: String(process.env.LIVE_ARMED || 'false').toLowerCase() === 'true',

  openaiKey: process.env.OPENAI_API_KEY || '',
  openaiModel: process.env.OPENAI_MODEL || 'gpt-5.6',

  binanceKey: process.env.BINANCE_API_KEY || '',
  binanceSecret: process.env.BINANCE_API_SECRET || '',
  binanceBase: process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com',
  wsUrl: process.env.BINANCE_FUTURES_WS || 'wss://fstream.binance.com/ws/!miniTicker@arr',
  spotWsUrl: process.env.BINANCE_SPOT_WS || 'wss://stream.binance.com:9443/ws/!miniTicker@arr',

  capital: Number(process.env.PAPER_START_CAPITAL || 10000),

  maxPositions: Math.min(12, Math.max(1, Number(process.env.MAX_POSITIONS || 12))),
  maxLongPositions: Math.min(6, Math.max(0, Number(process.env.MAX_LONG_POSITIONS || 6))),
  maxShortPositions: Math.min(6, Math.max(0, Number(process.env.MAX_SHORT_POSITIONS || 6))),

  maxTotalMarginPct: Math.min(50, Math.max(1, Number(process.env.MAX_TOTAL_MARGIN_PCT || 30))),
  maxPositionMarginPct: Math.min(5, Math.max(0.25, Number(process.env.MAX_POSITION_MARGIN_PCT || 2))),
  leverage: Math.min(10, Math.max(1, Number(process.env.LEVERAGE || 5))),

  scanMs: Math.max(5000, Number(process.env.SCAN_INTERVAL_MS || 10000)),
  fastScannerMs: Math.max(500, Number(process.env.FAST_SCANNER_MS || 1000)),
  fastHistoryMs: Math.max(3000, Number(process.env.FAST_HISTORY_MS || 8000)),
  btcReferenceSymbol: String(process.env.BTC_REFERENCE_SYMBOL || 'BTCUSDT').toUpperCase(),
  anomalyMinResidualPct: Math.max(0.02, Number(process.env.ANOMALY_MIN_RESIDUAL_PCT || 0.10)),
  anomalyStrongResidualPct: Math.max(0.05, Number(process.env.ANOMALY_STRONG_RESIDUAL_PCT || 0.25)),
  anomalyStaleMs: Math.max(1500, Number(process.env.ANOMALY_STALE_MS || 3000)),
  anomalyScoreWeight: Math.max(0, Number(process.env.ANOMALY_SCORE_WEIGHT || 0.55)),
  aiTimeoutMs: Math.max(5000, Number(process.env.AI_TIMEOUT_MS || 18000)),

  // Full-market discovery happens every cycle from the 24h ticker.
  // Deep technical analysis rotates through the whole universe.
  deepScanSymbols: Math.min(60, Math.max(24, Number(process.env.DEEP_SCAN_SYMBOLS || 48))),
  aiTopSymbols: Math.min(30, Math.max(12, Number(process.env.AI_TOP_SYMBOLS || 24))),
  traderTopSymbols: Math.min(48, Math.max(12, Number(process.env.TRADER_TOP_SYMBOLS || 36))),
  rotationSymbols: Math.min(30, Math.max(8, Number(process.env.ROTATION_SYMBOLS || 18))),

  klineLimit: Math.min(150, Math.max(50, Number(process.env.KLINE_LIMIT || 80))),
  marketConcurrency: Math.min(8, Math.max(2, Number(process.env.MARKET_CONCURRENCY || 5))),
  behaviorConcurrency: Math.min(8, Math.max(2, Number(process.env.BEHAVIOR_CONCURRENCY || 6))),
  edgeCacheMs: Math.max(10000, Number(process.env.EDGE_CACHE_MS || 30000)),
  earlyEntryEnabled: String(process.env.EARLY_ENTRY_ENABLED || 'true').toLowerCase() === 'true',
  earlyImpulseMinPct: Number(process.env.EARLY_IMPULSE_MIN_PCT || 0.08),
  earlyVolumeRatio: Number(process.env.EARLY_VOLUME_RATIO || 1.20),
  earlyBreakoutBonus: Number(process.env.EARLY_BREAKOUT_BONUS || 16),
  lateExtensionPct: Number(process.env.LATE_EXTENSION_PCT || 1.20),
  restTimeoutMs: Math.max(5000, Number(process.env.REST_TIMEOUT_MS || 12000)),

  // Expected net edge after estimated round-trip fees.
  minExpectedNetPct: Math.max(0.02, Number(process.env.MIN_EXPECTED_NET_PCT || 0.08)),
  estimatedFeeRate: Math.max(0.0001, Number(process.env.ESTIMATED_FEE_RATE || 0.0004)),

  maxDailyLossPct: Math.min(10, Math.max(0.25, Number(process.env.MAX_DAILY_LOSS_PCT || 1))),
  maxCycleLossUsd: Math.max(8, Number(process.env.MAX_CYCLE_LOSS_USD || 24)),
  maxDrawdownPct: Math.min(15, Math.max(1, Number(process.env.MAX_DRAWDOWN_PCT || 3))),
  minSecondsBetweenOrders: Math.max(2, Number(process.env.MIN_SECONDS_BETWEEN_ORDERS || 2)),
  maxActionsPerCycle: Math.min(12, Math.max(1, Number(process.env.MAX_ACTIONS_PER_CYCLE || 12))),

  paperTpPct: Number(process.env.PAPER_TP_PCT || 0.60),
  paperSlPct: Number(process.env.PAPER_SL_PCT || 0.55),
  paperMaxHoldMs: Number(process.env.PAPER_MAX_HOLD_MS || 600000), // 10 min
  paperProfitTakeUsd: Number(process.env.PAPER_PROFIT_TAKE_USD || 8),
  paperHardLossUsd: Number(process.env.PAPER_HARD_LOSS_USD || 12),
  paperProfitLockTriggerPct: Number(process.env.PAPER_PROFIT_LOCK_TRIGGER_PCT || 0.25),
  paperProfitGivebackPct: Number(process.env.PAPER_PROFIT_GIVEBACK_PCT || 0.12),
  paperMinLockedPct: Number(process.env.PAPER_MIN_LOCKED_PCT || 0.08),

  learningMaxTrades: Math.min(2000, Math.max(100, Number(process.env.LEARNING_MAX_TRADES || 500))),
  learningMinSamples: Math.min(50, Math.max(5, Number(process.env.LEARNING_MIN_SAMPLES || 8)))
};


const webPort = Number(process.env.PORT || 3000);
const webHost = '0.0.0.0';

function htmlEscape(value) {
  return String(value ?? '').replace(/[&<>\"']/g, ch => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[ch]));
}

function dashboardHtml() {
  const positions = Array.isArray(state.positions) ? state.positions : [];
  const rows = positions.map(p => `<tr><td>${htmlEscape(p.symbol)}</td><td>${htmlEscape(p.side)}</td><td>${Number(p.entry || 0).toFixed(6)}</td><td>${Number(p.mark || p.current || 0).toFixed(6)}</td><td>${Number(p.pnl || 0).toFixed(2)}</td><td>${Number(p.peakPnl || 0).toFixed(2)}</td><td>${Math.round((now() - Number(p.openedTs || now())) / 1000)}s</td></tr>`).join('');
  const e = state.edgeScanner || {};
  const fmt = x => x == null ? '—' : Number(x).toFixed(2);
  const topRows = (Array.isArray(state.ranking) ? state.ranking.slice(0, 8) : []).map(x => `<tr><td>${htmlEscape(x.symbol)}</td><td>${htmlEscape(x.preferredSide || '—')}</td><td>${fmt(x.edgeScore)}</td><td>${fmt(x.edgeLong)}</td><td>${fmt(x.edgeShort)}</td><td>${htmlEscape(x.behavior?.side || 'MIXTO')}</td><td>${fmt(x.premium?.fundingRatePct)}%</td><td>${fmt(x.openInterestChangePct)}%</td></tr>`).join('');
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GALAXI V38 · ANOMALY ENGINE · 12 POSITIONS · 6 LONG / 6 SHORT</title><meta http-equiv="refresh" content="10"><style>body{font-family:system-ui;background:#080b10;color:#eee;margin:0;padding:18px}main{max-width:1100px;margin:auto}.top{display:flex;justify-content:space-between;gap:12px;align-items:end;margin-bottom:16px}.sub{color:#8b949e}.pill{border:1px solid #2f81f7;border-radius:999px;padding:5px 10px;color:#58a6ff;font-size:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:10px}.card{background:#11161d;border:1px solid #252d38;border-radius:12px;padding:14px}.k{color:#8b949e;font-size:12px}.v{font-size:22px;font-weight:750;margin-top:5px}.section{margin-top:18px}.scanner{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:10px}.edge{background:#0f151c;border:1px solid #26303c;border-radius:12px;padding:14px}.edge b{font-size:20px}.muted{color:#8b949e;font-size:12px}.good{color:#3fb950}.warn{color:#d29922}table{width:100%;border-collapse:collapse;margin-top:10px;background:#11161d;border:1px solid #252d38;border-radius:12px;overflow:hidden}th,td{text-align:left;padding:9px;border-bottom:1px solid #252d38;font-size:13px}a{color:#58a6ff}.tag{display:inline-block;padding:2px 7px;border-radius:999px;background:#1b2330;color:#c9d1d9;font-size:11px;margin-right:4px}</style></head><body><main><div class="top"><div><h1 style="margin:0">GALAXI V38 · ANOMALY ENGINE · 12 POSITIONS · 6 LONG / 6 SHORT</h1><div class="sub">${htmlEscape(state.mode)} · IA ${htmlEscape(state.aiModel)} · ciclo ${state.cycle}</div></div><span class="pill">${state.wsConnected ? 'BINANCE LIVE DATA' : 'BINANCE DESCONECTADO'}</span></div><div class="grid"><div class="card"><div class="k">Equity</div><div class="v">$${Number(state.equity).toFixed(2)}</div></div><div class="card"><div class="k">Net PnL</div><div class="v">$${Number(state.realizedPnl + state.unrealizedPnl).toFixed(2)}</div></div><div class="card"><div class="k">Mercados</div><div class="v">${state.symbols}</div></div><div class="card"><div class="k">Deep / IA</div><div class="v">${state.deepScanned} / ${state.candidates}</div></div><div class="card"><div class="k">IA calls / errores</div><div class="v">${state.aiCalls} / ${state.aiErrors}</div></div><div class="card"><div class="k">Top Trader coverage</div><div class="v">${state.behaviorCoverage}%</div></div><div class="card"><div class="k">Edge tradeable</div><div class="v">${e.tradeableCount || 0}</div></div><div class="card"><div class="k">Posiciones</div><div class="v">${positions.length} · L${state.longOpen}/S${state.shortOpen}</div></div><div class="card"><div class="k">FAST SCANNER</div><div class="v">${state.fastScanner?.scanned || 0}/s</div><div class="muted">señales ${state.fastScanner?.freshSignals || 0} · Spot ${state.fastScanner?.spotConnected ? 'OK' : '—'}</div></div><div class="card"><div class="k">BTC ANOMALY ENGINE</div><div class="v">${state.anomalyScanner?.anomalous || 0}</div><div class="muted">residuales · BTC ${fmt(state.anomalyScanner?.btcMovePct)}%</div></div></div><div class="section"><div class="card"><div class="k">P&L AUDIT · TP / HARD LOSS</div><div class="v">+$${Number(state.paperProfitTakeUsd ?? cfg.paperProfitTakeUsd).toFixed(2)} / -$${Number(state.paperHardLossUsd ?? cfg.paperHardLossUsd).toFixed(2)}</div><div class="muted">Realizado $${Number(state.realizedPnl||0).toFixed(2)} · No realizado $${Number(state.unrealizedPnl||0).toFixed(2)} · discrepancia $${Number(state.pnlAudit?.discrepancy||0).toFixed(4)}</div></div></div><div class="section"><h2>FAST MISPRICING SCANNER</h2><div class="card"><div class="muted">VIGILANCIA CONTINUA</div><b>${state.fastScanner?.scanned || 0} mercados/s</b><div class="muted">Señales frescas ${state.fastScanner?.freshSignals || 0} · Spot ${state.fastScanner?.spotConnected ? 'CONECTADO' : 'DESCONECTADO'}</div><div class="muted">Mejor ${htmlEscape(state.fastScanner?.bestOverall?.symbol || '—')} ${htmlEscape(state.fastScanner?.bestOverall?.side || '')} · score ${fmt(state.fastScanner?.bestOverall?.score)}</div></div></div><div class="section"><h2>BTC-RELATIVE ANOMALY ENGINE</h2><div class="card"><div class="muted">REFERENCIA ${htmlEscape(cfg.btcReferenceSymbol)}</div><b>${state.anomalyScanner?.anomalous || 0} anomalías frescas</b><div class="muted">BTC ${fmt(state.anomalyScanner?.btcMovePct)}% · mejor ${htmlEscape(state.anomalyScanner?.bestOverall?.symbol || "—")} · score ${fmt(state.anomalyScanner?.bestOverall?.anomalyScore)}</div><div class="muted">Residual ${fmt(state.anomalyScanner?.bestOverall?.btcRelativeResidualPct)}% · velocidad relativa ${fmt(state.anomalyScanner?.bestOverall?.btcRelativeVelocityPctPerMin)}%/min</div></div></div><div class="section"><h2>EDGE SCANNER</h2><div class="scanner"><div class="edge"><div class="muted">MEJOR LONG</div><b>${htmlEscape(e.bestLong?.symbol || '—')}</b><div>Score <span class="good">${fmt(e.bestLong?.score)}</span></div><div class="muted">Trader ${htmlEscape(e.bestLong?.behavior || '—')} · funding ${fmt(e.bestLong?.funding)}% · basis ${fmt(e.bestLong?.basis)}%</div></div><div class="edge"><div class="muted">MEJOR SHORT</div><b>${htmlEscape(e.bestShort?.symbol || '—')}</b><div>Score <span class="good">${fmt(e.bestShort?.score)}</span></div><div class="muted">Trader ${htmlEscape(e.bestShort?.behavior || '—')} · funding ${fmt(e.bestShort?.funding)}% · basis ${fmt(e.bestShort?.basis)}%</div></div><div class="edge"><div class="muted">MEJOR OPORTUNIDAD</div><b>${htmlEscape(e.bestOverall?.symbol || '—')} ${htmlEscape(e.bestOverall?.side || '')}</b><div>Score <span class="good">${fmt(e.bestOverall?.score)}</span></div><div class="muted">Observados ${e.watchedCount || 0} · promedio ${fmt(e.avgEdge)}</div></div></div></div><div class="section"><h2>EDGE LEADERBOARD</h2><table><thead><tr><th>Símbolo</th><th>Sesgo</th><th>Edge</th><th>Long</th><th>Short</th><th>Top Trader</th><th>Funding</th><th>OI 5m</th></tr></thead><tbody>${topRows || '<tr><td colspan=8>Esperando scanner</td></tr>'}</tbody></table></div><div class="section"><h2>Posiciones</h2><table><thead><tr><th>Símbolo</th><th>Lado</th><th>Entrada</th><th>Mark</th><th>PnL</th><th>Pico</th><th>Edad</th></tr></thead><tbody>${rows || '<tr><td colspan=7>Sin posiciones abiertas</td></tr>'}</tbody></table></div><div class="section muted">Régimen: <span class="tag">${htmlEscape(state.regime)}</span> Comportamiento: <span class="tag">${htmlEscape(state.behaviorBias)}</span> · Confianza ${state.behaviorConfidence}% · <a href="/health">health</a> · <a href="/state">state</a></div></main></body></html>`;
}

const webServer = http.createServer((req, res) => {
  try {
    if (req.url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, running: state.running, wsConnected: state.wsConnected, mode: state.mode, cycle: state.cycle, lastError: state.lastError }));
      return;
    }
    if (req.url === '/state') {
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(state));
      return;
    }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    res.end(dashboardHtml());
  } catch (e) {
    res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(`GALAXI web error: ${e.message}`);
  }
});

webServer.listen(webPort, webHost, () => {
  console.log(`WEB_CONNECTED=1 | host=${webHost} | port=${webPort}`);
});

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
  paperProfitTakeUsd: cfg.paperProfitTakeUsd,
  paperHardLossUsd: cfg.paperHardLossUsd,
  pnlAudit: { initialCapital: cfg.capital, realizedPnl: 0, unrealizedPnl: 0, equity: cfg.capital, expectedEquity: cfg.capital, discrepancy: 0 },
  todayPnl: 0,
  drawdownPct: 0,
  dailyLossPct: 0,

  symbols: 0,
  deepScanned: 0,
  warmSymbols: 0,
  cycle: 0,
  cycleRealizedStart: 0,
  cycleLossUsd: 0,
  wsConnected: 0,
  spotConnected: 0,
  candidates: 0,
  riskApproved: 0,

  regime: 'MIXTO',
  longPct: 50,
  shortPct: 50,
  longOpen: 0,
  shortOpen: 0,

  behaviorCoverage: 0,
  behaviorBias: 'MIXTO',
  behaviorConfidence: 0,

  fastScanner: {
    scanned: 0,
    freshSignals: 0,
    spotConnected: 0,
    bestLong: null,
    bestShort: null,
    bestOverall: null,
    btcMovePct: 0,
    btcVelocityPctPerMin: 0,
    lastScanMs: 0
  },

  anomalyScanner: {
    reference: cfg.btcReferenceSymbol,
    scanned: 0,
    anomalous: 0,
    bestLong: null,
    bestShort: null,
    bestOverall: null,
    btcMovePct: 0,
    lastScanMs: 0
  },

  edgeScanner: {
    bestLong: null,
    bestShort: null,
    bestOverall: null,
    tradeableCount: 0,
    watchedCount: 0,
    avgEdge: 0
  },

  positions: [],
  ranking: [],
  history: [],
  learning: {
    trades: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    avgNetPct: 0,
    edgePatterns: 0
  },

  lastSignal: 'Esperando datos para el cerebro IA…',
  lastError: null,
  aiDecision: null,
  aiReasoning: '',
  lastExit: null,

  aiCalls: 0,
  aiErrors: 0,
  restCalls: 0,
  rate429: 0,
  rate418: 0,

  lastUpdate: null
};

const ticks = new Map();
const cooldown = new Map();
const marketInfo = new Map();
const behaviorCache = new Map();
const openInterestCache = new Map();
const premiumCache = new Map();
const scanMicro = new Map();
const spotTicks = new Map();
const fastHistory = new Map();
let spotWs = null;
let spotReconnectTimer = null;
let fastLoopBusy = false;
let cycleRealizedStart = 0;

let learningTrades = [];
let ws = null;
let stopped = false;
let reconnectTimer = null;
let loopBusy = false;
let lastOrderTs = 0;
let serverOffset = 0;
let lastAccount = null;
let rotationCursor = 0;

function clamp(n, a, b) { return Math.max(a, Math.min(b, n)); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function now() { return Date.now(); }

function round(n, d = 6) {
  const p = 10 ** d;
  return Math.round(Number(n) * p) / p;
}

function avg(arr) {
  const a = arr.filter(Number.isFinite);
  return a.length ? a.reduce((s, n) => s + n, 0) / a.length : 0;
}

function pctMove(current, previous) {
  return previous > 0 ? (current / previous - 1) * 100 : 0;
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
  return round(Math.floor(Number(qty) / step) * step, p);
}

function normalizePrice(symbol, price) {
  const m = marketInfo.get(symbol);
  if (!m) return Number(price);
  const step = m.tickSize;
  return round(Math.round(Number(price) / step) * step, precisionFromStep(step));
}

function safeJson(value) {
  try { return JSON.stringify(value); } catch { return '{}'; }
}

function writeState() {
  state.longOpen = state.positions.filter(p => p.side === 'LONG').length;
  state.shortOpen = state.positions.filter(p => p.side === 'SHORT').length;
  state.unrealizedPnl = state.positions.reduce((s, p) => s + Number(p.pnl || 0), 0);

  if (cfg.mode === 'PAPER') {
    state.equity = cfg.capital + state.realizedPnl + state.unrealizedPnl;
  }

  const dd = Math.max(0, cfg.capital - state.equity);
  state.drawdownPct = cfg.capital ? dd / cfg.capital * 100 : 0;
  state.dailyLossPct = Math.min(0, state.realizedPnl / cfg.capital * 100);
  state.pnlAudit = {
    initialCapital: round(cfg.capital, 2),
    realizedPnl: round(state.realizedPnl, 2),
    unrealizedPnl: round(state.unrealizedPnl, 2),
    equity: round(state.equity, 2),
    expectedEquity: round(cfg.capital + state.realizedPnl + state.unrealizedPnl, 2),
    discrepancy: round(state.equity - (cfg.capital + state.realizedPnl + state.unrealizedPnl), 6)
  };
  state.lastUpdate = new Date().toISOString();

  state.learning = learningSummary();
  try {
    fs.writeFileSync(runtimeFile, JSON.stringify(state, null, 2));
  } catch {}
}

function pushHistory(item) {
  state.history.unshift({ time: new Date().toISOString(), ...item });
  state.history = state.history.slice(0, 150);
}

async function rest(path, options = {}, signed = false) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), cfg.restTimeoutMs);

  const method = options.method || 'GET';
  const params = { ...(options.params || {}) };

  if (signed) {
    params.timestamp = Date.now() + serverOffset;
    params.recvWindow = 5000;
  }

  const query = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) query.set(k, String(v));
  }

  if (signed) {
    const signature = crypto.createHmac('sha256', cfg.binanceSecret)
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
    throw new Error(`BINANCE ${res.status}: ${data?.msg || `HTTP ${res.status}`}`);
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

function classifyMarket(baseAsset, onboardDate) {
  const b = String(baseAsset || '').toUpperCase();
  const ageDays = onboardDate ? (Date.now() - Number(onboardDate)) / 86400000 : 9999;

  const memeHints =
    /(DOGE|SHIB|PEPE|FLOKI|BONK|WIF|MEME|BOME|MEW|MOG|BRETT|TURBO|NEIRO|DOGS|PNUT|POPCAT|SATS|RATS|CHEEMS|MOODENG|GOAT|PENGU|TRUMP|MELANIA|TOSHI)/i;

  const meme = memeHints.test(b);
  const fresh = ageDays >= 0 && ageDays <= 30;

  if (fresh && meme) return 'NEW_MEME';
  if (fresh) return 'NEW';
  if (meme) return 'MEME';
  return 'NORMAL';
}

async function loadExchangeInfo() {
  const data = await rest('/fapi/v1/exchangeInfo');
  marketInfo.clear();

  for (const s of data.symbols || []) {
    if (
      s.status !== 'TRADING' ||
      s.quoteAsset !== 'USDT' ||
      s.contractType !== 'PERPETUAL'
    ) continue;

    const lot = (s.filters || []).find(x => x.filterType === 'LOT_SIZE');
    const price = (s.filters || []).find(x => x.filterType === 'PRICE_FILTER');

    marketInfo.set(s.symbol, {
      baseAsset: s.baseAsset,
      onboardDate: Number(s.onboardDate || 0),
      category: classifyMarket(s.baseAsset, s.onboardDate),
      qtyStep: Number(lot?.stepSize || 0.001),
      minQty: Number(lot?.minQty || 0),
      tickSize: Number(price?.tickSize || 0.00001)
    });
  }

  state.symbols = marketInfo.size;
}

async function fetch24hr() {
  const data = await rest('/fapi/v1/ticker/24hr');
  let n = 0;

  for (const t of data || []) {
    if (!marketInfo.has(t.symbol)) continue;

    const price = Number(t.lastPrice);
    const quoteVolume = Number(t.quoteVolume || 0);
    const changePct = Number(t.priceChangePercent || 0);

    if (!(price > 0)) continue;

    ticks.set(t.symbol, {
      price,
      volume: quoteVolume,
      changePct,
      high24h: Number(t.highPrice || 0),
      low24h: Number(t.lowPrice || 0),
      ts: now()
    });
    n++;
  }

  return n;
}

function discoveryScore(symbol) {
  const t = ticks.get(symbol);
  const m = marketInfo.get(symbol);
  if (!t || !m) return -Infinity;

  // Percentage movement is deliberately used instead of absolute price,
  // avoiding the old bias where expensive/large coins dominated selection.
  const move = Math.abs(Number(t.changePct || 0));
  const range = t.low24h > 0 ? Math.abs(t.high24h / t.low24h - 1) * 100 : 0;
  const liquidity = Math.log10(1 + Math.max(0, Number(t.volume || 0)));

  let categoryBonus = 0;
  if (m.category === 'NEW' || m.category === 'NEW_MEME') categoryBonus += 0.8;
  if (m.category === 'MEME' || m.category === 'NEW_MEME') categoryBonus += 0.4;

  return move * 1.4 + range * 0.7 + liquidity * 0.08 + categoryBonus;
}

function selectDeepUniverse() {
  const all = [...marketInfo.keys()];

  const ranked = all
    .map(symbol => ({ symbol, score: discoveryScore(symbol) }))
    .filter(x => Number.isFinite(x.score))
    .sort((a, b) => b.score - a.score)
    .map(x => x.symbol);

  const fastRank = all
    .map(symbol => ({ symbol, score: Number(ticks.get(symbol)?.fastMispricingScore || 0) }))
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(x => x.symbol);

  const fastLeaders = fastRank.slice(0, Math.min(12, Math.floor(cfg.deepScanSymbols / 4)));
  const movers = [...fastLeaders, ...ranked.slice(0, Math.max(12, cfg.deepScanSymbols - cfg.rotationSymbols))].filter((s, i, arr) => arr.indexOf(s) === i);

  // Rotation prevents the same 20-30 familiar symbols from monopolizing the AI.
  const remaining = ranked.filter(s => !movers.includes(s));
  const rotN = Math.min(cfg.rotationSymbols, remaining.length);

  if (rotN > 0) {
    const start = rotationCursor % remaining.length;
    for (let i = 0; i < rotN; i++) {
      const s = remaining[(start + i) % remaining.length];
      if (!movers.includes(s)) movers.push(s);
    }
    rotationCursor = (rotationCursor + rotN) % Math.max(1, remaining.length);
  }

  // If the market is small, fill from all.
  for (const s of ranked) {
    if (movers.length >= cfg.deepScanSymbols) break;
    if (!movers.includes(s)) movers.push(s);
  }

  return movers.slice(0, cfg.deepScanSymbols);
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
    if (d >= 0) gain += d;
    else loss -= d;
  }

  let ag = gain / period;
  let al = loss / period;

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
    const h = Number(klines[i][2]);
    const l = Number(klines[i][3]);
    const pc = Number(klines[i - 1][4]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }

  return avg(trs.slice(-period));
}

async function fetchKlines(symbol, interval, limit) {
  return rest('/fapi/v1/klines', { params: { symbol, interval, limit } });
}

async function fetchOpenInterest(symbol) {
  try {
    const [current, hist] = await Promise.all([
      rest('/fapi/v1/openInterest', { params: { symbol } }),
      rest('/futures/data/openInterestHist', { params: { symbol, period: '5m', limit: 2 } })
    ]);
    const oi = Number(current?.openInterest || 0);
    const rows = Array.isArray(hist) ? hist : [];
    const prev = Number(rows.at(-2)?.sumOpenInterest || 0);
    const changePct = prev > 0 ? (oi / prev - 1) * 100 : 0;
    openInterestCache.set(symbol, oi);
    return { value: oi, changePct: round(changePct, 3) };
  } catch {
    const cached = openInterestCache.get(symbol);
    return { value: cached || 0, changePct: 0 };
  }
}

function analyzeKlines(symbol, k1, k5) {
  const closes1 = k1.map(x => Number(x[4]));
  const highs1 = k1.map(x => Number(x[2]));
  const lows1 = k1.map(x => Number(x[3]));
  const volumes1 = k1.map(x => Number(x[5]));

  const price = closes1.at(-1) || Number(ticks.get(symbol)?.price || 0);
  const e9 = ema(closes1.slice(-45), 9);
  const e21 = ema(closes1.slice(-60), 21);
  const e50 = ema(closes1.slice(-70), 50);
  const r = rsi(closes1, 14);
  const a = atr(k1, 14);

  const volNow = avg(volumes1.slice(-8));
  const volPrev = avg(volumes1.slice(-28, -8)) || volNow;
  const volumeRatio = volPrev ? volNow / volPrev : 1;

  const m1 = pctMove(price, closes1.at(-2));
  const m5 = pctMove(price, closes1.at(-6));
  const m15 = pctMove(price, closes1.at(-16));
  const m30 = pctMove(price, closes1.at(-31));

  const closes5 = k5.map(x => Number(x[4]));
  const m5tf = pctMove(closes5.at(-1), closes5.at(-4));
  const e20_5 = ema(closes5.slice(-40), 20);
  const e50_5 = ema(closes5.slice(-70), 50);

  // Use completed candles for breakout detection so the signal can fire at
  // the beginning of a move instead of waiting for a fully formed candle.
  const completedHighs = highs1.slice(-21, -1);
  const completedLows = lows1.slice(-21, -1);
  const recentHigh = Math.max(...completedHighs);
  const recentLow = Math.min(...completedLows);

  const prevScan = scanMicro.get(symbol);
  const tsNow = now();
  const elapsedMin = prevScan?.ts ? Math.max(1 / 60, (tsNow - prevScan.ts) / 60000) : 1 / 3;
  const microMovePct = prevScan?.price ? (price / prevScan.price - 1) * 100 : 0;
  const microVelocityPctPerMin = microMovePct / elapsedMin;
  const priorVelocity = Number(prevScan?.velocity || 0);
  const microAcceleration = microVelocityPctPerMin - priorVelocity;
  const freshBreakoutUp = price > recentHigh && m1 > 0 && volumeRatio >= cfg.earlyVolumeRatio;
  const freshBreakoutDown = price < recentLow && m1 < 0 && volumeRatio >= cfg.earlyVolumeRatio;
  scanMicro.set(symbol, { price, ts: tsNow, velocity: microVelocityPctPerMin });

  const bull =
    e9 > e21 &&
    e21 > e50 &&
    m5 > 0 &&
    m15 > 0 &&
    e20_5 >= e50_5;

  const bear =
    e9 < e21 &&
    e21 < e50 &&
    m5 < 0 &&
    m15 < 0 &&
    e20_5 <= e50_5;

  return {
    symbol,
    price,
    bias: bull ? 'LONG' : bear ? 'SHORT' : 'NEUTRAL',
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

    breakoutUp: freshBreakoutUp || price > recentHigh * 0.9995,
    breakoutDown: freshBreakoutDown || price < recentLow * 1.0005,
    freshBreakoutUp,
    freshBreakoutDown,
    microMovePct: round(microMovePct, 4),
    microVelocityPctPerMin: round(microVelocityPctPerMin, 4),
    microAcceleration: round(microAcceleration, 4),
    earlyStageLong: Boolean(freshBreakoutUp || (microVelocityPctPerMin >= cfg.earlyImpulseMinPct && m5 >= 0)),
    earlyStageShort: Boolean(freshBreakoutDown || (microVelocityPctPerMin <= -cfg.earlyImpulseMinPct && m5 <= 0)),
    distanceFromHighPct: recentHigh ? round((price / recentHigh - 1) * 100, 4) : 0,
    distanceFromLowPct: recentLow ? round((price / recentLow - 1) * 100, 4) : 0,
    high20: recentHigh,
    low20: recentLow,

    quoteVolume24h: round(Number(ticks.get(symbol)?.volume || 0), 0),
    change24h: round(Number(ticks.get(symbol)?.changePct || 0), 3),
    category: marketInfo.get(symbol)?.category || 'NORMAL',
    fastMispricingScore: Number(freshFastData(symbol).fastMispricingScore || 0),
    fastPreferredSide: freshFastData(symbol).fastPreferredSide || '—',
    crossBasisPct: Number(freshFastData(symbol).crossBasisPct || 0),
    fastVelocityPctPerMin: Number(freshFastData(symbol).fastVelocityPctPerMin || 0),
    fastAcceleration: Number(freshFastData(symbol).fastAcceleration || 0),
    btcMovePct: Number(freshFastData(symbol).btcMovePct || 0),
    btcRelativeResidualPct: Number(freshFastData(symbol).btcRelativeResidualPct || 0),
    btcRelativeVelocityPctPerMin: Number(freshFastData(symbol).btcRelativeVelocityPctPerMin || 0),
    anomalyScore: Number(freshFastData(symbol).anomalyScore || 0),
    anomalyFresh: Boolean(freshFastData(symbol).anomalyFresh)
  };
}

function behaviorSide(accountRows, positionRows) {
  const a = avg(accountRows.map(x => Number(x.longShortRatio)));
  const p = avg(positionRows.map(x => Number(x.longShortRatio)));

  if (a >= 1.12 && p >= 1.12) return 'LONG';
  if (a <= 0.89 && p <= 0.89) return 'SHORT';
  return 'MIXTO';
}

function behaviorConsistency(rows) {
  if (!rows.length) return 0;
  const signs = rows.map(x => Number(x.longShortRatio) >= 1 ? 1 : -1);
  const mean = avg(signs);
  return Math.abs(mean);
}

async function fetchTopTraderBehavior(symbol) {
  const cached = behaviorCache.get(symbol);
  if (cached && now() - cached.ts < 60000) return cached;

  const params = { symbol, period: '5m', limit: 6 };

  try {
    const [accounts, positions] = await Promise.all([
      rest('/futures/data/topLongShortAccountRatio', { params }),
      rest('/futures/data/topLongShortPositionRatio', { params })
    ]);

    const accountRows = Array.isArray(accounts) ? accounts : [];
    const positionRows = Array.isArray(positions) ? positions : [];

    const latestA = Number(accountRows.at(-1)?.longShortRatio || 1);
    const latestP = Number(positionRows.at(-1)?.longShortRatio || 1);

    const side = behaviorSide(accountRows, positionRows);
    const consistency = avg([
      behaviorConsistency(accountRows),
      behaviorConsistency(positionRows)
    ]);

    const previousA = Number(accountRows.at(-2)?.longShortRatio || latestA);
    const delta = latestA - previousA;

    const result = {
      accountRatio: round(latestA, 4),
      positionRatio: round(latestP, 4),
      accountLongPct: round(Number(accountRows.at(-1)?.longAccount || 0) * 100, 2),
      accountShortPct: round(Number(accountRows.at(-1)?.shortAccount || 0) * 100, 2),
      positionLongPct: round(Number(positionRows.at(-1)?.longAccount || 0) * 100, 2),
      positionShortPct: round(Number(positionRows.at(-1)?.shortAccount || 0) * 100, 2),
      delta5m: round(delta, 4),
      side,
      consistency: round(consistency, 3),
      samples: Math.max(accountRows.length, positionRows.length),
      ts: now()
    };

    behaviorCache.set(symbol, result);
    return result;
  } catch (e) {
    return cached || {
      accountRatio: 1,
      positionRatio: 1,
      accountLongPct: 50,
      accountShortPct: 50,
      positionLongPct: 50,
      positionShortPct: 50,
      delta5m: 0,
      side: 'MIXTO',
      consistency: 0,
      samples: 0,
      ts: now(),
      error: e.message
    };
  }
}


async function fetchPremiumIndex(symbol) {
  const cached = premiumCache.get(symbol);
  if (cached && now() - cached.ts < cfg.edgeCacheMs) return cached;

  try {
    const data = await rest('/fapi/v1/premiumIndex', { params: { symbol } });
    const mark = Number(data?.markPrice || 0);
    const index = Number(data?.indexPrice || 0);
    const fundingRate = Number(data?.lastFundingRate || 0);
    const basisPct = index > 0 ? (mark / index - 1) * 100 : 0;
    const result = {
      markPrice: mark,
      indexPrice: index,
      basisPct: round(basisPct, 4),
      fundingRate: round(fundingRate, 6),
      fundingRatePct: round(fundingRate * 100, 4),
      nextFundingTime: Number(data?.nextFundingTime || 0),
      ts: now()
    };
    premiumCache.set(symbol, result);
    return result;
  } catch (e) {
    return cached || {
      markPrice: 0,
      indexPrice: 0,
      basisPct: 0,
      fundingRate: 0,
      fundingRatePct: 0,
      nextFundingTime: 0,
      ts: now(),
      error: e.message
    };
  }
}

function fastMetrics(symbol, reference = null) {
  const f = ticks.get(symbol);
  const sp = spotTicks.get(symbol);
  if (!f?.price) return null;
  const h = fastHistory.get(symbol) || [];
  const t = now();
  const cutoff = t - cfg.fastHistoryMs;
  const clean = h.filter(x => x.ts >= cutoff);
  const prev = clean[0];
  const prev2 = clean.length > 1 ? clean[Math.max(0, clean.length - 2)] : null;
  const price = Number(f.price);
  const spot = Number(sp?.price || 0);
  const crossBasisPct = spot > 0 ? (price / spot - 1) * 100 : 0;
  const movePct = prev?.price ? (price / prev.price - 1) * 100 : 0;
  const dtMin = prev ? Math.max(1 / 60, (t - prev.ts) / 60000) : 1 / 60;
  const velocityPctPerMin = movePct / dtMin;
  const priorMove = prev2?.price && prev?.price ? (prev.price / prev2.price - 1) * 100 : 0;
  const priorDtMin = prev2 ? Math.max(1 / 60, (prev.ts - prev2.ts) / 60000) : dtMin;
  const priorVelocity = priorMove / priorDtMin;
  const acceleration = velocityPctPerMin - priorVelocity;
  const refMovePct = Number(reference?.movePct || 0);
  const refVelocityPctPerMin = Number(reference?.velocityPctPerMin || 0);
  const btcRelativeResidualPct = movePct - refMovePct;
  const btcRelativeVelocityPctPerMin = velocityPctPerMin - refVelocityPctPerMin;
  const relativeSide = btcRelativeVelocityPctPerMin > 0 ? 'LONG' : btcRelativeVelocityPctPerMin < 0 ? 'SHORT' : 'NEUTRAL';
  const residualAbs = Math.abs(btcRelativeResidualPct);
  const velocityAbs = Math.abs(btcRelativeVelocityPctPerMin);
  const strongResidual = residualAbs >= cfg.anomalyStrongResidualPct ? 1.25 : 1;
  const anomalyScore = round(clamp(
    (residualAbs * 30 + velocityAbs * 2.5 + Math.abs(acceleration) * 0.7) * strongResidual,
    0,
    50
  ), 2);
  const anomalyFresh = residualAbs >= cfg.anomalyMinResidualPct &&
    velocityAbs >= cfg.anomalyMinResidualPct / Math.max(1, cfg.fastHistoryMs / 60000);
  fastHistory.set(symbol, [...clean.slice(-8), { price, ts: t }]);
  return {
    price,
    spotPrice: spot,
    crossBasisPct: round(crossBasisPct, 4),
    movePct: round(movePct, 4),
    velocityPctPerMin: round(velocityPctPerMin, 4),
    acceleration: round(acceleration, 4),
    btcReference: cfg.btcReferenceSymbol,
    btcMovePct: round(refMovePct, 4),
    btcVelocityPctPerMin: round(refVelocityPctPerMin, 4),
    btcRelativeResidualPct: round(btcRelativeResidualPct, 4),
    btcRelativeVelocityPctPerMin: round(btcRelativeVelocityPctPerMin, 4),
    relativeSide,
    anomalyScore,
    anomalyFresh,
    anomalyTs: t
  };
}

function mispricingScore(metrics, premium, side) {
  if (!metrics) return 0;
  const dir = side === 'LONG' ? 1 : -1;
  const basis = Number(metrics.crossBasisPct || 0);
  const markBasis = Number(premium?.basisPct || 0);
  const funding = Number(premium?.fundingRatePct || 0);
  const velocity = Number(metrics.velocityPctPerMin || 0) * dir;
  const acceleration = Number(metrics.acceleration || 0) * dir;

  // Dislocation is a detector, not a claim of risk-free arbitrage. It is
  // stronger when the observed impulse agrees with the side being evaluated.
  const crossDislocation = clamp((-basis * dir) * 22, 0, 18);
  const markDislocation = clamp((-markBasis * dir) * 18, 0, 12);
  const fundingDislocation = clamp((-funding * dir) * 10, 0, 8);
  const impulse = clamp(velocity * 5 + acceleration * 2, 0, 12);
  const reversalPenalty = velocity < 0 ? 8 : 0;
  return round(clamp(crossDislocation + markDislocation + fundingDislocation + impulse - reversalPenalty, 0, 50), 2);
}

function runFastScanner() {
  if (fastLoopBusy || stopped) return;
  fastLoopBusy = true;
  try {
    const rows = [];
    // Establish the BTC reference first so every symbol is measured against
    // the same short-window move and velocity.
    const btc = fastMetrics(cfg.btcReferenceSymbol, null);
    const reference = btc || { movePct: 0, velocityPctPerMin: 0 };

    for (const symbol of marketInfo.keys()) {
      const m = symbol === cfg.btcReferenceSymbol ? reference : fastMetrics(symbol, reference);
      if (!m) continue;
      const p = premiumCache.get(symbol);
      const longMis = mispricingScore(m, p, 'LONG');
      const shortMis = mispricingScore(m, p, 'SHORT');
      const bestSide = longMis >= shortMis ? 'LONG' : 'SHORT';
      const best = Math.max(longMis, shortMis);
      rows.push({ symbol, ...m, mispricingLong: longMis, mispricingShort: shortMis, mispricingScore: best });
    }

    rows.sort((a, b) => (b.anomalyScore + b.mispricingScore) - (a.anomalyScore + a.mispricingScore));
    const fresh = rows.filter(x => x.anomalyFresh || (x.mispricingScore >= 12 && Math.abs(x.velocityPctPerMin) >= 0.08)).slice(0, 16);
    const bestLong = [...rows].sort((a,b) => (b.mispricingLong + b.anomalyScore * 0.35) - (a.mispricingLong + a.anomalyScore * 0.35))[0] || null;
    const bestShort = [...rows].sort((a,b) => (b.mispricingShort + b.anomalyScore * 0.35) - (a.mispricingShort + a.anomalyScore * 0.35))[0] || null;
    const bestOverall = rows[0] || null;

    state.fastScanner = {
      scanned: rows.length,
      freshSignals: fresh.length,
      spotConnected: state.spotConnected ? 1 : 0,
      btcMovePct: round(reference.movePct || 0, 4),
      btcVelocityPctPerMin: round(reference.velocityPctPerMin || 0, 4),
      bestLong: bestLong ? {symbol: bestLong.symbol, score: bestLong.mispricingLong, anomaly: bestLong.anomalyScore, residual: bestLong.btcRelativeResidualPct, basis: bestLong.crossBasisPct, velocity: bestLong.velocityPctPerMin} : null,
      bestShort: bestShort ? {symbol: bestShort.symbol, score: bestShort.mispricingShort, anomaly: bestShort.anomalyScore, residual: bestShort.btcRelativeResidualPct, basis: bestShort.crossBasisPct, velocity: bestShort.velocityPctPerMin} : null,
      bestOverall: bestOverall ? {symbol: bestOverall.symbol, side: bestOverall.relativeSide, score: bestOverall.mispricingScore, anomaly: bestOverall.anomalyScore, residual: bestOverall.btcRelativeResidualPct, basis: bestOverall.crossBasisPct, velocity: bestOverall.velocityPctPerMin} : null,
      lastScanMs: now()
    };

    state.anomalyScanner = {
      reference: cfg.btcReferenceSymbol,
      scanned: rows.length,
      anomalous: rows.filter(x => x.anomalyFresh).length,
      btcMovePct: round(reference.movePct || 0, 4),
      bestLong: [...rows].filter(x => x.btcRelativeVelocityPctPerMin > 0).sort((a,b) => b.anomalyScore-a.anomalyScore)[0] || null,
      bestShort: [...rows].filter(x => x.btcRelativeVelocityPctPerMin < 0).sort((a,b) => b.anomalyScore-a.anomalyScore)[0] || null,
      bestOverall: bestOverall || null,
      lastScanMs: now()
    };

    // Feed fresh signals into the normal cycle, with a timestamp so stale
    // anomalies cannot keep influencing decisions after the dislocation fades.
    for (const x of fresh.slice(0, 24)) {
      const old = ticks.get(x.symbol) || {};
      ticks.set(x.symbol, {
        ...old,
        fastMispricingScore: x.mispricingScore,
        fastPreferredSide: x.relativeSide,
        fastVelocityPctPerMin: x.velocityPctPerMin,
        fastAcceleration: x.acceleration,
        crossBasisPct: x.crossBasisPct,
        btcMovePct: x.btcMovePct,
        btcRelativeResidualPct: x.btcRelativeResidualPct,
        btcRelativeVelocityPctPerMin: x.btcRelativeVelocityPctPerMin,
        anomalyScore: x.anomalyScore,
        anomalyFresh: x.anomalyFresh,
        anomalyTs: x.anomalyTs
      });
    }
  } finally {
    fastLoopBusy = false;
  }
}
function freshFastData(symbol) {
  const f = ticks.get(symbol) || {};
  const ts = Number(f.anomalyTs || 0);
  if (!ts || now() - ts > cfg.anomalyStaleMs) return {};
  return f;
}

function edgeForSide(row, side) {
  const dir = side === 'LONG' ? 1 : -1;
  const m1 = Number(row.momentum1m || 0) * dir;
  const m5 = Number(row.momentum5m || 0) * dir;
  const m15 = Number(row.momentum15m || 0) * dir;
  const m30 = Number(row.momentum30m || 0) * dir;
  const bias = row.bias === side ? 1 : row.bias === 'NEUTRAL' ? 0.45 : 0;
  const traderRatio = Number(row.behavior?.positionRatio || 1);
  const trader = side === 'LONG' ? traderRatio - 1 : 1 - traderRatio;
  const consistency = Number(row.behavior?.consistency || 0);
  const oi = Number(row.openInterestChangePct || 0) * dir;
  const funding = Number(row.premium?.fundingRatePct || 0) * dir;
  const basis = Number(row.premium?.basisPct || 0) * dir;
  const fast = freshFastData(row.symbol);
  const fastMispricing = Number(fast.fastMispricingScore || 0);
  const fastSide = fast.fastPreferredSide || '';
  const fastAgreement = fastSide === side ? 1 : 0;
  const anomalyScore = Number(fast.anomalyScore || 0);
  const anomalyVelocity = Number(fast.btcRelativeVelocityPctPerMin || 0) * dir;
  const anomalyAgreement = anomalyVelocity > 0 ? 1 : 0;
  const volume = Math.max(0, Number(row.volumeRatio || 1) - 1);

  const techScore = clamp((m5 * 3 + m15 * 2 + m30) * 1.8, 0, 30);
  const biasScore = bias * 12;
  const behaviorScore = clamp(trader * 45, 0, 18) * (0.65 + consistency * 0.35);
  const oiScore = clamp(oi * 2.5, 0, 12);
  const flowScore = clamp(volume * 7, 0, 10);
  const crowdingScore = clamp(funding * 4 + basis * 3, 0, 10);

  // Early-entry layer: reward the beginning of an impulse/fresh breakout and
  // penalize entries that are already extended. This is deliberately symmetric
  // for LONG and SHORT; no global market regime can select the side.
  const early = side === 'LONG'
    ? (row.earlyStageLong ? cfg.earlyBreakoutBonus : 0)
    : (row.earlyStageShort ? cfg.earlyBreakoutBonus : 0);
  const velocity = Number(row.microVelocityPctPerMin || 0) * dir;
  const acceleration = Number(row.microAcceleration || 0) * dir;
  const impulseScore = clamp(velocity * 18 + acceleration * 8, 0, 8)
    + clamp(fastMispricing * 0.45, 0, 18)
    + clamp(anomalyScore * cfg.anomalyScoreWeight, 0, 20) * anomalyAgreement
    + (fastAgreement ? 4 : 0);
  const move15 = Math.max(0, m15);
  const extension = move15 >= cfg.lateExtensionPct ? clamp((move15 - cfg.lateExtensionPct) * 5, 0, 12) : 0;

  const total = clamp(techScore + biasScore + behaviorScore + oiScore + flowScore + crowdingScore + early + impulseScore - extension, 0, 100);
  return round(total, 2);
}

function enrichEdge(row) {
  const long = edgeForSide(row, 'LONG');
  const short = edgeForSide(row, 'SHORT');
  const preferredSide = long >= short ? 'LONG' : 'SHORT';
  const edgeScore = Math.max(long, short);
  const behaviorSide = row.behavior?.side || 'MIXTO';
  const agreement = behaviorSide === preferredSide ? 'CONFIRMADA' : behaviorSide === 'MIXTO' ? 'NEUTRA' : 'CONTRARIA';
  return { ...row, edgeLong: long, edgeShort: short, edgeScore, preferredSide, behaviorAgreement: agreement, anomalyScore: Number(row.anomalyScore || 0) };
}

async function mapLimit(items, limit, worker) {
  const out = [];
  for (let i = 0; i < items.length; i += limit) {
    const batch = items.slice(i, i + limit);
    const results = await Promise.all(batch.map(worker));
    out.push(...results);
  }
  return out;
}

async function buildMarketSnapshot() {
  await fetch24hr();

  const deepSymbols = selectDeepUniverse();
  state.deepScanned = deepSymbols.length;

  const technicals = [];
  const results = await mapLimit(deepSymbols, cfg.marketConcurrency, async symbol => {
    try {
      const [k1, k5] = await Promise.all([
        fetchKlines(symbol, '1m', cfg.klineLimit),
        fetchKlines(symbol, '5m', 70)
      ]);

      if (!Array.isArray(k1) || k1.length < 35 || !Array.isArray(k5) || k5.length < 20) {
        return null;
      }

      return analyzeKlines(symbol, k1, k5);
    } catch {
      return null;
    }
  });

  for (const r of results) if (r) technicals.push(r);

  technicals.sort((a, b) => {
    const sa = Math.max(Math.abs(a.momentum5m), Math.abs(a.momentum15m), Math.abs(a.change24h) * 0.15);
    const sb = Math.max(Math.abs(b.momentum5m), Math.abs(b.momentum15m), Math.abs(b.change24h) * 0.15);
    return sb - sa;
  });

  state.warmSymbols = technicals.length;

  // First layer: technical discovery. Then behavior/OI is applied to the
  // entire deep universe, so the behavioral layer can promote a less-famous
  // coin into the AI shortlist instead of being calculated only after the
  // shortlist has already been decided.
  const behaviorUniverse = technicals.slice(0, Math.min(cfg.traderTopSymbols, technicals.length));

  const enrichedAll = await mapLimit(behaviorUniverse, cfg.behaviorConcurrency, async row => {
    const [behavior, oi, premium] = await Promise.all([
      fetchTopTraderBehavior(row.symbol),
      fetchOpenInterest(row.symbol),
      fetchPremiumIndex(row.symbol)
    ]);

    return enrichEdge({
      ...row,
      behavior,
      openInterest: oi.value,
      openInterestChangePct: oi.changePct,
      premium
    });
  });

  // Rank after behavior/OI/funding/basis enrichment. This is the
  // screenshot-inspired EDGE SCANNER: technicals find movement, while
  // behavior + OI + funding/basis decide whether that movement is actionable.
  enrichedAll.sort((a, b) => Number(b.edgeScore || 0) - Number(a.edgeScore || 0));

  const enriched = enrichedAll.slice(0, Math.min(cfg.aiTopSymbols, enrichedAll.length));
  state.candidates = enriched.length;
  state.ranking = enriched.slice(0, 20);

  const longRank = [...enrichedAll].sort((a, b) => Number(b.edgeLong || 0) - Number(a.edgeLong || 0));
  const shortRank = [...enrichedAll].sort((a, b) => Number(b.edgeShort || 0) - Number(a.edgeShort || 0));
  const bestLong = longRank[0] || null;
  const bestShort = shortRank[0] || null;
  const bestOverall = enrichedAll[0] || null;
  const tradeableCount = enrichedAll.filter(x => Number(x.edgeScore || 0) >= 55).length;
  state.edgeScanner = {
    bestLong: bestLong ? { symbol: bestLong.symbol, score: bestLong.edgeLong, behavior: bestLong.behavior?.side || 'MIXTO', funding: bestLong.premium?.fundingRatePct || 0, basis: bestLong.premium?.basisPct || 0 } : null,
    bestShort: bestShort ? { symbol: bestShort.symbol, score: bestShort.edgeShort, behavior: bestShort.behavior?.side || 'MIXTO', funding: bestShort.premium?.fundingRatePct || 0, basis: bestShort.premium?.basisPct || 0 } : null,
    bestOverall: bestOverall ? { symbol: bestOverall.symbol, side: bestOverall.preferredSide, score: bestOverall.edgeScore } : null,
    tradeableCount,
    watchedCount: enrichedAll.length,
    avgEdge: round(avg(enrichedAll.map(x => Number(x.edgeScore || 0))), 2)
  };

  const longN = enriched.filter(x => x.bias === 'LONG').length;
  const shortN = enriched.filter(x => x.bias === 'SHORT').length;

  state.longPct = enriched.length ? Math.round(longN / enriched.length * 100) : 50;
  state.shortPct = 100 - state.longPct;

  const avg15 = avg(enriched.map(x => Number(x.momentum15m || 0)));
  const avgTrader = avg(enriched.map(x => Number(x.behavior?.accountRatio || 1)));

  state.behaviorBias =
    avgTrader >= 1.10 ? 'LONG' :
    avgTrader <= 0.90 ? 'SHORT' :
    'MIXTO';

  state.behaviorConfidence = Math.round(
    avg(enriched.map(x => Number(x.behavior?.consistency || 0))) * 100
  );

  state.behaviorCoverage = enriched.length
    ? Math.round(enriched.filter(x => Number(x.behavior?.samples || 0) > 0).length / enriched.length * 100)
    : 0;

  state.regime =
    state.longPct >= 62 && avg15 > 0 ? 'ALCISTA' :
    state.shortPct >= 62 && avg15 < 0 ? 'BAJISTA' :
    'MIXTO';

  return enriched;
}

function loadLearning() {
  try {
    const raw = JSON.parse(fs.readFileSync(learningFile, 'utf8'));
    learningTrades = Array.isArray(raw?.trades) ? raw.trades.slice(-cfg.learningMaxTrades) : [];
  } catch {
    learningTrades = [];
  }
}

function saveLearning() {
  try {
    fs.writeFileSync(
      learningFile,
      JSON.stringify({
        version: 1,
        updatedAt: new Date().toISOString(),
        trades: learningTrades.slice(-cfg.learningMaxTrades)
      }, null, 2)
    );
  } catch {}
}

function patternKey(features) {
  return [
    features.side || 'NA',
    features.bias || 'NEUTRAL',
    features.behaviorSide || 'MIXTO',
    features.regime || 'MIXTO',
    features.volBucket || 'NORMAL',
    features.category || 'NORMAL'
  ].join('|');
}

function learningSummary() {
  const trades = learningTrades.length;
  if (!trades) {
    return { trades: 0, wins: 0, losses: 0, winRate: 0, avgNetPct: 0, edgePatterns: 0 };
  }

  const wins = learningTrades.filter(t => Number(t.netPnlPct) > 0).length;
  const avgNetPct = avg(learningTrades.map(t => Number(t.netPnlPct || 0)));

  const map = new Map();
  for (const t of learningTrades) {
    const key = t.patternKey || 'UNKNOWN';
    const arr = map.get(key) || [];
    arr.push(Number(t.netPnlPct || 0));
    map.set(key, arr);
  }

  let edgePatterns = 0;
  for (const arr of map.values()) {
    if (arr.length >= cfg.learningMinSamples && avg(arr) > 0) edgePatterns++;
  }

  return {
    trades,
    wins,
    losses: trades - wins,
    winRate: round(wins / trades * 100, 2),
    avgNetPct: round(avgNetPct, 4),
    edgePatterns
  };
}

function learnedEdge(features) {
  const key = patternKey(features);
  const arr = learningTrades
    .filter(t => t.patternKey === key)
    .map(t => Number(t.netPnlPct || 0));

  if (arr.length < cfg.learningMinSamples) return {
    samples: arr.length,
    avgNetPct: 0,
    winRate: 0,
    usable: false
  };

  const wins = arr.filter(x => x > 0).length;
  return {
    samples: arr.length,
    avgNetPct: round(avg(arr), 4),
    winRate: round(wins / arr.length * 100, 2),
    usable: true
  };
}

function learnFromTrade(position, exitReason) {
  const entry = Number(position.entry || 0);
  const pnl = Number(position.pnl || 0);
  const fees = Number(position.fees || 0);
  const notional = Math.max(0.0000001, Number(position.entryNotional || 0));

  // paperClose stores position.pnl net of entry + exit fees. Do not subtract fees twice.
  const grossPct = notional ? pnl / notional * 100 : 0;
  const netPnl = pnl;
  const netPct = notional ? netPnl / notional * 100 : 0;

  const features = {
    side: position.side,
    bias: position.features?.bias || 'NEUTRAL',
    behaviorSide: position.features?.behaviorSide || 'MIXTO',
    regime: position.features?.regime || 'MIXTO',
    volBucket: position.features?.volBucket || 'NORMAL',
    category: position.features?.category || 'NORMAL'
  };

  learningTrades.push({
    ts: new Date().toISOString(),
    symbol: position.symbol,
    side: position.side,
    holdSec: Math.max(0, Math.round((now() - Number(position.openedTs || now())) / 1000)),
    grossPnl: round(pnl, 6),
    fees: round(fees, 6),
    netPnl: round(netPnl, 6),
    grossPnlPct: round(grossPct, 5),
    netPnlPct: round(netPct, 5),
    exitReason,
    patternKey: patternKey(features),
    features
  });

  if (learningTrades.length > cfg.learningMaxTrades) {
    learningTrades = learningTrades.slice(-cfg.learningMaxTrades);
  }

  saveLearning();
}

function riskAllowsOpen(symbol, margin, side) {
  if (stopped) return { ok: false, reason: 'STOP' };
  if (state.positions.length >= cfg.maxPositions) return { ok: false, reason: 'MAX_POSITIONS' };

  // Portfolio capacity is strict: max 6 LONG + max 6 SHORT = 12 total.
  // The direction is still chosen independently for each symbol; global regime
  // and Top Trader bias never force the side.
  const openLongs = state.positions.filter(p => p.side === 'LONG').length;
  const openShorts = state.positions.filter(p => p.side === 'SHORT').length;
  if (side === 'LONG' && openLongs >= cfg.maxLongPositions) return { ok: false, reason: 'MAX_LONG_POSITIONS' };
  if (side === 'SHORT' && openShorts >= cfg.maxShortPositions) return { ok: false, reason: 'MAX_SHORT_POSITIONS' };

  const totalMargin = state.positions.reduce((s, p) => s + Number(p.margin || 0), 0);
  const maxTotal = state.equity * cfg.maxTotalMarginPct / 100;

  if (totalMargin + margin > maxTotal) return { ok: false, reason: 'MAX_TOTAL_MARGIN' };

  const realizedLossPct = Math.max(0, -Number(state.realizedPnl || 0) / cfg.capital * 100);
  const equityDrawdownPct = Math.max(0, (cfg.capital - Number(state.equity || cfg.capital)) / cfg.capital * 100);
  if (realizedLossPct >= cfg.maxDailyLossPct) {
    return { ok: false, reason: 'DAILY_LOSS' };
  }

  const cycleLossSoFar = Number(state.cycleRealizedStart || state.realizedPnl) - Number(state.realizedPnl || 0);
  if (cycleLossSoFar >= cfg.maxCycleLossUsd) {
    return { ok: false, reason: 'CYCLE_LOSS_GUARD' };
  }

  if (equityDrawdownPct >= cfg.maxDrawdownPct) {
    return { ok: false, reason: 'MAX_DRAWDOWN' };
  }

  if ((cooldown.get(symbol) || 0) > now()) {
    return { ok: false, reason: 'COOLDOWN' };
  }

  return { ok: true };
}

function marginFor() {
  const equity = Math.max(0, Number(state.equity || cfg.capital));
  const byPosition = equity * cfg.maxPositionMarginPct / 100;
  const totalMax = equity * cfg.maxTotalMarginPct / 100;
  const used = state.positions.reduce((s, p) => s + Number(p.margin || 0), 0);
  return Math.max(0, Math.min(byPosition, totalMax - used));
}

function cleanJsonText(text) {
  let s = String(text || '').trim();
  if (s.startsWith('```')) {
    s = s.replace(/^```(?:json)?/i, '').replace(/```$/i, '').trim();
  }
  const a = s.indexOf('{');
  const b = s.lastIndexOf('}');
  return a >= 0 && b > a ? s.slice(a, b + 1) : s;
}

function actionOpportunity(action, marketRow) {
  if (!marketRow) return null;

  const direction = action === 'OPEN_LONG' ? 1 : -1;
  const tech =
    direction * Number(marketRow.momentum5m || 0) * 0.8 +
    direction * Number(marketRow.momentum15m || 0) * 0.5;

  const traderRatio = Number(marketRow.behavior?.positionRatio || 1);
  const traderSignal = direction === 1
    ? Math.log(Math.max(0.2, traderRatio))
    : -Math.log(Math.max(0.2, traderRatio));

  const volume = Math.max(0, Number(marketRow.volumeRatio || 1) - 1) * 0.15;
  const consistency = Number(marketRow.behavior?.consistency || 0) * 0.3;

  return tech + traderSignal * 0.6 + volume + consistency;
}

async function askAI(market, account) {
  if (!cfg.openaiKey) throw new Error('OPENAI_API_KEY no configurada');

  const positions = (account.positions || []).map(p => ({
    symbol: p.symbol,
    side: p.side,
    entry: p.entry,
    mark: p.mark,
    pnl: p.pnl,
    margin: p.margin,
    ageSec: p.openedTs ? Math.round((now() - p.openedTs) / 1000) : undefined,
    thesis: p.thesis
  }));

  const longs = positions.filter(p => p.side === 'LONG').length;
  const shorts = positions.filter(p => p.side === 'SHORT').length;

  const compactMarket = market.slice(0, cfg.aiTopSymbols).map(x => {
    const features = {
      side: x.bias,
      bias: x.bias,
      behaviorSide: x.behavior?.side || 'MIXTO',
      regime: state.regime,
      volBucket: x.atrPct >= 1.2 ? 'HIGH' : x.atrPct <= 0.25 ? 'LOW' : 'NORMAL',
      category: x.category
    };
    const learned = learnedEdge(features);

    return {
      symbol: x.symbol,
      category: x.category,
      price: x.price,
      bias: x.bias,
      change24h: x.change24h,
      m1: x.momentum1m,
      m5: x.momentum5m,
      m15: x.momentum15m,
      m30: x.momentum30m,
      rsi: x.rsi,
      atrPct: x.atrPct,
      volumeRatio: x.volumeRatio,
      breakoutUp: x.breakoutUp,
      breakoutDown: x.breakoutDown,
      freshBreakoutUp: x.freshBreakoutUp,
      freshBreakoutDown: x.freshBreakoutDown,
      earlyStageLong: x.earlyStageLong,
      earlyStageShort: x.earlyStageShort,
      microMovePct: x.microMovePct,
      microVelocityPctPerMin: x.microVelocityPctPerMin,
      microAcceleration: x.microAcceleration,
      distanceFromHighPct: x.distanceFromHighPct,
      distanceFromLowPct: x.distanceFromLowPct,
      edgeScore: x.edgeScore,
      edgeLong: x.edgeLong,
      edgeShort: x.edgeShort,
      preferredSide: x.preferredSide,
      behaviorAgreement: x.behaviorAgreement,
      fundingRatePct: x.premium?.fundingRatePct || 0,
      basisPct: x.premium?.basisPct || 0,
      crossVenueBasisPct: x.crossBasisPct || 0,
      fastMispricingScore: x.fastMispricingScore || 0,
      fastPreferredSide: x.fastPreferredSide || '—',
      fastVelocityPctPerMin: x.fastVelocityPctPerMin || 0,
      fastAcceleration: x.fastAcceleration || 0,
      btcReference: cfg.btcReferenceSymbol,
      btcMovePct: x.btcMovePct || 0,
      btcRelativeResidualPct: x.btcRelativeResidualPct || 0,
      btcRelativeVelocityPctPerMin: x.btcRelativeVelocityPctPerMin || 0,
      anomalyScore: x.anomalyScore || 0,
      anomalyFresh: Boolean(x.anomalyFresh),

      topTraderAccountRatio: x.behavior?.accountRatio || 1,
      topTraderPositionRatio: x.behavior?.positionRatio || 1,
      topTraderAccountLongPct: x.behavior?.accountLongPct || 50,
      topTraderAccountShortPct: x.behavior?.accountShortPct || 50,
      topTraderPositionLongPct: x.behavior?.positionLongPct || 50,
      topTraderPositionShortPct: x.behavior?.positionShortPct || 50,
      topTraderSide: x.behavior?.side || 'MIXTO',
      topTraderConsistency: x.behavior?.consistency || 0,
      topTraderDelta5m: x.behavior?.delta5m || 0,

      openInterest: x.openInterest || 0,
      openInterestChangePct: x.openInterestChangePct || 0,
      learnedSamples: learned.samples,
      learnedAvgNetPct: learned.avgNetPct,
      learnedWinRate: learned.winRate,
      learnedPatternUsable: learned.usable
    };
  });

  const payload = {
    timestamp: new Date().toISOString(),
    regime: state.regime,

    portfolio: {
      equity: state.equity,
      openPositions: positions.length,
      longs,
      shorts,
      slotsRemaining: Math.max(0, cfg.maxPositions - positions.length),
      directionPolicy: 'CAPACIDAD 6 LONG + 6 SHORT: objetivo 50/50 cuando existan oportunidades válidas; nunca forzar una entrada'
      ,maxLongPositions: cfg.maxLongPositions
      ,maxShortPositions: cfg.maxShortPositions
      ,profitTakeUsd: cfg.paperProfitTakeUsd
    },

    marketCoverage: {
      completeUniverse: state.symbols,
      deepScannedThisCycle: state.deepScanned,
      aiAnalyzed: compactMarket.length,
      behaviorCoveragePct: state.behaviorCoverage,
      behaviorBiasDiagnosticOnly: state.behaviorBias,
      behaviorConfidenceDiagnosticOnly: state.behaviorConfidence,
      btcReference: cfg.btcReferenceSymbol,
      anomalyScanner: state.anomalyScanner
    },

    learning: learningSummary(),
    edgeScanner: state.edgeScanner,
    fastScanner: state.fastScanner,
    market: compactMarket,
    positions
  };

  const instructions = `
Eres GALAXI V38, un motor autónomo de decisión para Binance USDⓈ-M Futures.

OBJETIVO:
Encontrar operaciones con expectativa neta positiva después de comisiones, gestionar
las posiciones existentes y evitar operar por impulso o por familiaridad con una moneda.

CAPAS QUE DEBES COMBINAR:
1) Mercado completo: no te limites a BTC/ETH ni a las monedas ya operadas.
2) Momentum y estructura multitemporal.
3) Volumen, volatilidad, RSI, EMA y rupturas.
4) Comportamiento agregado de los Top Traders de Binance.
5) Open Interest cuando esté disponible.
6) Memoria de resultados propios de GALAXI.
7) Estado actual de la cartera y tesis de cada posición.
8) EDGE SCANNER: compara LONG vs SHORT usando score técnico, comportamiento, OI, funding y basis. Usa el score como filtro de calidad, no como garantía.
9) FAST MISPRICING: usa la discrepancia entre futuros y spot, basis mark/index, funding y micro-movimiento para detectar el inicio de una corrección/impulso. La discrepancia es una señal relativa, NO arbitraje garantizado.
10) BTC-RELATIVE ANOMALY ENGINE: compara el movimiento/velocidad de cada símbolo con BTCUSDT en la misma ventana corta. El residual es 'movimiento de la moneda - movimiento de BTC'. Un residual grande significa desviación relativa, NO garantía de reversión. Usa además la dirección de la velocidad relativa para decidir si la anomalía está acelerándose en LONG o SHORT.

MODELO TIPO TERMINAL:
- 'Mispricing' en cripto se aproxima con futuros-vs-spot + mark-vs-index + funding + micro-impulso; no significa arbitraje garantizado.
- El FAST SCANNER vigila los ~528 símbolos cada ~1s usando WebSocket y sólo eleva candidatos al ciclo profundo cuando detecta movimiento/discrepancia.
- El ANOMALY ENGINE usa BTCUSDT como referencia común y busca residuales relativos en decenas/cientos de mercados; no priorices una moneda por fama, precio nominal o tamaño.
- Un residual positivo/negativo por sí solo NO decide LONG/SHORT. Confirma con velocidad relativa, volumen, OI, funding, basis, estructura y expectativa neta.
- Si la anomalía se vuelve vieja, ignórala: los campos anomalyFresh/anomalyTs tienen prioridad sobre señales antiguas.
- No esperes al cierre de una vela si la discrepancia y el impulso aparecen antes.
- 'Wallet tracker' se sustituye por comportamiento agregado Top Trader de Binance;
  no hay que inventar identidades ni copiar wallets individuales.
- 'Copytrade' significa seguir el sesgo agregado sólo cuando coincide con el resto
  de las señales.
- El ranking debe buscar oportunidad relativa y no popularidad de la moneda.

TOP TRADERS:
Los datos son agregados del 20% superior por saldo de margen; NO son identidades
individuales. Úsalos como señal de comportamiento colectivo y no como copia ciega.

PORTFOLIO / DIRECCIÓN:
- El universo operativo es el mercado completo de Binance (todos los símbolos cargados; actualmente ~528).
- GALAXI dispone de hasta ${cfg.maxPositions} posiciones simultáneas.
- La cartera tiene 12 plazas: máximo 6 LONG y máximo 6 SHORT.
- El objetivo de cartera es 6 LONG + 6 SHORT cuando existan suficientes oportunidades válidas; NO inventes operaciones para llenar una plaza.
- Cada símbolo se evalúa de forma independiente en LONG y SHORT.
- El régimen global (ALCISTA/BAJISTA/MIXTO) es contexto, NUNCA una instrucción de dirección.
- El sesgo agregado de Top Traders es una señal por símbolo, NUNCA una orden global de comprar o vender.
- Si ya hay 6 LONG, no abras más LONG; busca SHORT. Si ya hay 6 SHORT, no abras más SHORT.
- Si hay oportunidades válidas en ambos lados, completa progresivamente hacia 6L/6S.
- No abras una operación sólo para llenar una plaza: cada operación debe superar los filtros de entrada.

ENTRADA:
- Sólo OPEN_LONG/OPEN_SHORT cuando la tesis individual del símbolo esté confirmada.
- Evalúa LONG y SHORT por separado para CADA símbolo.
- No compares las direcciones a nivel global para descartar una de ellas: un LONG de un símbolo puede coexistir con un SHORT de otro.
- Prefiere oportunidades con edge, expectativa neta, liquidez, comportamiento, OI y estructura que coincidan.
- expected_net_pct debe superar ${cfg.minExpectedNetPct}% después de comisiones.
- PRIORIDAD DE TIMING: busca el inicio del movimiento. Usa freshBreakout, microVelocity, microAcceleration, volumen y momentum 1m/5m para entrar en los primeros momentos cuando la señal recién se confirma.
- ANOMALÍA RELATIVA: prioriza una oportunidad cuando el residual frente a BTC es material y la velocidad relativa confirma el mismo lado; si el residual es grande pero la velocidad se contradice, exige evidencia adicional y no fuerces la entrada.
- Evita entradas tardías cuando el movimiento ya está demasiado extendido; un score alto por sí solo NO justifica perseguir una vela ya corrida.
- No repitas una moneda sólo porque funcionó antes.
- Las monedas nuevas y memecoins compiten por mérito; no reciben una operación automática.
- No uses el precio nominal de una moneda como criterio de oportunidad.

SALIDA:
- CLOSE tiene prioridad sobre nuevas entradas.
- TAKE PROFIT DURO: cuando el PnL neto de una posición alcance +$8, ciérrala inmediatamente y toma la utilidad.
- GESTIÓN ACTIVA DE GANANCIA: si una posición entra en beneficio y el impulso empieza a revertirse, prioriza proteger la ganancia en vez de esperar pasivamente al máximo tiempo.
- Si una posición tuvo un beneficio relevante y pierde momentum 1m/5m, considera CLOSE aunque todavía no haya alcanzado el TP máximo.
- Cierra si la tesis se invalida, si el comportamiento/estructura cambia de forma clara,
  o si la expectativa futura deja de justificar mantener la posición.
- No mantengas una posición sólo para evitar reconocer una pérdida.
- No cierres por un tick aislado.

APRENDIZAJE:
La memoria es evidencia, no una garantía. Un patrón sólo debe influir de forma relevante
cuando tenga suficientes muestras. No cambies código ni inventes reglas nuevas.

Devuelve ÚNICAMENTE JSON válido.

Formato:
{
  "regime": "ALCISTA|BAJISTA|MIXTO",
  "actions": [
    {
      "action": "OPEN_LONG|OPEN_SHORT|CLOSE|HOLD",
      "symbol": "BTCUSDT",
      "margin_pct": 1.0,
      "expected_net_pct": 0.20,
      "reason": "explicación breve",
      "confidence": 0.82
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
        'Authorization': `Bearer ${cfg.openaiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: cfg.openaiModel,
        instructions,
        input: safeJson(payload),
        text: {
          format: {
            type: 'json_schema',
            name: 'galaxi_v38_trade_decision',
            strict: true,
            schema: {
              type: 'object',
              additionalProperties: false,
              properties: {
                regime: {
                  type: 'string',
                  enum: ['ALCISTA', 'BAJISTA', 'MIXTO']
                },
                actions: {
                  type: 'array',
                  maxItems: cfg.maxActionsPerCycle,
                  items: {
                    type: 'object',
                    additionalProperties: false,
                    properties: {
                      action: {
                        type: 'string',
                        enum: ['OPEN_LONG', 'OPEN_SHORT', 'CLOSE', 'HOLD']
                      },
                      symbol: { type: 'string' },
                      margin_pct: { type: 'number' },
                      expected_net_pct: { type: 'number' },
                      reason: { type: 'string' },
                      confidence: { type: 'number' }
                    },
                    required: [
                      'action',
                      'symbol',
                      'margin_pct',
                      'expected_net_pct',
                      'reason',
                      'confidence'
                    ]
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
    if (!res.ok) throw new Error(`OPENAI ${res.status}: ${text.slice(0, 500)}`);

    const data = JSON.parse(text);
    const outputText =
      data.output_text ||
      data.output?.flatMap(x => x.content || [])
        .find(x => x.type === 'output_text')?.text ||
      '';

    const decision = JSON.parse(cleanJsonText(outputText));
    state.aiCalls++;
    return decision;
  } finally {
    clearTimeout(timer);
  }
}

function findMarketRow(market, symbol) {
  return market.find(x => x.symbol === symbol);
}

function validateAndRankActions(decision, market) {
  const actions = Array.isArray(decision.actions) ? decision.actions : [];
  const rows = [];

  for (const a of actions) {
    const action = String(a.action || '').toUpperCase();
    const symbol = String(a.symbol || '').toUpperCase();
    const row = findMarketRow(market, symbol);

    if (!['OPEN_LONG', 'OPEN_SHORT', 'CLOSE', 'HOLD'].includes(action)) continue;
    if (!symbol || !marketInfo.has(symbol)) continue;

    if (action === 'CLOSE') {
      const exists = state.positions.some(p => p.symbol === symbol);
      if (!exists) continue;
      rows.push({ ...a, action, symbol });
      continue;
    }

    if (action === 'HOLD') continue;

    if (!row) continue;

    const expected = Number(a.expected_net_pct || 0);
    const confidence = clamp(Number(a.confidence || 0), 0, 1);

    if (expected < cfg.minExpectedNetPct) continue;
    if (confidence < 0.55) continue;

    const side = action === 'OPEN_LONG' ? 'LONG' : 'SHORT';
    const marginPct = clamp(Number(a.margin_pct || 1), 0.25, cfg.maxPositionMarginPct);

    const features = {
      side,
      bias: row.bias,
      behaviorSide: row.behavior?.side || 'MIXTO',
      regime: state.regime,
      volBucket: row.atrPct >= 1.2 ? 'HIGH' : row.atrPct <= 0.25 ? 'LOW' : 'NORMAL',
      category: row.category
    };

    const learned = learnedEdge(features);
    const learningAdjustment =
      learned.usable ? clamp(learned.avgNetPct * 0.20, -0.15, 0.15) : 0;

    // A historically weak pattern must clear a slightly higher hurdle.
    if (learned.usable && learned.avgNetPct < 0 && expected + learningAdjustment < cfg.minExpectedNetPct + 0.05) {
      continue;
    }

    rows.push({
      ...a,
      action,
      symbol,
      side,
      margin_pct: marginPct,
      expected_net_pct: expected,
      confidence,
      learning: learned
    });
  }

  // CLOSE first: risk management is more important than filling new slots.
  rows.sort((a, b) => {
    if (a.action === 'CLOSE' && b.action !== 'CLOSE') return -1;
    if (b.action === 'CLOSE' && a.action !== 'CLOSE') return 1;

    const sa = Number(a.expected_net_pct || 0) * Number(a.confidence || 0);
    const sb = Number(b.expected_net_pct || 0) * Number(b.confidence || 0);
    return sb - sa;
  });

  return rows;
}

function paperOpen(a, marketRow) {
  const symbol = a.symbol;
  if (state.positions.some(p => p.symbol === symbol)) {
    return { skipped: true, reason: 'SYMBOL_ALREADY_OPEN' };
  }

  const side = a.action === 'OPEN_LONG' ? 'LONG' : 'SHORT';
  const marginPct = clamp(Number(a.margin_pct || 1), 0.25, cfg.maxPositionMarginPct);
  const margin = Math.min(state.equity * marginPct / 100, marginFor());

  const risk = riskAllowsOpen(symbol, margin, side);
  if (!risk.ok) return { skipped: true, reason: risk.reason };

  const price = Number(ticks.get(symbol)?.price || marketRow?.price || 0);
  if (!(price > 0) || !(margin > 0)) {
    return { skipped: true, reason: 'NO_PRICE_OR_MARGIN' };
  }

  const qty = normalizeQty(symbol, margin * cfg.leverage / price);
  if (!(qty > 0)) return { skipped: true, reason: 'QTY_TOO_SMALL' };

  const notional = qty * price;
  const entryFee = notional * cfg.estimatedFeeRate;

  const p = {
    id: `V32_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    symbol,
    side,
    entry: normalizePrice(symbol, price),
    current: normalizePrice(symbol, price),
    margin,
    qty,
    entryNotional: notional,
    fees: entryFee,
    pnl: -entryFee,
    unrealizedPct: -entryFee / margin * 100,
    leverage: cfg.leverage,

    confidence: Number(a.confidence || 0),
    expectedNetPct: Number(a.expected_net_pct || 0),
    strategy: 'V34_INDEPENDENT_12_EDGE',
    thesis: a.reason,

    features: {
      bias: marketRow.bias,
      behaviorSide: marketRow.behavior?.side || 'MIXTO',
      regime: state.regime,
      volBucket: marketRow.atrPct >= 1.2 ? 'HIGH' : marketRow.atrPct <= 0.25 ? 'LOW' : 'NORMAL',
      category: marketRow.category
    },

    openedAt: new Date().toISOString(),
    openedTs: now(),
    peakPnl: -entryFee,
    peakUnrealizedPct: -entryFee / margin * 100,
    profitLockActive: false
  };

  state.positions.push(p);
  cooldown.set(symbol, now() + 60000);

  pushHistory({
    action: 'AI_OPEN',
    symbol,
    side,
    margin: round(margin, 4),
    expectedNetPct: round(a.expected_net_pct, 4),
    reason: a.reason
  });

  return { opened: true };
}

function paperClose(a, reasonOverride = null) {
  const p = state.positions.find(x => x.symbol === a.symbol);
  if (!p) return { skipped: true, reason: 'NO_POSITION' };

  const t = ticks.get(p.symbol);
  if (t?.price) p.current = t.price;

  const move = p.side === 'LONG'
    ? p.current - p.entry
    : p.entry - p.current;

  const grossPnl = move * p.qty;
  const exitNotional = Math.abs(p.current * p.qty);
  const exitFee = exitNotional * cfg.estimatedFeeRate;

  const netPnl = grossPnl - p.fees - exitFee;
  p.fees += exitFee;
  p.pnl = netPnl;

  state.realizedPnl += netPnl;
  state.positions = state.positions.filter(x => x.id !== p.id);
  cooldown.set(p.symbol, now() + 60000);

  const reason = reasonOverride || a.reason || 'AI_CLOSE';
  learnFromTrade(p, reason);

  pushHistory({
    action: 'AI_CLOSE',
    symbol: p.symbol,
    side: p.side,
    pnl: round(p.pnl, 4),
    fees: round(p.fees, 4),
    reason
  });

  state.lastExit = { symbol: p.symbol, side: p.side, pnl: round(p.pnl, 4), reason, peakPnl: round(p.peakPnl || p.pnl, 4), time: new Date().toISOString() };
  state.lastSignal =
    `AI PAPER CLOSE ${p.symbol} ${p.side} · PnL ${p.pnl.toFixed(2)} · pico ${Number(p.peakPnl || p.pnl).toFixed(2)} · ${reason}`;

  return { closed: true };
}

function markPaperPositions(market) {
  const keep = [];
  const tNow = now();
  const marketMap = new Map((market || []).map(x => [x.symbol, x]));

  for (const p of state.positions) {
    const t = ticks.get(p.symbol);
    if (t?.price) p.current = t.price;

    const move = p.side === 'LONG'
      ? p.current - p.entry
      : p.entry - p.current;

    const grossPnl = move * p.qty;
    p.pnl = grossPnl - p.fees;
    p.unrealizedPct = p.margin ? p.pnl / p.margin * 100 : 0;

    p.peakPnl = Math.max(Number(p.peakPnl ?? p.pnl), Number(p.pnl));
    p.peakUnrealizedPct = Math.max(Number(p.peakUnrealizedPct ?? p.unrealizedPct), Number(p.unrealizedPct));
    if (p.peakUnrealizedPct >= cfg.paperProfitLockTriggerPct) p.profitLockActive = true;

    const age = tNow - p.openedTs;
    const tp = p.unrealizedPct >= cfg.paperTpPct;
    const sl = p.unrealizedPct <= -cfg.paperSlPct;
    const hardLoss = p.pnl <= -Math.abs(cfg.paperHardLossUsd);
    const timeout = age >= cfg.paperMaxHoldMs;
    const cashTake = p.pnl >= cfg.paperProfitTakeUsd;
    const giveback = p.profitLockActive &&
      p.unrealizedPct <= p.peakUnrealizedPct - cfg.paperProfitGivebackPct &&
      p.unrealizedPct >= cfg.paperMinLockedPct;

    const row = marketMap.get(p.symbol);
    const m1 = Number(row?.momentum1m || 0);
    const m5 = Number(row?.momentum5m || 0);
    const reversal = p.profitLockActive && p.pnl > 0 && (
      (p.side === 'LONG' && m1 < -0.10 && m5 < -0.03) ||
      (p.side === 'SHORT' && m1 > 0.10 && m5 > 0.03)
    );

    if (!Number.isFinite(p.current) || p.current <= 0) {
      keep.push(p);
      continue;
    }

    if (tp || cashTake || giveback || reversal || hardLoss || sl || timeout) {
      const reason = cashTake ? 'PROFIT_USD' : tp ? 'TP' : giveback ? 'PROFIT_LOCK' : reversal ? 'MOMENTUM_REVERSAL' : hardLoss ? 'HARD_LOSS_USD' : sl ? 'SL' : 'TIME';
      paperClose({ symbol: p.symbol, reason }, reason);
    } else {
      keep.push(p);
    }
  }

  const openIds = new Set(keep.map(x => x.id));
  state.positions = state.positions.filter(x => openIds.has(x.id));
}

function emergencyStopCheck() {
  if (fs.existsSync(controlFile)) {
    try {
      const c = JSON.parse(fs.readFileSync(controlFile, 'utf8'));
      if (c.stop) stopped = true;
    } catch {}
  }

  if (
    state.drawdownPct >= cfg.maxDrawdownPct ||
    Math.max(0, -state.dailyLossPct) >= cfg.maxDailyLossPct
  ) {
    stopped = true;
    state.lastSignal = 'STOP AUTOMÁTICO POR RIESGO';
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
        margin: Math.abs(Number(p.notional || 0)) / cfg.leverage,
        openedTs: undefined
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
      symbol: p.symbol,
      side: p.side,
      qty: p.qty,
      entry: p.entry,
      mark: p.current,
      pnl: p.pnl,
      margin: p.margin,
      openedTs: p.openedTs,
      thesis: p.thesis
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
    state.lastError = `Leverage ${symbol}: ${e.message}`;
  }
}

async function placeMarketOrder(symbol, side, qty, reduceOnly = false) {
  if (!cfg.binanceKey || !cfg.binanceSecret) {
    throw new Error('BINANCE API no configurada');
  }

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

  const order = await rest('/fapi/v1/order', {
    method: 'POST',
    params
  }, true);

  lastOrderTs = Date.now();
  return order;
}

async function executeLiveAction(a, marketRow) {
  if (!cfg.liveArmed) throw new Error('LIVE bloqueado: LIVE_ARMED=true requerido');

  const live = await getLiveAccount();
  const existing = live.positions.find(p => p.symbol === a.symbol);

  if (a.action === 'CLOSE') {
    if (!existing) return { skipped: true, reason: 'NO_POSITION' };

    const closeSide = existing.side === 'LONG' ? 'SELL' : 'BUY';
    const order = await placeMarketOrder(existing.symbol, closeSide, existing.qty, true);

    pushHistory({
      action: 'LIVE_CLOSE',
      symbol: existing.symbol,
      side: existing.side,
      orderId: order.orderId,
      reason: a.reason
    });

    return { order };
  }

  if (a.action === 'OPEN_LONG' || a.action === 'OPEN_SHORT') {
    if (existing) return { skipped: true, reason: 'SYMBOL_ALREADY_OPEN' };

    const sideName = a.action === 'OPEN_LONG' ? 'LONG' : 'SHORT';
    const margin = live.equity *
      clamp(Number(a.margin_pct || 1), 0.25, cfg.maxPositionMarginPct) / 100;

    const risk = riskAllowsOpen(a.symbol, margin, sideName);
    if (!risk.ok) return { skipped: true, reason: risk.reason };

    const notional = margin * cfg.leverage;
    const price = Number(ticks.get(a.symbol)?.price || marketRow?.price || 0);
    if (!(price > 0)) return { skipped: true, reason: 'NO_PRICE' };

    const qty = normalizeQty(a.symbol, notional / price);
    if (!(qty > 0)) return { skipped: true, reason: 'QTY_TOO_SMALL' };

    const side = a.action === 'OPEN_LONG' ? 'BUY' : 'SELL';
    const order = await placeMarketOrder(a.symbol, side, qty, false);

    cooldown.set(a.symbol, now() + 60000);

    pushHistory({
      action: 'LIVE_OPEN',
      symbol: a.symbol,
      side: sideName,
      qty,
      margin,
      expectedNetPct: a.expected_net_pct,
      orderId: order.orderId,
      reason: a.reason
    });

    return { order };
  }

  return { skipped: true, reason: 'HOLD' };
}

async function executeDecision(decision, market) {
  state.aiDecision = decision;
  state.aiReasoning = decision.summary || '';
  state.regime = decision.regime || state.regime;

  const actions = validateAndRankActions(decision, market)
    .slice(0, cfg.maxActionsPerCycle);

  state.riskApproved = actions.length;

  // CLOSE is always processed before new positions.
  const closes = actions.filter(a => a.action === 'CLOSE');
  const opens = actions.filter(a => a.action !== 'CLOSE');

  if (cfg.mode === 'PAPER') {
    for (const a of closes) {
      try { paperClose(a); }
      catch (e) { state.lastError = `PAPER CLOSE: ${e.message}`; }
    }

    for (const a of opens) {
      if (stopped) break;
      try {
        const row = findMarketRow(market, a.symbol);
        if (row) paperOpen(a, row);
      } catch (e) {
        state.lastError = `PAPER OPEN: ${e.message}`;
      }
    }

    return;
  }

  if (cfg.mode === 'LIVE') {
    if (!cfg.liveArmed) {
      state.lastSignal = 'LIVE seleccionado pero LIVE_ARMED=false';
      return;
    }

    for (const a of closes) {
      try { await executeLiveAction(a, findMarketRow(market, a.symbol)); }
      catch (e) {
        state.lastError = `LIVE CLOSE: ${e.message}`;
        pushHistory({ action: 'LIVE_ERROR', symbol: a.symbol, reason: e.message });
      }
    }

    for (const a of opens) {
      try { await executeLiveAction(a, findMarketRow(market, a.symbol)); }
      catch (e) {
        state.lastError = `LIVE OPEN: ${e.message}`;
        pushHistory({ action: 'LIVE_ERROR', symbol: a.symbol, reason: e.message });
      }
    }
  }
}

function connectSpot() {
  try {
    spotWs = new WebSocket(cfg.spotWsUrl);
    spotWs.on('open', () => { state.spotConnected = 1; });
    spotWs.on('close', () => {
      state.spotConnected = 0;
      if (!stopped && !spotReconnectTimer) {
        spotReconnectTimer = setTimeout(() => { spotReconnectTimer = null; connectSpot(); }, 3000);
      }
    });
    spotWs.on('error', e => { state.lastError = `Spot WS: ${e.message || 'error'}`; });
    spotWs.on('message', raw => {
      try {
        const arr = JSON.parse(raw.toString());
        if (!Array.isArray(arr)) return;
        for (const t of arr) {
          const symbol = t.s;
          if (!symbol || !symbol.endsWith('USDT')) continue;
          const price = Number(t.c);
          if (price > 0) spotTicks.set(symbol, { price, ts: now(), volume: Number(t.q || 0) });
        }
      } catch {}
    });
  } catch (e) { state.lastError = `Spot WS init: ${e.message}`; }
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

        for (const t of arr) {
          const symbol = t.s;
          if (!symbol?.endsWith('USDT') || !marketInfo.has(symbol)) continue;

          const price = Number(t.c);
          const volume = Number(t.q || 0);
          if (price > 0) {
            const old = ticks.get(symbol) || {};
            ticks.set(symbol, {
              ...old,
              price,
              volume,
              ts: now()
            });
          }
        }
      } catch {
        state.lastError = 'WS parse error';
      }
    });
  } catch (e) {
    state.lastError = `WS init: ${e.message}`;
  }
}

async function runCycle() {
  if (loopBusy || stopped) return;
  loopBusy = true;

  try {
    emergencyStopCheck();
    state.cycle++;
    cycleRealizedStart = state.realizedPnl;
    state.cycleRealizedStart = cycleRealizedStart;

    if (!state.wsConnected) {
      state.lastSignal = 'Esperando WebSocket de Binance…';
      writeState();
      return;
    }

    state.lastSignal =
      `MERCADO COMPLETO ${state.symbols} · descubriendo oportunidades…`;

    const market = await buildMarketSnapshot();

    if (market.length < 8) {
      state.lastSignal = `Calentando datos · deep=${market.length}`;
      writeState();
      return;
    }

    if (cfg.mode === 'PAPER') {
      markPaperPositions(market);
      writeState();
    } else {
      try {
        lastAccount = await getLiveAccount();
        state.equity = lastAccount.equity;
        state.unrealizedPnl = lastAccount.unrealizedPnl;
        state.positions = lastAccount.positions;
      } catch (e) {
        state.lastError = `Cuenta LIVE: ${e.message}`;
      }
    }

    const account =
      cfg.mode === 'LIVE'
        ? (lastAccount || await getLiveAccount())
        : paperAccount();

    const decision = await askAI(market, account);
    await executeDecision(decision, market);

    const cycleLoss = cycleRealizedStart - state.realizedPnl;
    state.cycleLossUsd = round(Math.max(0, cycleLoss), 2);
    if (cycleLoss >= cfg.maxCycleLossUsd) {
      stopped = true;
      state.lastSignal = `STOP: pérdida de ciclo $${cycleLoss.toFixed(2)} >= $${cfg.maxCycleLossUsd.toFixed(2)}`;
    }

    state.lastSignal = decision.summary || 'IA evaluó el mercado';

    if (state.cycle % 5 === 0) {
      console.log(
        `cycle=${state.cycle}` +
        ` universe=${state.symbols}` +
        ` deep=${state.deepScanned}` +
        ` ai=${market.length}` +
        ` portfolio=${state.positions.length}` +
        ` L=${state.longOpen} S=${state.shortOpen}` +
        ` equity=${Number(state.equity).toFixed(2)}` +
        ` regime=${state.regime}` +
        ` behavior=${state.behaviorBias}` +
        ` aiModel=${cfg.openaiModel}`
      );
    }
  } catch (e) {
    state.aiErrors += 1;
    state.lastError = e?.message || String(e);
    console.error('CYCLE_ERROR', state.lastError);
  } finally {
    writeState();
    loopBusy = false;
  }
}

function loadControl() {
  if (!fs.existsSync(controlFile)) {
    try {
      fs.writeFileSync(controlFile, JSON.stringify({ stop: false }, null, 2));
    } catch {}
  }
}

async function boot() {
  console.log(
    `GALAXI V38 ANOMALY ENGINE | mode=${cfg.mode}` +
    ` | model=${cfg.openaiModel}` +
    ` | scan=${cfg.scanMs}ms` +
    ` | deep=${cfg.deepScanSymbols}` +
    ` | ai=${cfg.aiTopSymbols}` +
    ` | slots=12 independiente` +
    ` | fast=${cfg.fastScannerMs}ms`
  );

  loadControl();
  loadLearning();

  if (!cfg.openaiKey) {
    console.warn('OPENAI_API_KEY is missing. AI decisions cannot run.');
  }

  if (cfg.mode === 'LIVE' && !cfg.liveArmed) {
    console.warn('LIVE is NOT ARMED. Set LIVE_ARMED=true only when intentional.');
  }

  await syncServerTime();
  await loadExchangeInfo();
  connect();
  connectSpot();
  setInterval(runFastScanner, cfg.fastScannerMs);

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
  try { spotWs?.close(); } catch {}
  process.exit(0);
});
