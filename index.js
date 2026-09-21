import fs from 'node:fs';
import WebSocket from 'ws';

const ROOT = new URL('.', import.meta.url).pathname;
const runtimeFile = `${ROOT}galaxi-runtime.json`;
const controlFile = `${ROOT}galaxi-control.json`;

const cfg = {
  mode: String(process.env.TRADING_MODE || 'PAPER').toUpperCase(),
  capital: Number(process.env.PAPER_START_CAPITAL || 10000),
  maxPositions: Math.min(12, Math.max(1, Number(process.env.MAX_POSITIONS || 12))),
  maxTotalMarginPct: Math.min(80, Math.max(1, Number(process.env.MAX_TOTAL_MARGIN_PCT || 30))),
  maxPositionMarginPct: Math.min(10, Math.max(0.25, Number(process.env.MAX_POSITION_MARGIN_PCT || 3))),
  interval: Math.max(5000, Number(process.env.SCAN_INTERVAL_MS || 20000)),
  dataRefreshMs: Math.max(30000, Number(process.env.CANDLE_REFRESH_MS || 60000)),
  candleSymbols: Math.min(50, Math.max(10, Number(process.env.CANDLE_SYMBOLS || 30))),
  maxQuoteVolume: Number(process.env.MIN_QUOTE_VOLUME || 0),
  takeProfitPct: Number(process.env.TAKE_PROFIT_PCT || 1.2),
  stopLossPct: Number(process.env.STOP_LOSS_PCT || 0.7),
  maxHoldMs: Number(process.env.MAX_HOLD_MS || 1800000),
  cooldownMs: Number(process.env.COOLDOWN_MS || 600000),
  feePct: Number(process.env.PAPER_FEE_PCT || 0.04),
  leverage: Math.min(20, Math.max(1, Number(process.env.PAPER_LEVERAGE || 5))),
  apiBase: process.env.BINANCE_FUTURES_REST || 'https://fapi.binance.com',
  wsUrl: process.env.BINANCE_FUTURES_WS || 'wss://fstream.binance.com/ws/!miniTicker@arr',
  tickerPollMs: Math.max(5000, Number(process.env.TICKER_POLL_MS || 5000)),
  maxTickerSymbols: Math.min(500, Math.max(50, Number(process.env.MAX_TICKER_SYMBOLS || 250)))
};

const state = {
  running: true, stopped: false, mode: cfg.mode, equity: cfg.capital, initialCapital: cfg.capital,
  realizedPnl: 0, unrealizedPnl: 0, todayPnl: 0, drawdownPct: 0, dailyLossPct: 0,
  symbols: 0, warmSymbols: 0, cycle: 0, wsConnected: 0, wsExpected: 1,
  candidates: 0, riskApproved: 0, portfolioCount: 0, regime: 'MIXTO',
  timeframes: { '20s': '—', '1m': '—', '3m': '—', '5m': '—' },
  longPct: 50, shortPct: 50, positions: [], ranking: [], history: [], news: [],
  lastSignal: 'Calentando mercado…', lastError: null, restCalls: 0, rate429: 0, rate418: 0,
  dataQuality: 'TICKS', lastUpdate: null, decisionNote: 'Esperando datos suficientes.'
};

const ticks = new Map();
const tickSeries = new Map();
const candles = new Map();
const cooldown = new Map();
let ws = null, stopped = false, reconnectTimer = null, lastCandleRefresh = 0, loopTimer = null, tickerTimer = null, wsLastMessage = 0;

const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const pctMove = (now, then) => then > 0 ? (now / then - 1) * 100 : 0;
const avg = (a, fn = x => x) => a.length ? a.reduce((s, x) => s + fn(x), 0) / a.length : 0;
const std = a => { if (a.length < 2) return 0; const m = avg(a); return Math.sqrt(avg(a, x => (x - m) ** 2)); };
const safe = n => Number.isFinite(n) ? n : 0;

function ema(values, period) {
  if (!values.length) return 0;
  const k = 2 / (period + 1);
  let e = values[0];
  for (let i = 1; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}
function rsi(values, period = 14) {
  if (values.length < period + 1) return 50;
  let gain = 0, loss = 0;
  for (let i = values.length - period; i < values.length; i++) {
    const d = values[i] - values[i - 1];
    if (d >= 0) gain += d; else loss -= d;
  }
  if (loss === 0) return 100;
  return 100 - (100 / (1 + gain / loss));
}
function atr(cs, period = 14) {
  if (cs.length < period + 1) return 0;
  const trs = [];
  for (let i = 1; i < cs.length; i++) {
    const c = cs[i], p = cs[i - 1];
    trs.push(Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close)));
  }
  return avg(trs.slice(-period));
}
function bollinger(values, period = 20) {
  const v = values.slice(-period); if (v.length < period) return { mid: avg(v), upper: 0, lower: 0, width: 0 };
  const mid = avg(v), s = std(v);
  return { mid, upper: mid + 2 * s, lower: mid - 2 * s, width: mid ? (4 * s / mid) * 100 : 0 };
}
function normalizeKlines(raw) {
  return raw.map(k => ({
    ts: Number(k[0]), open: Number(k[1]), high: Number(k[2]), low: Number(k[3]), close: Number(k[4]),
    volume: Number(k[5]), closeTs: Number(k[6]), quoteVolume: Number(k[7]), trades: Number(k[8]),
    takerBuyBase: Number(k[9]), takerBuyQuote: Number(k[10])
  })).filter(x => x.close > 0);
}
function sampleAt(a, msAgo) {
  const target = Date.now() - msAgo;
  for (let i = a.length - 1; i >= 0; i--) if (a[i].ts <= target) return a[i].price;
  return a[0]?.price || 0;
}
function pushTick(symbol, item) {
  let a = tickSeries.get(symbol); if (!a) { a = []; tickSeries.set(symbol, a); }
  a.push(item);
  const cutoff = Date.now() - 12 * 60 * 1000;
  while (a.length && a[0].ts < cutoff) a.shift();
}
function tickFeatures(symbol) {
  const a = tickSeries.get(symbol) || [];
  if (a.length < 12) return null;
  const now = a[a.length - 1].price;
  const p20 = sampleAt(a, 20000), p60 = sampleAt(a, 60000), p180 = sampleAt(a, 180000), p300 = sampleAt(a, 300000);
  const m20 = pctMove(now, p20), m60 = pctMove(now, p60), m180 = pctMove(now, p180), m300 = pctMove(now, p300);
  const recent = avg(a.slice(-8), x => x.price), prior = avg(a.slice(-24, -8).map(x => x.price)) || recent;
  const micro = pctMove(recent, prior);
  const returns = [];
  for (let i = Math.max(1, a.length - 80); i < a.length; i++) returns.push(pctMove(a[i].price, a[i - 1].price));
  return { m20, m60, m180, m300, micro, vol: std(returns), price: now };
}
function candleFeatures(symbol) {
  const byTf = candles.get(symbol);
  if (!byTf) return null;
  const out = {};
  for (const tf of ['1m', '5m', '15m']) {
    const cs = byTf[tf] || [];
    if (cs.length < 25) continue;
    const closes = cs.map(x => x.close);
    const e9 = ema(closes, 9), e21 = ema(closes, 21), e55 = ema(closes, 55);
    const last = cs.at(-1), a = atr(cs), bb = bollinger(closes), rv = rsi(closes);
    const recentHigh = Math.max(...cs.slice(-20).map(x => x.high));
    const recentLow = Math.min(...cs.slice(-20).map(x => x.low));
    const volNow = avg(cs.slice(-3), x => x.quoteVolume);
    const volBase = avg(cs.slice(-20, -3), x => x.quoteVolume) || volNow;
    const buyRatio = last.quoteVolume > 0 ? last.takerBuyQuote / last.quoteVolume : 0.5;
    out[tf] = {
      close: last.close, ema9: e9, ema21: e21, ema55: e55, rsi: rv, atr: a,
      atrPct: last.close ? a / last.close * 100 : 0, bbWidth: bb.width,
      breakoutUp: last.close > recentHigh * 0.999, breakoutDown: last.close < recentLow * 1.001,
      volumeRatio: volBase ? volNow / volBase : 1, buyRatio,
      bodyPct: last.open ? (last.close / last.open - 1) * 100 : 0
    };
  }
  return out;
}
function scoreOpportunity(symbol, tick, cf, market) {
  const t = tickFeatures(symbol); if (!t) return null;
  const f1 = cf?.['1m'], f5 = cf?.['5m'], f15 = cf?.['15m'];
  const technical = [];
  const add = (v, w) => technical.push([safe(v), w]);
  add(t.m20, 0.15); add(t.m60, 0.15); add(t.m180, 0.10); add(t.m300, 0.08); add(t.micro, 0.05);
  if (f1) { add((f1.ema9 / f1.ema21 - 1) * 100, 0.10); add((f1.ema21 / f1.ema55 - 1) * 100, 0.06); add((f1.rsi - 50) / 8, 0.05); add((f1.buyRatio - 0.5) * 10, 0.04); add((f1.volumeRatio - 1) * 0.8, 0.03); }
  if (f5) { add((f5.ema9 / f5.ema21 - 1) * 100, 0.08); add((f5.ema21 / f5.ema55 - 1) * 100, 0.06); add((f5.rsi - 50) / 10, 0.03); }
  if (f15) { add((f15.ema21 / f15.ema55 - 1) * 100, 0.06); add((f15.rsi - 50) / 12, 0.02); }
  let direction = technical.reduce((s, [v, w]) => s + v * w, 0);
  const marketBias = market.bias;
  direction += marketBias * 0.12;

  // Adaptive conviction: no fixed "score > X" entry gate. The engine ranks all usable markets.
  const side = direction >= 0 ? 'LONG' : 'SHORT';
  const abs = Math.abs(direction);
  const agreement = [t.m20, t.m60, t.m180, f1 ? f1.ema9 - f1.ema21 : 0, f5 ? f5.ema9 - f5.ema21 : 0]
    .map(v => Math.sign(v)).filter(v => v !== 0);
  const agreePct = agreement.length ? Math.abs(avg(agreement.map(v => v === Math.sign(direction) ? 1 : 0))) : 0;
  const volatility = f1?.atrPct || t.vol || 0;
  const volumeBoost = clamp(((f1?.volumeRatio || 1) - 1) * 12, -8, 10);
  const breakout = side === 'LONG' ? (f1?.breakoutUp ? 7 : 0) : (f1?.breakoutDown ? 7 : 0);
  const extension = f1 ? Math.abs((f1.close / f1.ema21 - 1) * 100) : Math.abs(t.m60);
  const overextended = extension > Math.max(0.9, volatility * 2.5) ? -8 : 0;
  const score = clamp(Math.round(50 + abs * 22 + agreePct * 16 + volumeBoost + breakout + overextended), 1, 99);
  const confidence = clamp(Math.round(50 + abs * 30 + agreePct * 15 + (f1 ? 5 : 0) + volumeBoost / 2), 1, 97);
  const expectedMove = clamp(Math.max(0.25, volatility * 1.8 + abs * 0.7), 0.25, 3.5);
  const rr = expectedMove / Math.max(0.2, cfg.stopLossPct);
  const regime = marketBias > 0.2 ? 'ALCISTA' : marketBias < -0.2 ? 'BAJISTA' : 'MIXTO';
  const strategy = breakout ? 'BREAKOUT_CONFIRMADO' : abs > 0.45 ? 'MOMENTUM_MULTI_TF' : agreePct > 0.65 ? 'CONTINUACION_ESTRUCTURAL' : 'REVERSIÓN_CONTEXTUAL';
  return {
    symbol, side, score, confidence, strategy, price: tick.price,
    momentum20s: Number(t.m20.toFixed(3)), momentum1m: Number(t.m60.toFixed(3)), momentum3m: Number(t.m180.toFixed(3)), momentum5m: Number(t.m300.toFixed(3)),
    volatility: Number(volatility.toFixed(4)), volumeRatio: Number((f1?.volumeRatio || 1).toFixed(2)), rsi1m: Number((f1?.rsi || 50).toFixed(1)),
    agreement: Number((agreePct * 100).toFixed(0)), expectedMove: Number(expectedMove.toFixed(2)), rr: Number(rr.toFixed(2)), regime,
    thesis: `${strategy} · ${side} · multi-TF ${agreePct * 100 | 0}% · vol ${volatility.toFixed(2)}% · RR ${rr.toFixed(2)}`,
    decision: abs < 0.03 ? 'HOLD' : side,
    evidence: { t, f1, f5, f15 }
  };
}
function marketContext(allTicks) {
  const arr = [];
  for (const [s] of allTicks) { const f = tickFeatures(s); if (f) arr.push(f); }
  if (!arr.length) return { bias: 0, regime: 'MIXTO' };
  const bias = clamp(avg(arr, x => clamp(x.m60 * 0.4 + x.m180 * 0.35 + x.m300 * 0.25, -2, 2)) / 1.2, -1, 1);
  return { bias, regime: bias > 0.2 ? 'ALCISTA' : bias < -0.2 ? 'BAJISTA' : 'MIXTO' };
}
function analyze() {
  const market = marketContext(ticks);
  const arr = [];
  for (const [symbol, tick] of ticks) {
    if (!tick.price || (cfg.maxQuoteVolume > 0 && tick.volume < cfg.maxQuoteVolume)) continue;
    const o = scoreOpportunity(symbol, tick, candleFeatures(symbol), market);
    if (o) arr.push(o);
  }
  arr.sort((a, b) => b.score - a.score || b.confidence - a.confidence || b.expectedMove - a.expectedMove);
  return { arr, market };
}
function updateRegime(all, market) {
  if (!all.length) { state.regime = 'MIXTO'; state.longPct = 50; state.shortPct = 50; return; }
  const longs = all.filter(x => x.side === 'LONG').length;
  state.longPct = Math.round(longs / all.length * 100); state.shortPct = 100 - state.longPct;
  state.regime = market.regime;
  const f = all.slice(0, 80);
  state.timeframes = {
    '20s': avg(f, x => x.momentum20s) > 0.02 ? '↑' : avg(f, x => x.momentum20s) < -0.02 ? '↓' : '↔',
    '1m': avg(f, x => x.momentum1m) > 0.03 ? '↑' : avg(f, x => x.momentum1m) < -0.03 ? '↓' : '↔',
    '3m': avg(f, x => x.momentum3m) > 0.05 ? '↑' : avg(f, x => x.momentum3m) < -0.05 ? '↓' : '↔',
    '5m': avg(f, x => x.momentum5m) > 0.07 ? '↑' : avg(f, x => x.momentum5m) < -0.07 ? '↓' : '↔'
  };
}
function marginFor() {
  const byPosition = cfg.capital * cfg.maxPositionMarginPct / 100;
  const total = cfg.capital * cfg.maxTotalMarginPct / 100;
  const used = state.positions.reduce((s, p) => s + p.margin, 0);
  return Math.max(0, Math.min(byPosition, total - used));
}
function diversityPenalty(candidate, open) {
  const sameSide = open.filter(p => p.side === candidate.side).length;
  const sameStrategy = open.filter(p => p.strategy === candidate.strategy).length;
  const base = Math.min(18, sameSide * 3 + sameStrategy * 2);
  return base;
}
function chooseEntries(all) {
  const held = new Set(state.positions.map(p => p.symbol));
  const now = Date.now();
  const available = all.filter(x => !held.has(x.symbol) && (cooldown.get(x.symbol) || 0) < now);
  if (!available.length) return [];
  const selected = [];
  // Rank globally, but deliberately penalize concentration and reward independent evidence.
  for (const c of available) {
    const adjusted = c.score - diversityPenalty(c, [...state.positions, ...selected]);
    c.selectionScore = Math.round(adjusted);
  }
  available.sort((a, b) => b.selectionScore - a.selectionScore || b.confidence - a.confidence);
  const maxNew = Math.min(2, cfg.maxPositions - state.positions.length);
  const wantBalanced = state.regime === 'MIXTO';
  for (const c of available) {
    if (selected.length >= maxNew) break;
    if (wantBalanced && selected.length === 1 && selected[0].side === c.side) {
      const opposite = available.find(x => x.side !== c.side && !selected.includes(x));
      if (opposite && opposite.selectionScore >= c.selectionScore - 10) { selected.push(opposite); continue; }
    }
    // Confidence is advisory; there is no universal score gate. Risk sizing remains separate.
    if (c.confidence < 45) continue;
    selected.push(c);
  }
  return selected;
}
function openPaper(all) {
  if (stopped || state.positions.length >= cfg.maxPositions) return;
  const selected = chooseEntries(all);
  const now = Date.now();
  for (const a of selected) {
    const margin = marginFor(); if (margin <= 0) break;
    const notional = margin * cfg.leverage;
    const qty = notional / a.price;
    state.positions.push({
      id: `G${now}_${Math.random().toString(36).slice(2,8)}`, symbol: a.symbol, side: a.side,
      entry: a.price, current: a.price, margin, notional, qty, pnl: 0, score: a.score, confidence: a.confidence,
      strategy: a.strategy, openedAt: new Date().toISOString(), openedTs: now,
      thesis: a.thesis, peakPnlPct: 0, minPnlPct: 0
    });
    cooldown.set(a.symbol, now + cfg.cooldownMs);
    state.lastSignal = `PAPER OPEN ${a.symbol} ${a.side} · ${a.strategy} · ${a.score}/100`;
    state.history.unshift({ time: new Date().toISOString(), action: 'OPEN', symbol: a.symbol, side: a.side, score: a.score, reason: a.thesis });
  }
}
function closePosition(p, reason) {
  const fee = Math.abs(p.notional) * (cfg.feePct / 100) * 2;
  const net = p.pnl - fee;
  state.realizedPnl += net;
  state.history.unshift({ time: new Date().toISOString(), action: 'CLOSE', symbol: p.symbol, side: p.side, pnl: Number(net.toFixed(4)), reason });
  cooldown.set(p.symbol, Date.now() + cfg.cooldownMs);
  state.lastSignal = `PAPER CLOSE ${p.symbol} ${p.side} · ${reason} · ${net.toFixed(2)}`;
}
function markAndClose(all) {
  const bySymbol = new Map(all.map(x => [x.symbol, x]));
  const now = Date.now(), keep = [];
  for (const p of state.positions) {
    const t = ticks.get(p.symbol); if (!t) { keep.push(p); continue; }
    p.current = t.price;
    const movePct = p.side === 'LONG' ? (p.current / p.entry - 1) * 100 : (p.entry / p.current - 1) * 100;
    p.pnl = p.notional * (movePct / 100);
    p.unrealizedPct = p.margin ? p.pnl / p.margin * 100 : 0;
    p.peakPnlPct = Math.max(p.peakPnlPct || 0, p.unrealizedPct);
    p.minPnlPct = Math.min(p.minPnlPct || 0, p.unrealizedPct);
    const a = bySymbol.get(p.symbol);
    const age = now - p.openedTs;
    const tp = p.unrealizedPct >= cfg.takeProfitPct;
    const sl = p.unrealizedPct <= -cfg.stopLossPct;
    const timeout = age >= cfg.maxHoldMs;
    const thesisFlip = a && a.side !== p.side && a.confidence >= 55 && a.score >= 55;
    const deterioration = a && a.side === p.side && a.confidence < 40 && p.unrealizedPct < 0;
    const trailing = p.peakPnlPct >= cfg.takeProfitPct * 0.65 && p.unrealizedPct < p.peakPnlPct - Math.max(0.25, cfg.takeProfitPct * 0.35);
    if (tp || sl || timeout || thesisFlip || deterioration || trailing) {
      closePosition(p, tp ? 'TP' : sl ? 'SL' : timeout ? 'TIME' : thesisFlip ? 'THESIS_FLIP' : trailing ? 'TRAIL' : 'DETERIORATION');
    } else keep.push(p);
  }
  state.positions = keep.slice(0, cfg.maxPositions);
}
function write() {
  state.portfolioCount = state.positions.length;
  state.unrealizedPnl = state.positions.reduce((s, p) => s + Number(p.pnl || 0), 0);
  state.equity = cfg.capital + state.realizedPnl + state.unrealizedPnl;
  const dd = Math.max(0, cfg.capital - state.equity);
  state.drawdownPct = cfg.capital ? dd / cfg.capital * 100 : 0;
  state.dailyLossPct = Math.min(0, state.realizedPnl / cfg.capital * 100);
  state.stopped = stopped; state.running = !stopped; state.lastUpdate = new Date().toISOString();
  fs.writeFileSync(runtimeFile, JSON.stringify(state, null, 2));
}
async function getKlines(symbol, interval, limit = 100) {
  const u = new URL('/fapi/v1/klines', cfg.apiBase);
  u.searchParams.set('symbol', symbol); u.searchParams.set('interval', interval); u.searchParams.set('limit', String(limit));
  state.restCalls++;
  const r = await fetch(u, { headers: { 'User-Agent': 'GALAXI/20' } });
  if (r.status === 429) { state.rate429++; throw new Error('Binance REST 429'); }
  if (r.status === 418) { state.rate418++; throw new Error('Binance REST 418'); }
  if (!r.ok) throw new Error(`Binance REST ${r.status}`);
  return normalizeKlines(await r.json());
}
async function refreshCandles() {
  const top = [...ticks.entries()]
    .filter(([, t]) => t.price > 0)
    .sort((a, b) => b[1].volume - a[1].volume)
    .slice(0, cfg.candleSymbols);
  for (const [symbol] of top) {
    try {
      const [m1, m5, m15] = await Promise.all([getKlines(symbol, '1m', 100), getKlines(symbol, '5m', 100), getKlines(symbol, '15m', 100)]);
      candles.set(symbol, { '1m': m1, '5m': m5, '15m': m15, fetchedAt: Date.now() });
    } catch (e) { state.lastError = e.message; }
  }
  lastCandleRefresh = Date.now();
  state.dataQuality = `TICKS + KLINES ${Math.min(top.length, cfg.candleSymbols)}`;
}
async function refreshTickerRest() {
  if (stopped) return;
  try {
    const u = new URL('/fapi/v1/ticker/24hr', cfg.apiBase);
    state.restCalls++;
    const r = await fetch(u, { headers: { 'User-Agent': 'GALAXI/21' } });
    if (r.status === 429) { state.rate429++; throw new Error('Binance REST 429'); }
    if (r.status === 418) { state.rate418++; throw new Error('Binance REST 418'); }
    if (!r.ok) throw new Error(`Binance REST ticker ${r.status}`);
    const data = await r.json();
    if (!Array.isArray(data)) throw new Error('Binance REST ticker formato inválido');
    const now = Date.now();
    const usable = data
      .filter(t => String(t.symbol || '').endsWith('USDT') && Number(t.lastPrice) > 0)
      .sort((a,b) => Number(b.quoteVolume||0) - Number(a.quoteVolume||0))
      .slice(0, cfg.maxTickerSymbols);
    for (const t of usable) {
      const symbol = String(t.symbol);
      const price = Number(t.lastPrice);
      const volume = Number(t.quoteVolume || 0);
      ticks.set(symbol, { price, volume, ts: now });
      pushTick(symbol, { price, volume, ts: now });
    }
    state.dataQuality = state.wsConnected ? `WS + REST ${usable.length}` : `REST ${usable.length}`;
    if (!state.wsConnected) state.lastError = null;
  } catch (e) {
    state.lastError = `REST ticker: ${e.message}`;
  }
}
function startTickerFallback() {
  clearInterval(tickerTimer);
  refreshTickerRest();
  tickerTimer = setInterval(() => {
    if (!stopped && (!state.wsConnected || Date.now() - wsLastMessage > cfg.tickerPollMs * 2)) refreshTickerRest();
  }, cfg.tickerPollMs);
}

function connect() {
  try {
    ws = new WebSocket(cfg.wsUrl);
    ws.on('open', () => { state.wsConnected = 1; wsLastMessage = Date.now(); state.lastError = null; console.log('WS_CONNECTED=1'); });
    ws.on('close', () => {
      state.wsConnected = 0;
      if (!stopped && !reconnectTimer) reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, 3000);
    });
    ws.on('error', () => { state.lastError = 'WebSocket error'; });
    ws.on('message', raw => {
      wsLastMessage = Date.now();
      try {
        const arr = JSON.parse(raw.toString()); if (!Array.isArray(arr)) return;
        const now = Date.now();
        for (const t of arr) {
          const symbol = t.s; if (!symbol?.endsWith('USDT')) continue;
          const price = Number(t.c), volume = Number(t.q || 0);
          if (price > 0) { ticks.set(symbol, { price, volume, ts: now }); pushTick(symbol, { price, volume, ts: now }); }
        }
      } catch { state.lastError = 'WS parse error'; }
    });
  } catch (e) { state.lastError = 'WS init: ' + e.message; }
}
async function loop() {
  try {
    if (fs.existsSync(controlFile)) {
      try { const c = JSON.parse(fs.readFileSync(controlFile, 'utf8')); if (c.stop) stopped = true; } catch {}
    }
    if (stopped) { write(); return; }
    state.cycle++;
    if (Date.now() - lastCandleRefresh >= cfg.dataRefreshMs && ticks.size) await refreshCandles();
    const { arr, market } = analyze();
    state.symbols = ticks.size; state.warmSymbols = arr.length; state.ranking = arr.slice(0, 30);
    state.candidates = arr.length; state.riskApproved = Math.min(arr.length, cfg.maxPositions * 2);
    updateRegime(arr, market);
    markAndClose(arr);
    if (cfg.mode === 'PAPER') openPaper(arr);
    state.decisionNote = `Contexto ${market.regime} · ranking dinámico · datos ${state.dataQuality}`;
    write();
    if (state.cycle % 5 === 0) console.log(`cycle=${state.cycle} universe=${state.symbols} ranked=${state.ranking.length} portfolio=${state.positions.length} equity=${state.equity.toFixed(2)} regime=${state.regime}`);
  } catch (e) { state.lastError = e.message; write(); }
  loopTimer = setTimeout(loop, cfg.interval);
}

console.log(`GALAXI | mode=${cfg.mode} | capital=${cfg.capital} | scan=${cfg.interval}ms | candles=${cfg.candleSymbols}`);
connect();
startTickerFallback();
loop();
process.on('SIGTERM', () => { stopped = true; clearTimeout(loopTimer); clearInterval(tickerTimer); try { ws?.close(); } catch {} write(); process.exit(0); });
