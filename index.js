import fs from 'node:fs';
import crypto from 'node:crypto';

/*
 GALAXI V22 FINAL
 - Binance USD-M Futures REST market/account/execution engine
 - OpenAI Responses API decision engine
 - PAPER by default; LIVE requires TRADING_MODE=LIVE + LIVE_ARMED=true
 - Exactly 8 Railway variables are required; everything else has safe defaults
 - No per-trade confirmation is required once LIVE is deliberately armed
 - Deterministic risk controls remain outside the AI
*/

const runtimeFile = 'galaxi-runtime.json';
const controlFile = 'galaxi-control.json';

const env = process.env;
const cfg = {
  mode: String(env.TRADING_MODE || 'PAPER').toUpperCase(),
  liveArmed: String(env.LIVE_ARMED || 'false').toLowerCase() === 'true',
  openaiKey: env.OPENAI_API_KEY || '',
  openaiModel: env.OPENAI_MODEL || '',
  binanceKey: env.BINANCE_API_KEY || '',
  binanceSecret: env.BINANCE_API_SECRET || '',
  maxPositions: clampInt(env.MAX_POSITIONS, 12, 1, 12),
  maxTotalMarginPct: clampNum(env.MAX_TOTAL_MARGIN_PCT, 30, 1, 50),

  binanceBase: env.BINANCE_FAPI_BASE || 'https://fapi.binance.com',
  capital: clampNum(env.PAPER_START_CAPITAL, 10000, 100, 100000000),
  leverage: 5,
  maxPositionMarginPct: 2,
  scanMs: 20000,
  aiTimeoutMs: 20000,
  aiTopSymbols: 12,
  klineLimit: 80,
  maxDailyLossPct: 5,
  maxDrawdownPct: 10,
  minSecondsBetweenOrders: 5,
  maxActionsPerCycle: 2,
  paperTpPct: 1.2,
  paperSlPct: 0.7,
  paperMaxHoldMs: 30 * 60 * 1000,
  cooldownMs: 60 * 1000
};

function clampNum(v, fallback, min, max) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}
function clampInt(v, fallback, min, max) { return Math.round(clampNum(v, fallback, min, max)); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function now() { return Date.now(); }
function round(n, d = 6) {
  const p = 10 ** d;
  return Math.round(Number(n) * p) / p;
}
function precisionFromStep(step) {
  const s = String(step);
  if (!s.includes('.')) return 0;
  return Math.max(0, s.split('.')[1].replace(/0+$/, '').length);
}

const state = {
  running: true,
  stopped: false,
  mode: cfg.mode,
  ai: Boolean(cfg.openaiKey),
  aiModel: cfg.openaiModel || 'NO_CONFIGURADO',
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
  wsConnected: 1,
  candidates: 0,
  riskApproved: 0,
  portfolioCount: 0,
  regime: 'MIXTO',
  longPct: 50,
  shortPct: 50,
  positions: [],
  ranking: [],
  history: [],
  lastSignal: 'Esperando datos de mercado…',
  lastError: null,
  aiDecision: null,
  aiReasoning: '',
  aiCalls: 0,
  aiErrors: 0,
  restCalls: 0,
  rate429: 0,
  lastUpdate: null
};

const ticks = new Map();
const marketInfo = new Map();
const cooldown = new Map();
let serverOffset = 0;
let lastOrderTs = 0;
let lastAccount = null;
let loopBusy = false;
let stopped = false;

function writeState() {
  state.portfolioCount = state.positions.length;
  state.unrealizedPnl = state.positions.reduce((s, p) => s + Number(p.pnl || 0), 0);
  if (cfg.mode === 'PAPER') state.equity = cfg.capital + state.realizedPnl + state.unrealizedPnl;
  const dd = Math.max(0, cfg.capital - Number(state.equity || 0));
  state.drawdownPct = cfg.capital ? dd / cfg.capital * 100 : 0;
  state.dailyLossPct = Math.min(0, Number(state.todayPnl || 0) / Math.max(1, cfg.capital) * 100);
  state.lastUpdate = new Date().toISOString();
  fs.writeFileSync(runtimeFile, JSON.stringify(state, null, 2));
}
function log(line) {
  state.history.unshift({ time: new Date().toISOString(), line });
  state.history = state.history.slice(0, 100);
  console.log(line);
}

async function rest(path, options = {}, signed = false) {
  const method = options.method || 'GET';
  const params = { ...(options.params || {}) };
  if (signed) {
    params.timestamp = Date.now() + serverOffset;
    params.recvWindow = 5000;
  }
  const query = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null) query.set(k, String(v));
  if (signed) {
    const sig = crypto.createHmac('sha256', cfg.binanceSecret).update(query.toString()).digest('hex');
    query.set('signature', sig);
  }
  const url = `${cfg.binanceBase}${path}${query.toString() ? `?${query}` : ''}`;
  const headers = {};
  if (cfg.binanceKey) headers['X-MBX-APIKEY'] = cfg.binanceKey;
  if (method !== 'GET') headers['Content-Type'] = 'application/x-www-form-urlencoded';

  state.restCalls++;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(url, { method, headers, body: method === 'GET' ? undefined : query.toString(), signal: controller.signal });
    const text = await res.text();
    if (res.status === 429) state.rate429++;
    let data;
    try { data = JSON.parse(text); } catch { data = { raw: text }; }
    if (!res.ok) throw new Error(`BINANCE ${res.status}: ${data?.msg || text.slice(0, 300)}`);
    return data;
  } finally { clearTimeout(timer); }
}

async function syncServerTime() {
  const data = await rest('/fapi/v1/time');
  serverOffset = Number(data.serverTime) - Date.now();
}

async function loadExchangeInfo() {
  const data = await rest('/fapi/v1/exchangeInfo');
  marketInfo.clear();
  for (const s of data.symbols || []) {
    if (s.status !== 'TRADING' || s.quoteAsset !== 'USDT' || s.contractType !== 'PERPETUAL') continue;
    const lot = (s.filters || []).find(x => x.filterType === 'LOT_SIZE');
    const price = (s.filters || []).find(x => x.filterType === 'PRICE_FILTER');
    marketInfo.set(s.symbol, {
      qtyStep: Number(lot?.stepSize || 0.001),
      minQty: Number(lot?.minQty || 0),
      tickSize: Number(price?.tickSize || 0.00001)
    });
  }
  state.symbols = marketInfo.size;
}

function normalizeQty(symbol, qty) {
  const m = marketInfo.get(symbol);
  if (!m) return 0;
  const step = m.qtyStep;
  const p = precisionFromStep(step);
  const q = Math.floor(Number(qty) / step) * step;
  const out = round(q, p);
  return out >= m.minQty ? out : 0;
}
function normalizePrice(symbol, price) {
  const m = marketInfo.get(symbol);
  if (!m) return Number(price);
  const step = m.tickSize;
  return round(Math.round(Number(price) / step) * step, precisionFromStep(step));
}
function pctMove(a, b) { return b > 0 ? (a / b - 1) * 100 : 0; }
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
  for (let i = 1; i <= period; i++) { const d = values[i] - values[i - 1]; if (d >= 0) gain += d; else loss -= d; }
  let ag = gain / period, al = loss / period;
  for (let i = period + 1; i < values.length; i++) {
    const d = values[i] - values[i - 1];
    ag = (ag * (period - 1) + Math.max(d, 0)) / period;
    al = (al * (period - 1) + Math.max(-d, 0)) / period;
  }
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}
function atr(klines, period = 14) {
  if (klines.length < period + 2) return 0;
  const trs = [];
  for (let i = 1; i < klines.length; i++) {
    const h = Number(klines[i][2]), l = Number(klines[i][3]), pc = Number(klines[i - 1][4]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  const a = trs.slice(-period);
  return a.reduce((x, y) => x + y, 0) / a.length;
}

async function refreshTickerUniverse() {
  const data = await rest('/fapi/v1/ticker/24hr');
  let count = 0;
  for (const t of data || []) {
    if (!marketInfo.has(t.symbol) || !t.symbol.endsWith('USDT')) continue;
    const price = Number(t.lastPrice), volume = Number(t.quoteVolume || 0);
    if (price > 0) { ticks.set(t.symbol, { price, volume, ts: now() }); count++; }
  }
  state.symbols = marketInfo.size;
  return count;
}
async function fetchKlines(symbol, interval, limit) {
  return rest('/fapi/v1/klines', { params: { symbol, interval, limit } });
}
function analyze(symbol, k1, k5) {
  if (!Array.isArray(k1) || k1.length < 35 || !Array.isArray(k5) || k5.length < 35) throw new Error('insufficient candles');
  const c1 = k1.map(x => Number(x[4]));
  const highs = k1.map(x => Number(x[2]));
  const lows = k1.map(x => Number(x[3]));
  const vols = k1.map(x => Number(x[5]));
  const c5 = k5.map(x => Number(x[4]));
  const price = c1.at(-1);
  const e9 = ema(c1.slice(-40), 9), e21 = ema(c1.slice(-60), 21), e50 = ema(c1.slice(-70), 50);
  const e20_5 = ema(c5.slice(-40), 20), e50_5 = ema(c5.slice(-70), 50);
  const volumeNow = vols.slice(-10).reduce((a,b) => a+b, 0) / 10;
  const volumePrev = vols.slice(-30,-10).reduce((a,b) => a+b, 0) / 20 || volumeNow;
  const m5 = pctMove(price, c1.at(-6)), m15 = pctMove(price, c1.at(-16)), m30 = pctMove(price, c1.at(-31));
  const m5tf = pctMove(c5.at(-1), c5.at(-4));
  const bull = e9 > e21 && e21 > e50 && m5 > 0 && m15 > 0 && e20_5 >= e50_5;
  const bear = e9 < e21 && e21 < e50 && m5 < 0 && m15 < 0 && e20_5 <= e50_5;
  return {
    symbol, price,
    bias: bull ? 'LONG' : bear ? 'SHORT' : 'NEUTRAL',
    rsi: round(rsi(c1), 2),
    atrPct: round(price ? atr(k1) / price * 100 : 0, 4),
    volumeRatio: round(volumePrev ? volumeNow / volumePrev : 1, 2),
    momentum1m: round(pctMove(price, c1.at(-2)), 3),
    momentum5m: round(m5, 3),
    momentum15m: round(m15, 3),
    momentum30m: round(m30, 3),
    momentum5mTF: round(m5tf, 3),
    ema9: round(e9, 8), ema21: round(e21, 8), ema50: round(e50, 8),
    ema20_5m: round(e20_5, 8), ema50_5m: round(e50_5, 8),
    breakoutUp: price >= Math.max(...highs.slice(-20)) * 0.9995,
    breakoutDown: price <= Math.min(...lows.slice(-20)) * 1.0005,
    quoteVolume24h: round(Number(ticks.get(symbol)?.volume || 0), 0)
  };
}

async function buildMarketSnapshot() {
  await refreshTickerUniverse();
  const candidates = [...ticks.entries()]
    .filter(([s, t]) => marketInfo.has(s) && t.price > 0)
    .sort((a,b) => b[1].volume - a[1].volume)
    .slice(0, Math.max(cfg.aiTopSymbols, 10));

  const rows = [];
  let errors = 0;
  for (const [symbol] of candidates) {
    try {
      const [k1, k5] = await Promise.all([fetchKlines(symbol, '1m', cfg.klineLimit), fetchKlines(symbol, '5m', 70)]);
      rows.push(analyze(symbol, k1, k5));
    } catch (e) {
      errors++;
      if (errors === 1) state.lastError = `KLINE ${symbol}: ${e.message}`;
    }
  }
  if (!rows.length) throw new Error(state.lastError || 'No se pudieron analizar velas de Binance');

  rows.sort((a,b) => Math.max(Math.abs(b.momentum5m), Math.abs(b.momentum15m)) - Math.max(Math.abs(a.momentum5m), Math.abs(a.momentum15m)));
  state.warmSymbols = rows.length;
  state.candidates = rows.length;
  state.ranking = rows.slice(0, 20);
  const longN = rows.filter(x => x.bias === 'LONG').length;
  const shortN = rows.filter(x => x.bias === 'SHORT').length;
  state.longPct = Math.round(longN / rows.length * 100);
  state.shortPct = 100 - state.longPct;
  const avg15 = rows.reduce((s,x) => s+x.momentum15m,0)/rows.length;
  state.regime = state.longPct >= 62 && avg15 > 0 ? 'ALCISTA' : state.shortPct >= 62 && avg15 < 0 ? 'BAJISTA' : 'MIXTO';
  return rows.slice(0, cfg.aiTopSymbols);
}

async function getLiveAccount() {
  if (!cfg.binanceKey || !cfg.binanceSecret) throw new Error('BINANCE_API_KEY/BINANCE_API_SECRET faltan');
  const [account, pos] = await Promise.all([
    rest('/fapi/v2/account', {}, true),
    rest('/fapi/v2/positionRisk', {}, true)
  ]);
  const positions = (pos || []).filter(p => Math.abs(Number(p.positionAmt || 0)) > 0).map(p => {
    const amt = Number(p.positionAmt), entry = Number(p.entryPrice), mark = Number(p.markPrice);
    return {
      symbol: p.symbol, side: amt > 0 ? 'LONG' : 'SHORT', qty: Math.abs(amt), entry, mark,
      current: mark, pnl: Number(p.unRealizedProfit || 0), margin: Math.abs(Number(p.notional || 0)) / cfg.leverage
    };
  });
  const equity = Number(account.totalMarginBalance || account.totalWalletBalance || 0);
  return { equity, wallet: Number(account.totalWalletBalance || 0), unrealizedPnl: Number(account.totalUnrealizedProfit || 0), positions };
}
function paperAccount() {
  return { equity: state.equity, wallet: state.equity, unrealizedPnl: state.unrealizedPnl, positions: state.positions.map(p => ({ ...p, mark: p.current })) };
}

function cleanJsonText(text) {
  let s = String(text || '').trim();
  if (s.startsWith('```')) s = s.replace(/^```(?:json)?/i,'').replace(/```$/i,'').trim();
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  return a >= 0 && b > a ? s.slice(a,b+1) : s;
}
async function askAI(market, account) {
  if (!cfg.openaiKey) throw new Error('OPENAI_API_KEY no configurada');
  if (!cfg.openaiModel) throw new Error('OPENAI_MODEL no configurado');
  const payload = {
    timestamp: new Date().toISOString(), regime: state.regime, longPct: state.longPct, shortPct: state.shortPct,
    equity: state.equity,
    positions: account.positions.map(p => ({symbol:p.symbol,side:p.side,entry:p.entry,mark:p.mark,pnl:p.pnl,margin:p.margin})),
    market
  };
  const instructions = `Eres GALAXI, motor autónomo de decisión para Binance USD-M. Analiza estructura, momentum multitemporal, RSI, EMA, ATR, volumen, rupturas, régimen y posiciones. No uses un score fijo. Busca LONG y SHORT y evita repetir símbolos sin una tesis nueva. Puedes decidir HOLD. Para CLOSE usa una posición existente. Para OPEN usa solo símbolos presentes en market. No inventes datos. Respeta siempre los límites del sistema. Devuelve únicamente el JSON solicitado.`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), cfg.aiTimeoutMs);
  try {
    const res = await fetch('https://api.openai.com/v1/responses', {
      method:'POST', signal:controller.signal,
      headers:{Authorization:`Bearer ${cfg.openaiKey}`,'Content-Type':'application/json'},
      body:JSON.stringify({
        model:cfg.openaiModel,
        instructions,
        input:JSON.stringify(payload),
        text:{format:{type:'json_schema',name:'galaxi_trade_decision',strict:true,schema:{
          type:'object',additionalProperties:false,
          properties:{
            regime:{type:'string',enum:['ALCISTA','BAJISTA','MIXTO']},
            actions:{type:'array',maxItems:cfg.maxActionsPerCycle,items:{type:'object',additionalProperties:false,properties:{
              action:{type:'string',enum:['OPEN_LONG','OPEN_SHORT','CLOSE','HOLD']},symbol:{type:'string'},margin_pct:{type:'number'},reason:{type:'string'},confidence:{type:'number'}
            },required:['action','symbol','margin_pct','reason','confidence']}},
            summary:{type:'string'}
          },required:['regime','actions','summary']
        }}}
      })
    });
    const raw = await res.text();
    if (!res.ok) throw new Error(`OPENAI ${res.status}: ${raw.slice(0,500)}`);
    const data = JSON.parse(raw);
    const outputText = data.output_text || data.output?.flatMap(x => x.content || []).find(x => x.type === 'output_text')?.text || '';
    const decision = JSON.parse(cleanJsonText(outputText));
    state.aiCalls++;
    return decision;
  } finally { clearTimeout(timer); }
}

function riskAllowsOpen(symbol, margin) {
  if (stopped) return {ok:false,reason:'STOP'};
  if (state.positions.length >= cfg.maxPositions) return {ok:false,reason:'MAX_POSITIONS'};
  const used = state.positions.reduce((s,p)=>s+Number(p.margin||0),0);
  const maxTotal = Number(state.equity) * cfg.maxTotalMarginPct / 100;
  if (used + margin > maxTotal) return {ok:false,reason:'MAX_TOTAL_MARGIN'};
  if (Math.max(0,-state.dailyLossPct) >= cfg.maxDailyLossPct) return {ok:false,reason:'DAILY_LOSS'};
  if (state.drawdownPct >= cfg.maxDrawdownPct) return {ok:false,reason:'MAX_DRAWDOWN'};
  if ((cooldown.get(symbol)||0) > now()) return {ok:false,reason:'COOLDOWN'};
  return {ok:true};
}
function marginFor() {
  const eq = Math.max(0,Number(state.equity||cfg.capital));
  const byPos = eq * cfg.maxPositionMarginPct / 100;
  const maxTotal = eq * cfg.maxTotalMarginPct / 100;
  const used = state.positions.reduce((s,p)=>s+Number(p.margin||0),0);
  return Math.max(0,Math.min(byPos,maxTotal-used));
}

async function setLeverage(symbol) {
  await rest('/fapi/v1/leverage',{method:'POST',params:{symbol,leverage:cfg.leverage}},true);
}
async function placeMarketOrder(symbol, side, qty, reduceOnly=false) {
  if (!cfg.binanceKey || !cfg.binanceSecret) throw new Error('BINANCE API no configurada');
  if (now()-lastOrderTs < cfg.minSecondsBetweenOrders*1000) throw new Error('Protección: espera entre órdenes');
  const quantity = normalizeQty(symbol,qty);
  if (!(quantity>0)) throw new Error(`Cantidad inválida ${symbol}`);
  await setLeverage(symbol);
  const params={symbol,side,type:'MARKET',quantity,newOrderRespType:'RESULT'};
  if (reduceOnly) params.reduceOnly='true';
  const order=await rest('/fapi/v1/order',{method:'POST',params},true);
  lastOrderTs=now();
  return order;
}
async function placeProtection(symbol, positionSide, entryPrice) {
  const entry = Number(entryPrice);
  if (!(entry>0)) return;
  const closeSide = positionSide === 'LONG' ? 'SELL' : 'BUY';
  const stop = positionSide === 'LONG' ? entry*(1-cfg.paperSlPct/100) : entry*(1+cfg.paperSlPct/100);
  const take = positionSide === 'LONG' ? entry*(1+cfg.paperTpPct/100) : entry*(1-cfg.paperTpPct/100);
  for (const [type, price] of [['STOP_MARKET',stop],['TAKE_PROFIT_MARKET',take]]) {
    try {
      await rest('/fapi/v1/order',{method:'POST',params:{symbol,side:closeSide,type,stopPrice:normalizePrice(symbol,price),closePosition:'true',workingType:'MARK_PRICE',priceProtect:'TRUE'}},true);
    } catch (e) {
      log(`PROTECTION_ERROR ${symbol} ${type}: ${e.message}`);
    }
  }
}

function paperOpen(a) {
  if (state.positions.some(p=>p.symbol===a.symbol)) return {skipped:true,reason:'SYMBOL_ALREADY_OPEN'};
  const margin=Math.min(state.equity*clampNum(a.margin_pct,1,.25,cfg.maxPositionMarginPct)/100,marginFor());
  const risk=riskAllowsOpen(a.symbol,margin); if(!risk.ok) return {skipped:true,reason:risk.reason};
  const price=Number(ticks.get(a.symbol)?.price||0); if(!(price>0)||!(margin>0)) return {skipped:true,reason:'NO_PRICE_OR_MARGIN'};
  const side=a.action==='OPEN_LONG'?'LONG':'SHORT';
  const p={id:`AI_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,symbol:a.symbol,side,entry:price,current:price,margin,qty:margin*cfg.leverage/price,pnl:0,leverage:cfg.leverage,confidence:Number(a.confidence||0),strategy:'AI',thesis:a.reason,openedAt:new Date().toISOString(),openedTs:now()};
  state.positions.push(p); cooldown.set(a.symbol,now()+cfg.cooldownMs); log(`PAPER_OPEN ${a.symbol} ${side} margin=${margin.toFixed(2)} confidence=${Math.round(a.confidence||0)}`); return {opened:true};
}
function paperClose(a) {
  const p=state.positions.find(x=>x.symbol===a.symbol); if(!p) return {skipped:true,reason:'NO_POSITION'};
  const t=ticks.get(p.symbol); if(t?.price) p.current=t.price;
  p.pnl=(p.side==='LONG'?p.current-p.entry:p.entry-p.current)*p.qty;
  state.realizedPnl+=p.pnl; state.todayPnl+=p.pnl; state.positions=state.positions.filter(x=>x.id!==p.id); cooldown.set(p.symbol,now()+cfg.cooldownMs);
  log(`PAPER_CLOSE ${p.symbol} ${p.side} pnl=${p.pnl.toFixed(4)} reason=${a.reason}`); return {closed:true};
}
function markPaperPositions() {
  const keep=[]; const tNow=now();
  for(const p of state.positions){
    const t=ticks.get(p.symbol); if(t?.price)p.current=t.price;
    p.pnl=(p.side==='LONG'?p.current-p.entry:p.entry-p.current)*p.qty;
    p.unrealizedPct=p.margin?p.pnl/p.margin*100:0;
    const tp=p.unrealizedPct>=cfg.paperTpPct, sl=p.unrealizedPct<=-cfg.paperSlPct, timeout=tNow-p.openedTs>=cfg.paperMaxHoldMs;
    if(tp||sl||timeout){state.realizedPnl+=p.pnl;state.todayPnl+=p.pnl;log(`PAPER_RISK_CLOSE ${p.symbol} ${p.side} pnl=${p.pnl.toFixed(4)} reason=${tp?'TP':sl?'SL':'TIME'}`);cooldown.set(p.symbol,tNow+cfg.cooldownMs);} else keep.push(p);
  }
  state.positions=keep;
}
async function executeLiveAction(a) {
  if(!cfg.liveArmed) throw new Error('LIVE bloqueado: LIVE_ARMED=true requerido');
  const live=lastAccount || await getLiveAccount();
  const existing=live.positions.find(p=>p.symbol===a.symbol);
  if(a.action==='CLOSE'){
    if(!existing)return {skipped:true,reason:'NO_POSITION'};
    const order=await placeMarketOrder(existing.symbol,existing.side==='LONG'?'SELL':'BUY',existing.qty,true);
    log(`LIVE_CLOSE ${existing.symbol} ${existing.side} order=${order.orderId}`); return {order};
  }
  if(a.action==='OPEN_LONG'||a.action==='OPEN_SHORT'){
    if(existing)return {skipped:true,reason:'SYMBOL_ALREADY_OPEN'};
    const margin=Math.min(live.equity*clampNum(a.margin_pct,1,.25,cfg.maxPositionMarginPct)/100,live.equity*cfg.maxPositionMarginPct/100);
    const risk=riskAllowsOpen(a.symbol,margin); if(!risk.ok)return {skipped:true,reason:risk.reason};
    const price=Number(ticks.get(a.symbol)?.price||0); if(!(price>0))return {skipped:true,reason:'NO_PRICE'};
    const qty=normalizeQty(a.symbol,margin*cfg.leverage/price); if(!(qty>0))return {skipped:true,reason:'QTY_TOO_SMALL'};
    const side=a.action==='OPEN_LONG'?'BUY':'SELL';
    const order=await placeMarketOrder(a.symbol,side,qty,false);
    cooldown.set(a.symbol,now()+cfg.cooldownMs);
    await sleep(250);
    try {
      const fresh=await getLiveAccount();
      const p=fresh.positions.find(x=>x.symbol===a.symbol);
      if(p) await placeProtection(a.symbol,p.side,p.entry);
    } catch(e) { log(`POST_OPEN_SYNC_ERROR ${a.symbol}: ${e.message}`); }
    log(`LIVE_OPEN ${a.symbol} ${a.action} qty=${qty} order=${order.orderId}`);
    return {order};
  }
  return {skipped:true,reason:'HOLD'};
}
async function executeDecision(decision) {
  state.aiDecision=decision; state.aiReasoning=decision.summary||''; state.regime=decision.regime||state.regime;
  const actions=Array.isArray(decision.actions)?decision.actions.slice(0,cfg.maxActionsPerCycle):[];
  state.riskApproved=0;
  for(const a of actions){
    try {
      let result;
      if(a.action==='OPEN_LONG'||a.action==='OPEN_SHORT'){
        const r=riskAllowsOpen(a.symbol,Math.max(0,Number(state.equity)*clampNum(a.margin_pct,1,.25,cfg.maxPositionMarginPct)/100));
        if(!r.ok){log(`RISK_SKIP ${a.action} ${a.symbol} ${r.reason}`);continue;}
        state.riskApproved++;
      }
      if(cfg.mode==='PAPER') result=a.action==='CLOSE'?paperClose(a):(a.action==='OPEN_LONG'||a.action==='OPEN_SHORT'?paperOpen(a):{skipped:true,reason:'HOLD'});
      else if(cfg.mode==='LIVE') result=await executeLiveAction(a);
      else result={skipped:true,reason:'INVALID_MODE'};
      if(result?.opened||result?.closed||result?.order) log(`ACTION ${a.action} ${a.symbol} ok`);
    } catch(e){state.aiErrors++;state.lastError=`ACTION ${a.action} ${a.symbol}: ${e.message}`;log(`ACTION_ERROR ${a.action} ${a.symbol}: ${e.message}`);}
  }
}
function emergencyStopCheck(){
  if(fs.existsSync(controlFile)){try{const c=JSON.parse(fs.readFileSync(controlFile,'utf8'));if(c.stop)stopped=true;}catch{}}
  if(state.drawdownPct>=cfg.maxDrawdownPct||Math.max(0,-state.dailyLossPct)>=cfg.maxDailyLossPct){stopped=true;state.stopped=true;state.lastSignal='STOP AUTOMÁTICO POR RIESGO';}
}
async function runCycle(){
  if(loopBusy||stopped)return; loopBusy=true;
  try{
    emergencyStopCheck(); state.cycle++;
    const market=await buildMarketSnapshot();
    if(cfg.mode==='PAPER') markPaperPositions();
    else {lastAccount=await getLiveAccount();state.equity=lastAccount.equity;state.unrealizedPnl=lastAccount.unrealizedPnl;state.positions=lastAccount.positions;}
    const account=cfg.mode==='LIVE'?lastAccount:paperAccount();
    const decision=await askAI(market,account); await executeDecision(decision);
    state.lastSignal=decision.summary||'IA evaluó el mercado'; state.lastError=null;
    if(state.cycle%5===0)log(`CYCLE ${state.cycle} universe=${state.symbols} analyzed=${market.length} positions=${state.positions.length} equity=${Number(state.equity).toFixed(2)} regime=${state.regime}`);
  }catch(e){state.aiErrors++;state.lastError=e.message;state.lastSignal='Motor esperando / error recuperable';console.error('CYCLE_ERROR',e.message);}finally{writeState();loopBusy=false;}
}

async function boot(){
  log(`ENGINE_START GALAXI V22 FINAL | mode=${cfg.mode} | model=${cfg.openaiModel||'MISSING'} | scan=${cfg.scanMs}`);
  if(!cfg.openaiKey) log('ERROR OPENAI_API_KEY missing');
  if(!cfg.openaiModel) log('ERROR OPENAI_MODEL missing');
  if(cfg.mode==='LIVE'&&!cfg.liveArmed) log('LIVE bloqueado: LIVE_ARMED=false');
  if(cfg.mode==='LIVE'&&(!cfg.binanceKey||!cfg.binanceSecret)) log('ERROR BINANCE API missing');
  await syncServerTime(); await loadExchangeInfo();
  writeState(); await runCycle();
  setInterval(runCycle,cfg.scanMs);
}

boot().catch(e=>{state.lastError=`BOOT: ${e.message}`;writeState();console.error('BOOT_ERROR',e);process.exit(1);});
process.on('SIGTERM',()=>{stopped=true;state.running=false;writeState();process.exit(0);});
