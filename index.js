import fs from 'node:fs';
import crypto from 'node:crypto';
import WebSocket from 'ws';

const runtimeFile = 'galaxi-runtime.json';
const controlFile = 'galaxi-control.json';

const num = (v, d) => Number.isFinite(Number(v)) ? Number(v) : d;
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const now = () => Date.now();
const round = (n, d = 6) => Number(Number(n).toFixed(d));

const cfg = {
  mode: String(process.env.TRADING_MODE || 'PAPER').toUpperCase() === 'LIVE' ? 'LIVE' : 'PAPER',
  liveArmed: String(process.env.LIVE_ARMED || 'false').toLowerCase() === 'true',
  openaiKey: process.env.OPENAI_API_KEY || '',
  openaiModel: process.env.OPENAI_MODEL || 'gpt-5.5',
  binanceKey: process.env.BINANCE_API_KEY || '',
  binanceSecret: process.env.BINANCE_API_SECRET || '',
  binanceBase: (process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com').replace(/\/$/, ''),
  wsUrl: process.env.BINANCE_FUTURES_WS || 'wss://fstream.binance.com/ws/!miniTicker@arr',
  capital: Math.max(100, num(process.env.PAPER_START_CAPITAL, 10000)),
  maxPositions: clamp(Math.floor(num(process.env.MAX_POSITIONS, 12)), 1, 12),
  maxTotalMarginPct: clamp(num(process.env.MAX_TOTAL_MARGIN_PCT, 30), 1, 50),
  maxPositionMarginPct: clamp(num(process.env.MAX_POSITION_MARGIN_PCT, 2), 0.25, 5),
  leverage: clamp(Math.floor(num(process.env.LEVERAGE, 5)), 1, 10),
  scanMs: Math.max(15000, Math.floor(num(process.env.SCAN_INTERVAL_MS, 20000))),
  aiTimeoutMs: Math.max(5000, Math.floor(num(process.env.AI_TIMEOUT_MS, 15000))),
  aiTopSymbols: clamp(Math.floor(num(process.env.AI_TOP_SYMBOLS, 20)), 8, 30),
  klineLimit: clamp(Math.floor(num(process.env.KLINE_LIMIT, 80)), 60, 150),
  maxDailyLossPct: clamp(num(process.env.MAX_DAILY_LOSS_PCT, 5), 0.5, 20),
  maxDrawdownPct: clamp(num(process.env.MAX_DRAWDOWN_PCT, 10), 1, 30),
  minSecondsBetweenOrders: Math.max(2, num(process.env.MIN_SECONDS_BETWEEN_ORDERS, 5)),
  maxActionsPerCycle: clamp(Math.floor(num(process.env.MAX_ACTIONS_PER_CYCLE, 2)), 1, 4),
  paperTpPct: Math.max(0.1, num(process.env.PAPER_TP_PCT, 1.2)),
  paperSlPct: Math.max(0.1, num(process.env.PAPER_SL_PCT, 0.7)),
  paperMaxHoldMs: Math.max(60000, num(process.env.PAPER_MAX_HOLD_MS, 1800000)),
};

const state = {
  running: true, mode: cfg.mode, ai: Boolean(cfg.openaiKey), aiModel: cfg.openaiModel,
  liveArmed: cfg.liveArmed, equity: cfg.capital, initialCapital: cfg.capital,
  realizedPnl: 0, unrealizedPnl: 0, todayPnl: 0, drawdownPct: 0, dailyLossPct: 0,
  symbols: 0, warmSymbols: 0, cycle: 0, wsConnected: 0, wsExpected: 1,
  candidates: 0, riskApproved: 0, portfolioCount: 0, regime: 'MIXTO',
  timeframes: {'20s':'—','1m':'—','3m':'—','5m':'—'}, longPct: 50, shortPct: 50,
  positions: [], ranking: [], history: [], news: [], lastSignal: 'Esperando datos para el cerebro IA…',
  lastError: null, aiDecision: null, aiReasoning: '', aiCalls: 0, aiErrors: 0,
  restCalls: 0, rate429: 0, rate418: 0, lastUpdate: null, peakEquity: cfg.capital,
  stopped: false, lastCycleMs: 0
};

const ticks = new Map();
const cooldown = new Map();
const marketInfo = new Map();
let ws = null, reconnectTimer = null, loopBusy = false, lastOrderTs = 0, serverOffset = 0;
let lastAccount = null, stopRequested = false, intervalHandle = null;

function writeState() {
  const day = new Date().toISOString().slice(0,10);
  if (state.dayKey !== day) { state.dayKey = day; state.dayStartEquity = num(state.equity, cfg.capital); state.todayPnl = 0; }
  state.portfolioCount = state.positions.length;
  state.unrealizedPnl = state.positions.reduce((s,p) => s + num(p.pnl,0), 0);
  if (cfg.mode === 'PAPER') state.equity = cfg.capital + state.realizedPnl + state.unrealizedPnl;
  state.peakEquity = Math.max(num(state.peakEquity,cfg.capital), num(state.equity,cfg.capital));
  state.drawdownPct = state.peakEquity > 0 ? Math.max(0,(state.peakEquity-state.equity)/state.peakEquity*100) : 0;
  state.dailyLossPct = state.dayStartEquity > 0 ? Math.min(0, state.todayPnl/state.dayStartEquity*100) : 0;
  state.lastUpdate = new Date().toISOString();
  fs.writeFileSync(runtimeFile, JSON.stringify(state, null, 2));
}
function pushHistory(item) {
  state.history.unshift({time:new Date().toISOString(), ...item});
  state.history = state.history.slice(0,100);
}

async function rest(path, options={}, signed=false) {
  const method = options.method || 'GET';
  const params = {...(options.params || {})};
  if (signed) { params.timestamp = Date.now() + serverOffset; params.recvWindow = 5000; }
  const q = new URLSearchParams();
  for (const [k,v] of Object.entries(params)) if (v !== undefined && v !== null) q.set(k,String(v));
  if (signed) q.set('signature', crypto.createHmac('sha256', cfg.binanceSecret).update(q.toString()).digest('hex'));
  const url = `${cfg.binanceBase}${path}${q.toString() ? `?${q}` : ''}`;
  const headers = {};
  if (cfg.binanceKey) headers['X-MBX-APIKEY'] = cfg.binanceKey;
  if (method !== 'GET') headers['Content-Type'] = 'application/x-www-form-urlencoded';
  state.restCalls++;
  const res = await fetch(url,{method,headers,body:method==='GET'?undefined:q.toString()});
  const text = await res.text();
  if (res.status===429) state.rate429++;
  if (res.status===418) state.rate418++;
  let data; try { data=JSON.parse(text); } catch { data={raw:text}; }
  if (!res.ok) throw new Error(`BINANCE ${res.status}: ${data?.msg || text.slice(0,300)}`);
  return data;
}

async function syncServerTime() {
  const d = await rest('/fapi/v1/time');
  serverOffset = num(d.serverTime,Date.now()) - Date.now();
}
async function loadExchangeInfo() {
  const data = await rest('/fapi/v1/exchangeInfo');
  marketInfo.clear();
  for (const s of data.symbols || []) {
    if (s.status!=='TRADING' || s.quoteAsset!=='USDT' || s.contractType!=='PERPETUAL') continue;
    const lot = (s.filters||[]).find(x=>x.filterType==='LOT_SIZE');
    const price = (s.filters||[]).find(x=>x.filterType==='PRICE_FILTER');
    const notional = (s.filters||[]).find(x=>x.filterType==='MIN_NOTIONAL');
    marketInfo.set(s.symbol,{qtyStep:num(lot?.stepSize,0.001),minQty:num(lot?.minQty,0),tickSize:num(price?.tickSize,0.00001),minNotional:num(notional?.notional,5)});
  }
  state.symbols=marketInfo.size;
}
function precisionFromStep(step) {
  const s=String(step); if(!s.includes('.')) return 0;
  return Math.max(0,s.split('.')[1].replace(/0+$/,'').length);
}
function normalizeQty(symbol, qty) {
  const m=marketInfo.get(symbol); if(!m) return 0;
  const step=m.qtyStep, p=precisionFromStep(step);
  return round(Math.floor(Number(qty)/step)*step,p);
}
function normalizePrice(symbol, price) {
  const m=marketInfo.get(symbol); if(!m) return Number(price);
  const p=precisionFromStep(m.tickSize);
  return round(Math.round(Number(price)/m.tickSize)*m.tickSize,p);
}
function pctMove(a,b) { return b>0 ? (a/b-1)*100 : 0; }
function ema(v,p) { if(!v.length) return 0; const k=2/(p+1); let e=v[0]; for(let i=1;i<v.length;i++) e=v[i]*k+e*(1-k); return e; }
function rsi(v,p=14) {
  if(v.length<=p) return 50; let g=0,l=0;
  for(let i=1;i<=p;i++){const d=v[i]-v[i-1]; if(d>=0)g+=d;else l-=d;}
  let ag=g/p, al=l/p;
  for(let i=p+1;i<v.length;i++){const d=v[i]-v[i-1];ag=((ag*(p-1))+Math.max(d,0))/p;al=((al*(p-1))+Math.max(-d,0))/p;}
  if(al===0)return 100; return 100-100/(1+ag/al);
}
function atr(k,p=14) { if(k.length<p+2)return 0; const tr=[]; for(let i=1;i<k.length;i++){const h=num(k[i][2],0),l=num(k[i][3],0),pc=num(k[i-1][4],0);tr.push(Math.max(h-l,Math.abs(h-pc),Math.abs(l-pc)));} const r=tr.slice(-p); return r.reduce((a,b)=>a+b,0)/r.length; }
async function fetchKlines(symbol,interval,limit=cfg.klineLimit) { return rest('/fapi/v1/klines',{params:{symbol,interval,limit}}); }
function analyzeKlines(symbol,k1,k5) {
  const c=k1.map(x=>num(x[4],0)), h=k1.map(x=>num(x[2],0)), l=k1.map(x=>num(x[3],0)), v=k1.map(x=>num(x[5],0));
  const price=c.at(-1)||num(ticks.get(symbol)?.price,0);
  const e9=ema(c.slice(-40),9),e21=ema(c.slice(-60),21),e50=ema(c.slice(-70),50), rr=rsi(c), aa=atr(k1);
  const volNow=v.slice(-10).reduce((a,b)=>a+b,0)/10, volPrev=v.slice(-30,-10).reduce((a,b)=>a+b,0)/20||volNow;
  const c5=k5.map(x=>num(x[4],0));
  const recentHigh=Math.max(...h.slice(-20)),recentLow=Math.min(...l.slice(-20));
  const bull=e9>e21&&e21>e50&&pctMove(price,c.at(-6))>0&&pctMove(price,c.at(-16))>0&&ema(c5.slice(-40),20)>=ema(c5.slice(-70),50);
  const bear=e9<e21&&e21<e50&&pctMove(price,c.at(-6))<0&&pctMove(price,c.at(-16))<0&&ema(c5.slice(-40),20)<=ema(c5.slice(-70),50);
  return {symbol,price,bias:bull?'LONG':bear?'SHORT':'NEUTRAL',rsi:round(rr,2),atrPct:price?round(aa/price*100,4):0,
    volumeRatio:round(volPrev?volNow/volPrev:1,2),momentum1m:round(pctMove(price,c.at(-2)),3),momentum5m:round(pctMove(price,c.at(-6)),3),
    momentum15m:round(pctMove(price,c.at(-16)),3),momentum30m:round(pctMove(price,c.at(-31)),3),momentum5mTF:round(pctMove(c5.at(-1),c5.at(-4)),3),
    ema9:round(e9,8),ema21:round(e21,8),ema50:round(e50,8),ema20_5m:round(ema(c5.slice(-40),20),8),ema50_5m:round(ema(c5.slice(-70),50),8),
    breakoutUp:price>recentHigh*0.9995,breakoutDown:price<recentLow*1.0005,high20:recentHigh,low20:recentLow};
}
function avg(a){if(!a.length)return '—';const x=a.reduce((s,n)=>s+num(n,0),0)/a.length;return `${x>=0?'+':''}${x.toFixed(2)}%`;}
async function buildMarketSnapshot(){
  const candidates=[...ticks.entries()].filter(([s,t])=>marketInfo.has(s)&&t.price>0).sort((a,b)=>num(b[1].volume,0)-num(a[1].volume,0)).slice(0,cfg.aiTopSymbols);
  const rows=[];
  for(const [symbol] of candidates){try{const [k1,k5]=await Promise.all([fetchKlines(symbol,'1m'),fetchKlines(symbol,'5m',70)]);rows.push({...analyzeKlines(symbol,k1,k5),quoteVolume24h:round(num(ticks.get(symbol)?.volume,0),0)});}catch{} }
  rows.sort((a,b)=>Math.max(Math.abs(b.momentum5m),Math.abs(b.momentum15m))-Math.max(Math.abs(a.momentum5m),Math.abs(a.momentum15m)));
  state.warmSymbols=rows.length; state.ranking=rows.slice(0,20); state.candidates=rows.length;
  const ln=rows.filter(x=>x.bias==='LONG').length,sn=rows.filter(x=>x.bias==='SHORT').length;
  state.longPct=rows.length?Math.round(ln/rows.length*100):50; state.shortPct=100-state.longPct;
  const a15=rows.length?rows.reduce((s,x)=>s+x.momentum15m,0)/rows.length:0;
  state.regime=state.longPct>=62&&a15>0?'ALCISTA':state.shortPct>=62&&a15<0?'BAJISTA':'MIXTO';
  state.timeframes={'20s':'tick','1m':avg(rows.map(x=>x.momentum1m)),'3m':avg(rows.map(x=>x.momentum5m)),'5m':avg(rows.map(x=>x.momentum5mTF))};
  return rows;
}
function marginCapacity(positions=state.positions,equity=state.equity){const eq=Math.max(0,num(equity,cfg.capital));const used=positions.reduce((s,p)=>s+num(p.margin,0),0);return Math.max(0,Math.min(eq*cfg.maxPositionMarginPct/100,eq*cfg.maxTotalMarginPct/100-used));}
function riskAllowsOpen(symbol,margin){
  if(stopRequested||state.stopped)return {ok:false,reason:'STOP'};
  if(state.positions.length>=cfg.maxPositions)return {ok:false,reason:'MAX_POSITIONS'};
  if(margin<=0)return {ok:false,reason:'NO_MARGIN'};
  const used=state.positions.reduce((s,p)=>s+num(p.margin,0),0),maxTotal=num(state.equity,0)*cfg.maxTotalMarginPct/100;
  if(used+margin>maxTotal+1e-9)return {ok:false,reason:'MAX_TOTAL_MARGIN'};
  if(Math.max(0,-state.dailyLossPct)>=cfg.maxDailyLossPct)return {ok:false,reason:'DAILY_LOSS'};
  if(state.drawdownPct>=cfg.maxDrawdownPct)return {ok:false,reason:'MAX_DRAWDOWN'};
  if((cooldown.get(symbol)||0)>now())return {ok:false,reason:'COOLDOWN'};
  return {ok:true};
}
function cleanJsonText(t){let s=String(t||'').trim();if(s.startsWith('```'))s=s.replace(/^```(?:json)?/i,'').replace(/```$/i,'').trim();const a=s.indexOf('{'),b=s.lastIndexOf('}');return a>=0&&b>a?s.slice(a,b+1):s;}
async function askAI(market,account){
  if(!cfg.openaiKey)throw new Error('OPENAI_API_KEY no configurada');
  const payload={timestamp:new Date().toISOString(),regime:state.regime,longPct:state.longPct,shortPct:state.shortPct,equity:state.equity,
    positions:(account.positions||[]).map(p=>({symbol:p.symbol,side:p.side,entry:p.entry,mark:p.mark,pnl:p.pnl,margin:p.margin})),market:market.slice(0,cfg.aiTopSymbols)};
  const instructions=`Eres GALAXI, un motor autónomo de decisión para Binance USD-M Futures. Analiza el snapshot completo antes de decidir. No uses un score fijo. Integra estructura, momentum multitemporal, EMA, RSI, ATR, volumen, rupturas, régimen y posiciones abiertas. Busca LONG y SHORT; no repitas símbolos sin una tesis nueva. Puedes usar HOLD. Puedes abrir como máximo ${cfg.maxActionsPerCycle} acciones por ciclo. Usa sólo símbolos presentes en market. CLOSE sólo para posiciones existentes. margin_pct debe estar entre 0.25 y ${cfg.maxPositionMarginPct}. confidence es 0-100. No inventes datos. Los límites de riesgo del sistema son obligatorios. Devuelve sólo JSON.`;
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),cfg.aiTimeoutMs);
  try{
    const res=await fetch('https://api.openai.com/v1/responses',{method:'POST',signal:controller.signal,headers:{Authorization:`Bearer ${cfg.openaiKey}`,'Content-Type':'application/json'},body:JSON.stringify({model:cfg.openaiModel,instructions,input:JSON.stringify(payload),text:{format:{type:'json_schema',name:'galaxi_trade_decision',strict:true,schema:{type:'object',additionalProperties:false,properties:{regime:{type:'string',enum:['ALCISTA','BAJISTA','MIXTO']},actions:{type:'array',maxItems:cfg.maxActionsPerCycle,items:{type:'object',additionalProperties:false,properties:{action:{type:'string',enum:['OPEN_LONG','OPEN_SHORT','CLOSE','HOLD']},symbol:{type:'string'},margin_pct:{type:'number'},reason:{type:'string'},confidence:{type:'number'}},required:['action','symbol','margin_pct','reason','confidence']}},summary:{type:'string'}},required:['regime','actions','summary']}}}})});
    const text=await res.text(); if(!res.ok)throw new Error(`OPENAI ${res.status}: ${text.slice(0,400)}`);
    const data=JSON.parse(text); const output=data.output_text||data.output?.flatMap(x=>x.content||[]).find(x=>x.type==='output_text')?.text||'';
    const decision=JSON.parse(cleanJsonText(output)); state.aiCalls++; return sanitizeDecision(decision,market,account.positions||[]);
  }finally{clearTimeout(timer);}
}
function sanitizeDecision(d,market,positions){
  const allowed=new Set(market.map(x=>x.symbol)),held=new Set(positions.map(x=>x.symbol));
  const actions=[];
  for(const raw of Array.isArray(d?.actions)?d.actions:[]){
    const a={action:String(raw.action||'HOLD'),symbol:String(raw.symbol||''),margin_pct:clamp(num(raw.margin_pct,1),0.25,cfg.maxPositionMarginPct),reason:String(raw.reason||''),confidence:clamp(num(raw.confidence,0),0,100)};
    if(!['OPEN_LONG','OPEN_SHORT','CLOSE','HOLD'].includes(a.action))continue;
    if(a.action==='HOLD'){actions.push(a);continue;}
    if(!allowed.has(a.symbol))continue;
    if(a.action==='CLOSE'&&!held.has(a.symbol))continue;
    if((a.action==='OPEN_LONG'||a.action==='OPEN_SHORT')&&held.has(a.symbol))continue;
    actions.push(a); if(actions.length>=cfg.maxActionsPerCycle)break;
  }
  return {regime:['ALCISTA','BAJISTA','MIXTO'].includes(d?.regime)?d.regime:'MIXTO',actions,summary:String(d?.summary||'IA evaluó el mercado').slice(0,500)};
}
async function getLiveAccount(){
  if(!cfg.binanceKey||!cfg.binanceSecret)throw new Error('BINANCE API no configurada');
  const [account,pos]=await Promise.all([rest('/fapi/v2/account',{},true),rest('/fapi/v2/positionRisk',{},true)]);
  const positions=(pos||[]).filter(p=>Math.abs(num(p.positionAmt,0))>0).map(p=>{const amt=num(p.positionAmt,0),entry=num(p.entryPrice,0),mark=num(p.markPrice,0),pnl=num(p.unRealizedProfit,0);return{symbol:p.symbol,side:amt>0?'LONG':'SHORT',qty:Math.abs(amt),entry,mark,pnl,margin:Math.abs(num(p.notional,0))/Math.max(1,cfg.leverage)};});
  const wallet=num(account.totalWalletBalance,0),unreal=num(account.totalUnrealizedProfit,0);
  return {equity:wallet+unreal,wallet,unrealizedPnl:unreal,positions};
}
function paperAccount(){return{equity:state.equity,wallet:state.equity,unrealizedPnl:state.unrealizedPnl,positions:state.positions.map(p=>({symbol:p.symbol,side:p.side,qty:p.qty,entry:p.entry,mark:p.current,pnl:p.pnl,margin:p.margin}))};}
async function setLeverage(symbol){await rest('/fapi/v1/leverage',{method:'POST',params:{symbol,leverage:cfg.leverage}},true);}
async function placeMarketOrder(symbol,side,qty,reduceOnly=false){
  if(!cfg.binanceKey||!cfg.binanceSecret)throw new Error('BINANCE API no configurada');
  const q=normalizeQty(symbol,qty),m=marketInfo.get(symbol); if(!(q>0)||q<(m?.minQty||0))throw new Error(`Cantidad inválida ${symbol}`);
  if(now()-lastOrderTs<cfg.minSecondsBetweenOrders*1000)throw new Error('Protección: intervalo mínimo entre órdenes');
  await setLeverage(symbol);
  const params={symbol,side,type:'MARKET',quantity:q,newOrderRespType:'RESULT'};if(reduceOnly)params.reduceOnly='true';
  const order=await rest('/fapi/v1/order',{method:'POST',params},true);lastOrderTs=now();return order;
}
async function placeProtection(symbol,positionSide,entry){
  const m=marketInfo.get(symbol); if(!m||!(entry>0))return;
  const exitSide=positionSide==='LONG'?'SELL':'BUY';
  const sl=normalizePrice(symbol,positionSide==='LONG'?entry*(1-cfg.paperSlPct/100):entry*(1+cfg.paperSlPct/100));
  const tp=normalizePrice(symbol,positionSide==='LONG'?entry*(1+cfg.paperTpPct/100):entry*(1-cfg.paperTpPct/100));
  const common={symbol,workingType:'MARK_PRICE',priceProtect:'TRUE',closePosition:'true'};
  await rest('/fapi/v1/order',{method:'POST',params:{...common,side:exitSide,type:'STOP_MARKET',stopPrice:sl}},true);
  await rest('/fapi/v1/order',{method:'POST',params:{...common,side:exitSide,type:'TAKE_PROFIT_MARKET',stopPrice:tp}},true);
}
async function cancelSymbolOrders(symbol){try{await rest('/fapi/v1/allOpenOrders',{method:'DELETE',params:{symbol}},true);}catch(e){pushHistory({action:'ORDER_CANCEL_ERROR',symbol,reason:e.message});}}
async function executeLiveAction(a){
  if(!cfg.liveArmed)throw new Error('LIVE bloqueado: LIVE_ARMED=false');
  const live=await getLiveAccount(),existing=live.positions.find(p=>p.symbol===a.symbol);
  if(a.action==='CLOSE'){
    if(!existing)return {skipped:true,reason:'NO_POSITION'};
    await cancelSymbolOrders(existing.symbol); const side=existing.side==='LONG'?'SELL':'BUY';
    const order=await placeMarketOrder(existing.symbol,side,existing.qty,true); pushHistory({action:'LIVE_CLOSE',symbol:existing.symbol,side:existing.side,orderId:order.orderId,reason:a.reason}); return{order};
  }
  if(a.action==='OPEN_LONG'||a.action==='OPEN_SHORT'){
    if(existing)return{skipped:true,reason:'SYMBOL_ALREADY_OPEN'};
    const margin=Math.min(live.equity*clamp(num(a.margin_pct,1),0.25,cfg.maxPositionMarginPct)/100,marginCapacity(live.positions,live.equity));
    const liveUsed=live.positions.reduce((s,p)=>s+num(p.margin,0),0);
    if(live.positions.length>=cfg.maxPositions)return{skipped:true,reason:'MAX_POSITIONS'};
    if(liveUsed+margin>live.equity*cfg.maxTotalMarginPct/100+1e-9)return{skipped:true,reason:'MAX_TOTAL_MARGIN'};
    if(Math.max(0,-state.dailyLossPct)>=cfg.maxDailyLossPct||state.drawdownPct>=cfg.maxDrawdownPct)return{skipped:true,reason:'RISK_STOP'};
    const risk=riskAllowsOpen(a.symbol,margin);if(!risk.ok)return{skipped:true,reason:risk.reason};
    const price=num(ticks.get(a.symbol)?.price,0);if(!(price>0))return{skipped:true,reason:'NO_PRICE'};
    const qty=normalizeQty(a.symbol,margin*cfg.leverage/price);if(!(qty>0))return{skipped:true,reason:'QTY_TOO_SMALL'};
    const side=a.action==='OPEN_LONG'?'BUY':'SELL'; const order=await placeMarketOrder(a.symbol,side,qty,false);
    const fill=num(order.avgPrice,price); await placeProtection(a.symbol,a.action==='OPEN_LONG'?'LONG':'SHORT',fill);
    cooldown.set(a.symbol,now()+60000); pushHistory({action:'LIVE_OPEN',symbol:a.symbol,side:a.action==='OPEN_LONG'?'LONG':'SHORT',qty,margin,orderId:order.orderId,entry:fill,reason:a.reason}); return{order};
  }
  return{skipped:true,reason:'HOLD'};
}
function paperOpen(a){
  if(state.positions.some(p=>p.symbol===a.symbol))return{skipped:true,reason:'SYMBOL_ALREADY_OPEN'};
  const margin=Math.min(state.equity*clamp(num(a.margin_pct,1),0.25,cfg.maxPositionMarginPct)/100,marginCapacity());const risk=riskAllowsOpen(a.symbol,margin);if(!risk.ok)return{skipped:true,reason:risk.reason};
  const price=num(ticks.get(a.symbol)?.price,0);if(!(price>0)||!(margin>0))return{skipped:true,reason:'NO_PRICE_OR_MARGIN'};
  const side=a.action==='OPEN_LONG'?'LONG':'SHORT',qty=margin*cfg.leverage/price;
  state.positions.push({id:`AI_${now()}_${Math.random().toString(36).slice(2,8)}`,symbol:a.symbol,side,entry:price,current:price,margin,qty,pnl:0,unrealizedPct:0,leverage:cfg.leverage,confidence:a.confidence,strategy:'AI',thesis:a.reason,openedAt:new Date().toISOString(),openedTs:now()});
  cooldown.set(a.symbol,now()+60000);pushHistory({action:'AI_OPEN',symbol:a.symbol,side,margin,reason:a.reason});return{opened:true};
}
function paperClose(a){const p=state.positions.find(x=>x.symbol===a.symbol);if(!p)return{skipped:true,reason:'NO_POSITION'};const t=ticks.get(p.symbol);if(t?.price)p.current=t.price;const move=p.side==='LONG'?p.current-p.entry:p.entry-p.current;p.pnl=move*p.qty;state.realizedPnl+=p.pnl;state.todayPnl+=p.pnl;state.positions=state.positions.filter(x=>x.id!==p.id);cooldown.set(p.symbol,now()+60000);pushHistory({action:'AI_CLOSE',symbol:p.symbol,side:p.side,pnl:round(p.pnl,4),reason:a.reason});return{closed:true};}
function markPaperPositions(){
  const keep=[],t=now();
  for(const p of state.positions){const tick=ticks.get(p.symbol);if(tick?.price)p.current=tick.price;const move=p.side==='LONG'?p.current-p.entry:p.entry-p.current;p.pnl=move*p.qty;p.unrealizedPct=p.margin?p.pnl/p.margin*100:0;const age=t-p.openedTs;const tp=p.unrealizedPct>=cfg.paperTpPct,sl=p.unrealizedPct<=-cfg.paperSlPct,timeout=age>=cfg.paperMaxHoldMs;
    if(tp||sl||timeout){state.realizedPnl+=p.pnl;state.todayPnl+=p.pnl;pushHistory({action:'RISK_CLOSE',symbol:p.symbol,side:p.side,pnl:round(p.pnl,4),reason:tp?'TP':sl?'SL':'TIME'});cooldown.set(p.symbol,t+60000);}else keep.push(p);
  } state.positions=keep;
}
function emergencyStopCheck(){
  if(fs.existsSync(controlFile)){try{const c=JSON.parse(fs.readFileSync(controlFile,'utf8'));if(c.stop)stopRequested=true;}catch{}}
  if(state.drawdownPct>=cfg.maxDrawdownPct||Math.max(0,-state.dailyLossPct)>=cfg.maxDailyLossPct){stopRequested=true;state.stopped=true;state.lastSignal='STOP AUTOMÁTICO POR RIESGO';}
  if(stopRequested)state.stopped=true;
}
function connect(){
  if(ws){try{ws.removeAllListeners();ws.close();}catch{}}
  try{ws=new WebSocket(cfg.wsUrl);ws.on('open',()=>{state.wsConnected=1;state.lastError=null;console.log('WS_CONNECTED=1');});ws.on('close',()=>{state.wsConnected=0;if(!stopRequested&&!reconnectTimer)reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect();},3000);});ws.on('error',e=>{state.lastError=`WebSocket: ${e.message||'error'}`;});ws.on('message',raw=>{try{const arr=JSON.parse(raw.toString());if(!Array.isArray(arr))return;const ts=now();for(const t of arr){const s=t.s;if(!s?.endsWith('USDT')||!marketInfo.has(s))continue;const price=num(t.c,0);if(price>0)ticks.set(s,{price,volume:num(t.q,0),ts});}}catch(e){state.lastError=`WS parse: ${e.message}`;}});}catch(e){state.lastError=`WS init: ${e.message}`;}
}
async function runCycle(){
  if(loopBusy||stopRequested)return;loopBusy=true;const started=now();
  try{emergencyStopCheck();state.cycle++;if(state.stopped){state.running=false;writeState();return;}if(!state.wsConnected){state.lastSignal='Esperando WebSocket de Binance…';writeState();return;}
    const market=await buildMarketSnapshot();if(market.length<5){state.lastSignal='Calentando datos de mercado…';writeState();return;}
    if(cfg.mode==='PAPER')markPaperPositions();else{lastAccount=await getLiveAccount();state.equity=lastAccount.equity;state.unrealizedPnl=lastAccount.unrealizedPnl;state.positions=lastAccount.positions;}
    const account=cfg.mode==='LIVE'?lastAccount:paperAccount();const decision=await askAI(market,account);state.aiDecision=decision;state.aiReasoning=decision.summary;state.regime=decision.regime;state.riskApproved=0;
    for(const a of decision.actions){if(a.action==='HOLD')continue;try{const result=cfg.mode==='PAPER'?(a.action==='OPEN_LONG'||a.action==='OPEN_SHORT'?paperOpen(a):paperClose(a)):await executeLiveAction(a);if(result?.opened||result?.closed||result?.order)state.riskApproved++;}catch(e){state.lastError=`${cfg.mode} action: ${e.message}`;pushHistory({action:`${cfg.mode}_ERROR`,symbol:a.symbol,reason:e.message});}}
    state.lastSignal=decision.summary;state.lastCycleMs=now()-started;
  }catch(e){state.aiErrors++;state.lastError=e?.message||String(e);console.error('CYCLE_ERROR',state.lastError);}finally{writeState();loopBusy=false;}
}
async function start(){
  console.log(`GALAXI V22 | mode=${cfg.mode} | model=${cfg.openaiModel} | scan=${cfg.scanMs}ms`);
  if(!cfg.openaiKey)console.warn('OPENAI_API_KEY missing: AI disabled until configured.');
  if(cfg.mode==='LIVE'&&(!cfg.liveArmed||!cfg.binanceKey||!cfg.binanceSecret))console.warn('LIVE no armado/configurado: no se enviarán órdenes.');
  try{await syncServerTime();await loadExchangeInfo();connect();await sleep(5000);writeState();await runCycle();intervalHandle=setInterval(runCycle,cfg.scanMs);}catch(e){state.lastError=`BOOT: ${e.message}`;writeState();console.error('BOOT_ERROR',e.message);throw e;}
}
function stop(){stopRequested=true;state.stopped=true;state.running=false;if(intervalHandle)clearInterval(intervalHandle);if(reconnectTimer)clearTimeout(reconnectTimer);try{ws?.close();}catch{}writeState();}
process.on('SIGTERM',()=>{stop();process.exit(0);});process.on('SIGINT',()=>{stop();process.exit(0);});
writeState();
export { cfg, state, start, stop, runCycle };
