import fs from 'node:fs';
import WebSocket from 'ws';

const runtimeFile='galaxi-runtime.json';
const controlFile='galaxi-control.json';

const cfg={
  mode:String(process.env.TRADING_MODE||'PAPER').toUpperCase(),
  capital:Number(process.env.PAPER_START_CAPITAL||10000),
  maxPositions:Math.min(12,Math.max(1,Number(process.env.MAX_POSITIONS||12))),
  maxTotalMarginPct:Math.min(80,Math.max(1,Number(process.env.MAX_TOTAL_MARGIN_PCT||30))),
  maxPositionMarginPct:Math.min(10,Math.max(.5,Number(process.env.MAX_POSITION_MARGIN_PCT||3))),
  interval:Math.max(5000,Number(process.env.SCAN_INTERVAL_MS||20000)),
  minScore:Math.min(95,Math.max(50,Number(process.env.MIN_SCORE||70))),
  minHistory:Number(process.env.MIN_HISTORY||12),
  takeProfitPct:Number(process.env.TAKE_PROFIT_PCT||1.20),
  stopLossPct:Number(process.env.STOP_LOSS_PCT||0.70),
  maxHoldMs:Number(process.env.MAX_HOLD_MS||1800000),
  cooldownMs:Number(process.env.COOLDOWN_MS||600000),
  minVolume:Number(process.env.MIN_QUOTE_VOLUME||500000)
};

const state={
  running:true, mode:cfg.mode, equity:cfg.capital, initialCapital:cfg.capital,
  realizedPnl:0, unrealizedPnl:0, todayPnl:0, drawdownPct:0, dailyLossPct:0,
  symbols:0,warmSymbols:0,cycle:0,wsConnected:0,wsExpected:1,candidates:0,riskApproved:0,
  portfolioCount:0,regime:'MIXTO',
  timeframes:{'20s':'—','1m':'—','3m':'—','5m':'—'},
  longPct:50,shortPct:50,positions:[],ranking:[],history:[],news:[],
  lastSignal:'Calentando mercado…',lastError:null,restCalls:0,rate429:0,rate418:0,
  lastUpdate:null
};

const ticks=new Map();
const series=new Map();
const cooldown=new Map();
let ws=null, stopped=false, reconnectTimer=null;

function clamp(n,a,b){return Math.max(a,Math.min(b,n));}
function pctMove(now,then){return then>0?(now/then-1)*100:0;}
function pushSeries(symbol,t){
  let a=series.get(symbol);
  if(!a){a=[];series.set(symbol,a);}
  a.push(t);
  const cutoff=Date.now()-6*60*1000;
  while(a.length && a[0].ts<cutoff) a.shift();
}
function sampleAt(a,msAgo){
  const target=Date.now()-msAgo;
  for(let i=a.length-1;i>=0;i--) if(a[i].ts<=target) return a[i].price;
  return a[0]?.price||0;
}
function avg(a,fn){
  if(!a.length)return 0;
  let s=0; for(const x of a)s+=fn(x); return s/a.length;
}
function volatility(a){
  if(a.length<3)return 0;
  const rets=[];
  for(let i=1;i<a.length;i++) rets.push((a[i].price/a[i-1].price-1)*100);
  const m=avg(rets,x=>x), v=avg(rets,x=>(x-m)**2);
  return Math.sqrt(v);
}
function trendScore(a){
  const now=a[a.length-1]?.price||0;
  if(!now || a.length<cfg.minHistory)return null;
  const p20=sampleAt(a,20000),p60=sampleAt(a,60000),p180=sampleAt(a,180000),p300=sampleAt(a,300000);
  if(!p60||!p180)return null;
  const m20=pctMove(now,p20),m60=pctMove(now,p60),m180=pctMove(now,p180),m300=pctMove(now,p300||p180);
  const recent=avg(a.slice(-8),x=>x.price);
  const prior=avg(a.slice(-24,-8),x=>x.price)||recent;
  const micro=pctMove(recent,prior);
  const vol=volatility(a);
  const direction=(m20*.35+m60*.30+m180*.20+m300*.10+micro*.05);
  return {m20,m60,m180,m300,micro,vol,price:now,direction};
}
function analyze(){
  const arr=[];
  for(const [symbol,t] of ticks){
    if(!t.price||t.volume<cfg.minVolume)continue;
    const a=series.get(symbol)||[];
    const x=trendScore(a);
    if(!x)continue;
    const direction=x.direction;
    const side=direction>=0?'LONG':'SHORT';
    const abs=Math.abs(direction);
    const consistency=(Math.sign(x.m20||0)===Math.sign(x.m60||0)?8:0)+(Math.sign(x.m60||0)===Math.sign(x.m180||0)?8:0);
    const activity=clamp(Math.log10(Math.max(1,t.volume))-5,0,4)*4;
    const stability=clamp(12-Math.min(x.vol,12),0,12);
    const score=Math.round(clamp(52+abs*18+consistency+activity+stability,0,99));
    const strategy=abs>0.45?(x.m20*x.m60>=0?'MOMENTUM_ALIGNMENT':'MOMENTUM_REVERSION'):abs>0.18?'REGIME_CONTINUATION':'MICRO_BREAKOUT';
    const confidence=Math.round(clamp(55+abs*25+consistency+stability/2,0,96));
    arr.push({
      symbol,side,score,confidence,strategy,price:x.price,
      momentum20s:Number(x.m20.toFixed(3)),momentum1m:Number(x.m60.toFixed(3)),
      momentum3m:Number(x.m180.toFixed(3)),momentum5m:Number(x.m300.toFixed(3)),
      volatility:Number(x.vol.toFixed(4)),
      thesis:`${strategy}: 20s ${x.m20.toFixed(2)}% · 1m ${x.m60.toFixed(2)}% · 3m ${x.m180.toFixed(2)}% · 5m ${x.m300.toFixed(2)}%`,
      riskApproved:score>=cfg.minScore && confidence>=65
    });
  }
  arr.sort((a,b)=>b.score-b.score || b.confidence-a.confidence);
  return arr;
}
function updateRegime(all){
  if(!all.length){state.regime='MIXTO';state.longPct=50;state.shortPct=50;return;}
  const longs=all.filter(x=>x.side==='LONG').length;
  state.longPct=Math.round(longs/all.length*100); state.shortPct=100-state.longPct;
  const breadth=(state.longPct-state.shortPct);
  const avgDir=avg(all.slice(0,50),x=>x.side==='LONG'?x.score:-x.score);
  state.regime=(breadth>12&&avgDir>4)?'ALCISTA':(breadth<-12&&avgDir<-4)?'BAJISTA':'MIXTO';
  state.timeframes={
    '20s':state.regime==='ALCISTA'?'↑':state.regime==='BAJISTA'?'↓':'↔',
    '1m':state.regime==='ALCISTA'?'↑':state.regime==='BAJISTA'?'↓':'↔',
    '3m':state.regime==='ALCISTA'?'↑':state.regime==='BAJISTA'?'↓':'↔',
    '5m':state.regime==='ALCISTA'?'↑':state.regime==='BAJISTA'?'↓':'↔'
  };
}
function marginFor(){
  const byPosition=cfg.capital*cfg.maxPositionMarginPct/100;
  const total=cfg.capital*cfg.maxTotalMarginPct/100;
  const remaining=Math.max(0,total-state.positions.reduce((s,p)=>s+p.margin,0));
  return Math.max(0,Math.min(byPosition,remaining));
}
function openPaper(all){
  if(stopped || state.positions.length>=cfg.maxPositions)return;
  const held=new Set(state.positions.map(p=>p.symbol));
  const now=Date.now();
  const regimeSide=state.regime==='ALCISTA'?'LONG':state.regime==='BAJISTA'?'SHORT':null;
  const pool=all.filter(a=>a.riskApproved && !held.has(a.symbol) && (cooldown.get(a.symbol)||0)<now);
  if(!pool.length)return;
  // In MIXTO, deliberately keep both directions available; in a directional regime,
  // still allow the opposite side only when its own score is strong.
  const preferred=regimeSide
    ? pool.sort((a,b)=>((b.side===regimeSide)-(a.side===regimeSide)) || b.score-a.score)
    : pool;
  const want=Math.min(2,cfg.maxPositions-state.positions.length);
  let opened=0;
  for(const a of preferred){
    if(opened>=want)break;
    const margin=marginFor(); if(margin<=0)break;
    state.positions.push({
      id:`L${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
      symbol:a.symbol,side:a.side,entry:a.price,current:a.price,margin,
      qty:margin/a.price,pnl:0,score:a.score,confidence:a.confidence,
      strategy:a.strategy,openedAt:new Date().toISOString(),openedTs:now
    });
    cooldown.set(a.symbol,now+cfg.cooldownMs);
    state.lastSignal=`PAPER OPEN ${a.symbol} ${a.side} · ${a.strategy} · ${a.score}/100`;
    opened++;
  }
}
function markAndClose(){
  const now=Date.now();
  const keep=[];
  for(const p of state.positions){
    const t=ticks.get(p.symbol);
    if(!t){keep.push(p);continue;}
    p.current=t.price;
    const move=p.side==='LONG'?p.current-p.entry:p.entry-p.current;
    p.pnl=move*p.qty;
    p.unrealizedPct=p.margin?100*p.pnl/p.margin:0;
    const age=now-p.openedTs;
    const tp=p.unrealizedPct>=cfg.takeProfitPct;
    const sl=p.unrealizedPct<=-cfg.stopLossPct;
    const timeout=age>=cfg.maxHoldMs;
    if(tp||sl||timeout){
      state.realizedPnl+=p.pnl;
      state.history.unshift({
        time:new Date().toISOString(),action:'CLOSE',symbol:p.symbol,side:p.side,
        pnl:Number(p.pnl.toFixed(4)),reason:tp?'TP':sl?'SL':'TIME'
      });
      cooldown.set(p.symbol,now+cfg.cooldownMs);
    }else keep.push(p);
  }
  state.positions=keep.slice(0,cfg.maxPositions);
}
function write(){
  state.portfolioCount=state.positions.length;
  state.unrealizedPnl=state.positions.reduce((s,p)=>s+Number(p.pnl||0),0);
  state.equity=cfg.capital+state.realizedPnl+state.unrealizedPnl;
  const dd=Math.max(0,cfg.capital-state.equity);
  state.drawdownPct=cfg.capital?dd/cfg.capital*100:0;
  state.dailyLossPct=Math.min(0,state.realizedPnl/cfg.capital*100);
  state.lastUpdate=new Date().toISOString();
  fs.writeFileSync(runtimeFile,JSON.stringify(state,null,2));
}
function connect(){
  try{
    ws=new WebSocket(process.env.BINANCE_FUTURES_WS||'wss://fstream.binance.com/ws/!miniTicker@arr');
    ws.on('open',()=>{state.wsConnected=1;state.lastError=null;console.log('WS_CONNECTED=1');});
    ws.on('close',()=>{state.wsConnected=0;if(!stopped&&!reconnectTimer)reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect();},3000);});
    ws.on('error',()=>{state.lastError='WebSocket error';});
    ws.on('message',raw=>{
      try{
        const arr=JSON.parse(raw.toString());
        if(!Array.isArray(arr))return;
        const now=Date.now();
        for(const t of arr){
          const symbol=t.s;
          if(!symbol?.endsWith('USDT'))continue;
          const price=Number(t.c), volume=Number(t.q||0);
          if(price>0){
            ticks.set(symbol,{price,volume,ts:now});
            pushSeries(symbol,{price,volume,ts:now});
          }
        }
      }catch(e){state.lastError='WS parse error';}
    });
  }catch(e){state.lastError='WS init: '+e.message;}
}
function loop(){
  if(fs.existsSync(controlFile)){
    try{if(JSON.parse(fs.readFileSync(controlFile,'utf8')).stop)stopped=true;}catch{}
  }
  state.cycle++;
  const all=analyze();
  state.symbols=ticks.size; state.warmSymbols=all.length;
  state.ranking=all.slice(0,20); state.candidates=all.filter(x=>x.riskApproved).length;
  state.riskApproved=Math.min(state.candidates,cfg.maxPositions);
  updateRegime(all);
  markAndClose();
  if(cfg.mode==='PAPER') openPaper(all);
  state.lastSignal=state.lastSignal||'Sin señal';
  write();
  if(state.cycle%5===0) console.log(`cycle=${state.cycle} universe=${state.symbols} ranked=${state.ranking.length} candidates=${state.candidates} portfolio=${state.positions.length} equity=${state.equity.toFixed(2)} regime=${state.regime}`);
  setTimeout(loop,cfg.interval);
}
console.log(`GALAXI | mode=${cfg.mode} | capital=${cfg.capital} | scan=${cfg.interval}ms`);
connect();
loop();
process.on('SIGTERM',()=>{stopped=true;try{ws?.close()}catch{}process.exit(0);});
