import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const root = path.dirname(fileURLToPath(import.meta.url));
const runtimeFile = path.join(root, 'galaxi-runtime.json');
const controlFile = path.join(root, 'galaxi-control.json');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');

const state = {
  running:false, mode:String(process.env.TRADING_MODE||'PAPER').toUpperCase(),
  equity:Number(process.env.PAPER_START_CAPITAL||10000),
  initialCapital:Number(process.env.PAPER_START_CAPITAL||10000),
  realizedPnl:0, unrealizedPnl:0, todayPnl:0, drawdownPct:0, dailyLossPct:0,
  symbols:0, warmSymbols:0, cycle:0, wsConnected:0, wsExpected:1,
  candidates:0, riskApproved:0, portfolioCount:0, regime:'MIXTO',
  timeframes:{'20s':'—','1m':'—','3m':'—','5m':'—'},
  longPct:50, shortPct:50, positions:[], ranking:[], history:[], news:[],
  chat:[], lastSignal:'Iniciando GALAXI…', lastError:null,
  restCalls:0, rate429:0, rate418:0, lastUpdate:null
};

let child = null;
let alive = false;

function mergeRuntime(){
  try{
    if(fs.existsSync(runtimeFile)) Object.assign(state, JSON.parse(fs.readFileSync(runtimeFile,'utf8')));
  }catch(e){ state.lastError = 'runtime: '+e.message; }
  state.running = alive;
  state.lastUpdate = new Date().toISOString();
}
function json(res, obj, code=200){
  res.writeHead(code, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
  res.end(JSON.stringify(obj));
}
function readBody(req){
  return new Promise((resolve,reject)=>{
    let s='';
    req.on('data', c=>{ s+=c; if(s.length>20000){req.destroy(); reject(new Error('body too large'));} });
    req.on('end', ()=>{ try{resolve(s?JSON.parse(s):{});}catch(e){reject(e);} });
    req.on('error',reject);
  });
}
function chatReply(q){
  const x=q.toLowerCase();
  const p=state.positions||[], r=state.ranking||[];
  if(x.includes('stop')) return 'STOP GALAXI solicitado. Se bloquean nuevas entradas PAPER.';
  if(x.includes('posición')||x.includes('posicion')) return p.length
    ? `Tengo ${p.length} posiciones PAPER. Equity ${money(state.equity)}, P&L no realizado ${money(state.unrealizedPnl)}.`
    : 'No hay posiciones PAPER abiertas.';
  if(x.includes('short')) return r.filter(a=>a.side==='SHORT').slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} SHORT · score ${a.score}/100 · ${a.strategy} · ${a.confidence}%`).join('\n') || 'No hay SHORT con score suficiente.';
  if(x.includes('long')) return r.filter(a=>a.side==='LONG').slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} LONG · score ${a.score}/100 · ${a.strategy} · ${a.confidence}%`).join('\n') || 'No hay LONG con score suficiente.';
  if(x.includes('mercado')||x.includes('ahora')) return `Régimen ${state.regime}. 20s ${state.timeframes?.['20s']||'—'} · 1m ${state.timeframes?.['1m']||'—'} · 3m ${state.timeframes?.['3m']||'—'} · 5m ${state.timeframes?.['5m']||'—'}. Long ${state.longPct}% / Short ${state.shortPct}%. ${state.candidates} candidatas.`;
  if(x.includes('oportun')) return r.slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} ${a.side} · ${a.score}/100 · ${a.strategy} · confianza ${a.confidence}%`).join('\n') || 'Calentando datos.';
  return `GALAXI está en ${state.mode}. Analizando ${state.symbols||0} mercados. Pregunta por mercado, oportunidades, LONG, SHORT o posiciones.`;
}
function money(n){return '$'+Number(n||0).toFixed(2);}

const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,'http://localhost');
  if(u.pathname==='/api/status'){ mergeRuntime(); return json(res,state); }
  if(u.pathname==='/api/health') return json(res,{ok:true,service:'galaxi',engineAlive:alive,mode:state.mode,time:new Date().toISOString()});
  if(u.pathname==='/api/chat' && req.method==='POST'){
    try{
      const b=await readBody(req), msg=String(b.message||''), reply=chatReply(msg);
      state.chat=[...(state.chat||[]),{role:'user',text:msg,time:Date.now()},{role:'lotus',text:reply,time:Date.now()}].slice(-40);
      return json(res,{ok:true,reply});
    }catch(e){return json(res,{ok:false,error:e.message},400);}
  }
  if(u.pathname==='/api/stop' && req.method==='POST'){
    try{
      fs.writeFileSync(controlFile, JSON.stringify({stop:true,time:Date.now()}));
      return json(res,{ok:true,message:'STOP GALAXI activado.'});
    }catch(e){return json(res,{ok:false,error:e.message},500);}
  }
  if(u.pathname==='/'||u.pathname==='/index.html'){
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
    return res.end(html);
  }
  res.writeHead(404); res.end('Not found');
});

function start(){
  child=spawn(process.execPath,['index.js'],{cwd:root,env:process.env,stdio:['ignore','pipe','pipe']});
  alive=true; state.running=true;
  child.stdout.on('data',d=>process.stdout.write(d));
  child.stderr.on('data',d=>process.stderr.write(d));
  child.on('exit',(code,signal)=>{
    alive=false; state.running=false;
    if(code!==0) state.lastError=`index.js terminó code=${code} signal=${signal||''}`;
  });
}
start();
server.listen(PORT,HOST,()=>console.log(`GALAXI WEB | http://${HOST}:${PORT}`));
process.on('SIGTERM',()=>{try{child?.kill('SIGTERM')}catch{} server.close(()=>process.exit(0));});
