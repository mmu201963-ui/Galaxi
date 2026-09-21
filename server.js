import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const root = path.dirname(fileURLToPath(import.meta.url));
const runtimeFile = path.join(root, 'galaxi-runtime.json');
const controlFile = path.join(root, 'galaxi-control.json');
const htmlPath = fs.existsSync(path.join(root, 'public/index.html'))
  ? path.join(root, 'public/index.html')
  : path.join(root, 'index.html');

const state = { running:false, mode:String(process.env.TRADING_MODE||'PAPER').toUpperCase(), lastError:null, chat:[] };
let child = null, alive = false;
function money(n){return '$'+Number(n||0).toFixed(2);}
function mergeRuntime(){
  try { if(fs.existsSync(runtimeFile)) Object.assign(state, JSON.parse(fs.readFileSync(runtimeFile,'utf8'))); }
  catch(e){ state.lastError='runtime: '+e.message; }
  state.running=alive && !state.stopped; state.lastUpdate=new Date().toISOString();
}
function json(res,obj,code=200){res.writeHead(code,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(obj));}
function readBody(req){return new Promise((resolve,reject)=>{let s='';req.on('data',c=>{s+=c;if(s.length>20000){req.destroy();reject(new Error('body too large'));}});req.on('end',()=>{try{resolve(s?JSON.parse(s):{});}catch(e){reject(e);}});req.on('error',reject);});}
function chatReply(q){
  const x=q.toLowerCase(), p=state.positions||[], r=state.ranking||[];
  if(x.includes('stop')) return 'STOP GALAXI solicitado. Se bloquean nuevas entradas.';
  if(x.includes('posición')||x.includes('posicion')) return p.length?`GALAXI tiene ${p.length} posiciones ${state.mode}. Equity ${money(state.equity)} · P&L ${money(state.unrealizedPnl)}.`:'No hay posiciones abiertas.';
  if(x.includes('short')) return r.filter(a=>a.side==='SHORT').slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} SHORT · ${a.score}/100 · ${a.strategy} · confianza ${a.confidence}%`).join('\n')||'No hay SHORT destacados en este ciclo.';
  if(x.includes('long')) return r.filter(a=>a.side==='LONG').slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} LONG · ${a.score}/100 · ${a.strategy} · confianza ${a.confidence}%`).join('\n')||'No hay LONG destacados en este ciclo.';
  if(x.includes('mercado')||x.includes('ahora')) return `Régimen ${state.regime}. 20s ${state.timeframes?.['20s']||'—'} · 1m ${state.timeframes?.['1m']||'—'} · 3m ${state.timeframes?.['3m']||'—'} · 5m ${state.timeframes?.['5m']||'—'}. LONG ${state.longPct}% / SHORT ${state.shortPct}%. Mercados ${state.symbols||0}.`;
  if(x.includes('oportun')) return r.slice(0,5).map((a,i)=>`${i+1}. ${a.symbol} ${a.side} · ${a.score}/100 · ${a.strategy} · ${a.confidence}%`).join('\n')||'Calentando datos.';
  return `GALAXI está en ${state.mode}, ciclo ${state.cycle||0}. Analiza el universo y decide dinámicamente. Datos: ${state.dataQuality||'—'}.`;
}
const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,'http://localhost');
  if(u.pathname==='/api/status'){mergeRuntime();return json(res,state);}
  if(u.pathname==='/api/health')return json(res,{ok:true,service:'galaxi',engineAlive:alive,stopped:state.stopped||false,mode:state.mode,time:new Date().toISOString()});
  if(u.pathname==='/api/chat'&&req.method==='POST'){try{const b=await readBody(req),msg=String(b.message||''),reply=chatReply(msg);state.chat=[...(state.chat||[]),{role:'user',text:msg,time:Date.now()},{role:'galaxi',text:reply,time:Date.now()}].slice(-40);return json(res,{ok:true,reply});}catch(e){return json(res,{ok:false,error:e.message},400);}}
  if(u.pathname==='/api/stop'&&req.method==='POST'){try{fs.writeFileSync(controlFile,JSON.stringify({stop:true,time:Date.now()}));return json(res,{ok:true,message:'STOP GALAXI activado.'});}catch(e){return json(res,{ok:false,error:e.message},500);}}
  if(u.pathname==='/api/resume'&&req.method==='POST'){try{fs.writeFileSync(controlFile,JSON.stringify({stop:false,time:Date.now()}));if(!alive)start();return json(res,{ok:true,message:'GALAXI reanudado.'});}catch(e){return json(res,{ok:false,error:e.message},500);}}
  if(u.pathname==='/'||u.pathname==='/index.html'){
    try {
      const html=fs.readFileSync(htmlPath,'utf8');
      res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
      return res.end(html);
    } catch(e) {
      state.lastError='index.html: '+e.message;
      return json(res,{ok:false,error:'Frontend not found: '+e.message},500);
    }
  }
  res.writeHead(404);res.end('Not found');
});
function start(){child=spawn(process.execPath,['index.js'],{cwd:root,env:process.env,stdio:['ignore','pipe','pipe']});alive=true;state.running=true;child.stdout.on('data',d=>process.stdout.write(d));child.stderr.on('data',d=>process.stderr.write(d));child.on('exit',(code,signal)=>{alive=false;state.running=false;if(code!==0)state.lastError=`index.js terminó code=${code} signal=${signal||''}`;});}
start();
server.listen(PORT,HOST,()=>console.log(`GALAXI WEB | http://${HOST}:${PORT}`));
process.on('SIGTERM',()=>{try{child?.kill('SIGTERM')}catch{}server.close(()=>process.exit(0));});
