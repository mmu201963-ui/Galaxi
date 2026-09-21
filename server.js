import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 8080);
const stateFile = path.join(__dirname, 'galaxi-runtime.json');
let child = null;
let logs = [];

function pushLog(s) {
  const lines = String(s).split(/\r?\n/).map(x => x.trim()).filter(Boolean);
  for (const line of lines) logs.push({time:new Date().toISOString(), line});
  logs = logs.slice(-200);
}
function readState() {
  try { return JSON.parse(fs.readFileSync(stateFile, 'utf8')); } catch { return {running:false}; }
}
function send(res, code, body, type='application/json; charset=utf-8') {
  res.writeHead(code, {'Content-Type':type,'Cache-Control':'no-store','Access-Control-Allow-Origin':'*'});
  res.end(body);
}
function startEngine() {
  if (child && !child.killed) return;
  child = spawn(process.execPath, ['index.js'], {cwd:__dirname, env:process.env, stdio:['ignore','pipe','pipe']});
  child.stdout.on('data', b => pushLog(b));
  child.stderr.on('data', b => pushLog('ERROR: '+b));
  child.on('exit', (code, signal) => pushLog(`ENGINE_EXIT code=${code} signal=${signal||'none'}`));
  child.on('error', e => pushLog(`ENGINE_ERROR ${e.message}`));
  pushLog('ENGINE_START');
}

startEngine();
const publicIndex = path.join(__dirname, 'public', 'index.html');
const embeddedDashboard = '<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GALAXI V22 · AI TRADER</title><style>\n:root{--bg:#070b12;--card:#101722;--line:#202b3a;--text:#eef4fa;--muted:#8d99aa;--ok:#35d39a;--warn:#ffbd59;--bad:#ff6262;--accent:#8e6cff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#15182a,#070b12 48%);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1180px;margin:auto;padding:18px}.head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px}.brand{display:flex;gap:12px;align-items:center}.logo{width:52px;height:52px;border-radius:15px;background:linear-gradient(135deg,#a987ff,#6542d6);display:grid;place-items:center;font-weight:900;font-size:22px}.sub{color:var(--muted);font-size:12px}.pill{padding:8px 11px;border:1px solid var(--line);border-radius:999px;background:#0d131d}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:rgba(16,23,34,.92);border:1px solid var(--line);border-radius:15px;padding:14px}.k{color:var(--muted);font-size:12px}.v{font-size:23px;font-weight:750;margin-top:5px}.wide{grid-column:span 2}.full{grid-column:1/-1}pre{white-space:pre-wrap;word-break:break-word;max-height:360px;overflow:auto;background:#080d14;padding:12px;border-radius:10px;color:#b9c7d8}.ok{color:var(--ok)}.bad{color:var(--bad)}button{border:1px solid var(--line);background:#171f2c;color:var(--text);padding:10px 14px;border-radius:10px}button.danger{background:#32171b;border-color:#663039}.pos{padding:8px 0;border-bottom:1px solid var(--line)}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}.wide{grid-column:span 2}}\n</style></head><body><div class="wrap"><div class="head"><div class="brand"><div class="logo">G</div><div><h2 style="margin:0">GALAXI V22</h2><div class="sub">AI autónoma · Binance USD-M · análisis continuo</div></div></div><div><span id="mode" class="pill">—</span> <button class="danger" onclick="stopBot()">STOP</button></div></div><div class="grid"><div class="card"><div class="k">EQUITY</div><div id="equity" class="v">—</div></div><div class="card"><div class="k">PnL REALIZADO</div><div id="real" class="v">—</div></div><div class="card"><div class="k">PnL NO REALIZADO</div><div id="unreal" class="v">—</div></div><div class="card"><div class="k">DRAWDOWN</div><div id="dd" class="v">—</div></div><div class="card wide"><div class="k">MERCADO / IA</div><div id="signal" class="v" style="font-size:17px">—</div><div id="meta" class="sub">—</div></div><div class="card wide"><div class="k">CICLOS / WS / MERCADOS</div><div id="stats" class="v" style="font-size:17px">—</div></div><div class="card full"><div class="k">POSICIONES</div><div id="positions">—</div></div><div class="card full"><div class="k">ÚLTIMA DECISIÓN IA</div><pre id="decision">—</pre></div><div class="card full"><div class="k">LOG</div><pre id="logs">—</pre></div></div></div><script>\nasync function refresh(){try{const r=await fetch(\'/api/status\',{cache:\'no-store\'});const s=await r.json();document.querySelector(\'#mode\').textContent=`${s.mode||\'—\'} · ${s.aiModel||\'IA\'}`;document.querySelector(\'#equity\').textContent=\'$\'+Number(s.equity||0).toFixed(2);document.querySelector(\'#real\').textContent=\'$\'+Number(s.realizedPnl||0).toFixed(2);document.querySelector(\'#unreal\').textContent=\'$\'+Number(s.unrealizedPnl||0).toFixed(2);document.querySelector(\'#dd\').textContent=Number(s.drawdownPct||0).toFixed(2)+\'%\';document.querySelector(\'#signal\').textContent=s.lastSignal||\'—\';document.querySelector(\'#meta\').textContent=`Régimen ${s.regime||\'—\'} · LONG ${s.longPct??\'—\'}% · SHORT ${s.shortPct??\'—\'}% · IA ${s.aiCalls||0} llamadas`;document.querySelector(\'#stats\').textContent=`ciclo ${s.cycle||0} · WS ${s.wsConnected?\'CONECTADO\':\'DESCONECTADO\'} · universo ${s.symbols||0} · analizados ${s.warmSymbols||0} · posiciones ${s.positions?.length||0}`;document.querySelector(\'#positions\').innerHTML=(s.positions||[]).map(p=>`<div class="pos"><b>${p.symbol}</b> ${p.side} · entrada ${p.entry} · actual ${p.current??p.mark??\'—\'} · PnL ${Number(p.pnl||0).toFixed(2)}</div>`).join(\'\')||\'<span class="sub">Sin posiciones</span>\';document.querySelector(\'#decision\').textContent=JSON.stringify(s.aiDecision||{},null,2);document.querySelector(\'#logs\').textContent=(s.logs||[]).slice(-60).map(x=>x.time+\' \'+x.line).join(\'\\n\');}catch(e){document.querySelector(\'#signal\').textContent=\'Dashboard esperando al motor…\'}}refresh();setInterval(refresh,2000);async function stopBot(){await fetch(\'/api/stop\',{method:\'POST\'});refresh()}</script></body></html>\n';

function dashboardHtml() {
  try {
    if (fs.existsSync(publicIndex)) return fs.readFileSync(publicIndex, 'utf8');
  } catch (e) {
    pushLog(`DASHBOARD_FILE_ERROR ${e.code || 'READ_ERROR'} ${publicIndex}`);
  }
  pushLog('DASHBOARD_FALLBACK_EMBEDDED');
  return embeddedDashboard;
}

const server = http.createServer((req,res)=>{
  const u = new URL(req.url, `http://${req.headers.host||'localhost'}`);
  if (u.pathname === '/api/health') return send(res,200,JSON.stringify({ok:true,service:'GALAXI V22',engine:!!child&&!child.killed}));
  if (u.pathname === '/api/status') return send(res,200,JSON.stringify({...readState(), logs}));
  if (u.pathname === '/api/restart' && req.method === 'POST') { try { child?.kill('SIGTERM'); } catch {} setTimeout(startEngine,500); return send(res,200,JSON.stringify({ok:true})); }
  if (u.pathname === '/api/stop' && req.method === 'POST') {
    try { fs.writeFileSync(path.join(__dirname,'galaxi-control.json'), JSON.stringify({stop:true},null,2)); } catch {}
    return send(res,200,JSON.stringify({ok:true,stop:true}));
  }
  if (u.pathname === '/') return send(res,200,dashboardHtml(),'text/html; charset=utf-8');
  return send(res,404,'Not found','text/plain; charset=utf-8');
});
server.on('clientError',(err,socket)=>{try{socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n')}catch{}});
server.listen(PORT,'0.0.0.0',()=>console.log(`GALAXI WEB listening on ${PORT}`));
process.on('SIGTERM',()=>{try{child?.kill('SIGTERM')}catch{} server.close(()=>process.exit(0));});
