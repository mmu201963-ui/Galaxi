import fs from 'node:fs';
import http from 'node:http';
import { spawn } from 'node:child_process';

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const RUNTIME = 'galaxi-runtime.json';

function readState() {
  try { return JSON.parse(fs.readFileSync(RUNTIME, 'utf8')); }
  catch { return { running: false, mode: 'UNKNOWN', lastError: 'Esperando al motor…' }; }
}

const child = spawn(process.execPath, ['index.js'], {
  cwd: process.cwd(), env: process.env, stdio: ['ignore', 'pipe', 'pipe']
});
child.stdout.on('data', d => process.stdout.write(d));
child.stderr.on('data', d => process.stderr.write(d));
child.on('exit', (code, signal) => console.log(`GALAXI ENGINE EXIT code=${code ?? 'null'} signal=${signal ?? 'null'}`));

const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

const HTML = `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GALAXI V24 AI Diagnostic</title>
<style>
body{margin:0;background:#080d16;color:#e9eef8;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:900px;margin:auto;padding:28px 18px 70px}.brand{font-size:34px;font-weight:800}.sub{color:#94a3b8;margin:5px 0 24px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.card{background:#101826;border:1px solid #243044;border-radius:20px;padding:18px}.label{color:#8c9ab0;font-size:13px;text-transform:uppercase;letter-spacing:.08em}.value{font-size:26px;font-weight:750;margin-top:7px}.ok{color:#65e6a2}.bad{color:#ff8b8b}.muted{color:#9aa8bb}.wide{grid-column:1/-1}.error{white-space:pre-wrap;word-break:break-word;font-size:13px;line-height:1.45;color:#ffaaa9}.small{font-size:13px;line-height:1.5;color:#aeb9ca}.pos{padding:9px 0;border-bottom:1px solid #202b3c}.pill{display:inline-block;padding:6px 10px;border-radius:999px;background:#192338}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
</style></head><body><div class="wrap"><div class="brand">GALAXI V24</div><div class="sub">AI autónoma · Binance USD-M · MULTIUNIVERSE · diagnóstico IA</div><div class="grid">
<div class="card"><div class="label">Modo</div><div id="mode" class="value">—</div></div>
<div class="card"><div class="label">Motor</div><div id="running" class="value">—</div></div>
<div class="card"><div class="label">Equity</div><div id="equity" class="value">$0.00</div></div>
<div class="card"><div class="label">PnL realizado</div><div id="realized" class="value">$0.00</div></div>
<div class="card"><div class="label">Mercados</div><div id="markets" class="value">0</div></div>
<div class="card"><div class="label">Ciclo</div><div id="cycle" class="value">0</div></div>
<div class="card"><div class="label">WebSocket</div><div id="ws" class="value">—</div></div>
<div class="card"><div class="label">Posiciones</div><div id="positionsCount" class="value">0</div></div>
<div class="card wide"><div class="label">Cerebro IA</div><div id="aiStatus" class="value">—</div><div class="small" id="aiMeta">—</div></div>
<div class="card wide"><div class="label">Último error IA</div><div id="aiError" class="error">Ninguno</div></div>
<div class="card wide"><div class="label">Último estado</div><div id="last" class="small">—</div></div>
<div class="card wide"><div class="label">Posiciones</div><div id="positions" class="small">Sin posiciones.</div></div>
<div class="card wide"><div class="label">Última decisión</div><div id="decision" class="small mono">—</div></div>
</div></div><script>
const $=id=>document.getElementById(id); const money=n=>'$'+Number(n||0).toFixed(2);
async function refresh(){try{const r=await fetch('/api/status?x='+Date.now(),{cache:'no-store'});const d=await r.json();
$('mode').textContent=d.mode||'—'; $('running').textContent=d.running?'ACTIVO':'DETENIDO'; $('running').className='value '+(d.running?'ok':'bad');
$('equity').textContent=money(d.equity); $('realized').textContent=money(d.realizedPnl); $('markets').textContent=(d.symbols||0)+' / analizados '+(d.candidates||0); $('cycle').textContent=d.cycle||0; $('ws').textContent=d.wsConnected?'CONECTADO':'DESCONECTADO'; $('ws').className='value '+(d.wsConnected?'ok':'bad'); $('positionsCount').textContent=d.positions?.length||0;
$('aiStatus').textContent=d.aiStatus||'—'; $('aiStatus').className='value '+(d.aiReady?'ok':((d.aiErrors||0)>0?'bad':''));
$('aiMeta').textContent='modelo='+ (d.aiModel||'—') +' · llamadas exitosas='+ (d.aiSuccess||0) +' · intentos='+ (d.aiAttempts||0) +' · errores='+ (d.aiErrors||0) +' · latencia='+ (d.aiLatencyMs||0)+' ms';
$('aiError').textContent=d.aiLastError||'Ninguno'; $('last').textContent=(d.lastSignal||'—')+' · '+(d.lastUpdate||'');
$('positions').innerHTML=(d.positions||[]).map(p=>'<div class="pos"><b>'+p.symbol+'</b> · '+p.side+' · PnL '+money(p.pnl)+' · entrada '+Number(p.entry||0)+'</div>').join('')||'Sin posiciones.';
$('decision').textContent=d.aiDecision?JSON.stringify(d.aiDecision,null,2):'Sin decisión todavía.';
}catch(e){$('running').textContent='SIN CONEXIÓN';$('running').className='value bad';}} refresh(); setInterval(refresh,2000);
</script></body></html>`;

const server = http.createServer((req,res)=>{
  const u = new URL(req.url, `http://${req.headers.host||'localhost'}`);
  const send=(code,body,type='application/json; charset=utf-8')=>{res.writeHead(code,{'Content-Type':type,'Cache-Control':'no-store'});res.end(body)};
  if(u.pathname==='/api/status') return send(200,JSON.stringify(readState()));
  if(u.pathname==='/api/health') return send(200,JSON.stringify({ok:true,engineAlive:!child.killed,time:new Date().toISOString()}));
  if(u.pathname==='/'||u.pathname==='/index.html') return send(200,HTML,'text/html; charset=utf-8');
  return send(404,JSON.stringify({error:'Not found'}));
});
server.listen(PORT,HOST,()=>console.log(`GALAXI WEB V24 | ${HOST}:${PORT}`));
function stop(){try{child.kill('SIGTERM')}catch{} try{server.close()}catch{} setTimeout(()=>process.exit(0),500)}
process.on('SIGTERM',stop); process.on('SIGINT',stop);
