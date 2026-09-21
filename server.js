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

function dashboardHtml() {
  try {
    return fs.readFileSync(publicIndex, 'utf8');
  } catch (e) {
    pushLog(`DASHBOARD_FILE_ERROR ${e.code || 'READ_ERROR'} ${publicIndex}`);
    return '<!doctype html><html lang=\"es\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>GALAXI</title></head><body style=\"font-family:system-ui;padding:30px\"><h1>GALAXI V22</h1><p>El motor está iniciado, pero falta public/index.html en el deployment.</p></body></html>';
  }
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
server.on('clientError',(err,socket)=>{try{socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')}catch{}});
server.listen(PORT,'0.0.0.0',()=>console.log(`GALAXI WEB listening on ${PORT}`));
process.on('SIGTERM',()=>{try{child?.kill('SIGTERM')}catch{} server.close(()=>process.exit(0));});
