import http from 'node:http';
import fs from 'node:fs';
import { spawn } from 'node:child_process';

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const runtimeFile = 'galaxi-runtime.json';
const controlFile = 'galaxi-control.json';

let child = null;
let restarting = false;
const bootTime = Date.now();
const logs = [];

function pushLog(line) {
  const text = String(line).replace(/\x1b\[[0-9;]*m/g,'').trim();
  if (!text) return;
  logs.push({time:new Date().toISOString(),line:text});
  if (logs.length > 120) logs.shift();
  process.stdout.write(text + '\n');
}

function readJson(file, fallback={}) {
  try { return JSON.parse(fs.readFileSync(file,'utf8')); } catch { return fallback; }
}

function send(res,status,body,type='application/json; charset=utf-8') {
  res.writeHead(status, {'Content-Type':type,'Cache-Control':'no-store'});
  res.end(body);
}

function startEngine() {
  if (child || restarting) return;
  restarting = false;
  child = spawn(process.execPath,['index.js'],{
    cwd:process.cwd(),
    env:process.env,
    stdio:['ignore','pipe','pipe']
  });
  pushLog(`WRAPPER_ENGINE_START pid=${child.pid}`);

  child.stdout.on('data', b => String(b).split(/\r?\n/).forEach(pushLog));
  child.stderr.on('data', b => String(b).split(/\r?\n/).forEach(x => x.trim() && pushLog('ERROR '+x)));

  child.on('error', err => {
    pushLog(`ENGINE_ERROR ${err.message}`);
  });

  child.on('exit',(code,signal)=>{
    pushLog(`ENGINE_EXIT code=${code ?? 'null'} signal=${signal ?? 'null'}`);
    child=null;
    const control=readJson(controlFile,{});
    if (!control.stop && !restarting) {
      restarting=true;
      setTimeout(()=>{restarting=false;startEngine();},5000);
    }
  });
}

const html = fs.readFileSync('index.html','utf8');

const server=http.createServer((req,res)=>{
  try {
    const u=new URL(req.url,`http://${req.headers.host||'localhost'}`);

    if (u.pathname==='/' || u.pathname==='/index.html') {
      return send(res,200,html,'text/html; charset=utf-8');
    }

    if (u.pathname==='/api/status') {
      const state=readJson(runtimeFile,{});
      return send(res,200,JSON.stringify({
        ...state,
        wrapper:{engineAlive:Boolean(child),uptimeSec:Math.floor((Date.now()-bootTime)/1000)},
        logs:logs.slice(-60)
      }));
    }

    if (u.pathname==='/api/health') {
      return send(res,200,JSON.stringify({
        ok:true,
        engineAlive:Boolean(child),
        uptimeSec:Math.floor((Date.now()-bootTime)/1000)
      }));
    }

    if (u.pathname==='/api/stop' && req.method==='POST') {
      fs.writeFileSync(controlFile,JSON.stringify({stop:true,updatedAt:new Date().toISOString()},null,2));
      try { child?.kill('SIGTERM'); } catch {}
      return send(res,200,JSON.stringify({ok:true,stopped:true}));
    }

    if (u.pathname==='/api/start' && req.method==='POST') {
      fs.writeFileSync(controlFile,JSON.stringify({stop:false,updatedAt:new Date().toISOString()},null,2));
      if (!child) startEngine();
      return send(res,200,JSON.stringify({ok:true,starting:true}));
    }

    return send(res,404,JSON.stringify({error:'Not found'}));
  } catch (e) {
    return send(res,500,JSON.stringify({error:e.message}));
  }
});

server.on('clientError',(err,socket)=>{
  try { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); } catch {}
});

server.listen(PORT,HOST,()=>{
  pushLog(`GALAXI WEB | listening ${HOST}:${PORT}`);
  startEngine();
});

function shutdown(){
  try { child?.kill('SIGTERM'); } catch {}
  try { server.close(); } catch {}
  setTimeout(()=>process.exit(0),500);
}
process.on('SIGTERM',shutdown);
process.on('SIGINT',shutdown);
