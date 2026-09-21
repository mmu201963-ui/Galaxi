import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const port = Number(process.env.PORT || 3000);
app.use(express.json());
app.use(express.static(__dirname));

function readState(){
  try { return JSON.parse(fs.readFileSync(path.join(__dirname,'galaxi-runtime.json'),'utf8')); }
  catch { return {mode:process.env.TRADING_MODE||'PAPER',aiModel:process.env.OPENAI_MODEL||'—',equity:Number(process.env.PAPER_START_CAPITAL||10000),positions:[],logs:[]}; }
}
app.get('/api/status',(req,res)=>{
  const s=readState();
  res.set('Cache-Control','no-store');
  res.json({...s,logs:(s.history||[]).map(x=>({time:x.time,line:x.line})),warmSymbols:s.warmSymbols||0});
});
app.post('/api/stop',(req,res)=>{
  fs.writeFileSync(path.join(__dirname,'galaxi-control.json'),JSON.stringify({stop:true,at:new Date().toISOString()},null,2));
  res.json({ok:true,stop:true});
});
app.get('*',(req,res)=>res.sendFile(path.join(__dirname,'index.html')));
app.listen(port,'0.0.0.0',()=>console.log(`GALAXI WEB listening on ${port}`));
