import fs from 'node:fs';
import http from 'node:http';
import express from 'express';
import cors from 'cors';
import { state, cfg, start, stop } from './index.js';

const app = express();
app.use(cors());
app.use(express.json());
app.get('/health', (_req,res)=>res.json({ok:true,service:'GALAXI',mode:cfg.mode,running:state.running,cycle:state.cycle,wsConnected:state.wsConnected,lastError:state.lastError}));
app.get('/api/state', (_req,res)=>res.json(state));
app.post('/api/stop', (_req,res)=>{fs.writeFileSync('galaxi-control.json',JSON.stringify({stop:true,at:new Date().toISOString()},null,2));stop();res.json({ok:true,stopped:true});});
const publicDir = new URL('./public/', import.meta.url).pathname;
app.use(express.static(publicDir));
app.get('/', (_req,res)=>res.sendFile(new URL('./public/index.html', import.meta.url).pathname));
app.get('*', (_req,res)=>res.sendFile(new URL('./public/index.html', import.meta.url).pathname));

const port=Number(process.env.PORT||3000);
const server=http.createServer(app);
server.on('clientError',(err,socket)=>{if(err.code==='HPE_INVALID_METHOD'||err.code==='HPE_INVALID_URL'){try{socket.destroy();}catch{}}else{try{socket.destroy();}catch{}}});
server.listen(port,'0.0.0.0',()=>console.log(`GALAXI WEB listening on ${port}`));
start().catch(err=>console.error('TRADER_START_ERROR',err.message));
