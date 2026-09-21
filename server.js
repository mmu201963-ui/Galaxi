import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = Number(process.env.PORT || 3000);
const RUNTIME = path.join(__dirname, 'galaxi-runtime.json');
const CONTROL = path.join(__dirname, 'galaxi-control.json');

app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));

function readJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch { return fallback; }
}

app.get('/health', (_req, res) => res.status(200).json({ ok: true, service: 'GALAXI', time: new Date().toISOString() }));

app.get('/api/status', (_req, res) => {
  const state = readJson(RUNTIME, {
    mode: process.env.TRADING_MODE || 'PAPER',
    aiModel: process.env.OPENAI_MODEL || 'IA',
    equity: Number(process.env.PAPER_START_CAPITAL || 10000),
    realizedPnl: 0, unrealizedPnl: 0, drawdownPct: 0,
    cycle: 0, wsConnected: false, symbols: 0, warmSymbols: 0,
    positions: [], aiDecision: {}, logs: []
  });
  res.set('Cache-Control', 'no-store');
  res.json(state);
});

app.post('/api/stop', (_req, res) => {
  fs.writeFileSync(CONTROL, JSON.stringify({ stop: true, at: new Date().toISOString() }, null, 2));
  res.json({ ok: true, stopped: true });
});

// Express 5 rejects app.get('*'). Use a final middleware instead of a wildcard route.
app.use((req, res) => {
  if (req.method !== 'GET') return res.status(404).json({ error: 'Not found' });
  res.sendFile(path.join(__dirname, 'public', 'index.html'), err => {
    if (err && !res.headersSent) res.status(err.statusCode || 404).end();
  });
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`GALAXI WEB listening on ${PORT}`);
});

// Start the trading engine after the HTTP server is ready so Railway health checks
// can reach the dashboard even if the engine later has a transient upstream error.
import('./index.js').catch(err => console.error('ENGINE_IMPORT_ERROR', err));

function shutdown() {
  try { server.close(); } catch {}
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
