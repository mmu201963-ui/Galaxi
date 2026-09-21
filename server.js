import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Start the trading engine in the same Railway process.
import './index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const port = Number(process.env.PORT || 3000);
const runtimeFile = path.join(__dirname, 'galaxi-runtime.json');
const controlFile = path.join(__dirname, 'galaxi-control.json');

app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));
app.use(express.static(__dirname, { index: 'index.html', extensions: ['html'] }));

function readState() {
  try {
    return JSON.parse(fs.readFileSync(runtimeFile, 'utf8'));
  } catch {
    return {
      mode: process.env.TRADING_MODE || 'PAPER',
      aiModel: process.env.OPENAI_MODEL || 'NO_CONFIGURADO',
      equity: Number(process.env.PAPER_START_CAPITAL || 10000),
      positions: [],
      history: []
    };
  }
}

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'GALAXI V22', time: new Date().toISOString() });
});

app.get('/api/status', (_req, res) => {
  const s = readState();
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.json({
    ...s,
    logs: Array.isArray(s.history) ? s.history.map(x => ({ time: x.time, line: x.line })) : [],
    warmSymbols: s.warmSymbols || 0
  });
});

app.post('/api/stop', (_req, res) => {
  fs.writeFileSync(controlFile, JSON.stringify({ stop: true, at: new Date().toISOString() }, null, 2));
  res.json({ ok: true, stop: true });
});

// Express 5 does not accept app.get('*') here; this fallback is deliberately
// registered as middleware so SPA/dashboard routes cannot crash the process.
app.use((req, res, next) => {
  if (req.method !== 'GET' || req.path.startsWith('/api/') || req.path === '/health') return next();
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.use((err, _req, res, _next) => {
  console.error('HTTP_ERROR', err);
  if (res.headersSent) return;
  res.status(500).json({ ok: false, error: 'Internal server error' });
});

const server = app.listen(port, '0.0.0.0', () => {
  console.log(`GALAXI WEB listening on ${port}`);
});

server.on('error', err => {
  console.error('SERVER_ERROR', err);
  process.exit(1);
});

process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
});
