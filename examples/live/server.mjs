// Live sync demo: a shop's orders change all the time (prices, new orders,
// cancelled ones); the page watches two queries and its rows move as the
// answers change.
//   node examples/live/server.mjs        then open http://127.0.0.1:8796
// Port 8795: the Vectro server (/live WebSocket, /sql); 8796: this page.
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { serve, liveTable } from '../../src/serve/index.mjs';

const LIVE_PORT = Number(process.env.LIVE_PORT ?? 8795), PAGE_PORT = Number(process.env.PAGE_PORT ?? 8796);
const regions = ['Europe', 'North America', 'Asia', 'South America', 'Africa', 'Oceania'];
const products = ['Desk lamp', 'Office chair', 'Notebook', 'Headphones', 'Keyboard', 'Monitor', 'Backpack', 'Water bottle', 'Coffee mug', 'Phone case', 'Charger', 'Mouse', 'Webcam', 'Speaker', 'Plant pot', 'Sticky notes', 'Pen set', 'Desk mat', 'USB hub', 'Cable kit'];
const pick = (a) => a[Math.floor(Math.random() * a.length)];
const orderOf = (id) => ({ id, region: pick(regions), product: pick(products), price: Math.round(5 + Math.random() * 95), qty: 1 + Math.floor(Math.random() * 4), ts: Date.now() });

const orders = liveTable({ key: 'id', columns: { id: 'number', region: 'text', product: 'text', price: 'number', qty: 'number', ts: 'date' } });
let nextId = 1;
orders.insert(Array.from({ length: 5000 }, () => orderOf(nextId++)));

const s = await serve({ live: { orders }, port: LIVE_PORT });

// the changes: every `every` ms a few of them (a burst from the page: many at once)
let every = 200, paused = false;
function change(n) {
  for (let i = 0; i < n; i++) {
    const r = Math.random();
    if (r < 0.4) orders.insert([orderOf(nextId++)]);
    else if (r < 0.6 && orders.length > 1000) orders.delete([[...orders.slots.keys()][Math.floor(Math.random() * orders.length)]]);
    else orders.upsert([{ id: [...orders.slots.keys()][Math.floor(Math.random() * orders.length)], price: Math.round(5 + Math.random() * 95), qty: 1 + Math.floor(Math.random() * 4) }]);
  }
}
const tick = () => { if (!paused) change(1 + Math.floor(Math.random() * 3)); setTimeout(tick, every); };
tick();

const files = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/live/client.mjs': ['../../src/live/client.mjs', 'text/javascript'],
  '/live/diff.mjs': ['../../src/live/diff.mjs', 'text/javascript'],
};
const page = createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'POST' && url.pathname === '/control') {
    const what = url.searchParams.get('do');
    if (what === 'pause') paused = !paused;
    else if (what === 'burst') change(200);
    else if (what === 'speed') every = Math.max(10, Math.min(2000, Number(url.searchParams.get('ms')) || 200));
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ paused, every, rows: orders.length }));
  }
  const f = files[url.pathname];
  if (!f) { res.writeHead(404); return res.end('not found'); }
  res.writeHead(200, { 'content-type': f[1], 'cache-control': 'no-store' });
  res.end(readFileSync(new URL(f[0], import.meta.url)));
});
page.listen(PAGE_PORT, '127.0.0.1', () => console.log(`live demo: http://127.0.0.1:${PAGE_PORT}  (Vectro server ${s.url}, WebSocket ${s.url.replace('http', 'ws')}/live)`));

const stop = async () => { page.close(); await s.close(); process.exit(0); };
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
