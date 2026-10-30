// Streams fed by WebSockets: each worker opens its own connection. Messages
// hold one or more whole lines, with or without a trailing newline.
import { createServer } from 'node:http';
import { SplitEngine } from '../src/node.mjs';
import { acceptWebSocket } from '../examples/stream/ws-server.mjs';

if (typeof WebSocket !== 'function') { console.log('WebSocket checks skipped: this Node has no WebSocket (Node 22+ does)'); process.exit(0); }

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const close = (a, b) => JSON.stringify(a, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x)) === JSON.stringify(b, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x));

let seed = 5;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC'];
const rows = Array.from({ length: 20_000 }, (_, i) => ({ ts: i, price: Math.round(rnd() * 1e5) / 100, region: REG[i % 3] }));

// every connection gets every row: single objects without a newline, or batches with one
const server = createServer();
server.on('upgrade', (req, socket) => {
  const ws = acceptWebSocket(req, socket);
  const format = new URL(req.url, 'http://x').searchParams.get('format');
  const line = (r) => (format === 'csv' ? `${r.ts},${r.price},${r.region}` : JSON.stringify(r));
  let i = 0;
  while (i < rows.length) {
    if (rnd() < 0.5) ws.send(line(rows[i++]));
    else { const k = 1 + Math.floor(rnd() * 300); ws.send(rows.slice(i, i + k).map(line).join('\n') + '\n'); i += k; }
  }
  ws.close();
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const url = `ws://127.0.0.1:${server.address().port}/feed`;

const want = (copies) => ({
  n: rows.length * copies,
  rev: rows.reduce((a, r) => a + r.price, 0) * copies,
  groups: REG.slice().sort().map((g) => ({ region: g, n: rows.filter((r) => r.region === g).length * copies })),
});
for (const format of ['ndjson', 'csv']) {
  for (const workers of [1, 3]) {
    const engine = new SplitEngine({ workers });
    const s = await engine.streamAsync({ format, columns: { ts: 'number', price: 'number', region: 'category' } });
    await Promise.all(engine.workers.map((_, w) => s.connect(w, { url: `${url}?format=${format}` })));
    let st;
    for (let k = 0; k < 200; k++) { st = await s.flushAsync(); if (st.rowsIn >= rows.length * workers) break; await new Promise((r) => setTimeout(r, 25)); }
    const got = { n: await s.countAsync(), rev: await s.sumAsync('price'), groups: (await s.groupBy('region').aggAsync({ n: 'count()' })).toRows() };
    checks++;
    if (!close(got, want(workers)) || st.badLines) fail(`${format} w=${workers}: ${JSON.stringify(got)} vs ${JSON.stringify(want(workers))}, bad lines ${st.badLines}`);
    await engine.close();
  }
}
// a WebSocket that can't connect rejects
{
  const engine = new SplitEngine({ workers: 1 });
  const s = await engine.streamAsync({ columns: { a: 'number' } });
  let msg = '';
  try { await s.connect(0, { url: 'ws://127.0.0.1:1/nothing' }); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('WebSocket')) fail(`dead WebSocket did not reject: ${msg}`);
  await engine.close();
}
server.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} WebSocket checks passed`);
process.exit(failures ? 1 : 0);
