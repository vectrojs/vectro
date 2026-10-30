// Emitter -> collector over binary frames: every answer equals plain
// JavaScript, labels arriving over time on different connections line up,
// mismatched schemas are reported, and a collector that stops reading makes
// the emitter wait (backpressure).
import { connect } from 'node:net';
import { createServer } from 'node:http';
import { Emitter, SplitEngine, BatchEncoder, encodeSchema, encodeEnd } from '../src/node.mjs';
import { acceptWebSocket } from '../examples/stream/ws-server.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const close = (a, b) => JSON.stringify(a, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x)) === JSON.stringify(b, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x));

let seed = 23;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const COLUMNS = { ts: 'number', price: 'number', qty: 'number', region: 'category' };
const waitRows = async (s, n) => { let st; for (let k = 0; k < 300; k++) { st = await s.flushAsync(); if (st.rowsIn >= n || st.errors.length) break; await new Promise((r) => setTimeout(r, 15)); } return st; };

for (const workers of [1, 3]) {
  const feed = await Emitter.listen({ columns: COLUMNS });
  const engine = new SplitEngine({ workers });
  const s = await engine.streamAsync({ format: 'vectro', columns: COLUMNS });
  const live = await s.windowAsync({ by: 'region', every: 1000, last: 100_000, timeColumn: 'ts', agg: { n: 'count()', rev: 'sum(price * qty)' } });
  await Promise.all(engine.workers.map((_, w) => s.connect(w, { port: feed.port })));
  await feed.waitForConnections(workers);
  // batches of many sizes; new region labels keep appearing
  const all = [];
  for (let b = 0; b < 40; b++) {
    const rows = [0, 1, 7, 1000, 30_000, 100_000][Math.floor(rnd() * 6)];
    const batch = { ts: new Float64Array(rows), price: new Float64Array(rows), qty: [], region: [] };
    for (let i = 0; i < rows; i++) {
      const r = { ts: all.length, price: Math.round(rnd() * 1e5) / 100, qty: 1 + Math.floor(rnd() * 9), region: `R${Math.floor(rnd() * (3 + b))}` };
      all.push(r);
      batch.ts[i] = r.ts; batch.price[i] = r.price; batch.qty.push(r.qty); batch.region.push(r.region);
    }
    await feed.write(batch);
  }
  const st = await waitRows(s, all.length);
  checks++; if (st.rowsIn !== all.length || st.errors.length) fail(`w=${workers}: ${st.rowsIn} of ${all.length} rows, errors ${st.errors}`);
  const sum = (rows) => rows.reduce((a, r) => a + r.price * r.qty, 0);
  const groups = (rows) => [...rows.reduce((m, r) => m.set(r.region, (m.get(r.region) ?? 0) + 1), new Map())].sort((a, b) => (a[0] < b[0] ? -1 : 1)).map(([region, n]) => ({ region, n }));
  const L = `w=${workers}`;
  let got = await s.aggAsync({ n: 'count()', rev: 'sum(price * qty)' });
  checks++; if (!close(got, { n: all.length, rev: sum(all) })) fail(`${L} totals ${JSON.stringify(got)}`);
  got = (await s.groupBy('region').aggAsync({ n: 'count()' })).toRows();
  checks++; if (!close(got, groups(all))) fail(`${L} groups`);
  got = await s.filter("startsWith(region, 'R1') && qty >= 5").countAsync();
  checks++; if (got !== all.filter((r) => r.region.startsWith('R1') && r.qty >= 5).length) fail(`${L} text filter ${got}`);
  const recent = all.filter((r) => r.ts >= all.length - 20_000);
  got = await s.filter('ts >= t', { t: all.length - 20_000 }).aggAsync({ n: 'count()', rev: 'sum(price * qty)' });
  checks++; if (!close(got, { n: recent.length, rev: sum(recent) })) fail(`${L} recent rows (zone maps)`);
  const newest = Math.floor((all.length - 1) / 1000), inWindow = all.filter((r) => Math.floor(r.ts / 1000) > newest - 100);
  got = await live.readAsync();
  const wantWin = [...inWindow.reduce((m, r) => { const g = m.get(r.region) ?? { region: r.region, n: 0, rev: 0 }; g.n++; g.rev += r.price * r.qty; return m.set(r.region, g); }, new Map()).values()].sort((a, b) => (a.region < b.region ? -1 : 1));
  checks++; if (!close(got, wantWin)) fail(`${L} window ${JSON.stringify(got).slice(0, 200)}`);
  // push() is for text formats
  let msg = '';
  try { await s.push(new Uint8Array(8)); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('connect each worker')) fail(`${L} push accepted: ${msg}`);
  await feed.close();
  await engine.close();
}

// a stream expecting a column the emitter doesn't send reports it
{
  const feed = await Emitter.listen({ columns: { ts: 'number' } });
  const engine = new SplitEngine({ workers: 1 });
  const s = await engine.streamAsync({ format: 'vectro', columns: { ts: 'number', price: 'number' } });
  await s.connect(0, { port: feed.port });
  await feed.waitForConnections(1);
  await feed.write({ ts: [1, 2, 3] });
  const st = await waitRows(s, 3);
  checks++; if (!st.errors.some((e) => e.includes('doesn\'t send column "price"'))) fail(`schema mismatch: ${JSON.stringify(st.errors)}`);
  await feed.close(); await engine.close();
}

// backpressure: a client that stops reading makes write() wait; reading again lets it finish
{
  const feed = await Emitter.listen({ columns: { a: 'number' } });
  const sock = connect(feed.port, '127.0.0.1');
  await feed.waitForConnections(1);
  sock.pause();
  const big = { a: new Float64Array(65_536) };
  let done = false;
  const writing = (async () => { for (let i = 0; i < 60; i++) await feed.write(big); done = true; })(); // ~31 MB
  await new Promise((r) => setTimeout(r, 400));
  checks++; if (done) fail('write() finished although nothing was being read');
  sock.resume();
  await writing;
  checks++; if (!done) fail('write() never finished after reading resumed');
  sock.destroy();
  await feed.close();
}

// WebSocket: binary frames, one or more per message (Node 22+ has WebSocket)
if (typeof WebSocket === 'function') {
  const server = createServer();
  server.on('upgrade', (req, socket) => {
    const ws = acceptWebSocket(req, socket);
    const enc = new BatchEncoder({ x: 'number', tag: 'category' });
    ws.sendBinary(encodeSchema({ x: 'number', tag: 'category' }));
    for (let b = 0; b < 10; b++) ws.sendBinary(enc.encode({ x: Array.from({ length: 1000 }, (_, i) => i), tag: Array.from({ length: 1000 }, (_, i) => `t${(i + b) % 5}`) }));
    ws.sendBinary(encodeEnd());
    ws.close();
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const engine = new SplitEngine({ workers: 2 });
  const s = await engine.streamAsync({ format: 'vectro', columns: { x: 'number', tag: 'category' } });
  await Promise.all(engine.workers.map((_, w) => s.connect(w, { url: `ws://127.0.0.1:${server.address().port}/` })));
  const st = await waitRows(s, 20_000);
  const got = await s.aggAsync({ n: 'count()', sum: 'sum(x)' });
  const tags = (await s.groupBy('tag').aggAsync({ n: 'count()' })).toRows();
  checks++; if (got.n !== 20_000 || got.sum !== 2 * 10 * 499_500 || tags.length !== 5 || tags.some((t) => t.n !== 4000) || st.errors.length) fail(`websocket: ${JSON.stringify(got)} ${JSON.stringify(tags)} ${st.errors}`);
  await engine.close();
  server.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} emitter checks passed`);
process.exit(failures ? 1 : 0);
