// Streams: CSV lines pushed in chunks cut anywhere, or read by each worker
// from its own TCP connection. Every answer must equal a normal table built
// from the same rows (or from the rows a stream is supposed to keep).
import { Worker } from 'node:worker_threads';
import { createServer } from 'node:net';
import { Engine } from '../src/node.mjs';
import { SplitEngine } from '../src/split.mjs';

let failures = 0, checks = 0;
const norm = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isInteger(x) ? Number(x.toPrecision(11)) : x));
const expect = (label, got, want) => { checks++; if (norm(got) !== norm(want)) { failures++; if (failures <= 15) console.log('FAIL', label, norm(got).slice(0, 300), 'vs', norm(want).slice(0, 300)); } };
const spawn = () => {
  const w = new Worker(new URL('../src/split-worker-node.mjs', import.meta.url));
  w.unref();
  return { post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate() };
};

let seed = 21;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const COLUMNS = { ts: 'number', price: 'number', qty: 'number', region: 'category' };
function rows(n, t0 = 0) {
  const out = [];
  for (let i = 0; i < n; i++) out.push({ ts: t0 + i, price: Math.round(rnd() * 1e5) / 100, qty: 1 + Math.floor(rnd() * 9), region: REG[Math.floor(rnd() * (i < n / 2 ? 2 : 5))] });
  return out;
}
const csv = (rs) => rs.map((r) => `${r.ts},${r.price},${r.qty},${r.region}`).join('\n') + '\n';
// cut text into pieces of random length (often mid-line, sometimes empty)
function pieces(text) {
  const out = [];
  for (let i = 0; i < text.length; ) { const k = Math.floor(rnd() * 7000); out.push(text.slice(i, i + k)); i += k; }
  return out;
}

const ref = new Engine();
const columnsOf = (rs) => ({ ts: rs.map((r) => r.ts), price: rs.map((r) => r.price), qty: rs.map((r) => r.qty), region: rs.map((r) => r.region) });
const QUERIES = [
  ['count', (t) => t.countAsync()],
  ['agg', (t) => t.aggAsync({ n: 'count()', s: 'sum(price * qty)', a: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' })],
  ['filtered agg', (t) => t.filter("region == 'EU' && price > m", { m: 300 }).aggAsync({ n: 'count()', s: 'sum(price)' })],
  ['label only some workers saw', (t) => t.filter("region == 'MEA'").aggAsync({ n: 'count()', s: 'sum(qty)' })],
  ['groupBy', async (t) => (await t.groupBy('region').aggAsync({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)' })).toRows()],
  ['filtered groupBy', async (t) => (await t.filter('qty >= 5').groupBy('region').aggAsync({ n: 'count()' })).toRows()],
];
// the reference table orders groups by first appearance; streams sort labels A to Z
const sortRows = (r) => (Array.isArray(r) && r[0]?.region ? [...r].sort((a, b) => (a.region < b.region ? -1 : 1)) : r);
async function compare(label, stream, keptRows) {
  const t = ref.table(columnsOf(keptRows));
  for (const [q, f] of QUERIES) expect(`${label} ${q}`, sortRows(await f(stream)), sortRows(await f(t)));
  t.dispose();
}

for (const workers of [1, 3, 8]) {
  const split = new SplitEngine({ workers, spawn });

  // pushed from this thread, chunks cut anywhere
  {
    const rs = rows(60_000);
    const s = await split.streamAsync({ columns: COLUMNS });
    for (const p of pieces(csv(rs))) await s.push(p);
    const st = await s.flushAsync();
    expect(`w=${workers} push: all rows arrived`, [st.rows, st.rowsIn, st.unparsedBytes, st.errors], [rs.length, rs.length, 0, []]);
    await compare(`w=${workers} push`, s, rs);
    // snapshot: a fixed copy; derive works on it, not on the live stream
    const snap = await s.snapshotAsync();
    expect(`w=${workers} snapshot rows`, await snap.countAsync(), rs.length);
    const d = await snap.deriveAsync('rev', 'price * qty');
    expect(`w=${workers} derive on snapshot`, await d.sumAsync('rev'), ref.table(columnsOf(rs)).sum('price * qty'));
    let threw = '';
    try { await s.deriveAsync('rev', 'price * qty'); } catch (err) { threw = err.message; }
    expect(`w=${workers} derive on live stream refused`, threw.includes('snapshotAsync'), true);
    // a filtered collect is a fixed table too
    const eu = await s.filter("region == 'EU'").collectAsync();
    expect(`w=${workers} filtered collect`, await eu.countAsync(), rs.filter((r) => r.region === 'EU').length);
    // more rows after a query are seen by the next query
    const more = rows(5_000, 60_000);
    await s.push(csv(more));
    await s.flushAsync();
    expect(`w=${workers} rows added later`, await s.countAsync(), rs.length + more.length);
    for (const t of [snap, d, eu, s]) await t.dispose();
  }

  // keepRows: the newest rows of the whole stream (push() deals whole chunks
  // round-robin, but keepRows counts rows over all workers)
  {
    const s = await split.streamAsync({ columns: COLUMNS, keepRows: 9_000 });
    const rs = rows(50_000);
    const chunks = [];
    for (let i = 0; i < rs.length; i += 1000) chunks.push(rs.slice(i, i + 1000));
    for (const c of chunks) await s.push(csv(c));
    await s.flushAsync();
    await compare(`w=${workers} keepRows`, s, rs.slice(-9_000));
    await s.dispose();
  }

  // keepMs: rows newer than (the stream's newest time - keepMs)
  {
    const s = await split.streamAsync({ columns: COLUMNS, keepMs: 2_000, timeColumn: 'ts' });
    const rs = rows(30_000);
    const chunks = [];
    for (let i = 0; i < rs.length; i += 700) chunks.push(rs.slice(i, i + 700));
    for (const c of chunks) await s.push(csv(c));
    await s.flushAsync();
    const newest = Math.max(...rs.map((r) => r.ts));
    const kept = rs.filter((r) => r.ts >= newest - 2_000);
    await compare(`w=${workers} keepMs`, s, kept);
    await s.dispose();
  }

  // each worker reads its own TCP connection
  {
    const parts = Array.from({ length: workers }, () => rows(20_000));
    const servers = await Promise.all(parts.map((rs) => new Promise((resolve) => {
      const server = createServer((socket) => {
        // write in odd-sized pieces, then end
        const text = csv(rs);
        let i = 0;
        (function more() {
          while (i < text.length) {
            const k = 1 + Math.floor(rnd() * 50_000);
            const ok = socket.write(text.slice(i, i + k));
            i += k;
            if (!ok) return socket.once('drain', more);
          }
          socket.end();
        })();
      });
      server.listen(0, '127.0.0.1', () => resolve(server));
    })));
    const s = await split.streamAsync({ columns: COLUMNS });
    await Promise.all(servers.map((server, w) => s.connect(w, { port: server.address().port })));
    const total = parts.reduce((a, p) => a + p.length, 0);
    let st;
    for (let k = 0; k < 200; k++) { st = await s.flushAsync(); if (st.rowsIn === total) break; await new Promise((r) => setTimeout(r, 25)); }
    expect(`w=${workers} tcp: all rows arrived`, [st.rowsIn, st.errors], [total, []]);
    await compare(`w=${workers} tcp`, s, parts.flat());
    await s.dispose();
    for (const server of servers) server.close();
  }

  // bad column kinds and keepMs without a time column are refused
  {
    let msg = '';
    try { await split.streamAsync({ columns: { a: 'text' } }); } catch (err) { msg = err.message; }
    expect(`w=${workers} bad kind refused`, msg.includes("'number', 'category' or 'date'"), true);
    msg = '';
    try { await split.streamAsync({ columns: { a: 'number' }, keepMs: 10 }); } catch (err) { msg = err.message; }
    expect(`w=${workers} keepMs needs timeColumn`, msg.includes('timeColumn'), true);
  }
  await split.close();
}
await ref.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} stream checks passed`);
process.exit(failures ? 1 : 0);
