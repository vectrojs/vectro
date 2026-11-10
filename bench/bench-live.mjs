// Live sync: how long from a change in the data to the page having the new
// answer? Server (serve({ live })) and a WebSocket client (connectLive) in one
// process, so both use one clock. For each table size and change size the
// client watches a GROUP BY over 1,000 products (every change alters some
// group) and a top-100 filter; we change rows, wait for the patch to be
// applied, then pause so each sample starts from a quiet server.
//   change → applied: from orders.upsert() to the client's rows updated
//   build: copying the changed live table into an engine table
//   query + diff: running the SQL and comparing it with the last answer
// Usage: node bench/bench-live.mjs   (SIZES=10000,100000,1000000 CHANGES=1,100 SAMPLES=200)
import { Engine } from '../src/node.mjs';
import { serve, liveTable } from '../src/serve/index.mjs';
import { connectLive } from '../src/live/client.mjs';
import { execSync } from 'node:child_process';

const SIZES = (process.env.SIZES ?? '10000,100000,1000000').split(',').map(Number);
const CHANGES = (process.env.CHANGES ?? '1,100').split(',').map(Number);
const SAMPLES = Number(process.env.SAMPLES ?? 200);
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const r2 = (x) => Math.round(x * 100) / 100;
let seed = 1; // (mulberry32: a 32-bit generator that stays exact in doubles)
const rand = () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };

const regions = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const GROUP = 'SELECT product, SUM(price * qty) AS revenue, COUNT(*) AS n FROM orders GROUP BY product ORDER BY revenue DESC';
const TOP = 'SELECT id, product, price FROM orders WHERE region = \'EU\' ORDER BY price DESC, id LIMIT 100';

const load = () => { try { return Number(execSync('sysctl -n vm.loadavg').toString().trim().replace(/[{}]/g, '').trim().split(/\s+/)[0]); } catch { return null; } };
const results = [];
console.log(`load before: ${load()}`);
for (const size of SIZES) {
  const engine = new Engine();
  const orders = liveTable({ key: 'id', columns: { id: 'number', region: 'text', product: 'text', price: 'number', qty: 'number' } });
  const rowOf = (id) => ({ id, region: regions[Math.floor(rand() * 8)], product: `p${Math.floor(rand() * 1000)}`, price: Math.round(1 + rand() * 999), qty: 1 + Math.floor(rand() * 5) });
  for (let i = 0; i < size; i += 100_000) orders.insert(Array.from({ length: Math.min(100_000, size - i) }, (_, k) => rowOf(i + k)));
  const s = await serve({ engine, live: { orders }, port: 0 });
  const live = connectLive(`ws://127.0.0.1:${s.port}/live`);
  let got = null;
  const group = live.watch(GROUP, { key: 'product' }, (rows, c) => { if (!c.snapshot) got?.(c); });
  const top = live.watch(TOP);
  await Promise.all([group.first, top.first]);
  await live.ready;
  await wait(100); // (the clock offset ping is in)
  for (const n of CHANGES) {
    const total = [], build = [], server = [], bytes = [];
    let missing = 0;
    for (let i = 0; i < SAMPLES + 10; i++) {
      const b0 = s.live.stats.bytes;
      const done = new Promise((r) => { got = r; setTimeout(() => r(null), 5000); }); // (no patch in 5 s: counted as missing)
      const t0 = performance.now();
      orders.upsert(Array.from({ length: n }, () => ({ id: Math.floor(rand() * size), price: Math.round(1 + rand() * 999) })));
      const c = await done;
      const ms = performance.now() - t0;
      if (!c) { missing++; continue; }
      await wait(25); // (the other watch's patch, if any, is sent; the server is quiet again)
      if (i < 10) continue; // (warm-up)
      total.push(ms); build.push(s.live.stats.buildMs); server.push(c.ms); bytes.push(s.live.stats.bytes - b0);
    }
    const row = {
      rows: size, changed: n, samples: total.length, missing,
      p50_ms: r2(q(total, 0.5)), p99_ms: r2(q(total, 0.99)),
      build_p50_ms: r2(q(build, 0.5)), query_diff_p50_ms: r2(q(server, 0.5)),
      bytes_p50: q(bytes, 0.5),
    };
    results.push(row);
    console.log(JSON.stringify(row));
  }
  live.close();
  await s.close();
  await engine.close();
}
console.log(`load after: ${load()}`);
console.log(JSON.stringify(results, null, 2));
