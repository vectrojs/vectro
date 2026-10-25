// Zone maps on 10M time-ordered rows: filtered queries with and without
// block skipping (same engine, same data, best of 7).
import { Engine } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 10_000_000);
let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const ts = new Float64Array(N), price = new Float64Array(N), qty = new Float64Array(N), reg = [];
const T0 = 1_700_000_000_000; // one row every 10 ms: ~28 hours of orders
for (let i = 0; i < N; i++) { ts[i] = T0 + i * 10; price[i] = Math.round(rnd() * 1e5) / 100; qty[i] = 1 + Math.floor(rnd() * 9); reg.push(REG[Math.floor(rnd() * 8)]); }
const engine = new Engine();
const t = engine.table({ ts, price, qty, region: reg });
const END = T0 + N * 10;
async function best(f, reps = 7) { await f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); await f(); m = Math.min(m, performance.now() - s); } return m; }
const H = 3_600_000;
const QUERIES = [
  ['revenue, last hour', (v) => v.aggAsync({ rev: 'sum(price * qty)', n: 'count()' }), 'ts >= t', { t: END - H }],
  ['revenue, last 6 hours', (v) => v.aggAsync({ rev: 'sum(price * qty)', n: 'count()' }), 'ts >= t', { t: END - 6 * H }],
  ['one hour in the middle, EU', (v) => v.aggAsync({ rev: 'sum(price * qty)' }), "ts >= a && ts < b && region == 'EU'", { a: T0 + 10 * H, b: T0 + 11 * H }],
  ['by region, last hour', (v) => v.groupBy('region').aggAsync({ rev: 'sum(price * qty)' }), 'ts >= t', { t: END - H }],
  ['top 10 orders, last hour', async (v) => (await v.topAsync(10, '-(price * qty)')).dispose(), 'ts >= t', { t: END - H }],
];
// first use pays for the zone maps of the filtered column
let s = performance.now();
await t.filter('ts >= t', { t: END - H }).aggAsync({ n: 'count()' });
const firstUse = performance.now() - s;
console.log(`${N.toLocaleString()} rows, one every 10 ms (time-ordered); zone maps of 16,384 rows`);
console.log(`first filtered query on ts (builds its zone maps): ${firstUse.toFixed(1)} ms`);
for (const [label, run, f, p] of QUERIES) {
  const on = await best(() => run(t.filter(f, p)));
  const off = await best(() => { const v = t.filter(f, p); v.zones = false; return run(v); });
  console.log(`${label.padEnd(30)} with zone maps ${on.toFixed(2).padStart(6)} ms   without ${off.toFixed(2).padStart(6)} ms   ${(off / on).toFixed(1)}x`);
}
await engine.close();
