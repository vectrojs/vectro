// What the async API buys on a Node server: the same queries on 10M rows,
// run with the blocking API and with the async API, while Node's event-loop
// delay monitor measures how long other work (e.g. an HTTP request) would
// have had to wait.
import { Engine } from '../src/node.mjs';
import { monitorEventLoopDelay } from 'node:perf_hooks';

const N = 10_000_000;
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const price = new Float64Array(N), qty = new Float64Array(N), cust = new Float64Array(N), region = new Array(N);
for (let i = 0; i < N; i++) { price[i] = Math.round(rnd() * 1e5) / 100; qty[i] = 1 + Math.floor(rnd() * 9); cust[i] = Math.floor(rnd() * 100_000); region[i] = REG[Math.floor(rnd() * 8)]; }

const e = new Engine();
const t = e.table({ price, qty, cust, region });
await new Promise((r) => setTimeout(r, 300));

// A request mix: dashboards, group-bys, a filtered copy, a derived column.
const SYNC = [
  () => t.filter("region == 'EU'").agg({ n: 'count()', avg: 'mean(price)', rev: 'sum(price * qty)' }),
  () => t.groupBy('region').agg({ rev: 'sum(price * qty)', n: 'count()' }).dispose(),
  () => t.groupBy('cust').agg({ rev: 'sum(price * qty)' }).dispose(),
  () => t.filter('price > 900').collect().dispose(),
  () => t.derive('rev', 'price * qty').dispose(),
];
const ASYNC = [
  () => t.filter("region == 'EU'").aggAsync({ n: 'count()', avg: 'mean(price)', rev: 'sum(price * qty)' }),
  async () => (await t.groupBy('region').aggAsync({ rev: 'sum(price * qty)', n: 'count()' })).dispose(),
  async () => (await t.groupBy('cust').aggAsync({ rev: 'sum(price * qty)' })).dispose(),
  async () => (await t.filter('price > 900').collectAsync()).dispose(),
  async () => (await t.deriveAsync('rev', 'price * qty')).dispose(),
];
const ROUNDS = 20; // 100 queries per run
for (const f of [...SYNC]) f(); // warm up: compile every kernel once
for (const f of ASYNC) await f();

async function measure(label, run) {
  const h = monitorEventLoopDelay({ resolution: 1 });
  // A 1 ms "heartbeat" stands in for incoming requests: count how many get served.
  let beats = 0;
  const hb = setInterval(() => { beats++; }, 1);
  await new Promise((r) => setTimeout(r, 20));
  beats = 0;
  h.enable();
  const s = performance.now();
  await run();
  const ms = performance.now() - s;
  h.disable();
  clearInterval(hb);
  return { mode: label, 'total time': `${ms.toFixed(0)} ms`, 'per query': `${(ms / (ROUNDS * 5)).toFixed(2)} ms`,
    'event loop delay p50': `${(h.percentile(50) / 1e6).toFixed(1)} ms`, 'p99': `${(h.percentile(99) / 1e6).toFixed(1)} ms`, 'max': `${(h.max / 1e6).toFixed(1)} ms`,
    'heartbeats served (1 ms timer)': beats };
}

const rows = [];
rows.push(await measure('blocking API (sync)', async () => {
  for (let r = 0; r < ROUNDS; r++) for (const f of SYNC) f(); // runs without ever yielding
}));
rows.push(await measure('async API, one at a time', async () => {
  for (let r = 0; r < ROUNDS; r++) for (const f of ASYNC) await f();
}));
rows.push(await measure('async API, all 100 at once', async () => {
  const jobs = [];
  for (let r = 0; r < ROUNDS; r++) for (const f of ASYNC) jobs.push(f());
  await Promise.all(jobs);
}));
console.log(`\n100 queries on ${N.toLocaleString()} rows (dashboards, group-bys, filtered copies, derived columns), M1 Pro, 8 cores\n`);
console.table(rows);

// Latency of a single small query, sync vs async
const one = () => t.filter("region == 'EU'").agg({ rev: 'sum(price * qty)' });
const oneA = () => t.filter("region == 'EU'").aggAsync({ rev: 'sum(price * qty)' });
const lat = async (f) => { const v = []; for (let i = 0; i < 60; i++) { const s = performance.now(); await f(); v.push(performance.now() - s); } v.sort((a, b) => a - b); return v[30]; };
console.log(`one dashboard query (median of 60): sync ${(await lat(one)).toFixed(2)} ms | async ${(await lat(oneA)).toFixed(2)} ms`);
await e.close();
