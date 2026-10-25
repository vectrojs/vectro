// Skewed filters on split tables. Rows are sorted by day (like real logs and
// orders), so "the last 36 days" is one contiguous range of rows. With one
// range per worker (step 12) it all lands in the last worker; with blocks
// dealt round-robin (this step) it lands on every worker.
// Usage: node bench-skew.mjs [path to another split.mjs to compare, e.g. ../12-cross-browser/src/split.mjs]
import { Worker } from 'node:worker_threads';
import { Engine } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 10_000_000), WORKERS = Number(process.env.WORKERS ?? 8);
const layouts = [['blocks (this step)', '../src/split.mjs', '../src/split-worker-node.mjs']];
if (process.argv[2]) layouts.unshift(['one range per worker', process.argv[2], new URL('split-worker-node.mjs', new URL(process.argv[2], import.meta.url)).href]);

let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const day = new Float64Array(N), price = new Float64Array(N), qty = new Float64Array(N), pick = new Float64Array(N), region = [];
for (let i = 0; i < N; i++) { day[i] = Math.floor((i * 365) / N); price[i] = Math.round(rnd() * 1e5) / 100; qty[i] = 1 + Math.floor(rnd() * 9); pick[i] = rnd(); region.push(REG[Math.floor(rnd() * 8)]); }

async function best(f, reps = 7) { await f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); await f(); m = Math.min(m, performance.now() - s); } return m; }
const fmt = (ms) => `${ms.toFixed(1)} ms`.padStart(9);
const FILTERS = { 'last 10% of days': 'day >= 329', 'random 10%': 'pick < 0.0986', 'last 50% of days': 'day >= 183' };

const single = new Engine({ threads: 1 });
const s1 = single.table({ day, price, qty, pick, region });
const results = {};
for (const [layout, mod, workerUrl] of layouts) {
  const { SplitEngine } = await import(new URL(mod, import.meta.url).href);
  const spawn = () => {
    const w = new Worker(new URL(workerUrl, import.meta.url));
    w.unref();
    return { post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate() };
  };
  const split = new SplitEngine({ workers: WORKERS, spawn });
  let load;
  const t0 = performance.now();
  const t = await split.tableAsync({ day, price, qty, pick, region });
  load = performance.now() - t0;
  for (const [label, f] of Object.entries(FILTERS)) {
    const v = t.filter(f);
    const lazy = await best(() => v.aggAsync({ rev: 'sum(price * qty)', n: 'count()' }));
    const first = await best(() => v.toRowsAsync(100));
    let c;
    const collect = await best(async () => { await c?.dispose(); c = await v.collectAsync(); }, 3);
    const r = {
      lazy, first, collect, workers: c.counts.map((x) => Math.round(x / 1000)).join(' '),
      agg: await best(() => c.aggAsync({ rev: 'sum(price * qty)', avg: 'mean(price)', hi: 'max(qty)' })),
      groupBy: await best(() => c.groupBy('region').aggAsync({ rev: 'sum(price * qty)' })),
      derive: await best(async () => (await c.deriveAsync('rev', 'price * qty')).dispose()),
    };
    await c.dispose();
    (results[label] ??= {})[layout] = r;
  }
  (results.load ??= {})[layout] = load;
  await t.dispose();
  await split.close();
}
for (const label of Object.keys(FILTERS)) {
  const sc = s1.filter(label && FILTERS[label]).collect();
  results[label]['1 thread'] = {
    agg: await best(() => sc.agg({ rev: 'sum(price * qty)', avg: 'mean(price)', hi: 'max(qty)' })),
    groupBy: await best(() => sc.groupBy('region').agg({ rev: 'sum(price * qty)' }).dispose()),
    derive: await best(() => sc.derive('rev', 'price * qty').dispose()),
  };
  sc.dispose();
}
console.log(`${N.toLocaleString()} rows sorted by day, ${WORKERS} split workers`);
console.log('load from arrays:', Object.entries(results.load).map(([k, v]) => `${k} ${v.toFixed(0)} ms`).join(', '));
const names = layouts.map(([l]) => l);
for (const label of Object.keys(FILTERS)) {
  const R = results[label];
  console.log(`\n${label}`);
  for (const l of names) console.log(`  thousand rows per worker after collect (${l}): ${R[l].workers}`);
  console.log(`  ${''.padEnd(26)}${[...names, '1 thread'].map((l) => l.slice(0, 20).padStart(22)).join('')}`);
  for (const k of ['lazy', 'first', 'collect', 'agg', 'groupBy', 'derive']) {
    const desc = { lazy: 'filter + aggregate', first: 'first 100 rows', collect: 'collect', agg: 'then aggregate', groupBy: 'then group by', derive: 'then derive' }[k];
    console.log(`  ${desc.padEnd(26)}${[...names, '1 thread'].map((l) => (R[l]?.[k] === undefined ? '' : fmt(R[l][k])).padStart(22)).join('')}`);
  }
}
await single.close();
