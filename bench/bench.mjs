// What each mode costs: the same 10M-row queries with
//   node    8 threads, compiled kernels (Node servers)
//   single  1 thread, compiled kernels (browsers without isolation)
//   interp  1 thread, vectorized interpreter (Cloudflare Workers / edge)
// plus hand-written single-thread JS loops over typed arrays for reference.
// Each mode runs in its own process; best of 5.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';

const CSV = process.env.CSV ?? '/tmp/vectro-orders-10000000.csv';
const MODES = ['node', 'single', 'interp', 'js'];
const fmt = (ms) => (ms < 1000 ? `${ms.toFixed(1)} ms` : `${(ms / 1000).toFixed(2)} s`);
const best = (f, reps = 5) => { const out = f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); f(); m = Math.min(m, performance.now() - s); } return { ms: m, out }; };

const QUERIES = [
  ['load CSV (233 MB)', null],
  ['filtered revenue', (t) => t.filter('price > 100 && qty < 5').sum('price * qty * (1 - discount)')],
  ['dashboard: 4 aggregates', (t) => Object.values(t.filter("region == 'EU'").agg({ n: 'count()', avg: 'mean(price)', top: 'max(qty)', rev: 'sum(price * qty)' }))],
  ['group by region', (t) => { const g = t.groupBy('region').agg({ rev: 'sum(price * qty)', n: 'count()' }); const n = g.length; g.dispose(); return n; }],
  ['filter into a new table', (t) => { const c = t.filter('price > 900').collect(); const n = c.length; c.dispose(); return n; }],
  ['new column', (t) => { const d = t.derive('rev', 'price * qty * (1 - discount)'); const n = d.length; d.dispose(); return n; }],
];

const mode = process.argv[2];
if (mode) {
  const bytes = new Uint8Array(readFileSync(CSV));
  const res = {};
  if (mode === 'js') {
    // hand-written loops over typed arrays (data parsed by vectro, not timed)
    const { Engine } = await import('../src/node.mjs');
    const e = new Engine();
    const t = e.parseCSV(bytes);
    const price = Float64Array.from(t.column('price')), qty = Float64Array.from(t.column('qty')), disc = Float64Array.from(t.column('discount'));
    const region = Float64Array.from(t.column('region')), EU = t.cols.get('region').codes.get('EU'), N = price.length;
    await e.close();
    res['filtered revenue'] = best(() => { let s = 0; for (let i = 0; i < N; i++) if (price[i] > 100 && qty[i] < 5) s += price[i] * qty[i] * (1 - disc[i]); return s; }).ms;
    res['dashboard: 4 aggregates'] = best(() => { let n = 0, sp = 0, top = -Infinity, rev = 0; for (let i = 0; i < N; i++) if (region[i] === EU) { n++; sp += price[i]; if (qty[i] > top) top = qty[i]; rev += price[i] * qty[i]; } return [n, sp / n, top, rev]; }).ms;
    res['group by region'] = best(() => { const r = new Float64Array(8), c = new Float64Array(8); for (let i = 0; i < N; i++) { r[region[i]] += price[i] * qty[i]; c[region[i]]++; } return r; }).ms;
    res['filter into a new table'] = best(() => { let c = 0; for (let i = 0; i < N; i++) if (price[i] > 900) c++; const out = [new Float64Array(c), new Float64Array(c), new Float64Array(c), new Float64Array(c), new Float64Array(c)]; for (let i = 0, k = 0; i < N; i++) if (price[i] > 900) { out[0][k] = price[i]; out[1][k] = qty[i]; out[2][k] = disc[i]; out[3][k] = region[i]; out[4][k++] = i; } return c; }).ms;
    res['new column'] = best(() => { const o = new Float64Array(N); for (let i = 0; i < N; i++) o[i] = price[i] * qty[i] * (1 - disc[i]); return o; }).ms;
  } else {
    let e;
    if (mode === 'node') e = new (await import('../src/node.mjs')).Engine();
    else e = new (await import('../src/portable.mjs')).Engine({ jit: mode === 'single', initialMemoryMB: 1024 });
    await new Promise((r) => setTimeout(r, 300));
    let t;
    res['load CSV (233 MB)'] = best(() => { t?.dispose(); t = e.parseCSV(bytes); return t.length; }, 3).ms;
    for (const [name, q] of QUERIES.slice(1)) res[name] = best(() => q(t)).ms;
    t.dispose();
    await e.close();
  }
  process.stdout.write('@@' + JSON.stringify(res));
  process.exit(0);
}

const results = {};
for (const m of MODES) {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), m], { encoding: 'utf8', maxBuffer: 1 << 24, env: { ...process.env, CSV } });
  const match = r.stdout.match(/@@(.*)$/s);
  if (!match) { console.error(m, 'failed:', r.stderr.slice(0, 2000)); process.exit(1); }
  results[m] = JSON.parse(match[1]);
}
console.log('\n10,000,000 orders, M1 Pro. Best of 5 (CSV load: best of 3).\n');
console.table(QUERIES.map(([name]) => ({
  query: name,
  'Node, 8 threads': fmt(results.node[name]),
  '1 thread, compiled (browser)': fmt(results.single[name]),
  '1 thread, interpreter (edge)': fmt(results.interp[name]),
  'plain JS loops, 1 thread': results.js[name] ? fmt(results.js[name]) : '-',
})));
