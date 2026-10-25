// What the planner saves: the same queries written step by step (eager table
// methods, each step done as written) and as one lazy query (optimized), on
// 10M orders. Answers are compared.
//   node bench/bench-plan.mjs
import { Engine } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 10_000_000);
let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const engine = new Engine();
const R = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const orders = engine.table({
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 100_000)),
  price: Float64Array.from({ length: N }, () => Math.round(rnd() * 100000) / 100),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  discount: Float64Array.from({ length: N }, () => Math.floor(rnd() * 30) / 100),
  region: Array.from({ length: N }, () => R[Math.floor(rnd() * 8)]),
  note: Float64Array.from({ length: N }, () => rnd()), x1: Float64Array.from({ length: N }, () => rnd()), x2: Float64Array.from({ length: N }, () => rnd()),
});
const customers = engine.table({ cust: Float64Array.from({ length: 100_000 }, (_, i) => i), tier: Float64Array.from({ length: 100_000 }, () => 1 + Math.floor(rnd() * 3)) });

// (sums may differ in the last digits: rows are added in a different order)
const same = (x, y) => JSON.stringify(x, (k, v) => (typeof v === 'number' ? Number(v.toPrecision(12)) : v)) === JSON.stringify(y, (k, v) => (typeof v === 'number' ? Number(v.toPrecision(12)) : v));
const time = (f) => { let best = Infinity, out; for (let i = 0; i < 4; i++) { const s = performance.now(); out = f(); best = Math.min(best, performance.now() - s); } return [best, out]; };
const CASES = [
  ['derive, filter on it, total', () => {
    const d = orders.derive('rev', 'price * qty * (1 - discount)');
    try { return d.filter('rev > 2000').agg({ s: 'sum(rev)', n: 'count()' }); } finally { d.dispose(); }
  }, () => orders.lazy().derive('rev', 'price * qty * (1 - discount)').filter('rev > 2000').agg({ s: 'sum(rev)', n: 'count()' })],
  ['join, then filter both sides', () => {
    const j = orders.join(customers, { on: 'cust' });
    try { return j.filter("tier == 3 && region == 'EU'").agg({ s: 'sum(price)', n: 'count()' }); } finally { j.dispose(); }
  }, () => orders.lazy().join(customers, { on: 'cust' }).filter("tier == 3 && region == 'EU'").agg({ s: 'sum(price)', n: 'count()' })],
  ['sort, then first 10', () => {
    const s = orders.sort('-price');
    try { const h = s.head(10); const r = h.sum('price'); h.dispose(); return r; } finally { s.dispose(); }
  }, () => { const t = orders.lazy().sort('-price').limit(10).collect(); try { return t.sum('price'); } finally { t.dispose(); } }],
  ['filter, 2 of 8 columns, sort', () => {
    const f = orders.filter('price > 900').collect();
    try { const s = f.select('cust', 'price').sort('-price'); const r = s.sum('price'); s.dispose(); return r; } finally { f.dispose(); }
  }, () => { const t = orders.lazy().filter('price > 900').select('cust', 'price').sort('-price').collect(); try { return t.sum('price'); } finally { t.dispose(); } }],
  ['group by a derived month bucket', () => {
    const d = orders.derive('band', 'floor(price / 100)');
    try { const g = d.filter("region == 'US'").groupBy('band').agg({ n: 'count()', s: 'sum(qty)' }); const r = g.sum('s'); g.dispose(); return r; } finally { d.dispose(); }
  }, () => { const g = orders.lazy().derive('band', 'floor(price / 100)').filter("region == 'US'").groupBy('band').agg({ n: 'count()', s: 'sum(qty)' }).collect(); try { return g.sum('s'); } finally { g.dispose(); } }],
];
console.log(`${N.toLocaleString()} orders, best of 4\n`);
console.log('Query'.padEnd(34), 'step by step'.padStart(13), 'lazy'.padStart(9), '  speed-up  same answer');
for (const [label, eager, lazy] of CASES) {
  const [a, x] = time(eager), [b, y] = time(lazy);
  console.log(label.padEnd(34), `${a.toFixed(1)} ms`.padStart(13), `${b.toFixed(1)} ms`.padStart(9), `${(a / b).toFixed(1)}x`.padStart(10), '  ', same(x, y) ? 'yes' : `NO ${JSON.stringify(x)} ${JSON.stringify(y)}`);
}
await engine.close();
