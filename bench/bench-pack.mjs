// Compressed columns: memory and query times on 10M orders, plain vs
// compressed (t.compress()). Answers are compared.
//   node bench/bench-pack.mjs
import { Engine } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 10_000_000);
let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const R = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const cols = {
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 100_000)),
  price: Float64Array.from({ length: N }, () => Math.round(rnd() * 100000) / 100),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  discount: Float64Array.from({ length: N }, () => Math.floor(rnd() * 30) / 100),
  region: Array.from({ length: N }, () => R[Math.floor(rnd() * 8)]),
  ts: Array.from({ length: N }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 3000)),
};
const engine = new Engine();
const plain = engine.table(cols);
const packed = engine.table(cols);
const t0 = performance.now();
packed.compress();
const packMs = performance.now() - t0;
const mb = (b) => `${(b / 2 ** 20).toFixed(0)} MB`;
const m = packed.memoryBytes();
console.log(`${N.toLocaleString()} orders: ${mb(m.uncompressed)} plain, ${mb(m.bytes)} compressed (${(m.uncompressed / m.bytes).toFixed(1)}x smaller; compress() took ${packMs.toFixed(0)} ms)`);
for (const [name, c] of packed.cols) console.log(`  ${name.padEnd(9)} ${c.enc ? `${c.enc.kind}${c.enc.div ? ` /${c.enc.div}` : c.enc.mul !== 1 ? ` x${c.enc.mul}` : ''}` : 'float64'}`);

const time = (f) => { let best = Infinity, out; for (let i = 0; i < 5; i++) { const s = performance.now(); out = f(); best = Math.min(best, performance.now() - s); } return [best, out]; };
// (group sums add per thread in an order that varies run to run: relative 1e-9)
const same = (x, y) => {
  if (typeof x === 'number' && typeof y === 'number') return x === y || Math.abs(x - y) <= 1e-9 * Math.max(1, Math.abs(x));
  if (x && typeof x === 'object' && y && typeof y === 'object') return Object.keys(x).length === Object.keys(y).length && Object.keys(x).every((k) => same(x[k], y[k]));
  return x === y;
};
const QUERIES = [
  ['filtered revenue', (t) => t.filter('price > 100 && qty < 5').sum('price * qty * (1 - discount)')],
  ['EU dashboard (4 aggregates)', (t) => t.filter("region == 'EU'").agg({ n: 'count()', a: 'mean(price)', q: 'max(qty)', r: 'sum(price * qty * (1 - discount))' })],
  ['last 7 days, revenue', (t) => t.filter("ts >= '2024-12-25'").sum('price * qty')],
  ['new column', (t) => { const d = t.derive('rev', 'price * qty * (1 - discount)'); const s = d.sum('rev'); d.dispose(); return s; }],
  ['group by region', (t) => t.groupBy('region').agg({ r: 'sum(price * qty)', n: 'count()' }).toRows()],
  ['top 10 by price', (t) => t.top(10, '-price').toRows()],
];
console.log(`\n${'Query'.padEnd(30)}${'plain'.padStart(10)}${'compressed'.padStart(12)}   same answer`);
for (const [label, q] of QUERIES) {
  const [a, x] = time(() => q(plain)), [b, y] = time(() => q(packed));
  console.log(`${label.padEnd(30)}${`${a.toFixed(1)} ms`.padStart(10)}${`${b.toFixed(1)} ms`.padStart(12)}   ${same(x, y) ? 'yes' : 'NO'}`);
}
await engine.close();
