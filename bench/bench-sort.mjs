// top(k) and sort on 10M rows, against plain JavaScript.
//   top 10:   Vectro top(10, '-price') | JS: one pass keeping the 10 largest | JS: sort everything, take 10
//   sort:     Vectro sort('-price')    | JS: Float64Array.sort (values only) | JS: sort row indices by value
import { Engine } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 10_000_000);
let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const price = new Float64Array(N), qty = new Float64Array(N);
for (let i = 0; i < N; i++) { price[i] = Math.round(rnd() * 1e7) / 100; qty[i] = 1 + Math.floor(rnd() * 9); }
const engine = new Engine();
const t = engine.table({ price, qty });

async function best(f, reps = 5) { await f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); await f(); m = Math.min(m, performance.now() - s); } return m; }
const fmt = (ms) => `${ms.toFixed(1)} ms`.padStart(10);

const topJsLoop = () => { // keep the 10 largest in a small sorted array
  const best = new Float64Array(10).fill(-Infinity), at = new Int32Array(10).fill(-1);
  for (let i = 0; i < N; i++) {
    const v = price[i];
    if (v <= best[9]) continue;
    let j = 9;
    while (j > 0 && best[j - 1] < v) { best[j] = best[j - 1]; at[j] = at[j - 1]; j--; }
    best[j] = v; at[j] = i;
  }
  return at;
};
const rows = [
  ['top 10 by -price', await best(async () => (await t.topAsync(10, '-price')).dispose()), await best(topJsLoop), await best(() => Float64Array.from(price).sort().subarray(N - 10), 2)],
  ['top 10 by -(price * qty)', await best(async () => (await t.topAsync(10, '-(price * qty)')).dispose()), NaN, NaN],
  ['sort by -price (all rows)', await best(async () => (await t.sortAsync('-price')).dispose(), 2), await best(() => Float64Array.from(price).sort(), 2), await best(() => Uint32Array.from({ length: N }, (_, i) => i).sort((a, b) => price[b] - price[a]), 1)],
];
console.log(`${N.toLocaleString()} rows\n${''.padEnd(28)}${'Vectro'.padStart(10)}${'JS, best'.padStart(12)}${'JS, sort'.padStart(12)}`);
for (const [label, v, a, b] of rows) console.log(`${label.padEnd(28)}${fmt(v)}${(Number.isNaN(a) ? '–' : fmt(a)).padStart(12)}${(Number.isNaN(b) ? '–' : fmt(b)).padStart(12)}`);
console.log('JS, best = one pass keeping the top 10 (top) or Float64Array.sort of the values alone, without rows (sort).');
console.log('JS, sort = sort all values then take 10 (top) or sort row indices with a comparator (sort).');
await engine.close();
