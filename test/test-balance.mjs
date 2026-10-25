// Skewed filters on split tables (sorted data, a range of it): the collected
// table must stay spread over the workers, keep row order, and give the same
// answers as a normal table.
import { Worker } from 'node:worker_threads';
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
// workers differ by at most two blocks
const balanced = (t) => Math.max(...t.counts) - Math.min(...t.counts) <= 2 * Math.max(...t.segs.map((s) => Math.max(0, ...s)));

let seed = 5;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];

const ref = new Engine();
// [label, filter]: rows are numbered by `i`, so ranges of i are ranges of slices
const N = 120_000;
const FILTERS = [
  ['last slice only', 'i >= 110000'],
  ['first slice only', 'i < 9000'],
  ['two slices far apart', '(i >= 5000 && i < 12000) || (i >= 100000 && i < 106000)'],
  ['middle, with a second condition', "i >= 50000 && i < 80000 && region != 'US'"],
  ['most rows, one slice missing', 'i >= 15000'],
  ['random (already even)', 'price > 500'],
  ['tiny result (not split)', 'i >= 119990'],
];
for (const workers of [2, 3, 8]) {
  const split = new SplitEngine({ workers, spawn });
  const cols = { i: Float64Array.from({ length: N }, (_, k) => k), price: Float64Array.from({ length: N }, () => Math.round(rnd() * 1e5) / 100), region: Array.from({ length: N }, () => REG[Math.floor(rnd() * 5)]) };
  const a = ref.table(cols), b = await split.tableAsync(cols);
  for (const [label, f] of FILTERS) {
    const L = `w=${workers} ${label}`;
    const ca = a.filter(f).collect(), cb = await b.filter(f).collectAsync();
    expect(`${L} length`, cb.length, ca.length);
    { checks++; if (!balanced(cb)) { failures++; console.log('FAIL', L, 'not balanced:', cb.counts.join(' ')); } }
    expect(`${L} all rows in order`, await cb.toRowsAsync(), ca.toRows());
    expect(`${L} agg`, await cb.aggAsync({ n: 'count()', s: 'sum(price)', m: 'mean(i)', lo: 'min(i)', hi: 'max(price)' }), ca.agg({ n: 'count()', s: 'sum(price)', m: 'mean(i)', lo: 'min(i)', hi: 'max(price)' }));
    expect(`${L} groupBy`, (await cb.groupBy('region').aggAsync({ n: 'count()', s: 'sum(price)' })).toRows(), ca.groupBy('region').agg({ n: 'count()', s: 'sum(price)' }).toRows());
    // a second filter + collect on the rebalanced table, then derive
    const cb2 = await cb.filter('price < 300').collectAsync(), ca2 = ca.filter('price < 300').collect();
    const db = await cb2.deriveAsync('twice', 'price * 2'), da = ca2.derive('twice', 'price * 2');
    expect(`${L} collect again + derive`, await db.toRowsAsync(), da.toRows());
    // toRows with a limit, on the view and on the collected table
    expect(`${L} first rows of the view`, await b.filter(f).toRowsAsync(7), a.filter(f).toRows(7));
    expect(`${L} first rows collected`, await cb.toRowsAsync(4100), ca.toRows(4100));
    for (const t of [cb, cb2, db]) await t.dispose();
    for (const t of [ca, ca2, da]) t.dispose();
  }
  await b.dispose(); a.dispose();

  // CSV with free text (quoted, escaped quotes, newlines, empty), sorted by id
  const lines = ['id,price,region,note'];
  for (let i = 0; i < 60_000; i++) {
    const note = i % 11 === 0 ? '' : i % 7 === 0 ? `"multi\nline ${i}"` : i % 5 === 0 ? `"say ""hi"", ${i}"` : `plain ${i}`;
    lines.push(`${i},${(rnd() * 1000).toFixed(2)},${REG[i % 5]},${note}`);
  }
  const csv = lines.join('\n');
  const ta = ref.parseCSV(csv, { types: { note: 'string' } }), tb = await split.parseCSVAsync(csv, { types: { note: 'string' } });
  for (const f of ['id >= 52000', 'id < 7000 || id >= 57000', 'id >= 20000 && id < 30000']) {
    const L = `w=${workers} csv ${f}`;
    const ca = ta.filter(f).collect(), cb = await tb.filter(f).collectAsync();
    expect(`${L} rows`, await cb.toRowsAsync(), ca.toRows());
    { checks++; if (!balanced(cb)) { failures++; console.log('FAIL', L, 'not balanced:', cb.counts.join(' ')); } }
    // free text survives a second move, and the source table can go away
    const again = await cb.filter('price > 100').collectAsync();
    expect(`${L} after a second collect`, await again.toRowsAsync(20), ca.filter('price > 100').toRows(20));
    await cb.dispose(); await again.dispose(); ca.dispose();
  }
  await tb.dispose(); ta.dispose();
  await split.close();
}
await ref.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} skewed-filter checks passed`);
process.exit(failures ? 1 : 0);
