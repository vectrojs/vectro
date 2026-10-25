// Split tables vs a normal engine on the same data: every answer must match.
// Workers are Node worker threads here, exchanging messages only (no shared
// memory), the same way Web Workers do on pages without isolation headers.
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

let seed = 11;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
function data(n) {
  const price = new Float64Array(n), qty = [], cust = new Float64Array(n), region = [];
  for (let i = 0; i < n; i++) { price[i] = Math.round(rnd() * 1e5) / 100; qty.push(1 + Math.floor(rnd() * 9)); cust[i] = Math.floor(rnd() * 3000); region.push(REG[(i * 7 + Math.floor(rnd() * 3)) % 5]); }
  return { price, qty, cust, region };
}

// [label, (table) => promise of an answer]; works on normal and split tables alike
const QUERIES = [
  ['agg filtered', (t) => t.filter("region == 'EU' && price > m", { m: 250 }).aggAsync({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)', big: 'count(qty > 6)' })],
  ['agg unfiltered', (t) => t.aggAsync({ s: 'sum(price)', a: 'mean(qty)', lo: 'min(qty)', hi: 'max(price)' })],
  ['shortcuts', async (t) => [await t.sumAsync('price * 2'), await t.meanAsync('price'), await t.minAsync('qty'), await t.maxAsync('qty'), await t.filter("region != 'US'").countAsync()]],
  ['filter matches nothing', (t) => t.filter("region == 'MARS'").aggAsync({ n: 'count()', s: 'sum(price)', a: 'mean(price)', lo: 'min(price)' })],
  ['groupBy text', async (t) => (await t.groupBy('region').aggAsync({ rev: 'sum(price * qty)', n: 'count()', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' })).toRows()],
  ['groupBy numeric, filtered', async (t) => (await t.filter('qty >= 3').groupBy('cust').aggAsync({ s: 'sum(price)', a: 'mean(qty)' })).toRows()],
  ['collect + agg', async (t) => { const c = await t.filter('price > 800').collectAsync(); return [c.length, await c.sumAsync('price'), await c.toRowsAsync(25)]; }],
  ['derive + filter', async (t) => { const d = await t.deriveAsync('rev', 'price * qty'); return [await d.filter('rev > 4000').sumAsync('rev'), await d.filter('rev > 4000').toRowsAsync(10)]; }],
  ['toRows filtered', (t) => t.filter("region == 'APAC' && qty == 9").toRowsAsync(40)],
];

const ref = new Engine();
for (const workers of [1, 3, 8]) {
  const split = new SplitEngine({ workers, spawn });
  for (const n of [5, 9_999, 200_003]) {
    const d = data(n);
    const a = ref.table(d), b = await split.tableAsync(d);
    expect(`w=${workers} n=${n} length`, b.length, a.length);
    for (const [label, q] of QUERIES) expect(`w=${workers} n=${n} ${label}`, await q(b), await q(a));
    await b.dispose();
    a.dispose();
  }

  // column order is kept (text in the middle), and text-only tables work
  {
    const cols = { region: ['EU', 'US', 'EU', 'APAC'], price: [1, 2, 3, 4], tag: ['a', 'b', 'a', 'c'] };
    const a = ref.table(cols), b = await split.tableAsync(cols);
    expect(`w=${workers} column order`, await b.toRowsAsync(), a.toRows());
    const onlyText = await split.tableAsync({ k: ['x', 'y', 'x'] });
    expect(`w=${workers} text-only table`, (await onlyText.groupBy('k').aggAsync({ n: 'count()' })).toRows(), [{ k: 'x', n: 2 }, { k: 'y', n: 1 }]);
  }

  // CSV with quotes, commas and newlines inside fields, cut across workers
  const lines = ['id,price,region,note'];
  for (let i = 0; i < 250_000; i++) {
    const note = i % 7 === 0 ? `"multi\nline ${i}"` : i % 5 === 0 ? `"say ""hi"", ${i}"` : `plain ${i}`;
    lines.push(`${i},${(rnd() * 1000).toFixed(2)},${REG[i % 5]},${note}`);
  }
  const csv = lines.join('\r\n');
  const a = ref.parseCSV(csv), b = await split.parseCSVAsync(csv);
  expect(`w=${workers} csv rows`, b.length, a.length);
  expect(`w=${workers} csv all rows`, await b.toRowsAsync(), a.toRows());
  expect(`w=${workers} csv groupBy`, (await b.groupBy('region').aggAsync({ n: 'count()', s: 'sum(price)' })).toRows(), a.groupBy('region').agg({ n: 'count()', s: 'sum(price)' }).toRows());

  // errors come back as rejected promises
  let msg = '';
  try { await b.aggAsync({ x: 'sum(nope)' }); } catch (err) { msg = err.message; }
  expect(`w=${workers} unknown column rejects`, msg.includes('unknown column'), true);
  await split.close();
}
// A worker that dies (in browsers: stopped for using too much memory) makes
// waiting and later calls fail with a clear error instead of hanging.
{
  const spawnWatched = () => {
    const w = new Worker(new URL('../src/split-worker-node.mjs', import.meta.url));
    w.unref();
    return {
      post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate(),
      onError: (fn) => { w.on('error', fn); w.on('exit', (code) => fn(new Error(`exited with code ${code}`))); },
    };
  };
  const split = new SplitEngine({ workers: 3, spawn: spawnWatched });
  const t = await split.tableAsync(data(30_000));
  const running = t.groupBy('region').aggAsync({ n: 'count()' });
  await split.workers[1].terminate();
  const outcome = async (p) => { try { await p; return 'resolved'; } catch (err) { return err.message; } };
  const first = await Promise.race([outcome(running), new Promise((r) => setTimeout(() => r('hung'), 3000))]);
  checks++; if (first !== 'resolved' && !first.includes('worker 1 stopped')) { failures++; console.log('FAIL dead worker, in flight:', first); }
  const later = await Promise.race([outcome(t.sumAsync('price')), new Promise((r) => setTimeout(() => r('hung'), 3000))]);
  expect('dead worker: later calls reject', later.includes('worker 1 stopped'), true);
  await split.close();
}

await ref.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} split-table checks passed`);
process.exit(failures ? 1 : 0);
