// Async API tests: async results equal sync results, many queries in flight
// at once, the event loop stays free, sync calls during an async run are
// refused, and errors reject instead of crashing.
// VECTRO_MODE=browser-sim runs this against the browser threading model
const { Engine } = process.env.VECTRO_MODE ? await import('./test-mode.mjs') : await import('../src/node.mjs');
import { Engine as PortableEngine } from '../src/portable.mjs';
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 20) console.log('FAIL', msg); };
const same = (a, b) => {
  checks++;
  const ok = JSON.stringify(a, (k, v) => (typeof v === 'number' && !Number.isInteger(v) ? Number(v.toPrecision(12)) : v)) ===
    JSON.stringify(b, (k, v) => (typeof v === 'number' && !Number.isInteger(v) ? Number(v.toPrecision(12)) : v));
  return ok;
};
const expectSame = (label, a, b) => { if (!same(a, b)) fail(`${label}: ${JSON.stringify(a).slice(0, 200)} vs ${JSON.stringify(b).slice(0, 200)}`); };

let seed = 5;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
function makeTable(e, n) {
  const price = new Float64Array(n), qty = new Float64Array(n), cust = new Float64Array(n), region = new Array(n);
  for (let i = 0; i < n; i++) { price[i] = Math.round(rnd() * 1e5) / 100; qty[i] = 1 + Math.floor(rnd() * 9); cust[i] = Math.floor(rnd() * 5000); region[i] = REG[Math.floor(rnd() * 5)]; }
  return e.table({ price, qty, cust, region });
}

// Every operation as a [sync, async] pair; results compared after normalizing.
const OPS = [
  ['agg filtered', (t) => t.filter("region == 'EU' && price > 200").agg({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' }),
    (t) => t.filter("region == 'EU' && price > 200").aggAsync({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' })],
  ['agg unfiltered', (t) => t.agg({ rev: 'sum(price * qty)', n: 'count(qty > 4)' }), (t) => t.aggAsync({ rev: 'sum(price * qty)', n: 'count(qty > 4)' })],
  ['sum/mean/min/max/count', (t) => [t.sum('price'), t.mean('qty'), t.min('price'), t.max('price'), t.filter('qty > 5').count()],
    async (t) => [await t.sumAsync('price'), await t.meanAsync('qty'), await t.minAsync('price'), await t.maxAsync('price'), await t.filter('qty > 5').countAsync()]],
  ['groupBy text', (t) => sortRows(t.groupBy('region').agg({ rev: 'sum(price * qty)', n: 'count()', avg: 'mean(price)' }).toRows(), 'region'),
    async (t) => sortRows(await (await t.groupBy('region').aggAsync({ rev: 'sum(price * qty)', n: 'count()', avg: 'mean(price)' })).toRowsAsync(), 'region')],
  ['groupBy 5000 ids', (t) => sortRows(t.filter('price > 100').groupBy('cust').agg({ s: 'sum(price)', m: 'max(qty)' }).toRows(), 'cust'),
    async (t) => sortRows(await (await t.filter('price > 100').groupBy('cust').aggAsync({ s: 'sum(price)', m: 'max(qty)' })).toRowsAsync(), 'cust')],
  ['collect', (t) => t.filter('price > 900').collect().toRows(50), async (t) => (await t.filter('price > 900').collectAsync()).toRowsAsync(50)],
  ['toRows filtered', (t) => t.filter("region == 'US' && qty == 9").toRows(30), (t) => t.filter("region == 'US' && qty == 9").toRowsAsync(30)],
  ['derive', (t) => Array.from(t.derive('rev', 'price * qty').column('rev').subarray(0, 1000)),
    async (t) => Array.from((await t.deriveAsync('rev', 'price * qty')).column('rev').subarray(0, 1000))],
  ['derive then filter', (t) => t.derive('rev', 'price * qty').filter('rev > 5000').sum('rev'),
    async (t) => (await t.deriveAsync('rev', 'price * qty')).filter('rev > 5000').sumAsync('rev')],
];
const sortRows = (rows, key) => rows.sort((a, b) => (a[key] < b[key] ? -1 : a[key] > b[key] ? 1 : 0));

const e = new Engine();
await new Promise((r) => setTimeout(r, 200));

// 1. async equals sync, small (one thread) and large (all threads)
for (const n of [500, 300_000]) {
  const t = makeTable(e, n);
  for (const [name, sync, async] of OPS) expectSame(`${name} n=${n}`, await async(t), sync(t));
  // evalAsync on raw arrays (several reductions fused)
  const a = e.alloc(Float64Array, n);
  for (let i = 0; i < n; i++) a[i] = rnd() * 100;
  expectSame(`evalAsync n=${n}`, await e.evalAsync(['sum(a)', 'max(a)', 'count(a > 50)'], { a }), e.eval(['sum(a)', 'max(a)', 'count(a > 50)'], { a }));
  const out1 = e.alloc(Float64Array, n), out2 = e.alloc(Float64Array, n);
  e.eval('a * 2 + 1', { a }, out1);
  await e.evalAsync('a * 2 + 1', { a }, out2);
  expectSame(`evalAsync map n=${n}`, Array.from(out2.subarray(0, 500)), Array.from(out1.subarray(0, 500)));
}

// 2. many queries in flight at once, each must get its own right answer
{
  const t = makeTable(e, 400_000);
  const jobs = [];
  for (let i = 0; i < 50; i++) {
    const [name, sync, async] = OPS[i % OPS.length];
    jobs.push({ name: `${name} #${i}`, want: sync(t), got: async(t) });
  }
  const got = await Promise.all(jobs.map((j) => j.got));
  jobs.forEach((j, i) => expectSame(`concurrent ${j.name}`, got[i], j.want));
}

// 3. CSV: async parse and async file read equal the sync ones
{
  const lines = ['id,price,region,note'];
  for (let i = 0; i < 200_000; i++) lines.push(`${i},${(rnd() * 1000).toFixed(2)},${REG[i % 5]},"note, ${i}"`);
  const text = lines.join('\n');
  const dir = mkdtempSync(join(tmpdir(), 'vectro-async-'));
  const path = join(dir, 'x.csv');
  writeFileSync(path, text);
  const s = e.parseCSV(text, { chunkBytes: 65536 });
  const want = [s.length, s.sum('price'), s.toRows(3), s.groupBy('region').agg({ n: 'count()' }).toRows()];
  for (const [label, load] of [['parseCSVAsync', () => e.parseCSVAsync(text, { chunkBytes: 65536 })], ['readCSVAsync', () => e.readCSVAsync(path, { chunkBytes: 65536 })]]) {
    const t = await load();
    expectSame(label, [t.length, await t.sumAsync('price'), await t.toRowsAsync(3), await (await t.groupBy('region').aggAsync({ n: 'count()' })).toRowsAsync()], want);
  }
  rmSync(dir, { recursive: true });
}

// 4. a sync call while an async query is on the workers is refused, clearly
{
  const t = makeTable(e, 4_000_000);
  const pending = t.groupBy('cust').aggAsync({ s: 'sum(price * qty)' });
  let waited = 0;
  while (!e.busy && waited++ < 1000) await new Promise((r) => setImmediate(r));
  checks++;
  if (!e.busy) fail('async query never showed as busy');
  else {
    let msg = '';
    try { t.agg({ s: 'sum(price)' }); } catch (err) { msg = err.message; }
    if (!msg.includes('async query is running')) fail(`sync call during async run: ${msg || 'no error'}`);
  }
  const g = await pending;
  // the refused sync call must not have disturbed the async query
  expectSame('async groupBy after refused sync call', (await g.toRowsAsync()).map((r) => [r.cust, r.s]), t.groupBy('cust').agg({ s: 'sum(price * qty)' }).toRows().map((r) => [r.cust, r.s]));
  expectSame('sync works again afterwards', t.agg({ n: 'count()' }).n, 4_000_000);
}

// 5. errors reject the promise; the engine keeps working
{
  const big = 'x'.repeat(600_000);
  const lines = ['id,note'];
  for (let i = 0; i < 2000; i++) lines.push(`${i},${i === 1500 ? big : `note ${i}`}`);
  let msg = '';
  try { await e.parseCSVAsync(lines.join('\n'), { types: { note: 'string' }, chunkBytes: 1024 }); } catch (err) { msg = err.message; }
  checks++;
  if (!msg.includes('longer than')) fail(`async worker error: ${msg || 'resolved'}`);
  const t = await e.parseCSVAsync('a\n1\n2\n3');
  expectSame('engine works after async error', await t.sumAsync('a'), 6);
  let msg2 = '';
  try { await t.aggAsync({ x: 'sum(nope)' }); } catch (err) { msg2 = err.message; }
  checks++;
  if (!msg2.includes('unknown column')) fail(`async bad query: ${msg2 || 'resolved'}`);
}
await e.close();

// 6. single-thread (portable) engines offer the same async API
{
  const p = new PortableEngine();
  const t = makeTable(p, 50_000);
  for (const [name, sync, async] of OPS) expectSame(`portable ${name}`, await async(t), sync(t));
  const interp = new PortableEngine({ jit: false });
  const ti = makeTable(interp, 50_000);
  for (const [name, sync, async] of OPS) expectSame(`interpreter ${name}`, await async(ti), sync(ti));
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `[${process.env.VECTRO_MODE ?? 'node'}] all ${checks} async checks passed`);
process.exit(failures ? 1 : 0);
