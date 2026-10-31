// Top-k over datasets (ORDER BY ... LIMIT k): the same rows as the table in
// memory, while chunks and blocks whose min / max can't beat the k-th row so
// far are not read, and wide columns are read only for candidate blocks.
// Chunks of 200,000 rows here (blocks of 65,536 inside), sorted keys, ties,
// missing values, dates, filters, offsets, derived keys, text keys.
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const SRC = process.env.SRC ?? new URL('../src/', import.meta.url).href;
const { Engine, sql } = await import((SRC.startsWith('/') ? 'file://' + SRC : SRC).replace(/\/?$/, '/') + 'node.mjs');

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const norm = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && v !== v ? 'NaN' : v));
let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const dir = mkdtempSync(join(tmpdir(), 'vectro-topk-'));
const N = 1_000_000;
const REG = ['EU', 'US', 'APAC'];
const cols = {
  price: Float64Array.from({ length: N }, () => (rnd() < 0.02 ? NaN : Math.round(rnd() * 1000) / 10)), // many ties
  rising: Float64Array.from({ length: N }, (_, i) => Math.floor(i / 10) + (rnd() < 0.001 ? NaN : 0)),     // sorted: zones prune
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 5000)),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  region: Array.from({ length: N }, () => REG[Math.floor(rnd() * 3)]),
  ts: Array.from({ length: N }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 1000)),
  note: Array.from({ length: N }, (_, i) => (i % 97 === 0 ? `n${i % 13}` : 'x')),
};
// a cluster of the best prices in one block of the middle chunk
for (let i = 450_000; i < 450_040; i++) cols.price[i] = 200 + (i % 7);

for (const [label, make] of [['default', () => new Engine()], ['single', () => new Engine({ threads: 0 })]]) {
  const e = make();
  const t = e.table(cols, { dates: ['ts'], text: ['note'] });
  const path = join(dir, `t-${label}.vectro`);
  e.writeDataset(path, t, { rowsPerChunk: 200_000 });
  const ds = e.openDataset(path);
  const QS = [
    ['price desc, ts', (x) => x.sort(['-price', 'ts']).limit(10)],
    ['price desc only (ties: table order)', (x) => x.sort('-price').limit(10)],
    ['price asc, cust', (x) => x.sort(['price', 'cust']).limit(25)],
    ['rising desc (newest first)', (x) => x.sort('-rising').limit(10)],
    ['rising asc', (x) => x.sort('rising').limit(10)],
    ['ts desc', (x) => x.sort('-ts').limit(7)],
    ['filter + price desc, ts', (x) => x.filter("region == 'US' && qty >= 5").sort(['-price', 'ts']).limit(10)],
    ['offset', (x) => x.sort(['-price', 'ts']).limit(5, 20)],
    ['k bigger than the matches', (x) => x.filter('qty == 9 && cust < 3').sort(['-price', 'ts']).limit(500)],
    ['no matches', (x) => x.filter('qty > 100').sort('-price').limit(10)],
    ['derived first key', (x) => x.derive('rev', 'price * qty').sort(['-rev', 'ts']).limit(10)],
    ['derived second key', (x) => x.derive('r', 'floor(cust / 7)').sort(['-price', 'r', 'ts']).limit(10)],
    ['text key first', (x) => x.sort(['region', '-price', 'ts']).limit(10)],
    ['text key second', (x) => x.sort(['-price', 'region', 'ts']).limit(10)],
    ['free text column out', (x) => x.sort(['-price', 'ts']).limit(10).select('note', 'price')],
    ['qty desc, rising desc (missing in the second key)', (x) => x.sort(['-qty', '-rising']).limit(10)],
    ['price, rising, cust', (x) => x.sort(['price', 'rising', '-cust']).limit(30)],
    ['qty, -price, -ts (offset)', (x) => x.filter("region != 'EU'").sort(['qty', '-price', '-ts']).limit(10, 3)],
    ['expression keys', (x) => x.sort(['-(qty * 2)', '-(price + 0)', 'ts']).limit(10)],
    ['big k', (x) => x.sort(['-price', 'ts']).limit(2000)],
    ['k = 0', (x) => x.sort('-price').limit(0)],
  ];
  for (const [name, q] of QS) {
    const want = norm(q(t.lazy()).toRows());
    check(norm(q(ds.lazy()).toRows()) === want, `${label} sync: ${name}`);
    check(norm(await q(ds.lazy()).toRowsAsync()) === want, `${label} async: ${name}`);
  }
  // SQL (the benchmark's shape)
  const s = "SELECT cust, price, qty FROM orders WHERE region = 'US' ORDER BY price DESC, ts LIMIT 10";
  check(norm(await sql(s, { orders: ds }).toRowsAsync()) === norm(sql(s, { orders: t }).toRows()), `${label} sql`);
  // pruning: the best rows sit in one chunk; a sorted key reads one chunk
  await ds.sort(['-price', 'ts']).limit(10).toRowsAsync();
  check(ds.lastRun.read < ds.chunkCount, `${label} chunks skipped by the k-th row: ${JSON.stringify(ds.lastRun)}`);
  await ds.sort('rising').limit(10).toRowsAsync();
  check(ds.lastRun.read === 1, `${label} sorted key reads one chunk: ${JSON.stringify(ds.lastRun)}`);
  // the same from a Blob and from a URL (range requests, neighbouring reads coalesced)
  const { openDatasetAsync, urlFile } = await import((SRC.startsWith('/') ? 'file://' + SRC : SRC).replace(/\/?$/, '/') + 'dataset.mjs');
  const bytes = readFileSync(path);
  const fakeFetch = async (url, { headers }) => {
    const [a, b] = headers.Range.slice(6).split('-').map(Number);
    const body = bytes.subarray(a, b + 1);
    return { status: 206, headers: { get: (h) => (h === 'content-range' ? `bytes ${a}-${b}/${bytes.length}` : null) }, body: null, arrayBuffer: async () => body.buffer.slice(body.byteOffset, body.byteOffset + body.length) };
  };
  for (const [how, open] of [['blob', () => openDatasetAsync(e, new Blob([bytes]))], ['url', async () => openDatasetAsync(e, await urlFile('http://x/t.vectro', { fetch: fakeFetch }))]]) {
    const d = await open();
    for (const [name, q] of QS.slice(0, 8)) check(norm(await q(d.lazy()).toRowsAsync()) === norm(q(t.lazy()).toRows()), `${label} ${how}: ${name}`);
    await d.close();
  }
  await ds.close();
  t.dispose();
  await e.close();
}
// float32 datasets: the k-th row's values and the block min / max are the same float32 numbers
{
  const e = new Engine();
  const t32 = e.table({ x: Float64Array.from({ length: 400_000 }, (_, i) => ((i * 7919) % 100_003) / 3), y: Float64Array.from({ length: 400_000 }, (_, i) => (i % 11 ? i / 7 : NaN)) }, { type: Float32Array });
  const p32 = join(dir, 'f32.vectro');
  e.writeDataset(p32, t32, { rowsPerChunk: 140_000 });
  const d32 = e.openDataset(p32);
  for (const by of [['-x', 'y'], ['x', '-y'], ['-y', 'x'], ['y']]) {
    check(norm(await d32.sort(by).limit(12).toRowsAsync()) === norm(t32.lazy().sort(by).limit(12).toRows()), `float32 ${by}`);
  }
  await d32.close(); t32.dispose(); await e.close();
}
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} of ${checks} top-k dataset checks FAILED` : `all ${checks} top-k dataset checks passed`);
process.exit(failures ? 1 : 0);
