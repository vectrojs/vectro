// Datasets on disk: every query gives the answer the same table gives in
// memory, read chunk by chunk (small chunks here, so there are many), on
// every engine; chunks the filters rule out are skipped; writers append,
// buffer and convert CSV of any size; errors are clear.
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, sql, Dataset } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';
import { tuning, stats, writeDataset, DatasetWriter, csvToDataset } from '../src/dataset.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 20) console.log('FAIL', msg); };
const check = (ok, msg) => { checks++; if (!ok) fail(msg); };
let seed = 21;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const dir = mkdtempSync(join(tmpdir(), 'vectro-ds-'));
const file = (name) => join(dir, name);
// (sums add in a different order chunk by chunk: compare 12 digits)
const norm = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' ? (v !== v ? 'NaN' : Number(v.toPrecision(12))) : v));
const same = (a, b, msg) => { const x = norm(a), y = norm(b); check(x === y, `${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); };

const N = 60_000;
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const cols = {
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 3000)),
  price: Float64Array.from({ length: N }, () => (rnd() < 0.03 ? NaN : Math.round(rnd() * 100000) / 100)),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  region: Array.from({ length: N }, (_, i) => (i < N / 3 ? REG[Math.floor(rnd() * 3)] : REG[Math.floor(rnd() * 5)])), // later chunks add labels
  ts: Array.from({ length: N }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 60_000)),
  noise: Float64Array.from({ length: N }, () => rnd()),
};
const customers = { cust: Float64Array.from({ length: 2500 }, (_, i) => i), tier: Float64Array.from({ length: 2500 }, (_, i) => 1 + (i % 3)), name: Array.from({ length: 2500 }, (_, i) => `c${i % 700}`) };

// [label, query on a table-like source (Table or Dataset), joined table]
const QUERIES = [
  ['totals', (t) => t.agg({ s: 'sum(price * qty)', n: 'count()', np: 'count(price)', m: 'mean(price)', lo: 'min(price)', hi: 'max(price)', d: 'countDistinct(cust)' })],
  ['filtered totals', (t) => t.filter("region == 'EU' && price > 500").agg({ s: 'sum(price)', c: 'count()', m: 'mean(qty)' })],
  ['time slice (skips chunks)', (t) => t.filter("ts >= '2024-01-20' && ts < '2024-01-25'").agg({ c: 'count()', h: 'sum(hour(ts))' })],
  ['nothing matches', (t) => t.filter('qty > 100').agg({ c: 'count()', s: 'sum(price)', lo: 'min(price)', m: 'mean(price)', d: 'countDistinct(cust)' })],
  ['group by label', (t) => t.lazy().groupBy('region').agg({ s: 'sum(price * qty)', m: 'mean(price)', lo: 'min(price)', n: 'count()', d: 'countDistinct(cust)' }).toRows()],
  ['group by decimals (missing last)', (t) => t.lazy().groupBy('price').agg({ n: 'count()', q: 'sum(qty)', m: 'mean(qty)', lo: 'min(cust)' }).toRows()],
  ['group by decimals, top', (t) => t.lazy().filter('qty > 2').groupBy(['price', 'region']).agg({ n: 'count()' }).sort(['-n', 'price', 'region']).limit(15).toRows()],
  ['group by many keys', (t) => t.lazy().groupBy('cust').agg({ s: 'sum(qty)', hi: 'max(price)', m: 'mean(price)' }).toRows()],
  ['group by two keys', (t) => t.lazy().groupBy(['region', 'qty']).agg({ n: 'count()', d: 'countDistinct(cust)' }).toRows()],
  ['group by day, filtered', (t) => t.lazy().filter('price > 900').derive('day', 'dayStart(ts)').groupBy('day').agg({ n: 'count()' }).toRows()],
  ['distinct: bitmap and pairs', (t) => t.lazy().derive('q2', 'qty * 2').groupBy(['region', 'qty']).agg({ a: 'countDistinct(price)', b: 'countDistinct(cust)', c: 'countDistinct(q2)', n: 'count()' }).toRows()],
  ['distinct of a filtered key', (t) => t.lazy().filter('qty > 4 && price > 300').groupBy('qty').agg({ b: 'countDistinct(cust)', r: 'countDistinct(region)' }).toRows()],
  ['group nothing', (t) => t.lazy().filter('qty > 100').groupBy('region').agg({ n: 'count()' }).toRows()],
  ['top', (t) => t.top(12, ['-price', 'cust']).toRows()],
  ['top by expression, offset', (t) => t.lazy().derive('rev', 'price * qty').sort('-rev').limit(7, 3).toRows()],
  ['head', (t) => t.lazy().limit(5).toRows()],
  ['limit offset over chunks', (t) => t.lazy().filter('qty == 9').limit(4, 2000).toRows()],
  ['collect', (t) => t.lazy().filter('cust < 20 && qty >= 8').select('cust', 'price', 'region').toRows()],
  ['derive + collect', (t) => t.lazy().derive('rev', 'price * qty').filter('rev > 8800').select('rev', 'ts').toRows()],
  ['full sort', (t) => t.lazy().filter('cust == 7').sort(['-qty', 'ts']).toRows()],
  ['join', (t, c) => t.lazy().join(c, { on: 'cust' }).filter('tier == 2 && qty == 9').groupBy('name').agg({ s: 'sum(price)' }).sort('-s').limit(5).toRows()],
  ['left join rows', (t, c) => t.lazy().filter('cust >= 2495 && qty > 6').join(c, { on: 'cust', how: 'left' }).select('cust', 'name', 'tier').toRows()],
  ['having (sql)', (t, c) => sql('SELECT region, COUNT(*) AS n, AVG(price) AS ap FROM o WHERE qty > 2 GROUP BY region HAVING COUNT(*) > 8000 ORDER BY n DESC', { o: t }).toRows()],
  ['sql join', (t, c) => sql('SELECT c.tier, SUM(o.qty) AS q FROM o JOIN c ON o.cust = c.cust GROUP BY c.tier ORDER BY 1', { o: t, c }).toRows()],
  ['sql distinct', (t) => sql('SELECT DISTINCT region FROM o ORDER BY region', { o: t }).toRows()],
  ['count', (t) => [t.count(), t.filter('qty == 3').count()]],
  ['approx is exact here', (t) => (t.isDataset ? t.agg({ d: 'approxDistinct(cust)' }).d : t.agg({ d: 'countDistinct(cust)' }).d)],
];

const engines = [['threads', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['interpreter', () => new PortableEngine({ jit: false })]];
for (const [label, make] of engines) {
  const e = make();
  const t = e.table(cols);
  const c = e.table(customers);
  const path = file(`orders-${label}.vectro`);
  // appended in uneven pieces: the writer fills whole chunks
  const w = new DatasetWriter(e, path, { rowsPerChunk: 7_000 });
  for (let lo = 0; lo < N; ) { const hi = Math.min(N, lo + 1 + Math.floor(rnd() * 15_000)); w.append(Object.fromEntries(Object.entries(cols).map(([k, v]) => [k, v.slice(lo, hi)]))); lo = hi; }
  const summary = w.close();
  check(summary.rows === N && summary.chunks === Math.ceil(N / 7_000), `${label}: summary ${JSON.stringify(summary)}`);
  const ds = new Dataset(e, path);
  check(ds.length === N && ds.columnNames.join() === Object.keys(cols).join(), `${label}: shape`);
  check(summary.bytes < N * 8 * 6 * 0.45, `${label}: ${summary.bytes} bytes on disk (packed?)`);
  // (500: partial groups merged again and again; dense from 1: the many-group
  // path everywhere; spill at 2000: partial groups on disk, partitions over
  // 3000 rows split again)
  for (const [mergeRows, denseMin, spillRows, partitionRows] of [[1 << 20], [500], [1 << 20, 1], [500, undefined, 2000, 3000]]) {
    Object.assign(tuning, { mergeRows, denseMin, spillRows: spillRows ?? 1 << 23, partitionRows: partitionRows ?? 1 << 24 });
    for (const [name, q] of QUERIES) {
      let got, want;
      try { got = q(ds, c); want = q(t, c); } catch (err) { fail(`${label} ${name}: ${err.stack}`); continue; }
      same(got, want, `${label} ${name} (merge at ${mergeRows}, dense from ${denseMin}, spill at ${spillRows})`);
    }
  }
  Object.assign(tuning, { mergeRows: 1 << 20, denseMin: undefined, spillRows: 1 << 23, partitionRows: 1 << 24 });
  check(stats.spills > 0 && stats.resplits > 0, `${label}: spilled ${JSON.stringify(stats)}`);
  // chunks ruled out by the filter aren't read; only used columns are
  ds.filter("ts >= '2024-01-20' && ts < '2024-01-25'").count();
  check(ds.lastRun.skipped >= ds.chunkCount - 3 && ds.lastRun.read <= 3, `${label}: skipped ${JSON.stringify(ds.lastRun)}`);
  ds.agg({ q: 'sum(qty)' });
  check(ds.lastRun.bytesRead <= N / 2 + 4 * ds.chunkCount, `${label}: four bits per qty (1..9), read ${ds.lastRun.bytesRead}`);
  check(ds.meta.chunks.every((ch) => ch.cols.qty.bits === 4 && ch.cols.cust.bits === 12 && ch.cols.price.bits === 17 && ch.cols.price.bmissing !== undefined), `${label}: bit widths ${JSON.stringify(ds.meta.chunks[0].cols.price)}`);
  ds.lazy().limit(3).toRows();
  check(ds.lastRun.read <= 2, `${label}: limit read ${ds.lastRun.read} chunks`);
  // async (the next chunk is read during the current one) gives the same
  same(await ds.lazy().groupBy('region').agg({ s: 'sum(price)', d: 'countDistinct(cust)' }).toRowsAsync(), t.lazy().groupBy('region').agg({ s: 'sum(price)', d: 'countDistinct(cust)' }).toRows(), `${label} async group`);
  same(await ds.filter('qty == 4').aggAsync({ m: 'mean(price)' }), t.filter('qty == 4').agg({ m: 'mean(price)' }), `${label} async totals`);
  same(await ds.lazy().join(c, { on: 'cust' }).top(3, '-price').toRowsAsync(), t.lazy().join(c, { on: 'cust' }).top(3, '-price').toRows(), `${label} async join`);
  check(ds.explain().includes('scan dataset'), `${label}: explain`);
  // a dataset on the right of a join, and two datasets
  const cpath = file(`customers-${label}.vectro`);
  writeDataset(e, cpath, c, { rowsPerChunk: 1000 });
  const cds = new Dataset(e, cpath);
  same(t.lazy().filter('qty == 2').join(cds, { on: 'cust' }).groupBy('tier').agg({ n: 'count()' }).toRows(), t.lazy().filter('qty == 2').join(c, { on: 'cust' }).groupBy('tier').agg({ n: 'count()' }).toRows(), `${label} dataset on the right`);
  same(ds.lazy().filter('qty == 2').join(cds.lazy().filter('tier != 3'), { on: 'cust' }).groupBy('tier').agg({ n: 'count()' }).toRows(), t.lazy().filter('qty == 2').join(c.lazy().filter('tier != 3'), { on: 'cust' }).groupBy('tier').agg({ n: 'count()' }).toRows(), `${label} two datasets`);
  // both sides too big for memory (made small here): partitioned by key range
  Object.assign(tuning, { joinRows: 300 });
  {
    const before = stats.joinPartitions;
    const sorted = (rows) => rows.map((r) => JSON.stringify(r)).sort();
    same(ds.lazy().filter('qty == 2').join(cds.lazy().filter('tier != 3'), { on: 'cust' }).groupBy('tier').agg({ n: 'count()', s: 'sum(price)' }).toRows(), t.lazy().filter('qty == 2').join(c.lazy().filter('tier != 3'), { on: 'cust' }).groupBy('tier').agg({ n: 'count()', s: 'sum(price)' }).toRows(), `${label} partitioned join, groups`);
    same(sorted(ds.lazy().filter('cust >= 2400 && qty > 6').join(cds, { on: 'cust', how: 'left' }).select('cust', 'ts', 'name', 'tier').toRows()), sorted(t.lazy().filter('cust >= 2400 && qty > 6').join(c, { on: 'cust', how: 'left' }).select('cust', 'ts', 'name', 'tier').toRows()), `${label} partitioned left join, rows`);
    same(ds.lazy().join(cds, { on: 'cust' }).top(5, ['-price', 'ts']).toRows(), t.lazy().join(c, { on: 'cust' }).top(5, ['-price', 'ts']).toRows(), `${label} partitioned join, top`);
    same(ds.lazy().filter('qty > 100').join(cds, { on: 'cust' }).agg({ n: 'count()', s: 'sum(price)' }), { n: 0, s: 0 }, `${label} partitioned join, no rows`);
    check(stats.joinPartitions - before === 4, `${label}: partitioned ${stats.joinPartitions - before} of 4 joins`);
  }
  Object.assign(tuning, { joinRows: 1 << 24 });

  // a query over a dataset written as a dataset (chunk by chunk), and an aggregate
  const fpath = file(`filtered-${label}.vectro`);
  const fs = writeDataset(e, fpath, ds.lazy().filter("region == 'MEA'").derive('rev', 'price * qty').select('ts', 'rev', 'region'), { rowsPerChunk: 3000 });
  const fds = new Dataset(e, fpath);
  same(fds.toRows(), t.lazy().filter("region == 'MEA'").derive('rev', 'price * qty').select('ts', 'rev', 'region').toRows(), `${label} filtered dataset (${fs.chunks} chunks)`);
  writeDataset(e, file(`agg-${label}.vectro`), ds.groupBy('qty').agg({ n: 'count()' }));
  same(new Dataset(e, file(`agg-${label}.vectro`)).toRows(), t.lazy().groupBy('qty').agg({ n: 'count()' }).toRows(), `${label} aggregate as dataset`);
  // whole-byte codes (bitPack: false) answer the same, in a bigger file
  {
    const bpath = file(`bytes-${label}.vectro`);
    const sb = writeDataset(e, bpath, t, { rowsPerChunk: 7_000, bitPack: false });
    const bds = new Dataset(e, bpath);
    check(sb.bytes > summary.bytes && bds.meta.chunks.every((ch) => Object.values(ch.cols).every((m) => !m.bits)), `${label}: bytes file ${sb.bytes} vs ${summary.bytes}`);
    for (const [name, q] of QUERIES.slice(0, 8)) same(q(bds, c), q(t, c), `${label} ${name} (whole bytes)`);
    await bds.close();
  }

  // a sorted dataset written in partitions (the sort needn't fit in memory)
  Object.assign(tuning, { partitionRows: 9000 });
  for (const [by, f] of [[['-price', 'ts'], 'qty > 3'], [['cust', '-ts'], 'qty > 0'], [['price'], 'qty > 8'], [['-noise'], 'qty > 100']]) {
    const before = stats.spills;
    const spath = file(`sorted-${label}.vectro`);
    writeDataset(e, spath, ds.lazy().filter(f).select('cust', 'price', 'ts', 'noise', 'region').sort(by), { rowsPerChunk: 4000 });
    const sds = new Dataset(e, spath);
    same(sds.toRows(), t.lazy().filter(f).select('cust', 'price', 'ts', 'noise', 'region').sort(by).toRows(), `${label} sorted dataset by ${by} where ${f}`);
    check(stats.spills > before || !sds.length, `${label}: sort by ${by} spilled`);
    await sds.close();
  }
  Object.assign(tuning, { partitionRows: 1 << 24 });

  // append to an existing dataset: new labels, same columns
  const apath = file(`append-${label}.vectro`);
  writeDataset(e, apath, e.table({ k: ['a', 'b'], v: [1, 2] }));
  const aw = new DatasetWriter(e, apath, { append: true });
  aw.append({ k: ['c', 'a', 'd'], v: [3, NaN, 5] });
  aw.close();
  const ads = new Dataset(e, apath);
  same(ads.toRows(), [{ k: 'a', v: 1 }, { k: 'b', v: 2 }, { k: 'c', v: 3 }, { k: 'a', v: NaN }, { k: 'd', v: 5 }], `${label} append`);
  same(ads.groupBy('k').agg({ s: 'sum(v)', n: 'count(v)' }).toRows(), [{ k: 'a', s: 1, n: 1 }, { k: 'b', s: 2, n: 1 }, { k: 'c', s: 3, n: 1 }, { k: 'd', s: 5, n: 1 }], `${label} append groups`);
  // a join hashes the other table once for all chunks (not once per chunk)
  for (const how of ['inner', 'left']) {
    const b0 = e._joinBuildCount ?? 0;
    ds.lazy().filter('qty > 3').join(c, { on: 'cust', how }).groupBy('tier').agg({ n: 'count()' }).toRows();
    check((e._joinBuildCount ?? 0) - b0 === 1 && ds.lastRun.read > 5, `${label}: ${how} join built ${(e._joinBuildCount ?? 0) - b0} times over ${ds.lastRun.read} chunks`);
  }
  // queries don't leave engine memory behind
  const before = e.sizes?.size;
  for (let i = 0; i < 5; i++) for (const [, q] of QUERIES) q(ds, c);
  check(!e.sizes || e.sizes.size === before, `${label}: engine memory blocks ${before} -> ${e.sizes?.size}`);
  for (const d of [ds, cds, fds, ads]) await d.close();
  await e.close();
}

// inside a chunk, only the blocks of rows that can match are read
{
  const e = new Engine();
  const n = 400_000;
  const t = e.table({
    ts: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 1000)),
    v: Float64Array.from({ length: n }, (_, i) => (i % 13) / 4),
    raw: Float64Array.from({ length: n }, () => rnd()), // (stays plain: not packable)
    note: Array.from({ length: n }, (_, i) => `n${i % 50}`),
  });
  const text = e.parseCSV('id,s\n' + Array.from({ length: 150_000 }, (_, i) => `${i},"row ${i}"`).join('\n'), { types: { s: 'string' } });
  writeDataset(e, file('blocks.vectro'), t, { rowsPerChunk: 300_000 });
  writeDataset(e, file('blocks-text.vectro'), text, { rowsPerChunk: 140_000 });
  const d = e.openDataset(file('blocks.vectro')), dt = e.openDataset(file('blocks-text.vectro'));
  for (const f of ["ts >= '2024-01-03T10:00:00Z' && ts < '2024-01-03T11:00:00Z'", "ts >= '2024-01-04T12:00:00Z'", "ts < '2024-01-01T00:30:00Z' && v > 2"]) {
    same(d.filter(f).agg({ n: 'count()', s: 'sum(v)', r: 'sum(raw)', m: 'max(raw)' }), t.filter(f).agg({ n: 'count()', s: 'sum(v)', r: 'sum(raw)', m: 'max(raw)' }), `blocks: ${f}`);
    same(d.lazy().filter(f).select('ts', 'raw', 'note').toRows(), t.lazy().filter(f).select('ts', 'raw', 'note').toRows(), `blocks rows: ${f}`);
  }
  d.filter("ts >= '2024-01-03T10:00:00Z' && ts < '2024-01-03T11:00:00Z'").count();
  check(d.lastRun.rows <= 2 * 65536 && d.lastRun.read === 1, `blocks read: ${JSON.stringify(d.lastRun)}`);
  same(dt.lazy().filter('id >= 139990 && id < 140010').toRows(), text.lazy().filter('id >= 139990 && id < 140010').toRows(), 'blocks with free text');
  check(dt.lastRun.rows < 140_000, `text blocks read: ${JSON.stringify(dt.lastRun)}`);
  await d.close(); await dt.close();
  await e.close();
}

// float32 tables, free text, an empty dataset, an all-missing column
{
  const e = new Engine();
  const t32 = e.table({ x: Float64Array.from({ length: 5000 }, (_, i) => i / 4), y: Float64Array.from({ length: 5000 }, (_, i) => (i % 7 ? i : NaN)) }, { type: Float32Array });
  e.writeDataset(file('f32.vectro'), t32, { rowsPerChunk: 999 });
  const d32 = e.openDataset(file('f32.vectro'));
  same(d32.filter('x > 100.5').agg({ s: 'sum(x)', m: 'max(y)', n: 'count(y)' }), t32.filter('x > 100.5').agg({ s: 'sum(x)', m: 'max(y)', n: 'count(y)' }), 'float32');
  check(d32.type === Float32Array, 'float32 type kept');
  const csv = 'id,note,city\n' + Array.from({ length: 3000 }, (_, i) => `${i},"note ${i}, with ""quotes""",${['Oslo', 'Rome', 'Lima'][i % 3]}`).join('\n');
  const tt = e.parseCSV(csv, { types: { note: 'string' } });
  e.writeDataset(file('text.vectro'), tt, { rowsPerChunk: 700 });
  const dt = e.openDataset(file('text.vectro'));
  same(dt.lazy().filter("city == 'Rome' && id > 2900").select('id', 'note').toRows(), tt.lazy().filter("city == 'Rome' && id > 2900").select('id', 'note').toRows(), 'free text');
  same(dt.top(3, '-id').toRows(), tt.top(3, '-id').toRows(), 'free text top');
  const empty = e.table({ a: new Float64Array(0), b: [] });
  e.writeDataset(file('empty.vectro'), empty);
  const de = e.openDataset(file('empty.vectro'));
  same([de.length, de.count(), de.agg({ s: 'sum(a)', m: 'min(a)' }), de.toRows()], [0, 0, { s: 0, m: NaN }, []], 'empty dataset');
  const miss = e.table({ a: [NaN, NaN, NaN], b: [1, 2, 3] });
  e.writeDataset(file('missing.vectro'), miss, { rowsPerChunk: 2 });
  const dm = e.openDataset(file('missing.vectro'));
  same([dm.filter('a > 0').count(), dm.filter('b >= 2').agg({ m: 'max(a)', n: 'count()' })], [0, { m: NaN, n: 2 }], 'all missing');
  for (const d of [d32, dt, de, dm]) await d.close();
  await e.close();
}

// CSV of any size, in small pieces (quoted newlines across piece ends)
{
  const e = new Engine();
  const lines = ['id,city,amount,day,comment'];
  for (let i = 0; i < 20_000; i++) lines.push(`${i},${['Oslo', 'Rome', 'Lima', 'Pune'][Math.floor(rnd() * (i < 10_000 ? 2 : 4))]},${(i % 1000) / 8},2024-0${1 + (i % 9)}-1${i % 10},${i % 97 === 0 ? '"two\nlines, quoted"' : 'plain'}`);
  const csvPath = file('big.csv');
  writeFileSync(csvPath, lines.join('\n') + '\n');
  const s = csvToDataset(e, csvPath, file('csv.vectro'), { pieceBytes: 4096, rowsPerChunk: 1500, types: { comment: 'category' } });
  const mem = e.readCSV(csvPath, { types: { comment: 'category' } });
  const dc = e.openDataset(file('csv.vectro'));
  check(s.rows === 20_000 && dc.length === 20_000, `csv rows ${s.rows}`);
  check(dc.cols.get('day').date && dc.cols.get('city').dict, 'csv types (date, category)');
  same(dc.lazy().groupBy('city').agg({ s: 'sum(amount)', n: 'count()' }).sort('city').toRows(), mem.lazy().groupBy('city').agg({ s: 'sum(amount)', n: 'count()' }).sort('city').toRows(), 'csv groups');
  same(dc.filter("day >= '2024-05-01' && comment == 'two\nlines, quoted'").count(), mem.filter("day >= '2024-05-01' && comment == 'two\nlines, quoted'").count(), 'csv quoted newlines');
  same(dc.toRows(3), mem.toRows(3), 'csv first rows');
  // async: the same dataset
  const sa = await e.csvToDatasetAsync(csvPath, file('csv-async.vectro'), { pieceBytes: 4096, rowsPerChunk: 1500, types: { comment: 'category' } });
  const da = e.openDataset(file('csv-async.vectro'));
  check(sa.rows === s.rows && sa.chunks === s.chunks, `async csv ${JSON.stringify(sa)} vs ${JSON.stringify(s)}`);
  same(da.toRows(), dc.toRows(), 'async csv rows');
  await da.close();
  await dc.close();
  await e.close();
}

// read-only datasets from a Blob or a URL (as in browsers): async reads only
{
  const { createServer } = await import('node:http');
  const { openDatasetAsync } = await import('../src/dataset.mjs');
  const e = new Engine(), single = new PortableEngine({ threads: 1 });
  const n = 300_000;
  const t = e.table({ ts: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 1000)), v: Float64Array.from({ length: n }, (_, i) => (i % 97) / 4), k: Array.from({ length: n }, (_, i) => ['a', 'b', 'c'][i % 3]) });
  const path = file('remote.vectro');
  writeDataset(e, path, t, { rowsPerChunk: 100_000 });
  const bytes = readFileSync(path);
  let requests = 0, served = 0;
  const server = createServer((req, res) => { // (a static file server with Range support)
    requests++;
    const m = /bytes=(\d+)-(\d+)/.exec(req.headers.range ?? '');
    if (!m) { res.writeHead(200, { 'content-length': bytes.length }); res.end(bytes); return; }
    const a = +m[1], b = Math.min(+m[2], bytes.length - 1);
    served += b - a + 1;
    res.writeHead(206, { 'content-range': `bytes ${a}-${b}/${bytes.length}`, 'content-length': b - a + 1 });
    res.end(bytes.subarray(a, b + 1));
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const url = `http://127.0.0.1:${server.address().port}/remote.vectro`;
  const Q = [
    (d) => d.filter("ts >= '2024-01-03T12:00:00Z' && k == 'b'").aggAsync({ n: 'count()', s: 'sum(v)' }),
    (d) => d.lazy().groupBy('k').agg({ n: 'count()', m: 'max(v)' }).toRowsAsync(),
    (d) => d.lazy().top(5, ['-v', 'ts']).toRowsAsync(),
  ];
  const want = [];
  for (const q of Q) want.push(await q(t));
  for (const [label, eng] of [['threads', e], ['single thread', single]]) {
    for (const [kind, src] of [['blob', new Blob([bytes])], ['url', url]]) {
      const d = await openDatasetAsync(eng, src);
      for (let i = 0; i < Q.length; i++) same(await Q[i](d), want[i], `${label} ${kind} query ${i}`);
      let msg = '';
      try { d.agg({ n: 'count()' }); } catch (err) { msg = err.message; }
      check(msg.includes('asynchronously'), `${label} ${kind}: sync call says use async (${msg})`);
      await d.close();
    }
  }
  // a time range reads a slice of the file, not all of it
  served = 0;
  const d = await openDatasetAsync(e, url);
  same(await d.filter("ts >= '2024-01-04T00:00:00Z' && ts < '2024-01-04T01:00:00Z'").aggAsync({ n: 'count()' }), { n: 3600 }, 'url time slice');
  check(served < bytes.length / 4, `url time slice read ${served} of ${bytes.length} bytes`);
  await d.close();
  server.close();
  await e.close();
  await single.close();
}

// writing without blocking, to a file that writes in the background (as
// browser files do): bytes queue in order, appendAsync / closeAsync wait
{
  const { writeDatasetAsync, csvToDatasetAsync, createDatasetAsync, openDatasetAsync } = await import('../src/dataset.mjs');
  const { memoryFile } = await import('../src/files-browser.mjs');
  const asyncFile = (name, before = null) => {
    const parts = [];
    let size = before?.length ?? 0, queued = 0, peak = 0, chain = Promise.resolve(), closed = false, aborted = false;
    const later = (fn) => { chain = chain.then(() => new Promise((r) => setTimeout(r, 0))).then(fn); };
    return {
      name, existed: !!before, size,
      async read(u8, pos) { u8.set(before.subarray(pos, pos + u8.length)); },
      write(u8, pos) { const d = u8.slice(); queued += d.length; peak = Math.max(peak, queued); later(() => { parts.push([pos, d]); size = Math.max(size, pos + d.length); queued -= d.length; }); },
      truncate(n) { later(() => { size = n; }); },
      close() { later(() => { closed = true; }); },
      abort() { aborted = true; },
      flush: () => chain,
      done: () => chain,
      bytes() { const out = new Uint8Array(size); if (before) out.set(before.subarray(0, Math.min(before.length, size))); for (const [p, d] of parts) out.set(d.subarray(0, Math.max(0, Math.min(d.length, size - p))), p); return out; },
      stats: () => ({ peak, closed, aborted }),
    };
  };
  const e = new Engine();
  const n = 120_000;
  const t1 = e.table({ id: Float64Array.from({ length: n }, (_, i) => i), r: Array.from({ length: n }, (_, i) => ['x', 'y'][i % 2]), ts: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 60_000)) });
  const t2 = e.table({ id: Float64Array.from({ length: 1000 }, (_, i) => n + i), r: Array.from({ length: 1000 }, () => 'z'), ts: Array.from({ length: 1000 }, (_, i) => new Date(Date.UTC(2025, 0, 1) + i * 1000)) });
  // a table
  const f = asyncFile('a.vectro');
  const s1 = await writeDatasetAsync(e, f, t1, { rowsPerChunk: 20_000 });
  check(f.stats().closed && s1.rows === n && s1.bytes === f.bytes().length, `async write ${JSON.stringify(s1)} ${JSON.stringify(f.stats())}`);
  const d1 = await openDatasetAsync(e, new Blob([f.bytes()]));
  same(await d1.lazy().groupBy('r').agg({ c: 'count()', m: 'max(id)' }).toRowsAsync(), t1.lazy().groupBy('r').agg({ c: 'count()', m: 'max(id)' }).toRows(), 'async-written dataset');
  // appended to (its footer read asynchronously)
  const g = asyncFile('a.vectro', f.bytes());
  const w = await createDatasetAsync(e, g, { rowsPerChunk: 20_000 });
  await w.appendAsync(t2);
  await w.closeAsync();
  const d2 = await openDatasetAsync(e, new Blob([g.bytes()]));
  same([d2.length, await d2.lazy().groupBy('r').agg({ c: 'count()' }).toRowsAsync()], [n + 1000, [{ r: 'x', c: n / 2 }, { r: 'y', c: n / 2 }, { r: 'z', c: 1000 }]], 'async append');
  // a query over a dataset, streamed: only about a chunk waits at a time
  const h = asyncFile('b.vectro');
  await writeDatasetAsync(e, h, d1.lazy().filter("r == 'y'").select('id', 'ts'), { rowsPerChunk: 5_000 });
  const d3 = await openDatasetAsync(e, new Blob([h.bytes()]));
  same(await d3.aggAsync({ c: 'count()', s: 'sum(id)' }), t1.filter("r == 'y'").agg({ c: 'count()', s: 'sum(id)' }), 'async streamed query');
  check(h.stats().peak < 200_000, `queued at most ${h.stats().peak} bytes`);
  // a CSV given as a Blob
  const csv = 'k,v,day\n' + Array.from({ length: 30_000 }, (_, i) => `${['p', 'q', 'r'][i % 3]},${i / 4},2024-0${1 + (i % 9)}-1${i % 10}`).join('\n');
  const c = asyncFile('c.vectro');
  const sc = await csvToDatasetAsync(e, new Blob([csv]), c, { pieceBytes: 50_000, rowsPerChunk: 4_000 });
  const d4 = await openDatasetAsync(e, new Blob([c.bytes()]));
  same([sc.rows, await d4.lazy().groupBy('k').agg({ s: 'sum(v)', n: 'count()' }).toRowsAsync()], [30_000, e.parseCSV(csv).lazy().groupBy('k').agg({ s: 'sum(v)', n: 'count()' }).toRows()], 'async csv from a Blob');
  check(d4.cols.get('day').date, 'csv date column from a Blob');
  // in memory (browsers without file storage)
  const m = memoryFile();
  await writeDatasetAsync(e, m, t2);
  same((await openDatasetAsync(e, m.blob())).length, 1000, 'memory file');
  // an error on the way: nothing is committed
  const bad = asyncFile('bad.vectro');
  let msg = '';
  try { await writeDatasetAsync(e, bad, d1.lazy().filter('nope > 1')); } catch (err) { msg = err.message; }
  check(msg.includes('nope') && bad.stats().aborted && !bad.stats().closed, `aborted write ${msg} ${JSON.stringify(bad.stats())}`);
  for (const d of [d1, d2, d3, d4]) await d.close();
  await e.close();
}

// spilling through temporary files that open, write and read asynchronously
// (as in browsers: datasets from Blobs, temp files in browser storage)
{
  const { setTempStorage, openDatasetAsync, writeDatasetAsync } = await import('../src/dataset.mjs');
  const { memoryFile } = await import('../src/files-browser.mjs');
  const files = new Map();
  let dirs = 0;
  const later = () => new Promise((r) => setTimeout(r, 0));
  const store = {
    async dir(prefix) { await later(); return `${prefix}${dirs++}`; },
    join: (d, name) => `${d}/${name}`,
    async openWrite(path) {
      await later();
      const m = memoryFile(path);
      let chain = Promise.resolve();
      return { ...m, write(u8, pos) { const d = u8.slice(); chain = chain.then(later).then(() => m.write(d, pos)); }, truncate(n) { chain = chain.then(() => m.truncate(n)); }, close() { chain = chain.then(async () => { files.set(path, new Uint8Array(await m.blob().arrayBuffer())); }); }, abort() {}, flush: () => chain, done: () => chain };
    },
    async openRead(path) { await later(); const b = files.get(path); return { name: path, size: b.length, async read(u8, pos) { u8.set(b.subarray(pos, pos + u8.length)); }, close() {} }; },
    async remove(path) { for (const k of [...files.keys()]) if (k === path || k.startsWith(`${path}/`)) files.delete(k); },
  };
  setTempStorage(store);
  const e = new Engine();
  const n = 40_000;
  const t = e.table({ k: Float64Array.from({ length: n }, (_, i) => ((i * 7919) % 9973) / 4), g: Array.from({ length: n }, (_, i) => ['a', 'b', 'c', 'd'][i % 4]), v: Float64Array.from({ length: n }, (_, i) => i % 13) });
  const acc = e.table({ k: Float64Array.from({ length: 9973 }, (_, i) => i / 4), seg: Float64Array.from({ length: 9973 }, (_, i) => i % 5) });
  const asBlob = (x, rows) => { writeDataset(e, file('tmp-blob.vectro'), x, { rowsPerChunk: rows }); return new Blob([readFileSync(file('tmp-blob.vectro'))]); };
  const d = await openDatasetAsync(e, asBlob(t, 4_000)), da = await openDatasetAsync(e, asBlob(acc, 2_000));
  Object.assign(tuning, { mergeRows: 500, spillRows: 2_000, partitionRows: 3_000, joinRows: 1_000 });
  const before = { ...stats };
  same(await d.lazy().groupBy('k').agg({ n: 'count()', s: 'sum(v)' }).toRowsAsync(), t.lazy().groupBy('k').agg({ n: 'count()', s: 'sum(v)' }).toRows(), 'async spilled groups');
  const m = memoryFile();
  await writeDatasetAsync(e, m, d.lazy().filter('v > 3').sort(['-k', 'v']), { rowsPerChunk: 3_000 });
  same(await (await openDatasetAsync(e, m.blob())).toRowsAsync(), t.lazy().filter('v > 3').sort(['-k', 'v']).toRows(), 'async external sort');
  same(await d.lazy().join(da, { on: 'k' }).groupBy('seg').agg({ n: 'count()', s: 'sum(v)' }).toRowsAsync(), t.lazy().join(acc, { on: 'k' }).groupBy('seg').agg({ n: 'count()', s: 'sum(v)' }).toRows(), 'async partitioned join');
  check(stats.spills > before.spills && stats.resplits > before.resplits && stats.joinPartitions > before.joinPartitions, `async spills ${JSON.stringify(stats)} vs ${JSON.stringify(before)}`);
  check(files.size === 0, `temporary files left: ${[...files.keys()].slice(0, 3)}`);
  Object.assign(tuning, { mergeRows: 1 << 20, spillRows: 1 << 23, partitionRows: 1 << 24, joinRows: 1 << 24 });
  setTempStorage(null);
  await d.close(); await da.close();
  await e.close();
}

// errors say what's wrong
{
  const e = new Engine();
  writeFileSync(file('not.vectro'), 'hello world, this is not a dataset at all.');
  const msg = (fn) => { try { fn(); } catch (err) { return err.message; } return ''; };
  check(msg(() => e.openDataset(file('not.vectro'))).includes('not a Vectro dataset'), 'not a dataset');
  const w = e.createDataset(file('schema.vectro'));
  w.append({ a: [1], b: ['x'] });
  check(msg(() => w.append({ a: [1] })).includes('differ'), 'columns differ');
  check(msg(() => w.append({ a: [1], b: [2] })).includes('category'), 'kinds differ');
  w.abort();
  check(msg(() => e.openDataset(file('schema.vectro'))).includes("wasn't closed"), 'aborted file');
  const d = e.writeDataset(file('ok.vectro'), e.table({ a: [1, 2] })) && e.openDataset(file('ok.vectro'));
  check(msg(() => d.filter('nope > 1').count()).includes('nope'), 'unknown column');
  await d.close();
  await e.close();
}

// SQL with subqueries, set operations and window functions: a dataset gives
// what the table gives (sync and async)
{
  const e = new Engine();
  const t = e.table(cols);
  writeDataset(e, file('sqlw.vectro'), t, { rowsPerChunk: 7000 });
  const ds = e.openDataset(file('sqlw.vectro'));
  const cs = e.table({ cust: customers.cust, tier: customers.tier });
  for (const q of [
    'WITH s AS (SELECT cust, SUM(qty) AS q FROM orders GROUP BY cust) SELECT cust, q FROM s ORDER BY q DESC, cust LIMIT 5',
    'SELECT cust FROM orders WHERE price > 999 UNION SELECT cust FROM customers WHERE tier = 3 AND cust < 30 ORDER BY cust',
    'SELECT cust FROM orders EXCEPT SELECT cust FROM customers ORDER BY cust LIMIT 20',
    'SELECT COUNT(*) AS n FROM (SELECT cust FROM orders UNION ALL SELECT cust FROM orders WHERE qty = 1) u',
    'SELECT COUNT(*) AS n FROM orders WHERE cust IN (SELECT cust FROM customers WHERE tier = 2)',
    'SELECT COUNT(*) AS n FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.cust AND o.price > 990)',
    'SELECT COUNT(*) AS n FROM orders o WHERE o.price > (SELECT AVG(p.price) FROM orders p WHERE p.region = o.region)',
    'SELECT region, cust, s FROM (SELECT region, cust, SUM(qty) AS s, RANK() OVER (PARTITION BY region ORDER BY SUM(qty) DESC) AS r FROM orders GROUP BY region, cust) t WHERE r <= 2 ORDER BY region, s DESC, cust',
    'SELECT cust, price, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY price DESC, ts) AS rn FROM orders WHERE cust < 9 AND price IS NOT NULL ORDER BY cust, rn LIMIT 30',
    'SELECT region, MEDIAN(price) AS m, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY qty) AS p FROM orders GROUP BY region ORDER BY region',
    "SELECT COUNT(lp) AS n, SUM(lp) AS s FROM (SELECT LAG(price) OVER (PARTITION BY region ORDER BY ts) AS lp FROM orders) x",
    'SELECT UPPER(region) AS r, COUNT(*) AS n FROM orders GROUP BY r ORDER BY r',
    "SELECT region || '-' || CAST(qty AS VARCHAR) AS k, SUM(price) AS s FROM orders GROUP BY k ORDER BY k LIMIT 10",
    "SELECT COUNT(*) AS n FROM orders WHERE LOWER(region) LIKE 'e%' OR LENGTH(region) > 3",
    "SELECT CASE WHEN price > 500 THEN 'hi' WHEN price > 0 THEN 'lo' ELSE 'none' END AS b, COUNT(*) AS n FROM orders GROUP BY b ORDER BY b",
    "SELECT STRFTIME(ts, '%Y-%m-%d %H') AS h, COUNT(*) AS n FROM orders GROUP BY h ORDER BY n DESC, h LIMIT 5",
    'SELECT cust, UPPER(region) AS r, CAST(ts AS VARCHAR) AS t FROM orders ORDER BY price DESC, cust LIMIT 5',
    'SELECT c.cust, COUNT(o.cust) AS n FROM orders o RIGHT JOIN customers c ON o.cust = c.cust GROUP BY c.cust ORDER BY n, c.cust LIMIT 10',
    'SELECT COUNT(*) AS n, COUNT(o.cust) AS a, COUNT(c.cust) AS b FROM orders o FULL JOIN customers c ON o.cust = c.cust',
  ]) {
    const want = sql(q, { orders: t, customers: cs }).toRows();
    let got, gotAsync;
    try { got = sql(q, { orders: ds, customers: cs }).toRows(); gotAsync = await sql(q, { orders: ds, customers: cs }).toRowsAsync(); } catch (err) { check(false, `${q}: ${err.message}`); continue; }
    same(got, want, `dataset SQL: ${q}`);
    same(gotAsync, want, `dataset SQL async: ${q}`);
  }
  // a GROUP BY whose result columns are renamed above it (as SQL does) still
  // adds the rows straight into the groups (the names made above it aren't
  // the dataset's columns made over)
  {
    const { Table } = await import('../src/node.mjs');
    const saved = tuning.denseMin, into = Table.prototype._groupInto;
    let calls = 0;
    Table.prototype._groupInto = function (...a) { calls++; return into.apply(this, a); };
    tuning.denseMin = 1;
    try {
      const q = 'SELECT cust, ROUND(SUM(price * qty), 2) AS spend FROM orders GROUP BY cust ORDER BY spend DESC, cust LIMIT 7';
      same(sql(q, { orders: ds }).toRows(), sql(q, { orders: t }).toRows(), 'renamed dense group-by');
      check(calls > 0, `renamed dense group-by: added into the groups (${calls} chunks)`);
    } finally { tuning.denseMin = saved; Table.prototype._groupInto = into; }
  }
  // an in-memory FULL / RIGHT JOIN (its unmatched rows made by its own join)
  // inside a query over a dataset
  {
    const c2 = e.table({ cust: Float64Array.from({ length: 900 }, (_, i) => 2000 + i * 2), w: Float64Array.from({ length: 900 }, (_, i) => i % 4) });
    for (const how of ['FULL', 'RIGHT', 'LEFT']) {
      const q = `SELECT COUNT(*) AS n, SUM(f.w) AS w FROM (SELECT COALESCE(c.cust, d.cust) AS kk, d.w AS w FROM customers c ${how} JOIN c2 d ON c.cust = d.cust) f JOIN orders o ON f.kk = o.cust`;
      const want = sql(q, { orders: t, customers: cs, c2 }).toRows();
      let got, gotAsync;
      try { got = sql(q, { orders: ds, customers: cs, c2 }).toRows(); gotAsync = await sql(q, { orders: ds, customers: cs, c2 }).toRowsAsync(); } catch (err) { check(false, `${how} JOIN under a dataset: ${err.message}`); continue; }
      same(got, want, `${how} JOIN under a dataset`);
      same(gotAsync, want, `${how} JOIN under a dataset (async)`);
    }
    c2.dispose();
  }
  await ds.close();
  await e.close();
}

rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} dataset checks passed`);
process.exit(failures ? 1 : 0);
