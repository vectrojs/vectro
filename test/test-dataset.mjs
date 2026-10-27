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

rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} dataset checks passed`);
process.exit(failures ? 1 : 0);
