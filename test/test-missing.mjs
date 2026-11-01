// Missing values and other edges, against plain JavaScript: SQL's three
// values under NOT / <> / NOT IN / NOT BETWEEN / NOT LIKE; blank text not
// counted (tables, datasets, split tables); null loading as missing; clear
// errors for text params, dates in Float32Array tables and SQL on split
// tables; readCSV's compress option.
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, SplitEngine, sql } from '../src/node.mjs';
import { writeDataset } from '../src/dataset.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); };
const throwsLike = (fn, re, msg) => { try { fn(); check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const dir = mkdtempSync(join(tmpdir(), 'vectro-missing-'));

let seed = 11;
const rnd = () => ((seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 4294967296);
const N = 20_000, R = ['EU', 'US', '', 'APAC', 'LATAM'];
const x = Array.from({ length: N }, () => (rnd() < 0.1 ? NaN : Math.floor(rnd() * 100)));
const r = Array.from({ length: N }, () => R[Math.floor(rnd() * R.length)]);
const g = Array.from({ length: N }, () => Math.floor(rnd() * 4));
const there = (v) => v === v && v !== '';

// SQL: a comparison with a missing value is unknown, and so is its NOT: the row is dropped
{
  const e = new Engine();
  const t = e.table({ x: Float64Array.from(x), r, g: Float64Array.from(g) });
  const count = (pred) => { let n = 0; for (let i = 0; i < N; i++) if (pred(x[i], r[i])) n++; return n; };
  const like = (s, p) => new RegExp(`^${p.replace(/%/g, '.*').replace(/_/g, '.')}$`).test(s);
  for (const [q, want] of [
    ['NOT (x > 50)', (a) => there(a) && !(a > 50)],
    ['x <> 50', (a) => there(a) && a !== 50],
    ['x != 50', (a) => there(a) && a !== 50],
    ['x NOT IN (10, 20, 30)', (a) => there(a) && ![10, 20, 30].includes(a)],
    ['x NOT BETWEEN 20 AND 70', (a) => there(a) && (a < 20 || a > 70)],
    ['NOT (x BETWEEN 20 AND 70)', (a) => there(a) && (a < 20 || a > 70)],
    ['NOT (x > 50 OR x < 10)', (a) => there(a) && a <= 50 && a >= 10],
    ['NOT (x > 50 AND x < 80)', (a) => there(a) && !(a > 50 && a < 80)],
    ['NOT NOT (x > 50)', (a) => a > 50],
    ['NOT (x IS NULL)', (a) => there(a)],
    ["r <> 'EU'", (a, s) => there(s) && s !== 'EU'],
    ["NOT (r = 'EU')", (a, s) => there(s) && s !== 'EU'],
    ["r NOT IN ('EU', 'US')", (a, s) => there(s) && s !== 'EU' && s !== 'US'],
    ["r NOT LIKE 'E%'", (a, s) => there(s) && !like(s, 'E%')],
    ["r LIKE '%'", (a, s) => there(s)],
    ["r ILIKE 'eu'", (a, s) => s === 'EU'],
    ["NOT (r < 'F')", (a, s) => there(s) && !(s < 'F')],
    ["NOT (x > 50) AND r <> 'US'", (a, s) => there(a) && !(a > 50) && there(s) && s !== 'US'],
  ]) {
    const got = sql(`SELECT COUNT(*) AS n FROM t WHERE ${q}`, { t }).toRows()[0].n;
    check(got === count(want), `WHERE ${q}: ${got}, want ${count(want)}`);
  }
  // table expressions follow JavaScript: x != 50 and !(x > 50) hold where x is missing
  check(t.filter('x != 50').count() === count((a) => a !== 50), 'table x != 50 (JavaScript rules)');
  check(t.filter('!(x > 50) && !isMissing(x)').count() === count((a) => there(a) && !(a > 50)), 'table !(x > 50) && !isMissing(x)');

  // blank text isn't counted (as SQL skips NULL)
  const distinct = (pred) => new Set(r.filter((s, i) => there(s) && pred(i))).size;
  same(sql('SELECT COUNT(r) AS n, COUNT(DISTINCT r) AS d, COUNT(*) AS rows_ FROM t', { t }).toRows(), [{ n: r.filter(there).length, d: distinct(() => true), rows_: N }], 'COUNT(text), COUNT(DISTINCT text)');
  same(t.agg({ n: 'count(r)', d: 'countDistinct(r)', a: 'approxDistinct(r)' }), { n: r.filter(there).length, d: distinct(() => true), a: distinct(() => true) }, 'count / countDistinct / approxDistinct of text');
  const byG = t.groupBy('g').agg({ n: 'count(r)', d: 'countDistinct(r)' }).toRows();
  same(byG, [0, 1, 2, 3].map((k) => ({ g: k, n: r.filter((s, i) => there(s) && g[i] === k).length, d: distinct((i) => g[i] === k) })), 'grouped count / countDistinct of text');

  // the same on a dataset (chunks, a bitmap of labels per group, partial counts merged)
  writeDataset(e, join(dir, 'm.vectro'), t, { rowsPerChunk: 3000 });
  const ds = e.openDataset(join(dir, 'm.vectro'));
  for (const q of ['SELECT COUNT(r) AS n, COUNT(DISTINCT r) AS d FROM t', 'SELECT g, COUNT(r) AS n, COUNT(DISTINCT r) AS d FROM t GROUP BY g ORDER BY g', "SELECT g, COUNT(*) AS n FROM t WHERE NOT (x > 50) AND r NOT IN ('US') GROUP BY g ORDER BY g"]) {
    same(sql(q, { t: ds }).toRows(), sql(q, { t }).toRows(), `dataset: ${q}`);
    same(await sql(q, { t: ds }).toRowsAsync(), sql(q, { t }).toRows(), `dataset async: ${q}`);
  }
  same(ds.groupBy('g').agg({ d: 'countDistinct(r)' }).toRows(), t.groupBy('g').agg({ d: 'countDistinct(r)' }).toRows(), 'dataset countDistinct of text');
  await ds.close();

  // null loads as missing (arrays, rows), a Date in rows makes a date column
  same([...e.table({ a: [1, null, 3, undefined] }).toColumns().a], [1, NaN, 3, NaN], 'null in a number array');
  const fromRows = e.tableFromRows([{ a: 1, d: new Date(Date.UTC(2024, 0, 2)) }, { a: null, d: new Date(Date.UTC(2024, 0, 3)) }]);
  same([...fromRows.toColumns().a], [1, NaN], 'null in rows');
  check(fromRows.toRows()[1].d instanceof Date, 'a Date in rows: a date column');
  same(fromRows.agg({ n: 'count(a)' }), { n: 1 }, 'count skips null from rows');

  // clear errors
  throwsLike(() => t.filter('x > lim', { lim: 'fifty' }).count(), /param "lim" must be a number or a Date/, 'text param');
  throwsLike(() => e.table({ ts: [new Date()] }, { type: Float32Array }), /Float32Array table can't hold times/, 'dates in a Float32Array table');
  throwsLike(() => e.table({ ts: [Date.now()] }, { type: Float32Array, dates: ['ts'] }), /Float32Array table can't hold times/, 'dates option in a Float32Array table');
  throwsLike(() => e.tableFromRows([{ d: new Date() }], { type: Float32Array }), /Float32Array table can't hold times/, 'Date rows in a Float32Array table');
  throwsLike(() => e.parseCSV('ts,v\n2024-01-01,1\n2024-01-02,2\n', { type: Float32Array }), /Float32Array table can't hold times/, 'CSV dates in a Float32Array table');

  // readCSV honours compress, like parseCSV
  writeFileSync(join(dir, 'c.csv'), 'a,b\n' + Array.from({ length: 5000 }, (_, i) => `${i % 9},${i % 4}`).join('\n'));
  const c1 = e.readCSV(join(dir, 'c.csv'), { compress: true }), c2 = await e.readCSVAsync(join(dir, 'c.csv'), { compress: true });
  check(c1.cols.get('a').enc && c2.cols.get('a').enc, 'readCSV / readCSVAsync with compress: true pack columns');
  same(c1.agg({ s: 'sum(a)' }), e.readCSV(join(dir, 'c.csv')).agg({ s: 'sum(a)' }), 'readCSV compressed: same answers');
  await e.close();
}

// split tables: blank text not counted; SQL refused with a clear error
{
  const s = new SplitEngine({ workers: 3 }), e = new Engine();
  const cols = { g: Float64Array.from(g.slice(0, 3000)), r: r.slice(0, 3000) };
  const st = await s.tableAsync(cols), t = e.table(cols);
  same(await st.aggAsync({ n: 'count(r)', d: 'countDistinct(r)', a: 'approxDistinct(r)' }), t.agg({ n: 'count(r)', d: 'countDistinct(r)', a: 'approxDistinct(r)' }), 'split count / distinct of text');
  same((await st.groupBy('g').aggAsync({ d: 'countDistinct(r)' })).toRows(), t.groupBy('g').agg({ d: 'countDistinct(r)' }).toRows(), 'split grouped countDistinct of text');
  let msg = '';
  try { await sql('SELECT COUNT(*) AS n FROM st', { st }).toRowsAsync(); } catch (err) { msg = err.message; }
  check(/SplitTable: SQL runs on Table and Dataset/.test(msg), `SQL on a split table: ${msg}`);
  const copy = s.local.table(await st.toColumnsAsync());
  same(sql('SELECT COUNT(r) AS n FROM copy', { copy }).toRows(), [{ n: cols.r.filter(there).length }], 'SQL on a split table copied into a Table');
  await s.close();
  await e.close();
}

rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} missing-value checks passed`);
process.exit(failures ? 1 : 0);
