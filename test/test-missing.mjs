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

// 0.1.2: the cases a comparison with DuckDB found (answers as DuckDB gives them; missing text reads '')
{
  const e = new Engine();
  const q = (text, tables) => sql(text, tables).toRows();
  // missing text sorts last, up or down; NULLS FIRST puts it first
  const s = e.table({ s: ['b', '', 'a'], n: [1, 2, 3] });
  same(s.sort('s').toRows().map((r) => r.n), [3, 1, 2], 'sort: missing text last');
  same(s.sort('-s').toRows().map((r) => r.n), [1, 3, 2], 'sort descending: missing text last');
  same(q('SELECT s FROM s ORDER BY s', { s }), [{ s: 'a' }, { s: 'b' }, { s: '' }], 'ORDER BY text: missing last');
  same(q('SELECT s FROM s ORDER BY s DESC NULLS FIRST', { s }), [{ s: '' }, { s: 'b' }, { s: 'a' }], 'ORDER BY text DESC NULLS FIRST');
  same(q('SELECT s FROM s ORDER BY s NULLS FIRST', { s }), [{ s: '' }, { s: 'a' }, { s: 'b' }], 'ORDER BY text NULLS FIRST');
  same(q('SELECT s, COUNT(*) AS n FROM s GROUP BY s ORDER BY s LIMIT 1', { s }), [{ s: 'a', n: 1 }], 'GROUP BY text ORDER BY LIMIT: missing group last');
  // null / undefined in a text column load as missing text, also when the column starts with one
  const nt = e.table({ s: ['a', null, 'b', undefined], s2: [null, 'x', 'x', 'y'], v: [1, 2, 3, 4] });
  same(q('SELECT COUNT(s) AS c, COUNT(DISTINCT s) AS d, COUNT(s2) AS c2 FROM nt', { nt }), [{ c: 2, d: 2, c2: 3 }], 'null in text columns: missing');
  same(q('SELECT COUNT(*) AS n FROM nt WHERE s IS NULL', { nt }), [{ n: 2 }], 'null in a text column: IS NULL');
  const fr = e.tableFromRows([{ s: null, v: 1 }, { s: 'a', v: 2 }]);
  same(fr.toRows(), [{ s: '', v: 1 }, { s: 'a', v: 2 }], 'tableFromRows: a text column starting with null');
  // a text join key that is missing matches nothing
  const ja = e.table({ k: ['x', ''], n: [1, 2] }), jb = e.table({ k: ['x', ''], m: [10, 20] });
  same(q('SELECT COUNT(*) AS n FROM ja JOIN jb ON ja.k = jb.k', { ja, jb }), [{ n: 1 }], 'text join: missing keys never match');
  same(q('SELECT ja.n, jb.m FROM ja LEFT JOIN jb ON ja.k = jb.k ORDER BY ja.n', { ja, jb }), [{ n: 1, m: 10 }, { n: 2, m: null }], 'text left join: missing key unmatched');
  same(ja.join(jb, { on: 'k' }).toRows(), [{ k: 'x', n: 1, m: 10 }], 'Table.join on text: missing keys never match');
  // conditions with a missing value: unknown (NULL), and IS TRUE / IS FALSE treat unknown as neither
  const b = e.table({ a: [5, NaN, -1] });
  same(q('SELECT a > 0 AS gt, NOT (a > 0) AS ngt, a IN (1, 5) AS i, a BETWEEN 0 AND 9 AS bt FROM b', { b }),
    [{ gt: 1, ngt: 0, i: 1, bt: 1 }, { gt: NaN, ngt: NaN, i: NaN, bt: NaN }, { gt: 0, ngt: 1, i: 0, bt: 0 }], 'conditions as values: 1 / 0 / NULL');
  for (const [w, n] of [['(a > 0) IS TRUE', 1], ['(a > 0) IS FALSE', 1], ['(a > 0) IS NOT TRUE', 2], ['(a > 0) IS NOT FALSE', 2], ['(a > 0) = (a < 9)', 1], ['(a > 0) <> (a < 0)', 2],
    ['a > 0 OR NULL', 1], ['a > 0 AND NULL', 0], ['NOT (a > 0 AND NULL)', 1], ['NULL', 0], ['TRUE', 3], ['1 = 1', 3], ['1 = 0', 0], ['FALSE', 0], ['a IS TRUE', 2]])
    same(q(`SELECT COUNT(*) AS n FROM b WHERE ${w}`, { b }), [{ n }], `WHERE ${w}`);
  same(q('SELECT COUNT(*) AS n FROM b GROUP BY a > 99 HAVING NULL', { b }), [], 'HAVING NULL');
  same(q('SELECT LEAST(a, 2) AS l, GREATEST(a, 2, NULL) AS g FROM b', { b }), [{ l: 2, g: 5 }, { l: 2, g: 2 }, { l: -1, g: 2 }], 'LEAST / GREATEST skip missing values');
  // x NOT IN (subquery): a missing value in it leaves nothing; an empty one keeps every row
  const t = e.table({ a: [1, NaN, 3, 4], g: [1, 1, 2, 3] }), d = e.table({ k: [1, NaN, 3], g: [1, 2, 2] }), u = e.table({ k: [3, 3] }), z = e.table({ k: new Float64Array(0) });
  same(q('SELECT a FROM t WHERE a NOT IN (SELECT k FROM d) ORDER BY a', { t, d }), [], 'NOT IN a subquery with a missing value');
  same(q('SELECT a FROM t WHERE a NOT IN (SELECT k FROM u) ORDER BY a', { t, u }), [{ a: 1 }, { a: 4 }], 'NOT IN a subquery');
  same(q('SELECT COUNT(*) AS n FROM t WHERE a NOT IN (SELECT k FROM z)', { t, z }), [{ n: 4 }], 'NOT IN an empty subquery: every row');
  same(q('SELECT a FROM t WHERE a NOT IN (SELECT k FROM u UNION SELECT 4 AS k) ORDER BY a', { t, u }), [{ a: 1 }], 'NOT IN a UNION');
  same(q('SELECT a, g FROM t WHERE a NOT IN (SELECT k FROM d WHERE d.g = t.g) ORDER BY a, g', { t, d }), [{ a: 4, g: 3 }], 'NOT IN a correlated subquery');
  const ts = e.table({ x: ['a', '', 'c'] }), tr = e.table({ y: ['a', 'b'] });
  same(q('SELECT x FROM ts WHERE x NOT IN (SELECT y FROM tr)', { ts, tr }), [{ x: 'c' }], 'NOT IN on text');
  same(q('SELECT y FROM tr WHERE y NOT IN (SELECT x FROM ts)', { ts, tr }), [], 'NOT IN text with a missing value');
  // the same aggregate twice; subqueries in a grouped SELECT
  const f = e.table({ g: [1, 1, 2], v: [NaN, NaN, 5] });
  same(q('SELECT g, SUM(v) AS s1, MAX(v) AS m, SUM(v) AS s2 FROM f GROUP BY g ORDER BY g', { f }), [{ g: 1, s1: NaN, m: NaN, s2: NaN }, { g: 2, s1: 5, m: 5, s2: 5 }], 'SUM twice over no values: NULL both times');
  const gt = e.table({ b: [1, 1, 2] }), gd = e.table({ k: [1, 1, 3] });
  same(q('SELECT b, (SELECT COUNT(*) FROM gd WHERE gd.k = gt.b) AS n FROM gt GROUP BY b ORDER BY b', { gt, gd }), [{ b: 1, n: 2 }, { b: 2, n: 0 }], 'correlated subquery in a grouped SELECT');
  same(q('SELECT b, COUNT(*) AS c, (SELECT MAX(k) FROM gd) AS m FROM gt GROUP BY b ORDER BY b', { gt, gd }), [{ b: 1, c: 2, m: 3 }, { b: 2, c: 1, m: 3 }], 'subquery value in a grouped SELECT');
  same(q('SELECT b FROM gt GROUP BY b HAVING COUNT(*) > (SELECT COUNT(*) FROM gd WHERE gd.k = gt.b) - 1 ORDER BY b', { gt, gd }), [{ b: 1 }, { b: 2 }], 'correlated subquery in HAVING');
  // filters with no column in them, in the dataframe API
  const c = e.table({ a: [1, 2, 3] });
  for (const [p, n] of [['1', 3], ['0', 0], ['1 == 1', 3], ['1 == 0', 0], ['1 < 2 && 3 > 2', 3]]) {
    same(c.filter(p).count(), n, `filter("${p}").count()`);
    same(c.filter(p).toRows().length, n, `filter("${p}").toRows()`);
  }
  same(c.filter('a > 1').filter('0').count(), 0, 'a constant false filter after another filter');
  // a text column compared with < or >: a clear error (it used to overflow the stack)
  const tx = e.table({ ts: ['2024-02-01', '2024-07-01'], region: ['EU', 'US'], price: [1, 2] });
  throwsLike(() => tx.filter("ts >= '2024-06-01'").groupBy('region').agg({ r: 'sum(price)' }).toRows(), /can only be compared with == or !=.*dates/, 'text compared with >= then groupBy');
  writeDataset(e, join(dir, 'tx.vectro'), tx);
  const ds = e.openDataset(join(dir, 'tx.vectro'));
  throwsLike(() => ds.filter("ts >= '2024-06-01'").groupBy('region').agg({ r: 'sum(price)' }).toRows(), /can only be compared with == or !=/, 'dataset: text compared with >= then groupBy');
  throwsLike(() => ds.filter('nosuch > 1').count(), /columns: ts, region, price\)/, 'dataset: unknown column lists every column');
  await e.close();
}

// an allocation too big for the 4GB memory fails cleanly and leaves the engine as it was
{
  const e = new Engine();
  const t = e.table({ g: Float64Array.from(g), x: Float64Array.from(x) });
  const want = t.groupBy('g').agg({ s: 'sum(x)', n: 'count(x)' }).toRows();
  const keep = e.alloc(Float64Array, 1000); e.free(e.alloc(Float64Array, 5000)); // (a free block to protect)
  const free = JSON.stringify(e.freeList);
  throwsLike(() => e.alloc(Uint32Array, 3_145_936_686), /out of memory \(4GB limit\)/, 'alloc past 4GB: out of memory');
  same(JSON.stringify(e.freeList), free, 'alloc past 4GB: free list untouched');
  for (let i = 0; i < 5; i++) e.alloc(Float64Array, 4000); // (allocations after it must not land on live data)
  same(t.groupBy('g').agg({ s: 'sum(x)', n: 'count(x)' }).toRows(), want, 'answers after a failed allocation');
  e.free(keep);
  await e.close();
}

rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} missing-value checks passed`);
process.exit(failures ? 1 : 0);
