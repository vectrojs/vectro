// Audit round 5 (final sweep): one fixed set of 294 SQL queries from areas 1-9
// (joins, group-by, windows, subqueries, ORDER BY / LIMIT, expressions, text,
// dates, percentiles, WHERE filters) and 22 dataframe operations on one edge
// data set must give the same answers in every engine mode, with 1 thread and
// many, sync and async, on a table, a dataset and a split table. Each answer is
// kept as a hash in fixtures/audit-r5-sweep.json (numbers to 9 significant
// digits, so sums added in another order on other threads still match); the
// answers were checked against DuckDB in vectro-qa-2026-11-05/sweep (293 of 294
// equal; the other is DuckDB writing 1.0 for a whole number). When a fix changes
// an answer on purpose, write the hashes again: node test/test-audit-r5-sweep.mjs --write
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const FIXTURE = new URL('./fixtures/audit-r5-sweep.json', import.meta.url);
let seed = 12345; // mulberry32, as in the audit harness
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const norm = (v) => (v === null || v === undefined || v === '' || (typeof v === 'number' && Number.isNaN(v)) ? null : typeof v === 'bigint' ? Number(v) : typeof v === 'number' ? (Object.is(v, -0) ? 0 : v === Infinity ? 'inf' : v === -Infinity ? '-inf' : +v.toPrecision(9)) : v instanceof Date ? (Number.isNaN(+v) ? null : v.toISOString()) : typeof v === 'boolean' ? +v : v);
const hash = (x) => createHash('sha1').update(JSON.stringify(x)).digest('hex').slice(0, 16);
const NUM = [0, -0, 1, -1, 2, 2.5, -2.5, 3, 7, 0.1, 100, 1e6, NaN, NaN, Infinity, -Infinity, 2 ** 53];
const TXT = ['a', 'b', 'B', 'é', 'é', '😀', '', 'İstanbul', 'a,b', 'x y', 'ab'];
function mk(n, keys) {
  const t = { id: [], k: [], k2: [], v: [], w: [], s: [], d: [] };
  for (let i = 0; i < n; i++) { t.id.push(i); t.k.push(pick(keys)); t.k2.push(Math.floor(rnd() * 4)); t.v.push(pick(NUM)); t.w.push(rnd() < 0.08 ? NaN : Math.floor(rnd() * 1000) / 4); t.s.push(pick(TXT)); t.d.push(rnd() < 0.08 ? new Date(NaN) : new Date(Date.UTC(1950 + Math.floor(rnd() * 90), Math.floor(rnd() * 12), 1 + Math.floor(rnd() * 28), Math.floor(rnd() * 24)))); }
  return t;
}
const T = { t: mk(3000, [0, -0, 1, 2, 3, 4, NaN, 9]), u: mk(300, [0, 1, 1, 2, NaN, 7]) };
const Q = [];

// joins (area 1)
for (const how of ['JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']) {
  Q.push(`SELECT t.id, u.id AS uid FROM t ${how} u ON t.k = u.k WHERE t.id < 400`);
  Q.push(`SELECT COUNT(*) AS c, SUM(t.w) AS s FROM t ${how} u ON t.k = u.k`);
  Q.push(`SELECT t.k2, COUNT(u.id) AS c FROM t ${how} u ON t.s = u.s GROUP BY t.k2`);
  Q.push(`SELECT COUNT(*) AS c FROM t ${how} u ON t.k = u.k AND t.k2 = u.k2`);
}
Q.push('SELECT COUNT(*) AS c FROM t CROSS JOIN u WHERE t.id < 20');
// group-by (area 2)
for (const g of ['k', 'k2', 's', 'k, k2', 's, k2', 'v', 'YEAR(d)', 'v > 1', 'COALESCE(k, -1)', 'k2 % 2'])
  for (const a of ['COUNT(*)', 'COUNT(v)', 'SUM(w)', 'AVG(w)', 'MIN(w)', 'MAX(v)', 'COUNT(DISTINCT s)', 'SUM(v)'])
    Q.push(`SELECT ${g.split(/,(?![^(]*\))/).map((x, i) => `${x.trim()} AS g${i}`).join(', ')}, ${a} AS a FROM t GROUP BY ${g}`);
Q.push('SELECT k2, COUNT(*) AS c FROM t GROUP BY k2 HAVING SUM(w) > 1000', 'SELECT COUNT(*) AS c, SUM(w) AS s, AVG(w) AS a, MIN(v) AS lo, MAX(v) AS hi FROM t', 'SELECT COUNT(*) AS c FROM t WHERE id < 0');
// windows (area 3)
for (const f of ['ROW_NUMBER() OVER (PARTITION BY k2 ORDER BY id)', 'RANK() OVER (PARTITION BY k2 ORDER BY w)', 'DENSE_RANK() OVER (ORDER BY k2)', 'SUM(w) OVER (PARTITION BY k2 ORDER BY id)',
  'SUM(w) OVER (PARTITION BY k2 ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)', 'AVG(w) OVER (PARTITION BY s ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)', 'LAG(w) OVER (PARTITION BY k2 ORDER BY id)',
  'LEAD(w, 2) OVER (ORDER BY id)', 'COUNT(*) OVER (PARTITION BY k)', 'MIN(w) OVER (PARTITION BY k2)', 'NTILE(4) OVER (ORDER BY id)', 'FIRST_VALUE(w) OVER (PARTITION BY k2 ORDER BY id)', 'PERCENT_RANK() OVER (PARTITION BY k2 ORDER BY id)', 'CUME_DIST() OVER (ORDER BY k2)'])
  Q.push(`SELECT id, ${f} AS x FROM t`);
// subqueries and CTEs (area 4)
Q.push('SELECT id FROM t WHERE k IN (SELECT k FROM u)', 'SELECT id FROM t WHERE k NOT IN (SELECT k FROM u WHERE k IS NOT NULL)', 'SELECT id FROM t a WHERE EXISTS (SELECT 1 FROM u b WHERE b.s = a.s)', 'SELECT id FROM t a WHERE NOT EXISTS (SELECT 1 FROM u b WHERE b.k = a.k)',
  'SELECT id, (SELECT COUNT(*) FROM u b WHERE b.k = a.k) AS n FROM t a', 'SELECT id FROM t WHERE w > (SELECT AVG(w) FROM u)', 'WITH x AS (SELECT k2, COUNT(*) AS n FROM t GROUP BY k2) SELECT a.k2, a.n, b.n AS m FROM x a JOIN x b ON a.k2 = b.k2',
  'SELECT k2, n FROM (SELECT k2, COUNT(*) AS n FROM t GROUP BY k2) z WHERE n > 100', 'SELECT id FROM t WHERE id < 50 UNION SELECT id FROM u WHERE id < 60', 'SELECT s FROM t INTERSECT SELECT s FROM u', 'SELECT s FROM t EXCEPT SELECT s FROM u');
// ORDER BY / LIMIT (area 5)
for (const o of ['id', 'w DESC, id', 's, id', 's DESC NULLS FIRST, id', 'v, id', 'v DESC NULLS FIRST, id', 'd, id', 'd DESC, id', 'k2, w DESC, id']) for (const l of [' LIMIT 10', ' LIMIT 5 OFFSET 7', ''])
  Q.push({ q: `SELECT id, ${o.split(',')[0].replace(/ (DESC|NULLS FIRST)/g, '')} AS x FROM t ORDER BY ${o}${l}`, ordered: true });
// expressions (area 6, cases that agree with DuckDB)
for (const x of ['v + w', 'v * 2 - w / 4', 'w / 3', 'ABS(v)', 'FLOOR(w)', 'CEIL(w)', 'ROUND(w, 1)', 'LEAST(v, w)', 'GREATEST(w, 10)', 'COALESCE(v, w, 0)', 'NULLIF(k2, 0)', 'CASE WHEN w > 100 THEN 1 WHEN w > 50 THEN 2 ELSE 3 END', 'CASE k2 WHEN 0 THEN \'zero\' WHEN 1 THEN \'one\' END', 'v IS NULL', 'NOT (w > 10)', 'k2 BETWEEN 1 AND 2', 'k IN (1, 2, NULL)', 'w > 10 AND (v > 0 OR k2 = 1)'])
  Q.push(`SELECT id, ${x} AS x FROM t`);
// text (area 7)
for (const x of ['UPPER(s)', 'LOWER(s)', 'LENGTH(s)', 'SUBSTRING(s, 2, 2)', 'LEFT(s, 1)', 'RIGHT(s, 2)', 'TRIM(s)', "REPLACE(s, 'a', 'Z')", "s || '!'", "CONCAT(s, '-', k2)", "LPAD(s, 4, '*')", "STRPOS(s, 'b')", "s LIKE 'a%'", "s ILIKE '%B%'", "s < 'b'", "STARTS_WITH(s, 'a')", "SPLIT_PART(s, ',', 1)"])
  Q.push(`SELECT id, ${x} AS x FROM t`);
for (const g of ['UPPER(s)', 'LENGTH(s)', 'LEFT(s, 1)']) Q.push(`SELECT ${g} AS g, COUNT(*) AS c FROM t GROUP BY ${g}`);
// dates (area 8)
for (const x of ['YEAR(d)', 'MONTH(d)', 'DAY(d)', 'HOUR(d)', 'EXTRACT(dow FROM d)', 'EXTRACT(epoch FROM d)', "STRFTIME(d, '%Y-%m-%d %H')", "STRFTIME(d, '%G-W%V')", "d > DATE '2000-01-01'", 'd IS NULL'])
  Q.push(`SELECT id, ${x} AS x FROM t`);
for (const [x, dx] of [["DATE_TRUNC('month', d)", "CAST(DATE_TRUNC('month', d) AS TIMESTAMP)"], ["DATE_TRUNC('week', d)", "CAST(DATE_TRUNC('week', d) AS TIMESTAMP)"], ["d + INTERVAL '1 day'", "CAST(d + INTERVAL '1 day' AS TIMESTAMP)"]])
  Q.push({ q: `SELECT id, ${x} AS x FROM t`, duck: `SELECT id, ${dx} AS x FROM t` });
Q.push("SELECT YEAR(d) AS y, COUNT(*) AS c FROM t GROUP BY 1", "SELECT COUNT(*) AS c FROM t WHERE d BETWEEN DATE '1970-01-01' AND DATE '1999-12-31'");
// percentiles (area 9, ascending forms)
for (const f of ['MEDIAN(w)', 'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY w)', 'PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY w)', 'QUANTILE_CONT(w, 0.75)', 'QUANTILE_DISC(w, 0.5)'])
  Q.push(`SELECT k2, ${f} AS p FROM t GROUP BY k2`, `SELECT ${f} AS p FROM t`);
// WHERE filters (areas 2, 6, 7, 8)
for (const w of ['w > 100', 'w <= 100 AND k2 = 1', 'v > 0 OR w < 10', 'NOT (v > 0)', 'v IS NULL', 'v IS NOT NULL AND v < 1e300', "s = 'é'", "s <> 'a'", "s IN ('a', 'b', '😀')", "s LIKE '%b%'", "s ILIKE 'B%'", "UPPER(s) = 'AB'",
  "d >= DATE '2000-01-01'", "d < TIMESTAMP '1960-06-01 12:00:00'", 'YEAR(d) = 1999', 'k IN (0, 1)', 'k NOT IN (2, 3)', 'k BETWEEN 1 AND 3', 'k2 = 0 AND s IS NULL', 'w > (SELECT MAX(w) FROM u) - 10',
  'COALESCE(v, 0) > 2', 'ABS(v - 1) < 0.5', 'w / 2 > 100', 'LENGTH(s) = 1', 'id % 7 = 0'])
  for (const sel of ['COUNT(*) AS c', 'SUM(w) AS s', 'MIN(id) AS lo']) Q.push(`SELECT ${sel} FROM t WHERE ${w}`);
for (const j of ['JOIN u USING (k)', 'LEFT JOIN u USING (k2)', 'JOIN u ON t.k = u.k AND t.s = u.s', 'JOIN u ON t.k2 = u.k2 WHERE u.w > 100'])
  Q.push(`SELECT COUNT(*) AS c FROM t ${j}`);

let failures = 0, checks = 0;
const want = process.argv.includes('--write') ? null : JSON.parse(readFileSync(FIXTURE, 'utf8'));
const written = { sql: [], df: [] };
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-sweep-'));
const cols = (t) => ({ id: Float64Array.from(t.id), k: Float64Array.from(t.k), k2: Float64Array.from(t.k2), v: Float64Array.from(t.v), w: Float64Array.from(t.w), s: t.s, d: t.d });
const RUNS = process.argv.includes('--write') ? [[{}, 'table', 'default']] : [[{}, 'table', 'default'], [{ threads: 1 }, 'table', 'serial'], [{}, 'table', 'async'], [{}, 'dataset', 'default'], [{}, 'dataset', 'async'], [{}, 'split', 'async']];
for (const [opts, SOURCE, OPTS] of RUNS) {
  const e = new Engine(opts), tag = `${SOURCE} ${OPTS}${opts.threads ? ' (1 thread)' : ''}`;
  if (SOURCE !== 'table' && MODE !== 'node') { await e.close(); continue; } // (datasets and split tables: Node)
  const V = { t: e.table(cols(T.t)), u: e.table(cols(T.u)) };
  if (SOURCE === 'dataset') for (const n of ['t', 'u']) { e.writeDataset(join(dir, n + '.vectro'), V[n], { rowsPerChunk: Math.ceil(T[n].id.length / 7) }); V[n] = e.openDataset(join(dir, n + '.vectro')); }
  const env = { V };
  const bad = [];
  for (let i = 0; i < Q.length; i++) {
    const item = Q[i], q = typeof item === 'string' ? item : item.q;
    let a; try { const r = sql(q, V); a = (OPTS === 'async' ? await r.toRowsAsync() : r.toRows()).map((x) => Object.values(x).map(norm)); if (!(typeof item !== 'string' && item.ordered)) a.sort((x, y) => (JSON.stringify(x) < JSON.stringify(y) ? -1 : 1)); } catch (er) { a = 'ERROR ' + er.message.split('\n')[0]; }
    const h = hash(a);
    if (!want) { written.sql.push([q, h]); continue; }
    checks++; if (h !== want.sql[i][1]) { failures++; bad.push(q); }
  }
  let D = V.t, se;
  if (SOURCE === 'split') { const { SplitEngine } = await import('../src/node.mjs'); se = new SplitEngine({ workers: 3 }); D = await se.tableAsync(cols(T.t)); }
  const A = SOURCE === 'split' || OPTS === 'async';
const DF = [['agg', (x) => (A ? x.aggAsync : x.agg).call(x, { c: 'count()', s: 'sum(w)', m: 'mean(w)', lo: 'min(v)', hi: 'max(v)', cv: 'count(v)', cc: 'count(w > 100)', ds: 'countDistinct(s)' })]];
for (const f of ["k2 == 1", "w > 100 && v > 0", "s == 'é'", "in(s, 'a', '😀')", "startsWith(s, 'a')", "like(s, '%b%')", 'isMissing(v)', '!(w > 10)', "d > date('2000-01-01')", 'year(d) == 1999', 'abs(v) < 5', 'coalesce(v, 0) > 1'])
  DF.push([`filter(${f}).agg`, (x) => (A ? x.filter(f).aggAsync : x.filter(f).agg).call(x.filter(f), { c: 'count()', s: 'sum(w)', lo: 'min(w)' })]);
for (const g of ['k2', 's', 'k']) DF.push([`groupBy(${g})`, async (x) => { const r = A ? await x.groupBy(g).aggAsync({ c: 'count()', s: 'sum(w)', m: 'max(v)' }) : x.groupBy(g).agg({ c: 'count()', s: 'sum(w)', m: 'max(v)' }); return (r.toRowsAsync ? await r.toRowsAsync() : r.toRows()).map((o) => Object.values(o).map(norm)).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)); }]);
for (const by of ['w', '-w', ['k2', '-w'], 's', '-d', ['-v', 'id']]) DF.push([`top(7, ${by})`, async (x) => { const r = A ? await x.topAsync(7, by) : x.top(7, by); return (r.toRowsAsync ? await r.toRowsAsync() : r.toRows()).map((o) => [norm(o.id)]); }]);

  for (let i = 0; i < DF.length; i++) {
    const [label, f] = DF[i];
    if (SOURCE === 'dataset' && A && /groupBy|top/.test(label)) continue; // (datasets have no groupBy().aggAsync / topAsync yet: O2)
    let a; try { a = await f(D); if (a && !Array.isArray(a)) a = Object.fromEntries(Object.entries(a).map(([k, v]) => [k, norm(v)])); } catch (er) { a = 'ERROR ' + er.message.split('\n')[0].slice(0, 80); }
    const h = hash(a);
    if (!want) { written.df.push([label, h]); continue; }
    checks++; if (h !== want.df[i][1]) { failures++; bad.push('dataframe ' + label); }
  }
  if (bad.length) console.log(`FAIL ${tag}: ${bad.length} answers differ from the table's\n  ${bad.slice(0, 8).join('\n  ')}`);
  if (se) await se.close();
  if (SOURCE === 'dataset') for (const n of ['t', 'u']) V[n].close();
  await e.close();
}
rmSync(dir, { recursive: true, force: true });
if (!want) { writeFileSync(FIXTURE, JSON.stringify(written, null, 0).replace(/\],\[/g, '],\n[') + '\n'); console.log(`wrote ${written.sql.length} SQL and ${written.df.length} dataframe hashes`); process.exit(0); }
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 sweep checks passed`);
process.exit(failures ? 1 : 0);
