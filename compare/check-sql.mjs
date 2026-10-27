// The same SQL in Vectro and DuckDB on the same rows, answers compared row by
// row: subqueries, set operations, window functions and percentiles (with
// missing prices). Not a benchmark: a check that both say the same thing.
//   node compare/check-sql.mjs        (needs this folder's npm install)
import { writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { Engine, sql } from '../src/node.mjs';

const N = Number(process.env.ROWS ?? 200_000);
const R = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
function rng(seed) { return () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
const r = rng(5);
const o = { cust: [], price: [], qty: [], region: [], ts: [] };
for (let i = 0; i < N; i++) {
  o.cust.push(Math.floor(r() * 1200)); o.price.push(r() < 0.03 ? NaN : Math.round(r() * 100000) / 100); o.qty.push(1 + Math.floor(r() * 9));
  o.region.push(R[Math.floor(r() * R.length)]); o.ts.push(new Date(Date.UTC(2023, 0, 1) + Math.floor(r() * 730 * 86400) * 1000));
}
const c = { cust: Array.from({ length: 1000 }, (_, i) => i), name: Array.from({ length: 1000 }, (_, i) => `c${i}`), tier: Array.from({ length: 1000 }, (_, i) => 1 + (i % 4)) };

const people = [[1, '  Ann Smith', 'Seattle'], [2, 'bob jones ', 'Austin'], [3, 'Çelik Ayşe', 'Ankara'], [4, 'dan', 'San José'], [5, 'Eve-Marie', 'Paris'], [6, "Frank O'Neil", null], [7, '東京 太郎', 'Tokyo'], [8, 'gus', 'Dublin']];
const QUERIES = [
  'WITH s AS (SELECT cust, SUM(qty) AS q FROM orders GROUP BY cust) SELECT cust, q FROM s WHERE q > 950 ORDER BY q DESC, cust LIMIT 20',
  'SELECT c.tier, COUNT(*) AS n, AVG(t.spend) AS a FROM (SELECT cust, SUM(price * qty) AS spend FROM orders GROUP BY cust) t JOIN customers c ON t.cust = c.cust GROUP BY c.tier ORDER BY c.tier',
  'SELECT cust FROM orders WHERE price > 999.5 UNION SELECT cust FROM customers WHERE tier = 4 AND cust < 40 ORDER BY cust',
  'SELECT cust FROM orders WHERE qty = 9 AND price > 990 INTERSECT SELECT cust FROM customers WHERE tier = 2 ORDER BY cust',
  'SELECT cust FROM orders EXCEPT SELECT cust FROM customers ORDER BY cust LIMIT 30',
  'SELECT COUNT(*) AS n FROM (SELECT region FROM orders WHERE qty = 1 UNION ALL SELECT name FROM customers) u',
  'SELECT region, COUNT(*) AS n FROM orders WHERE cust IN (SELECT cust FROM customers WHERE tier = 3) GROUP BY region ORDER BY region',
  'SELECT COUNT(*) AS n FROM orders WHERE cust NOT IN (SELECT cust FROM customers)',
  'SELECT COUNT(*) AS n FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.cust AND o.price > 999)',
  'SELECT COUNT(*) AS n FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.cust AND o.qty = 9 AND o.price > 900)',
  'SELECT region, COUNT(*) AS n FROM orders WHERE price > (SELECT AVG(price) FROM orders) GROUP BY region ORDER BY region',
  'SELECT COUNT(*) AS n FROM orders o WHERE o.price > (SELECT AVG(p.price) + 400 FROM orders p WHERE p.region = o.region)',
  'SELECT c.cust, (SELECT MAX(o.price) FROM orders o WHERE o.cust = c.cust) AS mx, (SELECT COUNT(*) FROM orders o WHERE o.cust = c.cust AND o.qty = 9) AS n9 FROM customers c ORDER BY c.cust LIMIT 25',
  'SELECT cust, price, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY price DESC, ts) AS rn FROM orders WHERE cust < 5 AND price IS NOT NULL ORDER BY cust, rn LIMIT 40',
  'SELECT region, cust, s FROM (SELECT region, cust, SUM(qty) AS s, RANK() OVER (PARTITION BY region ORDER BY SUM(qty) DESC) AS r FROM orders GROUP BY region, cust) t WHERE r <= 3 ORDER BY region, s DESC, cust',
  "SELECT d, n, SUM(n) OVER (ORDER BY d) AS running, n - LAG(n) OVER (ORDER BY d) AS change, AVG(n) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma FROM (SELECT DATE_TRUNC('month', ts) AS d, COUNT(*) AS n FROM orders GROUP BY 1) m ORDER BY d",
  "SELECT d, SUM(n) OVER (ORDER BY d RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW) AS wk, MIN(n) OVER (ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS mn FROM (SELECT DATE_TRUNC('day', ts) AS d, COUNT(*) AS n FROM orders GROUP BY 1) x ORDER BY d",
  'SELECT cust, DENSE_RANK() OVER (ORDER BY tier) AS dr, NTILE(7) OVER (ORDER BY cust) AS q, PERCENT_RANK() OVER (PARTITION BY tier ORDER BY cust) AS pr, CUME_DIST() OVER (ORDER BY tier) AS cd, LEAD(cust, 3, -1) OVER (PARTITION BY tier ORDER BY cust) AS ld, FIRST_VALUE(cust) OVER (PARTITION BY tier ORDER BY cust DESC) AS fv, COUNT(*) OVER (PARTITION BY tier) AS n FROM customers ORDER BY cust',
  'SELECT region, qty, SUM(price) OVER (PARTITION BY region ORDER BY qty) AS s, AVG(price) OVER (PARTITION BY region) AS a, MAX(price) OVER (PARTITION BY region ORDER BY qty RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS m FROM (SELECT DISTINCT region, qty, MAX(price) AS price FROM orders GROUP BY region, qty) t ORDER BY region, qty',
  'SELECT region, MEDIAN(price) AS med, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY price) AS p90, PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY qty) AS p25 FROM orders GROUP BY region ORDER BY region',
  'SELECT MEDIAN(price) AS m, QUANTILE_CONT(price, 0.99) AS p99 FROM orders',
  'SELECT cust, MEDIAN(price - 500) AS m, PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY price) AS p75 FROM orders GROUP BY cust ORDER BY cust',
  'SELECT cust, price FROM orders WHERE cust = 7 ORDER BY price NULLS FIRST, ts LIMIT 12',
  'SELECT COUNT(*) AS n FROM customers a CROSS JOIN customers b WHERE a.tier = b.tier AND a.cust < 100 AND b.cust < 50',
  // text
  'SELECT UPPER(region) AS r, LOWER(region) AS l, LENGTH(region) AS n, COUNT(*) AS c FROM orders GROUP BY region ORDER BY r',
  "SELECT region || '/' || CAST(qty AS VARCHAR) AS k, COUNT(*) AS n, SUM(price) AS s FROM orders GROUP BY 1 ORDER BY k",
  "SELECT CASE WHEN price >= 500 THEN 'high' WHEN price >= 100 THEN 'mid' WHEN price IS NULL THEN 'unknown' ELSE 'low' END AS band, COUNT(*) AS n FROM orders GROUP BY band ORDER BY band",
  "SELECT STRFTIME(ts, '%Y-%m') AS ym, COUNT(*) AS n, MAX(price) AS mx FROM orders GROUP BY ym ORDER BY ym",
  "SELECT id, TRIM(name) AS t, UPPER(name) AS u, LENGTH(name) AS len, SUBSTR(name, 2, 3) AS s, LEFT(TRIM(name), 3) AS l, RIGHT(name, 2) AS r, REPLACE(name, ' ', '_') AS rp, REVERSE(city) AS rv, LPAD(city, 8, '.') AS lp, SPLIT_PART(TRIM(name), ' ', 2) AS sp, STRPOS(name, 'a') AS pos, CONCAT(city, '!', id) AS c, city || '/' || id AS cc, CONCAT_WS('-', city, name) AS ws, TRIM(LEADING ' ' FROM name) AS lt FROM people ORDER BY id",
  "SELECT id, REGEXP_REPLACE(name, '[aeiou]', '*', 'g') AS r, REGEXP_EXTRACT(name, '([A-Z])[a-z]+', 1) AS x, REGEXP_MATCHES(name, 'an') AS m, name ILIKE '%AN%' AS il, STARTS_WITH(city, 'S') AS sw FROM people ORDER BY id",
  "SELECT name FROM customers WHERE name >= 'c4' AND name < 'c41' ORDER BY name",
  "SELECT region, COUNT(*) AS n FROM orders GROUP BY region HAVING LOWER(region) LIKE '%a%' ORDER BY region",
  'SELECT SUM(CAST(SUBSTR(name, 2) AS INTEGER)) AS s, COUNT(DISTINCT LEFT(name, 2)) AS d FROM customers',
  "SELECT CAST(cust AS VARCHAR) AS k FROM customers WHERE cust < 3 UNION SELECT region FROM orders ORDER BY k",
  // outer joins
  "SELECT o.cust, COALESCE(c.name, 'none') AS name, c.cust AS cc FROM orders o LEFT JOIN customers c ON o.cust = c.cust WHERE o.qty = 9 AND o.price > 990 ORDER BY o.ts, o.cust",
  'SELECT c.tier, COUNT(o.cust) AS n, COUNT(*) AS rows FROM (SELECT * FROM orders WHERE price > 999) o RIGHT JOIN customers c ON o.cust = c.cust GROUP BY c.tier ORDER BY c.tier',
  'SELECT COALESCE(o.cust, c.cust) AS k, o.n, c.name FROM (SELECT cust, COUNT(*) AS n FROM orders WHERE price > 999 GROUP BY cust) o FULL JOIN customers c ON o.cust = c.cust ORDER BY k',
  'SELECT cust, n, tier FROM (SELECT cust, COUNT(*) AS n FROM orders WHERE price > 999.5 GROUP BY cust) o FULL JOIN customers USING (cust) ORDER BY cust',
  'SELECT COUNT(*) AS n, COUNT(o.cust) AS a, COUNT(c.cust) AS b FROM orders o FULL OUTER JOIN customers c ON o.cust = c.cust',
  'SELECT cust AS qty, qty AS cust FROM orders WHERE price > 999.9 ORDER BY qty, cust',
];

const csv = join(tmpdir(), `vectro-check-sql-${N}.csv`);
writeFileSync(csv, 'cust,price,qty,region,ts\n' + o.cust.map((x, i) => `${x},${o.price[i] === o.price[i] ? o.price[i] : ''},${o.qty[i]},${o.region[i]},${o.ts[i].toISOString()}`).join('\n'));
const db = await (await DuckDBInstance.create(':memory:')).connect();
await db.run(`CREATE TABLE orders AS SELECT cust::INTEGER AS cust, price::DOUBLE AS price, qty::INTEGER AS qty, region, ts::TIMESTAMP AS ts FROM read_csv('${csv}')`);
await db.run('CREATE TABLE customers AS SELECT i::INTEGER AS cust, \'c\' || i AS name, (1 + i % 4)::INTEGER AS tier FROM range(1000) t(i)');
await db.run(`CREATE TABLE people AS SELECT * FROM (VALUES ${people.map(([id, name, city]) => `(${id}, '${name.replaceAll("'", "''")}', ${city === null ? 'NULL' : `'${city}'`})`).join(', ')}) t(id, name, city)`);
rmSync(csv);

const engine = new Engine();
const tables = { orders: engine.table(o), customers: engine.table(c), people: engine.table({ id: people.map((p) => p[0]), name: people.map((p) => p[1]), city: people.map((p) => p[2] ?? '') }) };
const norm = (x) => {
  if (x === null || x === undefined) return 'NULL';
  if (typeof x === 'bigint') x = Number(x);
  if (x instanceof Date) return x.getTime();
  if (typeof x === 'object' && 'micros' in x) return Number(x.micros / 1000n); // a DuckDB timestamp
  if (typeof x === 'boolean') return x ? 1 : 0;
  if (typeof x === 'number') return x !== x ? 'NULL' : Number(x.toPrecision(10));
  return x === '' ? 'NULL' : x;
};
let bad = 0;
for (const q of QUERIES) {
  const want = (await db.runAndReadAll(q)).getRowObjects().map((row) => Object.values(row).map(norm));
  let got;
  try { got = (await sql(q, tables).toRowsAsync()).map((row) => Object.values(row).map(norm)); } catch (err) { bad++; console.log(`ERROR ${q}\n  ${err.message}`); continue; }
  const diff = got.length !== want.length ? -1 : want.findIndex((w, i) => JSON.stringify(w) !== JSON.stringify(got[i]));
  if (diff !== -1 || got.length !== want.length) {
    bad++;
    const i = Math.max(0, diff);
    console.log(`DIFFER ${q}\n  rows: Vectro ${got.length}, DuckDB ${want.length}; row ${i}: Vectro ${JSON.stringify(got[i])}, DuckDB ${JSON.stringify(want[i])}`);
  } else console.log(`same   (${String(want.length).padStart(4)} rows) ${q.slice(0, 110)}`);
}
await engine.close();
console.log(bad ? `${bad} of ${QUERIES.length} differ` : `all ${QUERIES.length} queries give the same answers`);
process.exit(bad ? 1 : 0);
