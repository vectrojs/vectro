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
];

const csv = join(tmpdir(), `vectro-check-sql-${N}.csv`);
writeFileSync(csv, 'cust,price,qty,region,ts\n' + o.cust.map((x, i) => `${x},${o.price[i] === o.price[i] ? o.price[i] : ''},${o.qty[i]},${o.region[i]},${o.ts[i].toISOString()}`).join('\n'));
const db = await (await DuckDBInstance.create(':memory:')).connect();
await db.run(`CREATE TABLE orders AS SELECT cust::DOUBLE AS cust, price::DOUBLE AS price, qty::DOUBLE AS qty, region, ts::TIMESTAMP AS ts FROM read_csv('${csv}')`);
await db.run('CREATE TABLE customers AS SELECT i::DOUBLE AS cust, \'c\' || i AS name, (1 + i % 4)::DOUBLE AS tier FROM range(1000) t(i)');
rmSync(csv);

const engine = new Engine();
const tables = { orders: engine.table(o), customers: engine.table(c) };
const norm = (x) => {
  if (x === null || x === undefined) return 'NULL';
  if (typeof x === 'bigint') x = Number(x);
  if (x instanceof Date) return x.getTime();
  if (typeof x === 'object' && 'micros' in x) return Number(x.micros / 1000n); // a DuckDB timestamp
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
