// The same SQL text in Vectro and DuckDB: 10M orders, 100K customers. Each
// system alone in its own process, best of 5 after a warm-up; answers are
// compared (row count and a checksum of every number).
//   node compare/bench-sql.mjs        (needs this folder's npm install)
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 10_000_000);
const R = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const QUERIES = [
  ['totals with a filter', 'SELECT COUNT(*) AS n, SUM(price * qty * (1 - discount)) AS revenue FROM orders WHERE price > 100 AND qty < 5'],
  ['group by region', 'SELECT region, COUNT(*) AS n, SUM(price * qty) AS revenue, AVG(price) AS avg_price FROM orders GROUP BY region ORDER BY region'],
  ['join + filter + group', "SELECT c.tier, COUNT(*) AS n, SUM(o.price) AS spend FROM orders o JOIN customers c ON o.cust = c.cust WHERE o.region = 'EU' GROUP BY c.tier ORDER BY c.tier"],
  ['top 10 customers', 'SELECT cust, SUM(price * qty) AS spend FROM orders GROUP BY cust ORDER BY spend DESC LIMIT 10'],
  ['distinct customers per month', 'SELECT EXTRACT(YEAR FROM ts) * 100 + EXTRACT(MONTH FROM ts) AS ym, COUNT(DISTINCT cust) AS customers FROM orders GROUP BY 1 ORDER BY 1'],
  ['top 20 rows by price', 'SELECT cust, price, qty, region FROM orders WHERE region <> \'US\' ORDER BY price DESC, cust LIMIT 20'],
  ['CASE inside SUM', "SELECT region, SUM(CASE WHEN qty >= 5 THEN price ELSE 0 END) AS big FROM orders GROUP BY region HAVING SUM(qty) > 0 ORDER BY big DESC"],
  ['IN (subquery)', 'SELECT region, COUNT(*) AS n FROM orders WHERE cust IN (SELECT cust FROM customers WHERE tier = 1) GROUP BY region ORDER BY region'],
  ['correlated subquery', 'SELECT COUNT(*) AS n FROM orders o WHERE o.price > (SELECT AVG(p.price) FROM orders p WHERE p.region = o.region)'],
  ['top 3 per region (RANK)', 'SELECT region, cust, spend FROM (SELECT region, cust, SUM(price * qty) AS spend, RANK() OVER (PARTITION BY region ORDER BY SUM(price * qty) DESC) AS r FROM orders GROUP BY region, cust) t WHERE r <= 3 ORDER BY region, spend DESC, cust'],
  ['ROW_NUMBER over all rows', 'SELECT COUNT(*) AS n, SUM(rn) AS s FROM (SELECT ROW_NUMBER() OVER (PARTITION BY cust ORDER BY ts) AS rn FROM orders) t WHERE rn <= 3'],
  ['running total by day', "SELECT d, SUM(n) OVER (ORDER BY d) AS running FROM (SELECT DATE_TRUNC('day', ts) AS d, COUNT(*) AS n FROM orders GROUP BY 1) x ORDER BY d"],
  ['median per region', 'SELECT region, MEDIAN(price) AS m FROM orders GROUP BY region ORDER BY region'],
];
const csvOf = (name) => join(tmpdir(), `vectro-sql-${name}-${N}.csv`);
function rng(seed) { return () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
function makeData() {
  const r = rng(9);
  const o = { cust: new Float64Array(N), price: new Float64Array(N), qty: new Float64Array(N), discount: new Float64Array(N), region: new Array(N), ts: new Float64Array(N) };
  for (let i = 0; i < N; i++) {
    o.cust[i] = Math.floor(r() * 100_000); o.price[i] = Math.round(r() * 100000) / 100; o.qty[i] = 1 + Math.floor(r() * 9);
    o.discount[i] = Math.floor(r() * 30) / 100; o.region[i] = R[Math.floor(r() * 8)]; o.ts[i] = Date.UTC(2023, 0, 1) + Math.floor(r() * 730 * 86400) * 1000;
  }
  const c = { cust: Float64Array.from({ length: 100_000 }, (_, i) => i), tier: Float64Array.from({ length: 100_000 }, () => 1 + Math.floor(r() * 3)) };
  return { orders: o, customers: c };
}
const checksum = (rows) => {
  let s = 0;
  for (const row of rows) for (const v of Object.values(row)) { const x = typeof v === 'bigint' ? Number(v) : typeof v === 'number' ? v : typeof v === 'string' ? v.length : 0; if (x === x) s += x; }
  return [rows.length, s];
};

const child = process.argv[2];
if (child) {
  const out = {};
  let run, close;
  if (child === 'vectro') {
    const { Engine, sql } = await import('../src/node.mjs');
    const engine = new Engine();
    const d = makeData();
    const tables = { orders: engine.table({ ...d.orders, ts: Array.from(d.orders.ts, (t) => new Date(t)) }), customers: engine.table(d.customers) };
    run = (q) => checksum(sql(q, tables).toRows());
    close = () => engine.close();
  } else {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const c = await (await DuckDBInstance.create(':memory:')).connect();
    await c.run(`CREATE TABLE orders AS SELECT * FROM read_csv('${csvOf('orders')}', header = true, columns = { 'cust': 'DOUBLE', 'price': 'DOUBLE', 'qty': 'DOUBLE', 'discount': 'DOUBLE', 'region': 'VARCHAR', 'ts': 'TIMESTAMP' })`);
    await c.run(`CREATE TABLE customers AS SELECT * FROM read_csv('${csvOf('customers')}', header = true, columns = { 'cust': 'DOUBLE', 'tier': 'DOUBLE' })`);
    run = async (q) => checksum((await c.runAndReadAll(q)).getRowObjects());
    close = () => c.closeSync?.();
  }
  for (const [label, q] of QUERIES) {
    const answer = await run(q);
    let best = Infinity;
    for (let i = 0; i < 5; i++) { const s = performance.now(); await run(q); best = Math.min(best, performance.now() - s); }
    out[label] = { ms: best, answer };
  }
  await close();
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}

if (!existsSync(csvOf('orders'))) {
  process.stderr.write('writing CSV files for DuckDB... ');
  const d = makeData();
  for (const [name, cols] of Object.entries(d)) {
    const names = Object.keys(cols), n = cols[names[0]].length;
    const ws = createWriteStream(csvOf(name));
    ws.write(names.join(',') + '\n');
    for (let i = 0; i < n; i += 100_000) {
      let chunk = '';
      for (let j = i; j < Math.min(n, i + 100_000); j++) chunk += names.map((k) => (k === 'ts' ? new Date(cols.ts[j]).toISOString().replace('T', ' ').replace('.000Z', '') : cols[k][j])).join(',') + '\n';
      if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
    }
    await new Promise((r) => ws.end(r));
  }
  process.stderr.write('done\n');
}
const results = {};
for (const sys of ['vectro', 'duckdb']) {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), sys], { encoding: 'utf8', maxBuffer: 1 << 26 });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(sys, r.stderr.slice(-3000)); process.exit(1); }
  results[sys] = JSON.parse(r.stdout.slice(at + 8));
}
console.log(`The same SQL on ${N.toLocaleString()} orders + 100,000 customers (best of 5)\n`);
console.log('Query'.padEnd(32), 'Vectro'.padStart(10), 'DuckDB'.padStart(10), '  speed      answers');
for (const [label] of QUERIES) {
  const v = results.vectro[label], d = results.duckdb[label];
  const same = v.answer[0] === d.answer[0] && Math.abs(v.answer[1] - d.answer[1]) <= 1e-9 * Math.abs(d.answer[1]) + 1e-6;
  const ratio = d.ms / v.ms;
  console.log(label.padEnd(32), `${v.ms.toFixed(1)} ms`.padStart(10), `${d.ms.toFixed(1)} ms`.padStart(10), `  ${ratio >= 1 ? `${ratio.toFixed(1)}x faster` : `${(1 / ratio).toFixed(1)}x slower`}`.padEnd(15), same ? 'match' : `DIFFER ${v.answer} vs ${d.answer}`);
}
