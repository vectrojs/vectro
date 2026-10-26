// Tables bigger than memory: the same SQL on a Vectro dataset and a DuckDB
// database file, both on disk. Every value comes from the row number by
// whole-number arithmetic that is exact in JS and in DuckDB, so both hold
// exactly the same rows and the answers are compared.
//   ROWS=1000000000 node compare/bench-dataset.mjs     (needs this folder's npm install)
//   DATA_DIR=...  where the files go (default: the system temp folder)
// Each system runs alone in its own process: the first run of each query,
// then the best of 3. Peak memory (RSS) of each process is reported.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, statSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 100_000_000);
const DIR = process.env.DATA_DIR ?? tmpdir();
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const CUSTOMERS = 1_000_003;
const T0 = Date.UTC(2024, 0, 1);
const vectroFile = join(DIR, `vectro-orders-${N}.vectro`), duckFile = join(DIR, `duckdb-orders-${N}.duckdb`);
const lastDay = new Date(T0 + (N - 86_400) * 1000).toISOString().replace('T', ' ').replace('.000Z', '');

// row i (exact in float64 and in BIGINT: every product stays below 2^53)
const cols = {
  cust: (i) => ((i % 1_000_003) * 7919) % 1_000_003,
  qty: (i) => 1 + (((i % 999_983) * 31_337) % 9),
  price: (i) => (((i % 1_000_037) * 16_807) % 50 === 0 ? NaN : (((i % 1_000_033) * 48_271) % 100_000) / 100),
  region: (i) => ((i % 1_000_039) * 69_621) % 5,
};
const DUCK_ROWS = `SELECT ((i % 1000003) * 7919) % 1000003 AS cust,
  1 + (((i % 999983) * 31337) % 9) AS qty,
  CASE WHEN ((i % 1000037) * 16807) % 50 = 0 THEN NULL ELSE ((((i % 1000033) * 48271) % 100000) / 100.0)::DOUBLE END AS price,
  (['EU', 'US', 'APAC', 'LATAM', 'MEA'])[(((i % 1000039) * 69621) % 5) + 1] AS region,
  TIMESTAMP '2024-01-01 00:00:00' + to_seconds(i) AS ts
  FROM range(${N}) t(i)`;

const QUERIES = [
  ['scan everything', 'SELECT COUNT(*) AS n, SUM(price * qty) AS revenue FROM orders'],
  ['filter: EU, qty >= 5', "SELECT COUNT(*) AS n, SUM(price) AS s FROM orders WHERE region = 'EU' AND qty >= 5"],
  ['the last day (time range)', `SELECT COUNT(*) AS n, AVG(price) AS p FROM orders WHERE ts >= TIMESTAMP '${lastDay}'`],
  ['group by region', 'SELECT region, COUNT(*) AS n, AVG(price) AS p, MAX(price) AS hi FROM orders GROUP BY region ORDER BY region'],
  ['top 10 of 1M customers', 'SELECT cust, ROUND(SUM(price * qty), 2) AS spend FROM orders GROUP BY cust ORDER BY spend DESC, cust LIMIT 10'],
  ['top 10 rows by price', "SELECT cust, price, qty FROM orders WHERE region = 'US' ORDER BY price DESC, ts LIMIT 10"],
  ['join 1M customers', 'SELECT c.tier, COUNT(*) AS n, SUM(o.qty) AS q FROM orders o JOIN customers c ON o.cust = c.cust WHERE o.qty = 9 GROUP BY c.tier ORDER BY c.tier'],
  ['distinct customers per region', 'SELECT region, COUNT(DISTINCT cust) AS c FROM orders GROUP BY region ORDER BY region'],
];
const checksum = (rows) => {
  let s = 0;
  for (const row of rows) for (const v of Object.values(row)) { const x = typeof v === 'bigint' ? Number(v) : typeof v === 'number' ? v : typeof v === 'string' ? v.length : 0; if (x === x) s += x; }
  return [rows.length, s];
};
const mb = (b) => `${(b / 2 ** 20).toFixed(0)} MB`;
const gb = (b) => `${(b / 2 ** 30).toFixed(1)} GB`;

const child = process.argv[2];
if (child === 'make-vectro') {
  const { Engine } = await import('../src/node.mjs');
  const e = new Engine();
  const t0 = performance.now();
  const w = e.createDataset(vectroFile);
  const R = 1 << 22;
  for (let lo = 0; lo < N; lo += R) {
    const n = Math.min(R, N - lo);
    const c = { cust: new Float64Array(n), qty: new Float64Array(n), price: new Float64Array(n), region: { codes: new Uint8Array(n), labels: REG }, ts: new Float64Array(n) };
    for (let k = 0; k < n; k++) {
      const i = lo + k;
      c.cust[k] = cols.cust(i); c.qty[k] = cols.qty(i); c.price[k] = cols.price(i); c.region.codes[k] = cols.region(i); c.ts[k] = T0 + i * 1000;
    }
    const t = e.table(c, { dates: ['ts'], compress: false });
    w.append(t);
    t.dispose();
    if ((lo / R) % 25 === 0) process.stderr.write(`\r  vectro: ${((lo + n) / 1e6).toFixed(0)}M rows`);
  }
  const s = w.close();
  process.stderr.write('\n');
  process.stdout.write('@@RESULT' + JSON.stringify({ ms: performance.now() - t0, bytes: s.bytes }));
  process.exit(0);
}
if (child === 'make-duckdb') {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const t0 = performance.now();
  const c = await (await DuckDBInstance.create(duckFile)).connect();
  await c.run(`CREATE TABLE orders AS ${DUCK_ROWS}`);
  await c.run('CHECKPOINT');
  c.closeSync?.();
  process.stdout.write('@@RESULT' + JSON.stringify({ ms: performance.now() - t0, bytes: statSync(duckFile).size }));
  process.exit(0);
}
if (child === 'vectro' || child === 'duckdb') {
  const out = {};
  let run, close, extra = () => ({});
  if (child === 'vectro') {
    const { Engine, sql } = await import('../src/node.mjs');
    const engine = new Engine();
    const orders = engine.openDataset(vectroFile);
    const customers = engine.table({ cust: Float64Array.from({ length: CUSTOMERS }, (_, i) => i), tier: Float64Array.from({ length: CUSTOMERS }, (_, i) => 1 + (i % 3)) });
    const tables = { orders, customers };
    run = async (q) => checksum(await sql(q, tables).toRowsAsync());
    close = async () => { await orders.close(); await engine.close(); };
    extra = () => ({ engineMemory: engine.memory.buffer.byteLength, lastRun: orders.lastRun });
  } else {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const c = await (await DuckDBInstance.create(duckFile)).connect();
    await c.run(`CREATE TEMP TABLE customers AS SELECT i AS cust, 1 + (i % 3) AS tier FROM range(${CUSTOMERS}) t(i)`);
    run = async (q) => checksum((await c.runAndReadAll(q)).getRowObjects());
    close = () => c.closeSync?.();
  }
  for (const [label, q] of QUERIES) {
    let s = performance.now();
    const answer = await run(q);
    const first = performance.now() - s;
    let best = first;
    for (let i = 0; i < 3; i++) { s = performance.now(); await run(q); best = Math.min(best, performance.now() - s); }
    out[label] = { first, best, answer, ...extra() };
    process.stderr.write(`  ${child}: ${label} ${best.toFixed(0)} ms\n`);
  }
  await close();
  out.peakRss = process.resourceUsage().maxRSS * 1024;
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}

const spawn = (arg) => {
  const r = spawnSync(process.execPath, ['--max-old-space-size=4096', fileURLToPath(import.meta.url), arg], { encoding: 'utf8', maxBuffer: 1 << 26, stdio: ['ignore', 'pipe', 'inherit'] });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(arg, 'failed', r.status, r.stdout.slice(-2000)); process.exit(1); }
  return JSON.parse(r.stdout.slice(at + 8));
};
if (process.env.REBUILD) for (const f of [vectroFile, duckFile, `${duckFile}.wal`]) rmSync(f, { force: true });
const made = {};
if (!existsSync(vectroFile)) { console.error(`writing ${N.toLocaleString()} rows as a Vectro dataset...`); made.vectro = spawn('make-vectro'); }
if (!existsSync(duckFile)) { console.error(`writing ${N.toLocaleString()} rows as a DuckDB database...`); made.duckdb = spawn('make-duckdb'); }
const results = { vectro: spawn('vectro'), duckdb: spawn('duckdb') };
const plain = N * (8 * 4 + 8); // as float64 columns in memory (text as codes)
console.log(`${N.toLocaleString()} orders on disk: Vectro dataset ${gb(statSync(vectroFile).size)}, DuckDB file ${gb(statSync(duckFile).size)} (in memory as plain float64 columns: ${gb(plain)})`);
if (made.vectro || made.duckdb) console.log(`written in: Vectro ${made.vectro ? `${(made.vectro.ms / 1000).toFixed(1)} s` : '(existing)'}, DuckDB ${made.duckdb ? `${(made.duckdb.ms / 1000).toFixed(1)} s` : '(existing)'}`);
console.log(`peak memory (RSS): Vectro ${mb(results.vectro.peakRss)}, DuckDB ${mb(results.duckdb.peakRss)}\n`);
console.log('Query'.padEnd(32), 'Vectro'.padStart(10), 'DuckDB'.padStart(10), '  speed       first run (V / D)    answers');
for (const [label] of QUERIES) {
  const v = results.vectro[label], d = results.duckdb[label];
  const same = v.answer[0] === d.answer[0] && Math.abs(v.answer[1] - d.answer[1]) <= 1e-9 * Math.abs(d.answer[1]) + 1e-6;
  const ratio = d.best / v.best;
  console.log(label.padEnd(32), `${v.best.toFixed(0)} ms`.padStart(10), `${d.best.toFixed(0)} ms`.padStart(10), `  ${ratio >= 1 ? `${ratio.toFixed(1)}x faster` : `${(1 / ratio).toFixed(1)}x slower`}`.padEnd(14), `${v.first.toFixed(0)} / ${d.first.toFixed(0)} ms`.padStart(18), '  ', same ? 'match' : `DIFFER ${v.answer} vs ${d.answer}`);
}
console.log(`\nVectro engine memory: ${mb(Math.max(...QUERIES.map(([l]) => results.vectro[l].engineMemory)))} at most; chunks read for the last-day query: ${results.vectro['the last day (time range)'].lastRun.read} of ${results.vectro['the last day (time range)'].lastRun.chunks}`);
