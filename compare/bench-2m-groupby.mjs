// The LinkedIn test "Postgres 17 vs DuckDB 2.0": 2M orders,
//   SELECT product_id, sum(amount) FROM 'orders.parquet' GROUP BY 1
// run on DuckDB (1 thread and all threads), Vectro (1 thread and all, through
// sql() and groupBy().agg()) and, if one is running here, PostgreSQL. In
// memory and from a file on disk: DuckDB over Parquet and its own .duckdb
// file, Vectro over its .vectro dataset (Vectro can't read Parquet).
// The post doesn't say how many products there are, so it runs at several.
// Each way of running it (system, threads, API, file) alone in its own
// process, so none is measured warmer than another; 3 warm-up runs, then the
// median of 7; every answer (every product's sum) checked against
// DuckDB's.
//   node compare/bench-2m-groupby.mjs        (needs this folder's npm install)
//   PRODUCTS=1000,100000   product_id cardinalities (default)
//   PGDATABASE=...         scratch database to create, load and drop (default vectro_bench_2m);
//                          other PG* variables pick the server. NO_PG=1 skips Postgres.
//   WARM=3                 warm-up runs before the 7 timed ones (more: steady state)
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, mkdirSync, statSync, createWriteStream, rmSync, writeFileSync } from 'node:fs';
import { tmpdir, availableParallelism, cpus, totalmem } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 2_000_000);
const PRODUCTS = (process.env.PRODUCTS ?? '1000,100000').split(',').map(Number);
const ALL = availableParallelism();
const DIR = join(process.env.DATA_DIR ?? tmpdir(), 'vectro-2m-groupby');
const WARM = Number(process.env.WARM ?? 3), RUNS = 7;
const T0 = Date.UTC(2024, 0, 1), DAY = 86_400_000;
const PGDB = process.env.PGDATABASE ?? 'vectro_bench_2m';
const file = (k, ext) => join(DIR, `orders-${N}-${k}.${ext}`);
const DUCK_CASES = ['memory: sql()', 'file: sql()', '.duckdb file'];
const VECTRO_CASES = ['memory: sql()', 'memory: groupBy().agg()', 'file: sql()', 'file: groupBy().agg()'];
const QUERY = (from) => `SELECT product_id, sum(amount) FROM ${from} GROUP BY 1`;

function makeColumns(k) {
  let seed = 2024; // mulberry32
  const rnd = () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  const id = new Int32Array(N), product_id = new Int32Array(N), amount = new Float64Array(N), date = new Float64Array(N);
  for (let i = 0; i < N; i++) {
    id[i] = i + 1;
    product_id[i] = 1 + Math.floor(rnd() * k);
    amount[i] = Math.round(rnd() * 100000) / 100; // 0.00 .. 1000.00
    date[i] = T0 + Math.floor(rnd() * 730) * DAY;
  }
  return { id, product_id, amount, date };
}
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[s.length >> 1]; };
// [product_id, sum] pairs -> Map
const toMap = (pairs) => new Map(pairs.map(([p, s]) => [Number(p), Number(s)]));

// Time fn: WARM runs, then RUNS timed. fn returns [result, release]; the
// result is turned into an answer once, outside the timing.
async function time(fn, answerOf) {
  for (let i = 0; i < WARM; i++) { const [, release] = await fn(); await release?.(); }
  const ms = [];
  let last;
  for (let i = 0; i < RUNS; i++) {
    const s = performance.now();
    const [r, release] = await fn();
    ms.push(performance.now() - s);
    if (i === RUNS - 1) last = await answerOf(r);
    await release?.();
  }
  return { ms: median(ms), all: ms, answer: last };
}

const [child, karg, targ, which] = process.argv.slice(2);
const k = Number(karg), threads = Number(targ);
// (exit once the pipe has taken it all: answers run to megabytes)
const done = (out) => new Promise(() => process.stdout.write('@@RESULT' + JSON.stringify(out), () => process.exit(0)));

if (child === 'make-vectro') {
  const { Engine } = await import('../src/node.mjs');
  const e = new Engine();
  const t = e.table(makeColumns(k), { dates: ['date'] });
  const w = e.createDataset(file(k, 'vectro'));
  w.append(t);
  w.close();
  t.dispose();
  await e.close();
  await done({});
}
if (child === 'make-duckdb') {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const c = await (await DuckDBInstance.create(file(k, 'duckdb'))).connect();
  await c.run(`CREATE TABLE orders AS SELECT * FROM read_csv('${file(k, 'csv')}', header = true,
    columns = { 'id': 'INTEGER', 'product_id': 'INTEGER', 'amount': 'DOUBLE', 'date': 'DATE' })`);
  await c.run(`COPY orders TO '${file(k, 'parquet')}' (FORMAT parquet)`);
  await c.run('CHECKPOINT');
  c.closeSync?.();
  await done({});
}
if (child === 'vectro') {
  const { Engine, sql } = await import('../src/node.mjs');
  const engine = new Engine({ threads });
  await new Promise((r) => setTimeout(r, 300)); // workers up
  const memory = which.startsWith('memory');
  const orders = memory ? engine.table(makeColumns(k), { dates: ['date'] }) : engine.openDataset(file(k, 'vectro'));
  const rows = (t) => t.toRows().map((r) => Object.values(r));
  const release = (t) => () => t.dispose();
  const run = which.endsWith('sql()') ? () => { const t = sql(QUERY('orders'), { orders }).collect(); return [t, release(t)]; }
    : memory ? () => { const t = orders.groupBy('product_id').agg({ s: 'sum(amount)' }); return [t, release(t)]; }
    : () => { const t = orders.groupBy('product_id').agg({ s: 'sum(amount)' }).collect(); return [t, release(t)]; };
  const out = { [which]: await time(run, rows) };
  if (!memory) await orders.close();
  await engine.close();
  await done(out);
}
if (child === 'duckdb') {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const mem = await (await DuckDBInstance.create(':memory:')).connect();
  const disk = await (await DuckDBInstance.create(file(k, 'duckdb'), { access_mode: 'READ_ONLY' })).connect();
  for (const c of [mem, disk]) if (threads !== ALL) await c.run(`SET threads = ${threads}`);
  await mem.run(`CREATE TABLE orders AS SELECT * FROM '${file(k, 'parquet')}'`);
  const threadsSeen = Number((await mem.runAndReadAll("SELECT current_setting('threads') AS t")).getRows()[0][0]);
  const version = (await mem.runAndReadAll('SELECT version()')).getRows()[0][0];
  // the result stays in DuckDB's vectors, as Vectro's stays a table in its memory
  const run = (c, q) => async () => [await c.runAndReadAll(q)];
  const rows = (r) => r.getRows();
  const out = {
    [which]: await time(which === 'memory: sql()' ? run(mem, QUERY('orders')) : which === 'file: sql()' ? run(mem, QUERY(`'${file(k, 'parquet')}'`)) : run(disk, QUERY('orders')), rows),
  };
  mem.closeSync?.(); disk.closeSync?.();
  await done({ ...out, threadsSeen, version });
}

// ---- PostgreSQL (run from here through psql) -------------------------------------------
const psql = (args, input) => spawnSync('psql', ['-X', '-q', '-v', 'ON_ERROR_STOP=1', ...args], { encoding: 'utf8', input, maxBuffer: 1 << 28 });
function postgres(k) {
  const t = `orders_${k}`;
  let r = psql(['-d', PGDB], `DROP TABLE IF EXISTS ${t};
CREATE TABLE ${t} (id integer, product_id integer, amount double precision, date date);
\\copy ${t} FROM '${file(k, 'csv')}' CSV HEADER
VACUUM ANALYZE ${t};`);
  if (r.status) throw new Error(r.stderr);
  const bytes = Number(psql(['-d', PGDB, '-Atc', `SELECT pg_total_relation_size('${t}')`]).stdout);
  const q = QUERY(t);
  const out = { bytes };
  for (const [label, set] of [['default (parallel)', ''], ['1 process', 'SET max_parallel_workers_per_gather = 0;']]) {
    // as a client sees it: psql's \timing, rows received and thrown away
    r = psql(['-d', PGDB], `${set}\n\\o /dev/null\n\\timing on\n${`${q};\n`.repeat(WARM + RUNS)}`);
    if (r.status) throw new Error(r.stderr);
    const client = [...r.stdout.matchAll(/Time: ([\d.]+) ms/g)].map((m) => Number(m[1])).slice(WARM);
    // inside the server: EXPLAIN ANALYZE's execution time (no rows sent)
    r = psql(['-d', PGDB, '-At'], `${set}\n${`EXPLAIN (ANALYZE, TIMING OFF) ${q};\n`.repeat(WARM + RUNS)}`);
    if (r.status) throw new Error(r.stderr);
    const server = [...r.stdout.matchAll(/Execution Time: ([\d.]+) ms/g)].map((m) => Number(m[1])).slice(WARM);
    const workers = Math.max(0, ...[...r.stdout.matchAll(/Workers Launched: (\d+)/g)].map((m) => Number(m[1])));
    const answer = psql(['-d', PGDB, '-At', '-F', ','], `${set}\n${q};`).stdout.trim().split('\n').map((l) => l.split(',').map(Number));
    out[label] = { ms: median(client), all: client, serverMs: median(server), processes: 1 + workers, answer };
  }
  psql(['-d', PGDB, '-c', `DROP TABLE ${t}`]);
  return out;
}

// ---- the run ----------------------------------------------------------------------------
const spawn = (...args) => {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), ...args.map(String)], { encoding: 'utf8', maxBuffer: 1 << 28, stdio: ['ignore', 'pipe', 'inherit'] });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(args.join(' '), 'failed', r.status, r.stdout.slice(-2000)); process.exit(1); }
  return JSON.parse(r.stdout.slice(at + 8));
};
mkdirSync(DIR, { recursive: true });
let pgVersion = null;
if (!process.env.NO_PG) {
  const v = psql(['-d', 'postgres', '-Atc', 'SHOW server_version']);
  if (v.status === 0) {
    pgVersion = v.stdout.trim();
    const exists = psql(['-d', 'postgres', '-Atc', `SELECT 1 FROM pg_database WHERE datname = '${PGDB}'`]).stdout.trim();
    if (!exists && psql(['-d', 'postgres', '-c', `CREATE DATABASE ${PGDB}`]).status) pgVersion = null;
  }
}
const results = {};
for (const k of PRODUCTS) {
  if (!existsSync(file(k, 'csv'))) {
    process.stderr.write(`writing ${N.toLocaleString()} orders, ${k.toLocaleString()} products... `);
    const c = makeColumns(k);
    const ws = createWriteStream(file(k, 'csv'));
    ws.write('id,product_id,amount,date\n');
    for (let i = 0; i < N; i += 100_000) {
      let chunk = '';
      for (let j = i; j < Math.min(N, i + 100_000); j++) chunk += `${c.id[j]},${c.product_id[j]},${c.amount[j]},${new Date(c.date[j]).toISOString().slice(0, 10)}\n`;
      if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
    }
    await new Promise((r) => ws.end(r));
    process.stderr.write('done\n');
  }
  if (!existsSync(file(k, 'duckdb'))) spawn('make-duckdb', k);
  if (!existsSync(file(k, 'vectro'))) spawn('make-vectro', k);
  const res = { sizes: { csv: statSync(file(k, 'csv')).size, parquet: statSync(file(k, 'parquet')).size, duckdb: statSync(file(k, 'duckdb')).size, vectro: statSync(file(k, 'vectro')).size } };
  for (const t of [1, ALL]) {
    process.stderr.write(`  ${k} products, ${t} thread(s): duckdb... `);
    res[`duckdb-${t}`] = Object.assign({}, ...DUCK_CASES.map((c) => spawn('duckdb', k, t, c)));
    process.stderr.write('vectro...\n');
    res[`vectro-${t}`] = Object.assign({}, ...VECTRO_CASES.map((c) => spawn('vectro', k, t, c)));
  }
  if (pgVersion) { process.stderr.write(`  ${k} products: postgres...\n`); res.postgres = postgres(k); }
  results[k] = res;
}
if (pgVersion) psql(['-d', 'postgres', '-c', `DROP DATABASE ${PGDB}`]);

// ---- report -----------------------------------------------------------------------------
const same = (a, ref) => {
  const A = toMap(a), R = toMap(ref);
  if (A.size !== R.size) return false;
  for (const [p, s] of R) { const v = A.get(p); if (v === undefined || Math.abs(v - s) > 1e-9 * Math.abs(s) + 1e-6) return false; }
  return true;
};
const mb = (b) => `${(b / 2 ** 20).toFixed(1)} MB`;
const fmt = (ms) => (ms < 10 ? ms.toFixed(2) : ms.toFixed(1)).padStart(8) + ' ms';
const any = results[PRODUCTS[0]];
console.log(`${cpus()[0].model}, ${ALL} cores, ${(totalmem() / 2 ** 30).toFixed(0)} GB; Node ${process.version}; ${any['duckdb-1'].version}${pgVersion ? `; PostgreSQL ${pgVersion}` : ''}`);
console.log(`${N.toLocaleString()} orders; ${QUERY("'orders.parquet'")}; median of ${RUNS} after ${WARM} warm-up runs\n`);
let allMatch = true;
for (const k of PRODUCTS) {
  const r = results[k], ref = r[`duckdb-${ALL}`]['memory: sql()'].answer;
  console.log(`${k.toLocaleString()} products  (on disk: Parquet ${mb(r.sizes.parquet)}, .duckdb ${mb(r.sizes.duckdb)}, .vectro ${mb(r.sizes.vectro)}${r.postgres ? `, Postgres table ${mb(r.postgres.bytes)}` : ''}; CSV ${mb(r.sizes.csv)})`);
  console.log('  '.padEnd(36), '1 thread'.padStart(11), `${ALL} threads`.padStart(11), '  answers');
  const line = (label, a, b) => {
    const ok = [a, b].every((x) => same(x.answer, ref));
    allMatch &&= ok;
    console.log(`  ${label}`.padEnd(36), fmt(a.ms), fmt(b.ms), ok ? '  match' : '  DIFFER');
  };
  for (const q of ['memory: sql()', 'file: sql()', '.duckdb file']) line(`DuckDB ${q.replace('file: sql()', 'Parquet file')}`, r['duckdb-1'][q], r[`duckdb-${ALL}`][q]);
  for (const q of ['memory: sql()', 'memory: groupBy().agg()', 'file: sql()', 'file: groupBy().agg()']) line(`Vectro ${q.replace('file:', '.vectro file:')}`, r['vectro-1'][q], r[`vectro-${ALL}`][q]);
  if (r.postgres) {
    const p1 = r.postgres['1 process'], pd = r.postgres['default (parallel)'];
    const ok = same(p1.answer, ref) && same(pd.answer, ref);
    allMatch &&= ok;
    console.log('  Postgres (psql \\timing)'.padEnd(36), fmt(p1.ms), fmt(pd.ms), ok ? '  match' : '  DIFFER', `  (default: ${pd.processes} processes)`);
    console.log('  Postgres (EXPLAIN ANALYZE)'.padEnd(36), fmt(p1.serverMs), fmt(pd.serverMs));
  }
  console.log('');
}
console.log(allMatch ? 'every answer matches DuckDB\'s, product by product' : 'SOME ANSWERS DIFFER');
const json = join(DIR, 'results.json');
writeFileSync(json, JSON.stringify({ machine: { cpu: cpus()[0].model, cores: ALL, ram: totalmem(), node: process.version, pg: pgVersion }, N, PRODUCTS, results }, (key, v) => (key === 'answer' ? undefined : v), 1));
console.log(`(raw timings: ${json})`);
if (process.env.CLEAN) rmSync(DIR, { recursive: true, force: true });
