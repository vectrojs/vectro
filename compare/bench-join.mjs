// Joins: Vectro vs DuckDB on 10M orders. Each system alone in its own process,
// best of 5 after a warm-up; the joined table is built in full (DuckDB: CREATE
// TEMP TABLE AS), then counted and summed so answers can be cross-checked.
//   node compare/bench-join.mjs        (needs this folder's npm install)
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 10_000_000);
const REGIONS = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const dir = tmpdir();
const QUERIES = [
  ['orders x 100K customers', 'customers', 'customer', 'inner'],
  ['orders x 1M accounts', 'accounts', 'account', 'inner'],
  ['left join, half match', 'half', 'customer', 'left'],
  ['two keys: region + day', 'calendar', ['region', 'day'], 'inner'],
];

function rng(seed) { return () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
function makeData() {
  const r = rng(5);
  const orders = { customer: new Float64Array(N), account: new Float64Array(N), region: new Array(N), day: new Float64Array(N), price: new Float64Array(N) };
  for (let i = 0; i < N; i++) {
    orders.customer[i] = Math.floor(r() * 100_000);
    orders.account[i] = 1e9 + Math.floor(r() * 1_000_000) * 7919;
    orders.region[i] = REGIONS[Math.floor(r() * 8)];
    orders.day[i] = Math.floor(r() * 365);
    orders.price[i] = Math.round(r() * 100000) / 100;
  }
  const customers = { customer: Float64Array.from({ length: 100_000 }, (_, i) => i), loyalty: Float64Array.from({ length: 100_000 }, () => Math.floor(r() * 5)) };
  const accounts = { account: Float64Array.from({ length: 1_000_000 }, (_, i) => 1e9 + i * 7919), credit: Float64Array.from({ length: 1_000_000 }, () => Math.floor(r() * 1000)) };
  const half = { customer: Float64Array.from({ length: 50_000 }, (_, i) => i * 2), loyalty: Float64Array.from({ length: 50_000 }, () => Math.floor(r() * 5)) };
  const calendar = { region: [], day: [], factor: [] };
  for (const g of REGIONS) for (let d = 0; d < 365; d++) { calendar.region.push(g); calendar.day.push(d); calendar.factor.push(1 + (d % 7) / 10); }
  return { orders, customers, accounts, half, calendar };
}
const valueCol = { customers: 'loyalty', accounts: 'credit', half: 'loyalty', calendar: 'factor' };

const child = process.argv[2];
if (child) {
  const data = makeData();
  const out = {};
  let run, close;
  if (child === 'vectro') {
    const { Engine } = await import('../src/node.mjs');
    const engine = new Engine();
    const tables = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, engine.table(v)]));
    run = (right, on, how) => {
      const j = tables.orders.join(tables[right], { on, how });
      const v = valueCol[right];
      const answer = [j.length, Math.round(j.filter(`${v} == ${v}`).sum(`price * ${v}`))];
      j.dispose();
      return answer;
    };
    close = () => engine.close();
  } else {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const inst = await DuckDBInstance.create(':memory:');
    const c = await inst.connect();
    for (const [name, cols] of Object.entries(data)) {
      const path = join(dir, `vectro-join-${name}-${N}.csv`);
      const types = Object.fromEntries(Object.keys(cols).map((k) => [k, k === 'region' ? 'VARCHAR' : k === 'account' ? 'BIGINT' : 'DOUBLE']));
      await c.run(`CREATE TABLE ${name} AS SELECT * FROM read_csv('${path}', header = true, columns = ${JSON.stringify(types).replaceAll('"', "'").replaceAll(':', ': ')})`);
    }
    run = async (right, on, how) => {
      const keys = [on].flat();
      await c.run(`CREATE OR REPLACE TEMP TABLE j AS SELECT * FROM orders ${how === 'left' ? 'LEFT' : ''} JOIN ${right} USING (${keys.join(', ')})`);
      const v = valueCol[right];
      const r = (await c.runAndReadAll(`SELECT count(*) AS n, sum(price * ${v}) AS s FROM j`)).getRowObjects()[0];
      return [Number(r.n), Math.round(Number(r.s ?? 0))];
    };
    close = () => c.closeSync?.();
  }
  for (const [label, right, on, how] of QUERIES) {
    const answer = await run(right, on, how);
    let best = Infinity;
    for (let i = 0; i < 5; i++) { const s = performance.now(); await run(right, on, how); best = Math.min(best, performance.now() - s); }
    out[label] = { ms: best, answer };
  }
  await close();
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}

const data = makeData();
for (const [name, cols] of Object.entries(data)) {
  const path = join(dir, `vectro-join-${name}-${N}.csv`);
  if (existsSync(path)) continue;
  const names = Object.keys(cols), n = cols[names[0]].length;
  const ws = createWriteStream(path);
  ws.write(names.join(',') + '\n');
  for (let i = 0; i < n; i += 100_000) {
    let chunk = '';
    for (let j = i; j < Math.min(n, i + 100_000); j++) chunk += names.map((k) => cols[k][j]).join(',') + '\n';
    if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
  }
  await new Promise((r) => ws.end(r));
}
const results = {};
for (const sys of ['vectro', 'duckdb']) {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), sys], { encoding: 'utf8', maxBuffer: 1 << 26 });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(sys, r.stderr.slice(-2000)); process.exit(1); }
  results[sys] = JSON.parse(r.stdout.slice(at + 8));
}
console.log(`Joins on ${N.toLocaleString()} orders, joined table built in full, then counted and summed (best of 5)\n`);
console.log('Join'.padEnd(28), 'Vectro'.padStart(10), 'DuckDB'.padStart(10), '  answers');
for (const [label] of QUERIES) {
  const v = results.vectro[label], d = results.duckdb[label];
  const same = v.answer[0] === d.answer[0] && Math.abs(v.answer[1] - d.answer[1]) <= 1e-9 * Math.abs(d.answer[1]) + 1;
  console.log(label.padEnd(28), `${v.ms.toFixed(0)} ms`.padStart(10), `${d.ms.toFixed(0)} ms`.padStart(10), same ? `  match (${v.answer[0].toLocaleString()} rows)` : `  DIFFER ${v.answer} vs ${d.answer}`);
}
