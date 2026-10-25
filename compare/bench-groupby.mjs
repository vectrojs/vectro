// Group-by on keys that aren't small: 1M big account IDs, two keys, money
// amounts as keys. Vectro vs DuckDB on 10M rows; each system alone in its own
// process, best of 5 after a warm-up; answers (groups, total) cross-checked.
//   node compare/bench-groupby.mjs        (needs this folder's npm install)
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 10_000_000);
const REGIONS = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const CSV = join(tmpdir(), `vectro-groupby-${N}.csv`);
const QUERIES = [
  ['account (1M big IDs)', ['account']],
  ['region + day (2,920 pairs)', ['region', 'day']],
  ['account + region (~5M pairs)', ['account', 'region']],
  ['price (~100K amounts)', ['price']],
];

function makeColumns() {
  let seed = 11; // mulberry32: every value of a range shows up (an LCG's low bits repeat)
  const rnd = () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  const account = new Float64Array(N), day = new Float64Array(N), price = new Float64Array(N), qty = new Float64Array(N);
  const region = new Array(N);
  for (let i = 0; i < N; i++) {
    account[i] = 1_000_000_000 + Math.floor(rnd() * 1_000_000) * 7919;
    region[i] = REGIONS[Math.floor(rnd() * 8)];
    day[i] = Math.floor(rnd() * 365);
    price[i] = Math.round(rnd() * 100000) / 100;
    qty[i] = 1 + Math.floor(rnd() * 9);
  }
  return { account, region, day, price, qty };
}

const child = process.argv[2];
if (child) {
  const out = {};
  let run, close;
  if (child === 'vectro') {
    const { Engine } = await import('../src/node.mjs');
    const engine = new Engine();
    const t = engine.table(makeColumns());
    run = (keys) => {
      const g = t.groupBy(keys).agg({ n: 'count()', s: 'sum(price * qty)' });
      const s = g.sum('s'), groups = g.length;
      g.dispose();
      return [groups, Math.round(s)];
    };
    close = () => engine.close();
  } else {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const inst = await DuckDBInstance.create(':memory:');
    const c = await inst.connect();
    await c.run(`CREATE TABLE t AS SELECT * FROM read_csv('${CSV}', header = true,
      columns = { 'account': 'BIGINT', 'region': 'VARCHAR', 'day': 'INTEGER', 'price': 'DOUBLE', 'qty': 'DOUBLE' })`);
    run = async (keys) => {
      const r = (await c.runAndReadAll(`SELECT count(*) AS groups, sum(s) AS s FROM (SELECT ${keys.join(', ')}, count(*) AS n, sum(price * qty) AS s FROM t GROUP BY ${keys.join(', ')})`)).getRowObjects()[0];
      return [Number(r.groups), Math.round(Number(r.s))];
    };
    close = () => c.closeSync?.();
  }
  for (const [label, keys] of QUERIES) {
    const answer = await run(keys);
    let best = Infinity;
    for (let i = 0; i < 5; i++) { const s = performance.now(); await run(keys); best = Math.min(best, performance.now() - s); }
    out[label] = { ms: best, answer };
  }
  await close();
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}

if (!existsSync(CSV)) {
  process.stderr.write(`writing ${N.toLocaleString()} rows to ${CSV}... `);
  const c = makeColumns();
  const ws = createWriteStream(CSV);
  ws.write('account,region,day,price,qty\n');
  for (let i = 0; i < N; i += 100_000) {
    let chunk = '';
    for (let j = i; j < Math.min(N, i + 100_000); j++) chunk += `${c.account[j]},${c.region[j]},${c.day[j]},${c.price[j]},${c.qty[j]}\n`;
    if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
  }
  await new Promise((r) => ws.end(r));
  process.stderr.write('done\n');
}
const results = {};
for (const sys of ['vectro', 'duckdb']) {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), sys], { encoding: 'utf8', maxBuffer: 1 << 26 });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(sys, r.stderr.slice(-2000)); process.exit(1); }
  results[sys] = JSON.parse(r.stdout.slice(at + 8));
}
console.log(`Group-by on ${N.toLocaleString()} rows: count(*) and sum(price * qty) per group (best of 5)\n`);
console.log('Keys'.padEnd(32), 'Vectro'.padStart(10), 'DuckDB'.padStart(10), '  answers');
for (const [label] of QUERIES) {
  const v = results.vectro[label], d = results.duckdb[label];
  const same = v.answer[0] === d.answer[0] && Math.abs(v.answer[1] - d.answer[1]) <= 1e-9 * Math.abs(d.answer[1]) + 1;
  console.log(label.padEnd(32), `${v.ms.toFixed(0)} ms`.padStart(10), `${d.ms.toFixed(0)} ms`.padStart(10), same ? `  match (${v.answer[0].toLocaleString()} groups)` : `  DIFFER ${v.answer} vs ${d.answer}`);
}
