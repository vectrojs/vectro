// Distinct counts: Vectro vs DuckDB on 10M rows (the bench-groupby data);
// each system alone in its own process, best of 5; answers cross-checked.
//   node compare/bench-distinct.mjs        (needs this folder's npm install)
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const N = Number(process.env.ROWS ?? 10_000_000);
const CSV = join(tmpdir(), `vectro-groupby-${N}.csv`); // written by bench-groupby.mjs
const QUERIES = [
  ['distinct accounts (1M)', null, 'account'],
  ['distinct days (365)', null, 'day'],
  ['distinct accounts per region', 'region', 'account'],
  ['distinct regions per day', 'day', 'region'],
];

const child = process.argv[2];
if (child) {
  const out = {};
  let run, close;
  if (child === 'vectro') {
    const { Engine } = await import('../src/node.mjs');
    const engine = new Engine();
    const t = engine.parseCSV((await import('node:fs')).readFileSync(CSV), { types: { region: 'category' } });
    run = (by, col) => {
      if (!by) return [1, t.agg({ c: `countDistinct(${col})` }).c];
      const g = t.groupBy(by).agg({ c: `countDistinct(${col})` });
      const r = [g.length, g.sum('c')];
      g.dispose();
      return r;
    };
    close = () => engine.close();
  } else {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const c = await (await DuckDBInstance.create(':memory:')).connect();
    await c.run(`CREATE TABLE t AS SELECT * FROM read_csv('${CSV}', header = true, columns = { 'account': 'BIGINT', 'region': 'VARCHAR', 'day': 'INTEGER', 'price': 'DOUBLE', 'qty': 'DOUBLE' })`);
    run = async (by, col) => {
      const sql = by ? `SELECT count(*) AS g, sum(c) AS s FROM (SELECT ${by}, count(DISTINCT ${col}) AS c FROM t GROUP BY ${by})` : `SELECT 1 AS g, count(DISTINCT ${col}) AS s FROM t`;
      const r = (await c.runAndReadAll(sql)).getRowObjects()[0];
      return [Number(r.g), Number(r.s)];
    };
    close = () => c.closeSync?.();
  }
  for (const [label, by, col] of QUERIES) {
    const answer = await run(by, col);
    let best = Infinity;
    for (let i = 0; i < 5; i++) { const s = performance.now(); await run(by, col); best = Math.min(best, performance.now() - s); }
    out[label] = { ms: best, answer };
  }
  await close();
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}
if (!existsSync(CSV)) { console.error(`run compare/bench-groupby.mjs first (it writes ${CSV})`); process.exit(1); }
const results = {};
for (const sys of ['vectro', 'duckdb']) {
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), sys], { encoding: 'utf8', maxBuffer: 1 << 26 });
  const at = r.stdout.indexOf('@@RESULT');
  if (at < 0) { console.error(sys, r.stderr.slice(-2000)); process.exit(1); }
  results[sys] = JSON.parse(r.stdout.slice(at + 8));
}
console.log(`Distinct counts on ${N.toLocaleString()} rows (best of 5; Vectro exact, DuckDB count(DISTINCT))\n`);
console.log('Query'.padEnd(32), 'Vectro'.padStart(10), 'DuckDB'.padStart(10), '  answers');
for (const [label] of QUERIES) {
  const v = results.vectro[label], d = results.duckdb[label];
  const same = v.answer[0] === d.answer[0] && v.answer[1] === d.answer[1];
  console.log(label.padEnd(32), `${v.ms.toFixed(0)} ms`.padStart(10), `${d.ms.toFixed(0)} ms`.padStart(10), same ? `  match (${v.answer[1].toLocaleString()})` : `  DIFFER ${v.answer} vs ${d.answer}`);
}
