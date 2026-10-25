// vectro vs Arquero vs DuckDB on 10M orders. Each system runs alone in its
// own process (no shared threads or garbage collector); best of 5 runs after
// a warm-up. Answers are compared across systems.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { N, makeColumns, REGIONS } from './data.mjs';
import { QUERIES } from './systems.mjs';

const SYSTEMS = ['vectro', 'arquero', 'duckdb'];
const CSV = process.env.CSV ?? join(tmpdir(), `vectro-orders-${N}.csv`);
const fmt = (ms) => (ms < 1 ? `${(ms * 1000).toFixed(0)} µs` : ms < 1000 ? `${ms.toFixed(1)} ms` : `${(ms / 1000).toFixed(2)} s`);

const child = process.argv[2];
if (child) {
  // ---- one system, in its own process ----
  const cols = makeColumns();
  const sys = await (await import('./systems.mjs'))[child](cols, CSV);
  const out = { load: sys.load, queries: {} };
  for (const [id] of QUERIES) {
    const answer = await sys[id]();
    let best = Infinity;
    for (let i = 0; i < 5; i++) { const s = performance.now(); await sys[id](); best = Math.min(best, performance.now() - s); }
    out.queries[id] = { ms: best, answer };
  }
  await sys.close();
  process.stdout.write('@@RESULT' + JSON.stringify(out));
  process.exit(0);
}

// ---- runner ----
if (!existsSync(CSV)) {
  process.stderr.write(`writing ${N.toLocaleString()} rows to ${CSV} for DuckDB... `);
  const c = makeColumns();
  const ws = createWriteStream(CSV);
  ws.write('price,qty,discount,region,customer\n');
  for (let i = 0; i < N; i += 100_000) {
    let chunk = '';
    for (let j = i; j < Math.min(N, i + 100_000); j++) chunk += `${c.price[j]},${c.qty[j]},${c.discount[j]},${REGIONS[c.regionCode[j]]},${c.customer[j]}\n`;
    if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
  }
  await new Promise((r) => ws.end(r));
  process.stderr.write('done\n');
}

const results = {};
for (const name of SYSTEMS) {
  process.stderr.write(`running ${name}...\n`);
  const r = spawnSync(process.execPath, ['--max-old-space-size=8192', fileURLToPath(import.meta.url), name], { encoding: 'utf8', maxBuffer: 1 << 26 });
  const m = r.stdout.match(/@@RESULT(.*)$/s);
  if (!m) { console.error(`${name} failed:\n${r.stderr || r.stdout}`); process.exit(1); }
  results[name] = JSON.parse(m[1]);
}

// Normalize answers so they can be compared. Floating-point sums depend on
// the order values are added in, so compare to 10 significant digits.
const sig = (v) => (Number.isInteger(v) ? String(v) : v.toPrecision(10));
const norm = (id, a) => {
  if (id === 'q3') return [...a].sort((x, y) => (x.region < y.region ? -1 : 1)).map((g) => `${g.region}:${sig(g.revenue)}/${g.orders}/${sig(g.avg)}`).join(' ');
  if (Array.isArray(a)) return a.map(sig).join(',');
  return sig(a);
};

console.log(`\n${N.toLocaleString()} orders, M1 Pro (8 cores). Best of 5. vectro and DuckDB use 8 threads; Arquero is single-threaded JS.\n`);
console.table(QUERIES.map(([id, label]) => {
  const t = Object.fromEntries(SYSTEMS.map((s) => [s, results[s].queries[id].ms]));
  const answers = SYSTEMS.map((s) => norm(id, results[s].queries[id].answer));
  const fastest = SYSTEMS.reduce((a, b) => (t[a] <= t[b] ? a : b));
  return {
    query: label,
    vectro: fmt(t.vectro),
    Arquero: fmt(t.arquero),
    DuckDB: fmt(t.duckdb),
    'vectro vs DuckDB': t.vectro <= t.duckdb ? `${(t.duckdb / t.vectro).toFixed(1)}x faster` : `${(t.vectro / t.duckdb).toFixed(1)}x slower`,
    fastest,
    'same answer': answers.every((a) => a === answers[0]) ? 'yes' : `NO: ${answers.join(' | ')}`,
  };
}));
console.table(SYSTEMS.map((s) => ({ system: s, 'load 10M rows': fmt(results[s].load), from: s === 'duckdb' ? 'CSV file (read_csv)' : 'JS arrays in memory' })));
