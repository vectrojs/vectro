// CSV loading: vectro readCSV vs DuckDB read_csv vs Arquero fromCSV vs the
// naive split()/Number() approach. Same files for all; each system runs alone
// in its own process; best of 3. Loaded data is checked to agree.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync, createWriteStream, readFileSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { N, makeColumns, REGIONS } from './data.mjs';

const DIR = process.env.CSV_DIR ?? tmpdir();
const FILES = {
  plain: join(DIR, `vectro-orders-${N}.csv`),        // numbers + bare text
  quoted: join(DIR, `vectro-orders-quoted-${N}.csv`), // + quoted text with commas
};
const SYSTEMS = ['vectro', 'duckdb', 'arquero', 'naive'];
const fmt = (ms) => (ms < 1000 ? `${ms.toFixed(0)} ms` : `${(ms / 1000).toFixed(2)} s`);

// Each loader returns a fingerprint of what it loaded: rows, sum(price), sum(qty), distinct regions.
const LOADERS = {
  async vectro(path) {
    const { Engine } = await import('../src/node.mjs');
    const e = new Engine();
    await new Promise((r) => setTimeout(r, 300));
    return {
      load: () => { const t = e.readCSV(path); const fp = [t.length, t.sum('price'), t.sum('qty'), t.groupBy('region').agg({ n: 'count()' }).length]; t.dispose(); return fp; },
      close: () => e.close(),
    };
  },
  async duckdb(path) {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const inst = await DuckDBInstance.create(':memory:');
    const c = await inst.connect();
    return {
      load: async () => {
        await c.run(`CREATE OR REPLACE TABLE orders AS SELECT * FROM read_csv('${path}', header = true)`);
        const r = (await c.runAndReadAll('SELECT count(*) AS n, sum(price) AS p, sum(qty) AS q, count(DISTINCT region) AS g FROM orders')).getRows()[0];
        return r.map(Number);
      },
      close: async () => {},
    };
  },
  async arquero(path) {
    const aq = await import('arquero');
    return {
      load: () => {
        const t = aq.fromCSV(readFileSync(path, 'utf8'));
        const r = t.rollup({ n: () => aq.op.count(), p: (d) => aq.op.sum(d.price), q: (d) => aq.op.sum(d.qty), g: (d) => aq.op.distinct(d.region) }).objects()[0];
        return [r.n, r.p, r.q, r.g];
      },
      close: async () => {},
    };
  },
  async naive(path) {
    return {
      load: () => {
        // What a lot of code does: read the text, split lines, split fields.
        // (Doesn't handle quotes, so it only runs on the plain file.)
        const lines = readFileSync(path, 'utf8').split('\n');
        const rows = [];
        for (let i = 1; i < lines.length; i++) {
          if (!lines[i]) continue;
          const f = lines[i].split(',');
          rows.push({ price: Number(f[0]), qty: Number(f[1]), discount: Number(f[2]), region: f[3], customer: Number(f[4]) });
        }
        let p = 0, q = 0; const g = new Set();
        for (const r of rows) { p += r.price; q += r.qty; g.add(r.region); }
        return [rows.length, p, q, g.size];
      },
      close: async () => {},
    };
  },
};

const [, , child, which] = process.argv;
if (child) {
  const sys = await LOADERS[child](FILES[which]);
  let fp, best = Infinity;
  for (let i = 0; i < 3; i++) {
    const s = performance.now();
    fp = await sys.load();
    best = Math.min(best, performance.now() - s);
  }
  await sys.close();
  process.stdout.write('@@RESULT' + JSON.stringify({ ms: best, fp }));
  process.exit(0);
}

// ---- write the files once ----
async function writeCSV(path, quoted) {
  if (existsSync(path)) return;
  process.stderr.write(`writing ${path}... `);
  const c = makeColumns();
  const ws = createWriteStream(path);
  ws.write(quoted ? 'price,qty,discount,region,customer,note\n' : 'price,qty,discount,region,customer\n');
  for (let i = 0; i < N; i += 100_000) {
    let chunk = '';
    for (let j = i; j < Math.min(N, i + 100_000); j++) {
      const region = REGIONS[c.regionCode[j]];
      chunk += quoted
        ? `${c.price[j]},${c.qty[j]},${c.discount[j]},"${region}",${c.customer[j]},"order ${j}, ${c.qty[j] > 5 ? 'bulk' : 'single'}"\n`
        : `${c.price[j]},${c.qty[j]},${c.discount[j]},${region},${c.customer[j]}\n`;
    }
    if (!ws.write(chunk)) await new Promise((r) => ws.once('drain', r));
  }
  await new Promise((r) => ws.end(r));
  process.stderr.write('done\n');
}
await writeCSV(FILES.plain, false);
await writeCSV(FILES.quoted, true);

const rows = [];
for (const [which, path] of Object.entries(FILES)) {
  const mb = statSync(path).size / 1e6;
  const res = {};
  for (const name of SYSTEMS) {
    if (name === 'naive' && which === 'quoted') continue;
    process.stderr.write(`${which}: ${name}...\n`);
    const r = spawnSync(process.execPath, ['--max-old-space-size=12000', fileURLToPath(import.meta.url), name, which], { encoding: 'utf8', maxBuffer: 1 << 24 });
    const m = r.stdout.match(/@@RESULT(.*)$/s);
    res[name] = m ? JSON.parse(m[1]) : { error: (r.stderr || r.stdout).split('\n').slice(0, 3).join(' ') };
  }
  const fps = Object.values(res).filter((r) => r.fp).map((r) => r.fp.map((v) => (Number.isInteger(v) ? v : v.toPrecision(10))).join(','));
  for (const [name, r] of Object.entries(res)) {
    rows.push({
      file: `${which} (${mb.toFixed(0)} MB, ${N.toLocaleString()} rows)`,
      system: name,
      time: r.ms ? fmt(r.ms) : `failed: ${r.error}`,
      'MB/s': r.ms ? Math.round(mb / (r.ms / 1000)) : '-',
      'vectro is': r.ms && res.vectro?.ms && name !== 'vectro'
        ? (r.ms >= res.vectro.ms ? `${(r.ms / res.vectro.ms).toFixed(1)}x faster` : `${(res.vectro.ms / r.ms).toFixed(1)}x slower`)
        : '-',
      'same data': fps.every((f) => f === fps[0]) ? 'yes' : 'NO',
    });
  }
}
console.log(`\nCSV loading, M1 Pro (8 cores). Best of 3. vectro and DuckDB use 8 threads; Arquero and naive are single-threaded.\n`);
console.table(rows);
