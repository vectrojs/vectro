// vectro vs SQLite (built into Node as node:sqlite) on the same 10M orders,
// plus the "SQLite stores, vectro analyzes" pattern: how long it takes to
// pull the rows out of SQLite into a vectro table. Each system runs in its
// own process; best of 3.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { N, makeColumns, REGIONS } from './data.mjs';

const fmt = (ms) => (ms < 1000 ? `${ms.toFixed(1)} ms` : `${(ms / 1000).toFixed(2)} s`);
const best = async (f, reps = 3) => { const out = await f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); await f(); m = Math.min(m, performance.now() - s); } return { ms: m, out }; };
const sig = (v) => (Array.isArray(v) ? v.map(sig).join(',') : Number.isInteger(v) ? String(v) : Number(v).toPrecision(8));

const QUERIES = [
  ['filtered revenue', 'SELECT sum(price*qty*(1-discount)) AS v FROM orders WHERE price > 100 AND qty < 5',
    (t) => t.filter('price > 100 && qty < 5').sum('price * qty * (1 - discount)')],
  ['EU dashboard (4 aggregates)', "SELECT count(*) AS n, avg(price) AS a, max(qty) AS m, sum(price*qty*(1-discount)) AS r FROM orders WHERE region = 'EU'",
    (t) => Object.values(t.filter("region == 'EU'").agg({ n: 'count()', a: 'mean(price)', m: 'max(qty)', r: 'sum(price * qty * (1 - discount))' }))],
  ['group by region (8 groups)', 'SELECT region, sum(price*qty*(1-discount)) AS r, count(*) AS n FROM orders GROUP BY region ORDER BY region',
    (t) => { const g = t.groupBy('region').agg({ r: 'sum(price * qty * (1 - discount))', n: 'count()' }); const rows = g.toRows().sort((a, b) => (a.region < b.region ? -1 : 1)); g.dispose(); return rows.map((x) => x.n); }],
  ['group by customer (100K groups)', 'SELECT count(*) AS v FROM (SELECT customer, sum(price*qty) FROM orders GROUP BY customer)',
    (t) => { const g = t.groupBy('customer').agg({ s: 'sum(price * qty)' }); const n = g.length; g.dispose(); return n; }],
  ['filter into a new table', 'CREATE TEMP TABLE IF NOT EXISTS f AS SELECT 1 WHERE 0; DROP TABLE f; CREATE TEMP TABLE f AS SELECT * FROM orders WHERE price > 900',
    (t) => { const c = t.filter('price > 900').collect(); const n = c.length; c.dispose(); return n; }],
  ['new column', 'CREATE TEMP TABLE IF NOT EXISTS d AS SELECT 1 WHERE 0; DROP TABLE d; CREATE TEMP TABLE d AS SELECT price*qty*(1-discount) AS rev FROM orders',
    (t) => { const d = t.derive('rev', 'price * qty * (1 - discount)'); const n = d.length; d.dispose(); return n; }],
];

const child = process.argv[2];
if (child) {
  const c = makeColumns();
  const res = { queries: {} };
  if (child === 'sqlite') {
    const { DatabaseSync } = await import('node:sqlite');
    const db = new DatabaseSync(':memory:');
    db.exec('CREATE TABLE orders (price REAL, qty REAL, discount REAL, region TEXT, customer INTEGER)');
    let s = performance.now();
    const ins = db.prepare('INSERT INTO orders VALUES (?, ?, ?, ?, ?)');
    db.exec('BEGIN');
    for (let i = 0; i < N; i++) ins.run(c.price[i], c.qty[i], c.discount[i], REGIONS[c.regionCode[i]], c.customer[i]);
    db.exec('COMMIT');
    db.exec('CREATE INDEX idx_region ON orders(region)');
    res.load = performance.now() - s;
    for (const [name, sql] of QUERIES) {
      const run = () => {
        const parts = sql.split(';').map((x) => x.trim()).filter(Boolean);
        const last = parts.pop();
        for (const p of parts) db.exec(p);
        if (/^CREATE/i.test(last)) { db.exec(last); return db.prepare(`SELECT count(*) AS v FROM ${last.match(/TABLE (\w+)/)[1]}`).get().v; }
        const rows = db.prepare(last).all();
        return rows.length > 1 ? rows.map((r) => r.n) : Object.values(rows[0]);
      };
      res.queries[name] = await best(run);
    }
    // pull everything back out of SQLite into a vectro table
    const { Engine } = await import('../src/node.mjs');
    const e = new Engine();
    s = performance.now();
    const cols = { price: new Float64Array(N), qty: new Float64Array(N), discount: new Float64Array(N), region: new Array(N), customer: new Float64Array(N) };
    const stmt = db.prepare('SELECT price, qty, discount, region, customer FROM orders');
    stmt.setReturnArrays?.(true);
    let i = 0;
    for (const r of stmt.iterate()) {
      if (Array.isArray(r)) { cols.price[i] = r[0]; cols.qty[i] = r[1]; cols.discount[i] = r[2]; cols.region[i] = r[3]; cols.customer[i] = r[4]; }
      else { cols.price[i] = r.price; cols.qty[i] = r.qty; cols.discount[i] = r.discount; cols.region[i] = r.region; cols.customer[i] = r.customer; }
      i++;
    }
    const t = e.table(cols);
    res.sqliteToVectro = performance.now() - s;
    t.dispose();
    await e.close();
  } else {
    const { Engine } = await import('../src/node.mjs');
    const e = new Engine();
    await new Promise((r) => setTimeout(r, 300));
    const s = performance.now();
    const t = e.table({ price: c.price, qty: c.qty, discount: c.discount, region: c.region, customer: c.customer });
    res.load = performance.now() - s;
    for (const [name, , q] of QUERIES) res.queries[name] = await best(() => q(t));
    await e.close();
  }
  process.stdout.write('@@' + JSON.stringify(res));
  process.exit(0);
}

const out = {};
for (const sys of ['vectro', 'sqlite']) {
  process.stderr.write(`running ${sys}...\n`);
  const r = spawnSync(process.execPath, ['--max-old-space-size=12000', '--no-warnings', fileURLToPath(import.meta.url), sys], { encoding: 'utf8', maxBuffer: 1 << 24 });
  const m = r.stdout.match(/@@(.*)$/s);
  if (!m) { console.error(sys, 'failed', r.stderr.slice(-2000)); process.exit(1); }
  out[sys] = JSON.parse(m[1]);
}
console.log(`\n${N.toLocaleString()} orders, M1 Pro. SQLite ${'3.51.2'} in memory (node:sqlite, index on region); vectro on 8 threads. Best of 3.\n`);
console.table(QUERIES.map(([name]) => {
  const a = out.vectro.queries[name], b = out.sqlite.queries[name];
  return { query: name, vectro: fmt(a.ms), SQLite: fmt(b.ms), 'vectro is': `${(b.ms / a.ms).toFixed(0)}x faster`, 'same answer': sig(a.out) === sig(b.out) ? 'yes' : `${sig(a.out)} vs ${sig(b.out)}` };
}));
console.log(`load 10M rows: vectro table from JS arrays ${fmt(out.vectro.load)} | SQLite inserts + index ${fmt(out.sqlite.load)}`);
console.log(`SQLite -> vectro (read every row out of SQLite, build the table): ${fmt(out.sqlite.sqliteToVectro)}`);
