// Installs the packed npm package into an empty project, as a user would, and
// runs the README's quick start against it: the engine, SQL, a dataset, the
// HTTP and PostgreSQL servers, and the vectro command.
//   npm pack && node scripts/smoke-install.mjs [vectrojs-<version>.tgz]   (default: the one npm pack made here)
import { mkdtempSync, writeFileSync, rmSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { execSync } from 'node:child_process';

const tarball = resolve(process.argv[2] ?? readdirSync('.').filter((f) => /^vectrojs-.*\.tgz$/.test(f)).sort().at(-1) ?? 'no tarball: run npm pack first');
const dir = mkdtempSync(join(tmpdir(), 'vectro-install-'));
const run = (cmd) => execSync(cmd, { cwd: dir, stdio: ['ignore', 'pipe', 'inherit'], encoding: 'utf8' });
try {
  run('npm init -y');
  run(`npm install --no-audit --no-fund "${tarball}"`);
  writeFileSync(join(dir, 'orders.csv'), 'cust,price,region\n1,10.5,EU\n2,20,US\n3,30,EU\n');
  writeFileSync(join(dir, 'try.mjs'), `
import { Engine, sql } from 'vectrojs';
import { serve, servePg } from 'vectrojs/serve';
import { Engine as PortableEngine } from 'vectrojs/portable';
import { connect } from 'node:net';

const engine = new Engine();
const orders = engine.readCSV('orders.csv');
const totals = orders.filter("region == 'EU'").agg({ n: 'count()', revenue: 'sum(price)' });
if (totals.n !== 2 || totals.revenue !== 40.5) throw new Error('agg: ' + JSON.stringify(totals));
const rows = sql('SELECT region, SUM(price) AS revenue FROM orders GROUP BY 1 ORDER BY 1', { orders }).toRows();
if (JSON.stringify(rows) !== JSON.stringify([{ region: 'EU', revenue: 40.5 }, { region: 'US', revenue: 20 }])) throw new Error('sql: ' + JSON.stringify(rows));
engine.writeDataset('orders.vectro', orders);
const ds = engine.openDataset('orders.vectro');
if (sql('SELECT COUNT(*) AS n FROM orders', { orders: ds }).toRows()[0].n !== 3) throw new Error('dataset');
await ds.close();
const web = await serve({ tables: { orders }, port: 0 });
const j = await (await fetch(web.url + '/sql?q=' + encodeURIComponent('SELECT COUNT(*) AS n FROM orders'))).json();
if (j.rows[0][0] !== 3) throw new Error('http: ' + JSON.stringify(j));
await web.close();
const pg = await servePg({ tables: { orders }, port: 0 });
await new Promise((ok, fail) => { const s = connect(pg.port, '127.0.0.1', () => { s.end(); ok(); }); s.on('error', fail); });
await pg.close();
const p = new PortableEngine({ jit: false });
if (p.table({ x: [1, 2, 3] }).sum('x') !== 6) throw new Error('portable');
await p.close();
await engine.close();
console.log('quick start works');
`);
  process.stdout.write(run('node try.mjs'));
  const help = run('npx --no-install vectro --help');
  if (!/vectro serve --root/.test(help)) throw new Error(`vectro --help: ${help}`);
  console.log('the vectro command works');
} finally {
  rmSync(dir, { recursive: true, force: true });
}
