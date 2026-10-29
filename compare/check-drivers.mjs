// Vectro's PostgreSQL server through the drivers BI tools use, checked:
//   pgJDBC 42.7.4 (Tableau, DBeaver, Metabase): DatabaseMetaData (schemas,
//     tables, columns, keys, types), a LIMIT 0 query, a prepared statement
//     run six times, a cursor fetched one row at a time
//   Npgsql 4.0.17 (the one inside Power BI's PostgreSQL connector): type
//     loading, Power BI's own catalog queries, binary rows, a folded group-by,
//     a parameter, GetSchema
//   node compare/check-drivers.mjs      (needs java and dotnet; downloads both drivers)
import { mkdirSync, existsSync, writeFileSync, readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { join, dirname } from 'node:path';
import { Engine } from '../src/node.mjs';
import { servePg } from '../src/serve/pg.mjs';

const here = dirname(fileURLToPath(import.meta.url)), drivers = join(here, 'drivers'), cache = join(drivers, '.cache');
mkdirSync(cache, { recursive: true });
const JAR = join(cache, 'postgresql-42.7.4.jar'), JAR_SHA1 = '264310fd7b2cd76738787dc0b9f7ea2e3b11adc1';
if (!existsSync(JAR)) {
  const r = await fetch('https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar');
  writeFileSync(JAR, Buffer.from(await r.arrayBuffer()));
}
if (createHash('sha1').update(readFileSync(JAR)).digest('hex') !== JAR_SHA1) throw new Error('the pgJDBC jar does not match its checksum');

const run = (cmd, args, opts = {}) => new Promise((done) => {
  const p = spawn(cmd, args, { ...opts, env: { ...process.env, DOTNET_CLI_TELEMETRY_OPTOUT: '1', DOTNET_NOLOGO: '1', ...opts.env } });
  let out = '';
  p.stdout.on('data', (d) => { out += d; });
  p.stderr.on('data', (d) => { out += d; });
  p.on('error', (err) => done({ code: -1, out: err.message }));
  p.on('close', (code) => done({ code, out }));
});

const engine = new Engine();
const orders = engine.table({ cust: [1, 2], price: [10.5, NaN], qty: [2, 1], region: ['EU', 'US'], ts: [new Date('2024-01-02T03:04:05Z'), NaN], d: [new Date('2024-01-02'), NaN], ok: [1, 0] });
const customers = engine.table({ cust: [1], name: ['Ann'], tier: [1] });
const server = await servePg({ tables: { orders, customers }, port: 0, user: 'postgres', password: 'secret', engine });
let bad = 0;
const expect = (label, out, patterns) => {
  const missing = patterns.filter((p) => !p.test(out));
  if (missing.length) { bad++; console.log(`DIFFER ${label}: missing ${missing.join(', ')}\n${out.slice(0, 3000)}`); } else console.log(`same   ${label}`);
};

const jdbc = await run('java', ['-cp', JAR, join(drivers, 'Capture.java'), `jdbc:postgresql://127.0.0.1:${server.port}/vectro`, 'postgres', 'secret']);
expect('pgJDBC (Tableau, DBeaver, Metabase)', jdbc.out, [
  /product PostgreSQL 16/, /public \| null/, /null \| public \| orders \| TABLE/, /orders \| cust \| -5 \| int8/, /orders \| price \| 8 \| float8/,
  /orders \| region \| 12 \| text/, /orders \| ts \| 93 \| timestamp/, /orders \| d \| 91 \| date/, /\[cust:int8, price:float8, qty:int8, region:text, ts:timestamp, d:date, ok:int8\]/,
  /prepared x6\n {2}\[region:text, sum:price:ok:float8, cnt:int8\]\n {2}EU \| 10\.5 \| 1\n {2}US \| null \| 1/, /fetch size 1[\s\S]*1 \| 10\.5 \| 2 \| EU \| 2024-01-02 03:04:05 \| 2024-01-02 \| 1/,
]);
const npgsql = await run('dotnet', ['run', '--project', join(drivers, 'powerbi'), '--', `Host=127.0.0.1;Port=${server.port};Username=postgres;Password=secret;Database=vectro`]);
expect('Npgsql 4.0.17 (Power BI)', npgsql.out, [
  /server 16\.0/, /UTF8/, /public \| orders \| BASE TABLE/, /cust:bigint:Int64, price:double precision:Double, qty:bigint:Int64, region:text:String, ts:timestamp:DateTime, d:date:DateTime/,
  /1 \| 10\.5 \| 2 \| EU \| 2024-01-02T03:04:05\.0000000 \| 2024-01-02T00:00:00\.0000000 \| 1/, /EU \| 10\.5\n {2}\(1 rows\)/, /param query: 1/, /GetSchema Tables: 2 rows/, /GetSchema Columns: 7 rows/,
]);
await server.close();
await engine.close();
console.log(bad ? `${bad} of 2 drivers differ` : 'both drivers get the answers PostgreSQL would give');
process.exit(bad ? 1 : 0);
