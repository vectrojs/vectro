#!/usr/bin/env node
// vectro serve --root ~/data [--port 8787] [--pg 5433] ...
//   Serves the CSV, TSV, JSON-lines, Arrow and .vectro files in the folder as
//   SQL tables over HTTP and / or the PostgreSQL protocol (src/serve/).
import { serve, servePg, openTables } from '../src/serve/index.mjs';

const [command, ...rest] = process.argv.slice(2);
const HELP = `usage: vectro serve --root <folder> [options]

  Every CSV, TSV, JSON-lines, Arrow / Feather and .vectro file in the folder
  becomes a table named after its file.

  HTTP (Excel, Power BI Web, Google Sheets, pandas):
    --port 8787            the HTTP port (--no-http: none)
    --token <secret>       requests need "Authorization: Bearer <secret>"
    --cors                 allow pages from other sites
  PostgreSQL (Power BI, Tableau, Metabase, Grafana, psql):
    --pg 5433              also speak the PostgreSQL protocol on this port
    --user <name> --password <secret>   (or VECTRO_PG_PASSWORD; needed with --host)
    --auth scram|md5|password           how the password is checked (scram)
    --tls-cert <file> --tls-key <file>  encrypted connections
    --database <name>      the database name reported (vectro)
    --log-queries          print every statement clients send
  Both:
    --host 127.0.0.1       the address to listen on (0.0.0.0: every network)
    --quiet

  curl 'http://127.0.0.1:8787/sql?q=SELECT region, COUNT(*) FROM orders GROUP BY 1&format=csv'
  psql -h 127.0.0.1 -p 5433 -U vectro vectro`;

function option(name, fallback) {
  const i = rest.indexOf(`--${name}`);
  if (i < 0) return fallback;
  const v = rest[i + 1];
  if (v === undefined || v.startsWith('--')) { console.error(`vectro: --${name} needs a value\n\n${HELP}`); process.exit(2); }
  return v;
}

if (command !== 'serve') {
  console.log(HELP);
  process.exit(command === undefined || command === 'help' || command === '--help' ? 0 : 2);
}
const quiet = rest.includes('--quiet');
const log = quiet ? () => {} : (msg) => process.stderr.write(`[vectro] ${msg}\n`);
try {
  const host = option('host', '127.0.0.1');
  const catalog = await openTables({ root: option('root', process.cwd()), log });
  const running = [];
  if (!rest.includes('--no-http')) {
    const s = await serve({ catalog, port: Number(option('port', process.env.PORT ?? 8787)), host, token: option('token', process.env.VECTRO_TOKEN ?? null), cors: rest.includes('--cors'), log });
    running.push(s);
    log(`HTTP at ${s.url}: try ${s.url}/sql?q=${encodeURIComponent(`SELECT * FROM ${[...catalog.entries.keys()][0] ?? 'mytable'} LIMIT 5`)}`);
  }
  const pgPort = option('pg', null);
  if (pgPort !== null) {
    const certFile = option('tls-cert', null), keyFile = option('tls-key', null);
    const p = await servePg({
      catalog, port: Number(pgPort), host, user: option('user', null), password: option('password', process.env.VECTRO_PG_PASSWORD ?? null),
      auth: option('auth', 'scram'), tls: certFile && keyFile ? { certFile, keyFile } : null, database: option('database', 'vectro'),
      log, logQueries: rest.includes('--log-queries'),
    });
    running.push(p);
    log(`PostgreSQL at ${p.host}:${p.port} (database ${option('database', 'vectro')}${certFile ? ', TLS' : ''}): psql -h ${p.host} -p ${p.port} -U ${option('user', 'vectro')} ${option('database', 'vectro')}`);
  }
  log(`${catalog.entries.size} table(s): ${[...catalog.entries].map(([n, e]) => `${n} (${e.table.length.toLocaleString()} rows)`).join(', ') || 'none'}`);
  const stop = () => Promise.all(running.map((s) => s.close())).then(() => catalog.close()).then(() => process.exit(0));
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
} catch (err) {
  console.error(`vectro: ${err.message.replace(/^vectro:\s*/, '')}`);
  process.exit(1);
}
