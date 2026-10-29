#!/usr/bin/env node
// vectro serve --root ~/data [--port 8787] [--host 127.0.0.1] [--token secret] [--cors]
//   Serves the CSV, TSV, JSON-lines, Arrow and .vectro files in the folder as
//   SQL tables over HTTP (see src/serve/http.mjs).
import { serve } from '../src/serve/http.mjs';

const [command, ...rest] = process.argv.slice(2);
const HELP = `usage: vectro serve --root <folder> [--port 8787] [--host 127.0.0.1] [--token <secret>] [--cors] [--quiet]

  Every CSV, TSV, JSON-lines, Arrow / Feather and .vectro file in the folder
  becomes a table named after its file. Then:
    curl 'http://127.0.0.1:8787/sql?q=SELECT region, COUNT(*) FROM orders GROUP BY 1&format=csv'
  Excel / Power BI: Get Data -> From Web, the same URL with format=csv.`;

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
  const s = await serve({
    root: option('root', process.cwd()),
    port: Number(option('port', process.env.PORT ?? 8787)),
    host: option('host', '127.0.0.1'),
    token: option('token', process.env.VECTRO_TOKEN ?? null),
    cors: rest.includes('--cors'),
    log,
  });
  log(`serving ${s.tables.size} table(s) at ${s.url}: ${[...s.tables].map(([n, e]) => `${n} (${e.table.length.toLocaleString()} rows)`).join(', ') || 'none'}`);
  log(`try: ${s.url}/sql?q=${encodeURIComponent(`SELECT * FROM ${[...s.tables.keys()][0] ?? 'mytable'} LIMIT 5`)}`);
  const stop = () => s.close().then(() => process.exit(0));
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
} catch (err) {
  console.error(`vectro: ${err.message.replace(/^vectro:\s*/, '')}`);
  process.exit(1);
}
