// The HTTP SQL server: every format gives what sql(...).toRows() gives, for
// tables given in code and files in a data folder (CSV, JSON lines, Arrow,
// .vectro datasets); tokens, limits and errors behave.
import { mkdtempSync, writeFileSync, symlinkSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, sql } from '../src/node.mjs';
import { serve } from '../src/serve/http.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 15) console.log('FAIL', msg); } };
let seed = 11;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const N = 30_000;
const orders = {
  cust: Array.from({ length: N }, () => Math.floor(rnd() * 400)),
  price: Array.from({ length: N }, () => (rnd() < 0.05 ? NaN : Math.round(rnd() * 10000) / 100)),
  region: Array.from({ length: N }, () => pick(['EU', 'US', 'say "hi"', 'a,b', 'line\nbreak', 'Zürich', ''])),
  ts: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 400) * 86_400_000)),
};

// answers as JSON would have them: dates as ISO text, missing as null
const plain = (rows) => rows.map((r) => Object.values(r).map((v) => (v instanceof Date ? v.toISOString() : typeof v === 'number' && !Number.isFinite(v) ? null : v === '' ? null : v)));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function parseCSV(text) { // RFC 4180, for the test
  const rows = [];
  let row = [], field = '', quoted = false, i = 0, wasQuoted = false;
  for (; i < text.length; i++) {
    const c = text[i];
    if (quoted) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else quoted = false; } else field += c; continue; }
    if (c === '"') { quoted = true; wasQuoted = true; } else if (c === ',') { row.push(wasQuoted || field !== '' ? field : null); field = ''; wasQuoted = false; } else if (c === '\r') continue;
    else if (c === '\n') { row.push(wasQuoted || field !== '' ? field : null); rows.push(row); row = []; field = ''; wasQuoted = false; } else field += c;
  }
  return rows;
}

const dir = mkdtempSync(join(tmpdir(), 'vectro-serve-'));
const engine = new Engine();
const t = engine.table(orders);
const customers = engine.table({ cust: Array.from({ length: 300 }, (_, i) => i), tier: Array.from({ length: 300 }, (_, i) => 1 + (i % 3)) });
// a data folder: CSV, JSON lines, Arrow, a dataset, and a link to a file outside it
writeFileSync(join(dir, 'people.csv'), 'id,name\n1,Ann\n2,"Bo, Jr"\n');
writeFileSync(join(dir, 'events.jsonl'), '{"k":"a","v":1}\n{"k":"b","v":2.5}\n');
writeFileSync(join(dir, 'tiers.arrow'), customers.toArrow({ file: true }));
engine.writeDataset(join(dir, 'big.vectro'), t, { rowsPerChunk: 7000 });
const outside = mkdtempSync(join(tmpdir(), 'vectro-outside-'));
writeFileSync(join(outside, 'secret.csv'), 'x\n42\n');
symlinkSync(join(outside, 'secret.csv'), join(dir, 'secret.csv'));

const s = await serve({ tables: { orders: t, customers }, root: dir, port: 0, engine });
const get = (path, opts) => fetch(s.url + path, opts);
const q = (text, format = 'json', extra = '') => get(`/sql?q=${encodeURIComponent(text)}&format=${format}${extra}`);

const QUERIES = [
  'SELECT * FROM orders ORDER BY ts, cust, price LIMIT 500',
  'SELECT region, COUNT(*) AS n, SUM(price) AS s, MIN(ts) AS first FROM orders GROUP BY region ORDER BY region',
  'SELECT c.tier, COUNT(*) AS n FROM orders o JOIN customers c ON o.cust = c.cust GROUP BY c.tier ORDER BY c.tier',
  "SELECT UPPER(region) AS r, COUNT(*) AS n FROM big WHERE region <> '' GROUP BY 1 ORDER BY 1",
  'SELECT name, id FROM people ORDER BY id',
  'SELECT k, v FROM events ORDER BY k',
  'SELECT tier, COUNT(*) AS n FROM tiers GROUP BY tier ORDER BY tier',
];
const tablesNow = { orders: t, customers, big: s.tables.get('big')?.table, people: s.tables.get('people')?.table, events: s.tables.get('events')?.table, tiers: s.tables.get('tiers')?.table };
check(s.tables.has('big') && s.tables.has('people') && s.tables.has('events') && s.tables.has('tiers'), `folder tables: ${[...s.tables.keys()]}`);
check(!s.tables.has('secret'), 'a link to a file outside the folder is not served');
for (const text of QUERIES) {
  const want = sql(text, tablesNow).toRows();
  const wantPlain = plain(want);
  const j = await (await q(text)).json();
  check(same(j.rows, wantPlain) && j.row_count === want.length, `json: ${text}\n  ${JSON.stringify(j).slice(0, 300)}`);
  check(same(j.columns.map((c) => c.name), Object.keys(want[0] ?? {})), `json columns: ${text}`);
  const csv = parseCSV(await (await q(text, 'csv')).text());
  check(same(csv.slice(1), wantPlain.map((r) => r.map((v) => (v === null ? null : String(v))))) && same(csv[0], Object.keys(want[0] ?? {})), `csv: ${text}\n  ${JSON.stringify(csv.slice(0, 3))}`);
  const nd = (await (await q(text, 'ndjson')).text()).split('\n').filter(Boolean).map((l) => Object.values(JSON.parse(l)));
  check(same(nd, wantPlain), `ndjson: ${text}`);
  const arrow = engine.fromArrow(new Uint8Array(await (await q(text, 'arrow')).arrayBuffer()));
  check(same(plain(arrow.toRows()), wantPlain), `arrow: ${text}`);
  arrow.dispose();
}
// the SQL as a POST body, as JSON, a limit, table export
{
  const text = 'SELECT cust, price FROM orders WHERE price > 99 ORDER BY price DESC, cust';
  const want = plain(sql(text, tablesNow).toRows());
  const a = await (await get('/sql', { method: 'POST', body: text })).json();
  check(same(a.rows, want), 'POST body');
  const b = await (await get('/sql', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ sql: text, limit: 3 }) })).json();
  check(same(b.rows, want.slice(0, 3)), 'POST JSON with limit');
  const c = await (await q(text, 'json', '&limit=0')).json();
  check(c.rows.length === 0 && c.columns.length === 2, 'limit=0 gives the columns only');
  const rows = parseCSV(await (await get('/tables/orders/rows?format=csv')).text());
  check(rows.length === N + 1, `table export: ${rows.length} lines`);
  const info = await (await get('/tables/big')).json();
  check(info.rows === N && info.on_disk && info.file === 'big.vectro', `table info: ${JSON.stringify(info)}`);
}
// many at once
{
  const text = 'SELECT region, COUNT(*) AS n FROM orders GROUP BY region ORDER BY region';
  const want = plain(sql(text, tablesNow).toRows());
  const all = await Promise.all(Array.from({ length: 12 }, (_, i) => q(i % 2 ? text : text.replace('orders', 'big')).then((r) => r.json())));
  check(all.every((x) => same(x.rows, want)), 'twelve queries at once');
}
// errors
{
  const bad = await q('SELECT nope FROM orders');
  check(bad.status === 400 && (await bad.json()).error.includes('no column "nope"'), 'bad SQL: 400 with the message');
  check((await q('SELECT 1', 'xml')).status === 400, 'unknown format');
  check((await get('/tables/missing')).status === 404, 'unknown table');
  check((await get('/nowhere')).status === 404, 'unknown endpoint');
  check((await get('/sql')).status === 400, 'no SQL');
}
// a new file appears: reload
{
  writeFileSync(join(dir, 'later.csv'), 'a\n1\n2\n');
  const r = await (await get('/reload', { method: 'POST' })).json();
  check(r.loaded.includes('later'), `reload: ${JSON.stringify(r)}`);
  const j = await (await q('SELECT SUM(a) AS s FROM later')).json();
  check(same(j.rows, [[3]]), 'the new table answers');
}
await s.close();

// a token
{
  const s2 = await serve({ tables: { orders: t }, port: 0, token: 'sesame', cors: true });
  const url = `${s2.url}/sql?q=${encodeURIComponent('SELECT COUNT(*) AS n FROM orders')}`;
  check((await fetch(url)).status === 401, 'no token: 401');
  check((await fetch(url, { headers: { authorization: 'Bearer wrong' } })).status === 401, 'wrong token: 401');
  const ok = await fetch(url, { headers: { authorization: 'Bearer sesame' } });
  check(ok.status === 200 && ok.headers.get('access-control-allow-origin') === '*' && same((await ok.json()).rows, [[N]]), 'token in a header');
  check((await fetch(`${url}&token=sesame`)).status === 200, 'token in the URL');
  await s2.close();
}
await engine.close();
rmSync(dir, { recursive: true, force: true });
rmSync(outside, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} server checks passed`);
process.exit(failures ? 1 : 0);
