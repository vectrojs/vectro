// Audit round 5 (servers): the edge kit (NaN, ±Infinity, 2^53, emoji, quotes,
// tabs, newlines, 100 KB text, dates before 1970 and in 9999) over HTTP (json,
// csv, ndjson, arrow), the PostgreSQL protocol and MCP must give the engine's own
// answer; vectro-qa-2026-11-05/servers also checked them with the pg driver, psql
// and 20 PostgreSQL clients at once on a 100,000-row result. Here: ±Infinity
// survive HTTP json / csv / ndjson and MCP (they came out as null / empty, so as
// missing values, while PostgreSQL, OData and Arrow keep them); a PostgreSQL text
// parameter may hold both ' and " (it was refused: "text with both ' and \" can't
// be used in an expression"); parameters can't change what a query means.
import { MODE } from './test-mode.mjs';
if (MODE !== 'node') { console.log('the servers run in Node: skipped in this mode'); process.exit(0); }
import { connect } from 'node:net';
import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
const { Engine, sql } = await import('../src/node.mjs');
const { serve, servePg } = await import('../src/serve/index.mjs');

let failures = 0, checks = 0;
const same = (got, want, msg) => { checks++; const g = JSON.stringify(got), w = JSON.stringify(want); if (g !== w) { failures++; console.log(`FAIL ${msg}\n  got  ${g.slice(0, 300)}\n  want ${w.slice(0, 300)}`); } };

const e = new Engine({ threads: 1 });
const BIG = 'é😀"\'\\\t,\n'.repeat(12000);
const t = e.table({ id: [1, 2, 3, 4, 5], n: Float64Array.from([Infinity, -Infinity, NaN, 1.5, 2 ** 53]), s: ['é😀', "it's", 'b"c', BIG, ''], d: [-1, 0, NaN, Date.UTC(9999, 11, 31, 23, 59, 59, 999), Date.UTC(1950, 5, 15)].map((x) => new Date(x)) });
const tables = { t };

// HTTP: infinities stay infinities in every text format
const http = await serve({ tables, port: 0 });
const q = encodeURIComponent('SELECT id, n, MIN(n) OVER () AS lo FROM t ORDER BY id');
const j = await (await fetch(`${http.url}/sql?q=${q}`)).json();
same(j.rows.map((r) => r[1]), ['Infinity', '-Infinity', null, 1.5, 2 ** 53].map((x) => x), 'HTTP json: ±Infinity are not missing (written as "Infinity" / "-Infinity", as OData writes INF)');
const csv = await (await fetch(`${http.url}/sql?q=${q}&format=csv`)).text();
same(csv.split('\r\n').slice(1, 3).map((l) => l.split(',')[1]), ['Infinity', '-Infinity'], 'HTTP csv: ±Infinity are written');
const nd = (await (await fetch(`${http.url}/sql?q=${q}&format=ndjson`)).text()).split('\n').filter(Boolean).map((l) => JSON.parse(l).n);
same(nd.slice(0, 3), ['Infinity', '-Infinity', null], 'HTTP ndjson: ±Infinity are not missing');
const back = e.fromArrow(new Uint8Array(await (await fetch(`${http.url}/sql?q=${q}&format=arrow`)).arrayBuffer())).toRows().map((r) => r.n);
same(back.map(String), ['Infinity', '-Infinity', 'NaN', '1.5', String(2 ** 53)], 'HTTP arrow: ±Infinity kept');
const sAll = await (await fetch(`${http.url}/sql?q=${encodeURIComponent('SELECT s FROM t ORDER BY id')}`)).json();
same(sAll.rows.map((r) => r[0]), ['é😀', "it's", 'b"c', BIG, null], 'HTTP json: emoji, quotes and 100 KB text come back exactly');
await http.close();

// PostgreSQL: a small client (startup, then Parse / Bind with text parameters / Execute / Sync)
const pgs = await servePg({ tables, port: 0 });
const sock = connect(pgs.port, '127.0.0.1');
await new Promise((r) => sock.once('connect', r));
sock.on('error', () => {}); // (close() below resets the socket)
let buf = Buffer.alloc(0); const queue = [], waiters = [];
sock.on('data', (d) => { buf = Buffer.concat([buf, d]); while (buf.length >= 5 && buf.length >= buf.readInt32BE(1) + 1) { const len = buf.readInt32BE(1); queue.push({ type: String.fromCharCode(buf[0]), body: buf.subarray(5, len + 1) }); buf = buf.subarray(len + 1); waiters.shift()?.(); } });
const next = async () => { while (!queue.length) await new Promise((r) => waiters.push(r)); return queue.shift(); };
const send = (type, body) => { const h = Buffer.alloc(5); h.write(type, 0, 'latin1'); h.writeInt32BE(body.length + 4, 1); sock.write(Buffer.concat([h, body])); };
const i16 = (v) => { const b = Buffer.alloc(2); b.writeInt16BE(v); return b; }, i32 = (v) => { const b = Buffer.alloc(4); b.writeInt32BE(v); return b; };
{ const p = Buffer.from('user\0me\0database\0vectro\0\0'), h = Buffer.alloc(8); h.writeInt32BE(8 + p.length); h.writeInt32BE(196608, 4); sock.write(Buffer.concat([h, p])); for (;;) { const m = await next(); if (m.type === 'Z') break; if (m.type === 'E') throw new Error('PG login: ' + m.body.toString()); } }
const run = async (text, params) => {
  send('P', Buffer.concat([Buffer.from(`\0${text}\0`), i16(0)]));
  send('B', Buffer.concat([Buffer.from('\0\0'), i16(0), i16(params.length), ...params.flatMap((v) => (v === null ? [i32(-1)] : [i32(Buffer.byteLength(v)), Buffer.from(v)])), i16(0)]));
  send('E', Buffer.concat([Buffer.from('\0'), i32(0)])); send('S', Buffer.alloc(0));
  const rows = []; let error = null;
  for (;;) {
    const m = await next();
    if (m.type === 'D') { let at = 2; const row = []; for (let k = 0; k < m.body.readInt16BE(0); k++) { const len = m.body.readInt32BE(at); at += 4; row.push(len < 0 ? null : m.body.subarray(at, at + len).toString()); at += Math.max(0, len); } rows.push(row); }
    else if (m.type === 'E') error = m.body.toString().split('\0').find((f) => f.startsWith('M'))?.slice(1);
    else if (m.type === 'Z') return error ? { error } : rows;
  }
};
for (const [v, want] of [["it's", [['2']]], ['b"c', [['3']]], [BIG, [['4']]], [`both ' and "`, []], ["x' OR s <> 'x", []], ['x" OR s <> "x', []], ["x' OR 1=1 --", []], [null, []]])
  same(await run('SELECT id FROM t WHERE s = $1', [v]), want, `PG text parameter ${JSON.stringify(v).slice(0, 30)}`);
same(await run('SELECT n FROM t ORDER BY id', []), [['Infinity'], ['-Infinity'], [null], ['1.5'], ['9007199254740992']], 'PG: ±Infinity, NULL, 2^53');
same(await run("SELECT d FROM t ORDER BY id", []), [['1969-12-31 23:59:59.999'], ['1970-01-01 00:00:00'], [null], ['9999-12-31 23:59:59.999'], ['1950-06-15 00:00:00']], 'PG: timestamps before 1970 and in 9999');
send('X', Buffer.alloc(0)); sock.end();
await pgs.close();

// MCP: infinities in the sql tool's answer
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-srv-'));
mkdirSync(join(dir, 'data'));
writeFileSync(join(dir, 'data', 't.arrow'), t.toArrow({ file: true }));
const mcp = spawn(process.execPath, [fileURLToPath(new URL('../mcp/server.mjs', import.meta.url)), '--root', join(dir, 'data')], { stdio: ['pipe', 'pipe', 'pipe'] });
const pending = new Map(); let nid = 0;
createInterface({ input: mcp.stdout }).on('line', (l) => { const m = JSON.parse(l); pending.get(m.id)?.(m); pending.delete(m.id); });
const rpc = (method, params) => new Promise((r) => { const id = ++nid; pending.set(id, r); mcp.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
await rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 't', version: '1' } });
await rpc('tools/call', { name: 'load_file', arguments: { path: 't.arrow' } });
const r = await rpc('tools/call', { name: 'sql', arguments: { query: 'SELECT id, n FROM t ORDER BY id' } });
same(r.result?.structuredContent?.rows?.map((x) => x.n), ['Infinity', '-Infinity', null, 1.5, 2 ** 53], 'MCP sql: ±Infinity are not missing');
mcp.kill();
rmSync(dir, { recursive: true, force: true });
same(sql('SELECT COUNT(*) AS c FROM t', tables).toRows(), [{ c: 5 }], 'the engine still answers');
await e.close();

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 server checks passed`);
process.exit(failures ? 1 : 0);
