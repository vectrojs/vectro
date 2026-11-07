// Audit round 6 (run 4, servers): a client that hung up in the middle of a
// big HTTP answer left the request waiting for 'drain' for ever: the answer's
// table was never freed (about 7 MB per aborted 300K-row download) and the
// tables a reload replaced stayed in memory. Two files with one table name
// (dup.csv and dup.jsonl): the later one replaced the first without a word,
// and the first table was never freed. Parameters from the pg driver (text with
// no type) failed in id IN ($1, $2), BETWEEN $1 AND $2 and LIMIT $1.
// The MCP tools gave missing text as "" (null over the other protocols). The
// vectro command had no --version and showed raw Node errors for a wrong
// --root or port. FROM constructor (toString, __proto__) with no such table said
// "sql() got not a Vectro Table" (found on the prototype)
// (vectro-qa-2026-11-07/run4).
import { serve } from '../src/serve/index.mjs';
import { paramLiteral, bindParams } from '../src/serve/pgtypes.mjs';
import { Engine, sql } from '../src/node.mjs';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

let failures = 0, checks = 0;
const ok = (cond, msg) => { checks++; if (!cond) { failures++; console.log(`FAIL ${msg}`); } };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const dir = mkdtempSync(join(tmpdir(), 'vectro-r6-servers-'));
let csv = 'id,x,s\n';
for (let i = 0; i < 200_000; i++) csv += `${i},${i * 0.5},label${i % 1000}\n`;
writeFileSync(join(dir, 'big.csv'), csv);
const s = await serve({ root: dir, port: 0 });

// a big answer, the client stops reading and hangs up; then a reload: the old table is freed
const old = s.catalog.entries.get('big').table;
let freed = false;
const dispose = old.dispose.bind(old);
old.dispose = () => { freed = true; return dispose(); };
for (const format of ['json', 'csv', 'ndjson']) {
  await new Promise((done) => {
    const req = http.get(`${s.url}/sql?q=${encodeURIComponent('SELECT id * 2 a, x + 1 b, s FROM big')}&format=${format}`, (res) => {
      res.once('data', () => { res.pause(); setTimeout(() => { req.destroy(); done(); }, 50); });
    });
    req.on('error', () => {});
  });
}
await wait(200);
await s.reload();
await wait(200);
ok(freed, 'the tables a reload replaced are freed after clients hung up mid-answer');
const r = await fetch(`${s.url}/sql?q=${encodeURIComponent('SELECT COUNT(*) n FROM big')}`).then((x) => x.json());
ok(r.rows?.[0]?.[0] === 200_000, `the server still answers after the aborted downloads (${JSON.stringify(r)})`);

await s.close();
rmSync(dir, { recursive: true, force: true });

// two files with one table name (dup.csv and dup.jsonl, "my file.csv" and "my_file.csv"): the first
// is kept and the other one is reported, not loaded and then lost (its memory was never freed)
const dir2 = mkdtempSync(join(tmpdir(), 'vectro-r6-servers-'));
writeFileSync(join(dir2, 'dup.csv'), 'a\n1\n');
writeFileSync(join(dir2, 'dup.jsonl'), '{"b":2}\n');
writeFileSync(join(dir2, 'my file.csv'), 'c\n3\n');
writeFileSync(join(dir2, 'my_file.csv'), 'd\n4\n');
const s2 = await serve({ root: dir2, port: 0 });
const seen = [];
for (const [, e] of s2.catalog.entries) { const d = e.table.dispose.bind(e.table); e.table.dispose = () => { seen.push(e.source); return d(); }; }
const got = await s2.reload();
ok(JSON.stringify(got.loaded.sort()) === JSON.stringify(['dup', 'my_file']), `loaded once each: ${JSON.stringify(got.loaded)}`);
ok(got.failed.length === 2 && got.failed.every((f) => /same table name/.test(f.error)), `the other file reported: ${JSON.stringify(got.failed)}`);
const t2 = await fetch(`${s2.url}/tables`).then((x) => x.json());
ok(JSON.stringify(t2.tables.map((t) => t.file).sort()) === JSON.stringify(['dup.csv', 'my file.csv']), `the first file wins: ${JSON.stringify(t2.tables.map((t) => t.file))}`);
await s2.close();
ok(seen.length === 2, `every table freed on reload (${seen.length} of 2)`);
rmSync(dir2, { recursive: true, force: true });

// PostgreSQL drivers (the pg package) send parameters as text with no type: id IN ($1, $2),
// id BETWEEN $1 AND $2 and LIMIT $1 failed ("can only be compared with a text column"),
// though id = $1 worked. As the PostgreSQL server binds them: quoted text.
const engine = new Engine();
const t = engine.table({ id: [1, 2, 4, NaN], ts: [Date.UTC(2024, 0, 1), Date.UTC(2024, 1, 1), Date.UTC(2024, 2, 1), NaN] }, { dates: ['ts'] });
const bound = (text, ...values) => bindParams(text, values.map((v) => paramLiteral(v === null ? null : Buffer.from(String(v)), 0, 0)));
const ids = (text, ...values) => { try { return sql(bound(text, ...values), { t }).toRows().map((r) => r.id); } catch (e) { return e.message; } };
const eq = (a, b, msg) => ok(JSON.stringify(a) === JSON.stringify(b), `${msg}: ${JSON.stringify(a)}`);
eq(ids('SELECT id FROM t WHERE id = $1', 1), [1], 'id = $1');
eq(ids('SELECT id FROM t WHERE id IN ($1, $2)', 1, 4), [1, 4], 'id IN ($1, $2)');
eq(ids('SELECT id FROM t WHERE id IN ($1)', 2), [2], 'id IN ($1)');
eq(ids('SELECT id FROM t WHERE id IN ($1, $2)', 1, null), [1], 'id IN ($1, NULL)');
eq(ids('SELECT id FROM t WHERE id NOT IN ($1, $2)', 1, 2), [4], 'id NOT IN ($1, $2)');
eq(ids('SELECT id FROM t WHERE id IN (1, $1)', 4), [1, 4], 'id IN (1, $1)');
eq(ids('SELECT id FROM t WHERE id BETWEEN $1 AND $2', 1, 2), [1, 2], 'id BETWEEN $1 AND $2');
eq(ids('SELECT id FROM t WHERE id NOT BETWEEN $1 AND $2', 1, 2), [4], 'id NOT BETWEEN $1 AND $2');
eq(ids('SELECT id FROM t WHERE ts IN ($1)', '2024-02-01'), [2], 'date IN ($1)');
eq(ids('SELECT id FROM t WHERE ts BETWEEN $1 AND $2', '2024-01-15', '2024-03-01'), [2, 4], 'date BETWEEN $1 AND $2');
eq(ids('SELECT id FROM t LIMIT $1', 2), [1, 2], 'LIMIT $1');
eq(ids('SELECT id FROM t LIMIT $1 OFFSET $2', 2, 1), [2, 4], 'LIMIT $1 OFFSET $2');
ok(/LIMIT needs a number/.test(ids('SELECT id FROM t LIMIT $1', 'x')), 'LIMIT with text that is not a number is refused');
// a table name that is also on every object's prototype (constructor, toString, __proto__): "no table", not
// "sql() got not a Vectro Table or Dataset"; a table really named so still works
for (const name of ['constructor', 'toString', '__proto__', 'hasOwnProperty', 'valueOf']) {
  ok(/no table/.test(ids(`SELECT * FROM ${name === '__proto__' ? name : `"${name}"`}`)), `FROM ${name} with no such table: ${ids(`SELECT * FROM "${name}"`)}`);
}
ok(sql('SELECT COUNT(*) n FROM "constructor"', { constructor: t }).toRows()[0].n === 4, 'a table named constructor');
await engine.close();

// MCP: missing text came back as "" (an LLM reads it as a label ""), where HTTP, PostgreSQL and OData say null
const dir3 = mkdtempSync(join(tmpdir(), 'vectro-r6-servers-'));
writeFileSync(join(dir3, 'o.csv'), 'id,region\n1,EU\n2,\n3,US\n');
const mcp = spawn(process.execPath, [fileURLToPath(new URL('../mcp/server.mjs', import.meta.url)), '--root', dir3], { stdio: ['pipe', 'pipe', 'ignore'] });
let mbuf = '', mid = 0;
const waiting = new Map();
mcp.stdout.on('data', (d) => { mbuf += d; let i; while ((i = mbuf.indexOf('\n')) >= 0) { const o = JSON.parse(mbuf.slice(0, i)); mbuf = mbuf.slice(i + 1); waiting.get(o.id)?.(o); } });
const rpc = (method, params) => new Promise((r) => { const id = ++mid; waiting.set(id, r); mcp.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const call = async (name, args) => JSON.parse((await rpc('tools/call', { name, arguments: args })).result.content[0].text);
await rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '1' } });
const loaded = await call('load_file', { path: 'o.csv' });
eq(loaded.sample.map((r) => r.region), ['EU', null, 'US'], 'MCP load_file sample: missing text is null');
eq((await call('sql', { query: 'SELECT region, COUNT(*) n FROM o GROUP BY 1 ORDER BY 1 NULLS LAST' })).rows.map((r) => r.region), ['EU', 'US', null], 'MCP sql: missing text is null');
eq((await call('query', { table: 'o', group_by: 'region', aggregates: { n: 'count()' }, sort: 'region' })).rows.length, 3, 'MCP query groups');
eq((await call('query', { table: 'o', filter: 'id == 2' })).rows.map((r) => r.region), [null], 'MCP query rows: missing text is null');
mcp.stdin.end();
await new Promise((r) => mcp.on('exit', r));
rmSync(dir3, { recursive: true, force: true });

// the vectro command: --version, and plain messages for a wrong --root or port (raw ENOENT / ENOTDIR / RangeError before)
const { spawnSync } = await import('node:child_process');
const cli = (...args) => { const r = spawnSync(process.execPath, [fileURLToPath(new URL('../bin/vectro.mjs', import.meta.url)), ...args], { encoding: 'utf8', timeout: 20_000 }); return { code: r.status, out: r.stdout.trim(), err: r.stderr.trim() }; };
const version = JSON.parse((await import('node:fs')).readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version;
for (const flag of ['--version', '-v', 'version']) { const r = cli(flag); ok(r.code === 0 && r.out === version, `vectro ${flag}: ${JSON.stringify(r)}`); }
const dir4 = mkdtempSync(join(tmpdir(), 'vectro-r6-servers-'));
writeFileSync(join(dir4, 'a.csv'), 'x\n1\n');
let r4 = cli('serve', '--root', join(dir4, 'nope'), '--port', '0', '--quiet');
ok(r4.code === 1 && /no folder .*nope/.test(r4.err) && !/ENOENT|lstat/.test(r4.err), `missing --root: ${r4.err}`);
r4 = cli('serve', '--root', join(dir4, 'a.csv'), '--port', '0', '--quiet');
ok(r4.code === 1 && /is a file, not a folder/.test(r4.err) && !/ENOTDIR|scandir/.test(r4.err), `--root a file: ${r4.err}`);
for (const port of ['abc', '99999', '-1', '1.5']) {
  r4 = cli('serve', '--root', dir4, '--port', port, '--quiet');
  ok(r4.code === 2 && /--port needs a port number/.test(r4.err) && !/options\.port|RangeError/.test(r4.err), `--port ${port}: ${r4.err}`);
}
r4 = cli('serve', '--root', dir4, '--no-http', '--pg', 'x', '--quiet');
ok(r4.code === 2 && /--pg needs a port number/.test(r4.err), `--pg x: ${r4.err}`);
rmSync(dir4, { recursive: true, force: true });

console.log(`${checks - failures} / ${checks} checks pass (audit r6 servers)`);
if (failures) process.exit(1);
