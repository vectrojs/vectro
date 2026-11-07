// Audit round 6 (run 4, servers): a client that hung up in the middle of a
// big HTTP answer left the request waiting for 'drain' for ever: the answer's
// table was never freed (about 7 MB per aborted 300K-row download) and the
// tables a reload replaced stayed in memory. Two files with one table name
// (dup.csv and dup.jsonl): the later one replaced the first without a word,
// and the first table was never freed. Parameters from the pg driver (text with
// no type) failed in id IN ($1, $2), BETWEEN $1 AND $2 and LIMIT $1
// (vectro-qa-2026-11-07/run4).
import { serve } from '../src/serve/index.mjs';
import { paramLiteral, bindParams } from '../src/serve/pgtypes.mjs';
import { Engine, sql } from '../src/node.mjs';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import http from 'node:http';

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
await engine.close();

console.log(`${checks - failures} / ${checks} checks pass (audit r6 servers)`);
if (failures) process.exit(1);
