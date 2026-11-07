// Audit round 6 (run 4, servers): a client that hung up in the middle of a
// big HTTP answer left the request waiting for 'drain' for ever: the answer's
// table was never freed (about 7 MB per aborted 300K-row download) and the
// tables a reload replaced stayed in memory (vectro-qa-2026-11-07/run4).
import { serve } from '../src/serve/index.mjs';
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
console.log(`${checks - failures} / ${checks} checks pass (audit r6 servers)`);
if (failures) process.exit(1);
