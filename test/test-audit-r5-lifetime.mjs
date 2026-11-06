// Audit round 5 (memory and engine lifetime), each part in its own process:
// mixed queries (SQL sync and async, dataframe with its result tables disposed,
// joins, windows, percentiles, datasets, Arrow) leave the engine's live bytes and
// blocks where they were (vectro-qa-2026-11-05/life ran 10,000 of them and 200
// create / close cycles of every kind of object: flat); close() while each kind of
// work runs rejects the work with "this engine is closed" and the process ends by
// itself. Also: a table made inside a chain (t.sort(...).head(20)) is freed once
// dropped (it couldn't be disposed and was kept until close(): 150 calls held
// 900 MB); a SplitEngine nobody closed doesn't keep the process alive; a
// script piped into node can start worker threads.
import { MODE } from './test-mode.mjs';
if (MODE !== 'node') { console.log('lifetime checks run Node child processes: skipped in this mode'); process.exit(0); }
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const same = (got, want, msg) => { checks++; const g = JSON.stringify(got), w = JSON.stringify(want); if (g !== w) { failures++; console.log(`FAIL ${msg}\n  got  ${g.slice(0, 300)}\n  want ${w.slice(0, 300)}`); } };
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-life-'));
const src = new URL('../src/', import.meta.url).href;
const head = `import { Engine, SplitEngine, sql } from '${src}node.mjs'; import { serve } from '${src}serve/index.mjs';
import { mkdtempSync, writeFileSync } from 'node:fs'; import { tmpdir } from 'node:os'; import { join } from 'node:path';
process.on('unhandledRejection', (r) => { console.log('UNHANDLED ' + String(r?.message ?? r)); process.exitCode = 3; });
const big = (n) => ({ id: Float64Array.from({ length: n }, (_, i) => i), v: Float64Array.from({ length: n }, (_, i) => (i % 13 === 0 ? NaN : (i * 7919) % 1000)), g: Array.from({ length: n }, (_, i) => ['EU', 'US', 'é😀', '', 'APAC'][i % 5]) });
const live = (e) => { let b = 0; for (const s of e.sizes.values()) b += s; return b; };
const settle = async () => { for (let i = 0; i < 4; i++) { globalThis.gc(); await new Promise((r) => setTimeout(r, 20)); } };
const outcome = (p) => p.then(() => 'resolved', (er) => (/closed/.test(er?.message) ? 'rejected: closed' : 'rejected: ' + String(er?.message).slice(0, 60)));\n`;
const run = (name, body, timeout = 30000, env = {}) => {
  const file = join(dir, name + '.mjs'); writeFileSync(file, head + body);
  const r = spawnSync(process.execPath, ['--expose-gc', file], { encoding: 'utf8', timeout, env: { ...process.env, ...env } });
  return { out: r.stdout.trim(), status: r.signal ? 'killed after ' + timeout / 1000 + ' s' : r.status, err: r.stderr.split('\n').filter((l) => /Error|UNHANDLED/.test(l)).slice(0, 2).join(' ') };
};

// 1. mixed queries: live bytes and blocks come back to where they were
{
  const r = run('mixed', `const e = new Engine(), t = e.table(big(100000)), u = e.table({ g: ['EU', 'US', 'APAC'], w: [1, 2, 3] });
const d0 = mkdtempSync(join(tmpdir(), 'mx-')); e.writeDataset(join(d0, 't.vectro'), t, { rowsPerChunk: 20000 }); const ds = e.openDataset(join(d0, 't.vectro'));
const W = [() => sql('SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY g', { t }).toRows(), () => sql('SELECT g, COUNT(*) AS c FROM t GROUP BY g', { t }).toRowsAsync(),
  () => sql("SELECT id, UPPER(g) || '-' || CAST(v AS VARCHAR) AS k FROM t WHERE v > 990 ORDER BY id LIMIT 50", { t }).toRows(), () => sql('SELECT t.g, SUM(v * w) AS s FROM t JOIN u ON t.g = u.g GROUP BY t.g', { t, u }).toRows(),
  () => sql('SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn FROM t WHERE id < 5000', { t }).toRows(), () => sql('SELECT g, MEDIAN(v) AS m FROM t GROUP BY g', { t }).toRows(),
  () => { const r = t.filter('v > 500').groupBy('g').agg({ n: 'count()' }); r.toRows(); r.dispose(); }, () => { const a = t.sort(['g', '-v']), r = a.head(20); r.toRows(); r.dispose(); a.dispose(); },
  () => { const r = t.join(u, { on: 'g' }); r.agg({ s: 'sum(w)' }); r.dispose(); }, () => ds.filter('v > 900').agg({ n: 'count()' }), () => sql('SELECT g, COUNT(*) AS c FROM ds WHERE v > 100 GROUP BY g', { ds }).toRowsAsync(),
  () => { const r = e.fromArrow(t.filter('id < 1000').toArrow()); r.toRows(1); r.dispose(); }, () => t.filter('v > 100').aggAsync({ s: 'sum(v)' })];
for (let i = 0; i < 100; i++) await W[i % W.length]();
await settle(); const a = [live(e), e.sizes.size];
for (let i = 0; i < 1300; i++) await W[i % W.length]();
await settle(); console.log(JSON.stringify([a, [live(e), e.sizes.size]])); ds.close(); await e.close();`, 120000);
  let got; try { const [a, b] = JSON.parse(r.out); got = b[0] === a[0] && b[1] === a[1] ? 'flat' : `live ${a[0]} -> ${b[0]} bytes, blocks ${a[1]} -> ${b[1]}`; } catch { got = r; }
  same(got, 'flat', '1,300 mixed queries leave the engine memory where it was');
}
// 2. a table made inside a chain is freed when dropped
{
  const r = run('chain', `const e = new Engine(), t = e.table(big(200000));
t.sort(['g', '-v']).head(20).toRows(); await settle(); const a = live(e);
for (let i = 0; i < 30; i++) { t.sort(['g', '-v']).head(20).toRows(); t.derive('x', 'v * 2').filter('x > 10').top(5, '-x').toRows(); t.filter('v > 500').groupBy('g').agg({ n: 'count()' }).toRows(); }
await settle(); console.log(Math.round((live(e) - a) / 2 ** 20)); await e.close();`, 120000);
  same(Number(r.out) <= 20 ? 'freed' : `${r.out} MB still held after 30 chains`, 'freed', 'tables made inside a dataframe chain are freed once dropped');
}
{ // (a view shares its source's arrays without referencing the source; freed memory is overwritten here)
  const r = run('chain-view', `const e = new Engine(), t = e.table(big(300000));
const v = t.sort(['-v']).filter('v > 990'), s = t.derive('x', 'v * 2').select(['x', 'g']), col = t.sort(['v']).column('v');
const ask = () => JSON.stringify([v.toRows().length, s.head(3).toRows(), col[299999]]), want = ask();
await settle(); for (let i = 0; i < 20; i++) { t.sort(['g', '-v']).head(20).toRows(); await new Promise((r) => setTimeout(r, 0)); }
await settle(); console.log(ask() === want ? 'same' : 'changed'); await e.close();`, 120000, { VECTRO_POISON: '1' });
  same(r.out, 'same', 'views and column() arrays of a dropped table keep their values');
}
// 3. close() while work runs: the work is rejected as closed, nothing is unhandled, the process ends by itself
const STOP = {
  'async SQL': `const e = new Engine(), t = e.table(big(1000000)); const q = outcome(sql('SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY g ORDER BY s DESC', { t }).toRowsAsync()); await e.close(); console.log(await q);`,
  'async sort': `const e = new Engine(), t = e.table(big(1000000)); const q = outcome(t.sortAsync(['g', '-v'])); await new Promise((r) => setTimeout(r, 5)); await e.close(); console.log(await q);`,
  'split table query': `const se = new SplitEngine({ workers: 3 }); const st = await se.tableAsync(big(500000)); const q = outcome(st.groupBy('g').aggAsync({ n: 'count()' })); await se.close(); console.log(await q);`,
  'stream push': `const se = new SplitEngine({ workers: 2 }); const st = await se.streamAsync({ format: 'csv', columns: { a: 'number', b: 'category' } }); let s = ''; for (let i = 0; i < 100000; i++) s += i + ',x' + (i % 99) + '\\n'; const q = outcome(st.push(s)); await se.close(); console.log(await q);`,
  'dataset async query': `const e = new Engine(); const d0 = mkdtempSync(join(tmpdir(), 'st-')); e.writeDataset(join(d0, 'x.vectro'), e.table(big(500000)), { rowsPerChunk: 50000 }); const d = e.openDataset(join(d0, 'x.vectro')); const q = outcome(d.filter('v > 1').toRowsAsync()); d.close(); await e.close(); console.log(await q);`,
  'readCSVAsync': `const e = new Engine(); const d0 = mkdtempSync(join(tmpdir(), 'st-')); let s = 'a,b\\n'; for (let i = 0; i < 200000; i++) s += i + ',x' + (i % 7) + '\\n'; writeFileSync(join(d0, 'a.csv'), s); const q = outcome(e.readCSVAsync(join(d0, 'a.csv'))); await e.close(); console.log(await q);`,
  'csvToDatasetAsync': `const e = new Engine(); const d0 = mkdtempSync(join(tmpdir(), 'st-')); let s = 'a,b\\n'; for (let i = 0; i < 200000; i++) s += i + ',x' + (i % 7) + '\\n'; writeFileSync(join(d0, 'a.csv'), s); const q = outcome(e.csvToDatasetAsync(join(d0, 'a.csv'), join(d0, 'a.vectro'), { pieceBytes: 100000 })); await e.close(); console.log(await q);`,
};
for (const [label, body] of Object.entries(STOP)) {
  const r = run('stop-' + label.replace(/\W/g, ''), body, 20000);
  same([r.status, r.out, r.err], [0, 'rejected: closed', ''], `close() during ${label}`);
}
{
  const r = run('server', `const e = new Engine(), t = e.table(big(300000)); const s = await serve({ tables: { t }, port: 0 }); await e.close(); const x = await fetch(s.url + '/sql?q=' + encodeURIComponent('SELECT COUNT(*) AS c FROM t')); console.log(x.status); await s.close();`, 20000);
  same([r.status, r.out], [0, '400'], 'a server whose engine was closed answers with an error and closes');
}
// 4. nothing left running: a script that never calls close() still ends
for (const [label, body] of [['Engine', `const e = new Engine(); await e.table(big(300000)).aggAsync({ s: 'sum(v)' }); console.log('done');`], ['SplitEngine', `const se = new SplitEngine({ workers: 2 }); await (await se.tableAsync(big(1000))).aggAsync({ n: 'count()' }); console.log('done');`]]) {
  const r = run('open-' + label, body, 8000);
  same([r.status, r.out], [0, 'done'], `a script that leaves a ${label} open still ends by itself`);
}
// 5. a script piped into node (or given with --input-type -e) can start worker threads (they refused --input-type)
{
  const script = `import { Engine, SplitEngine } from '${src}node.mjs'; const e = new Engine(); const t = e.table({ a: Float64Array.from({ length: 2e6 }, (_, i) => i % 7) });
console.log(t.sum('a')); const se = new SplitEngine({ workers: 2 }); console.log((await (await se.tableAsync({ a: [1, 2, 3] })).aggAsync({ n: 'count()' })).n); await se.close(); await e.close();`;
  const r = spawnSync(process.execPath, ['--input-type=module'], { input: script, encoding: 'utf8', timeout: 20000 });
  same([r.status, r.stdout.trim().split('\n')], [0, ['5999995', '3']], 'a script piped into node --input-type=module runs worker threads');
}
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 lifetime checks passed`);
process.exit(failures ? 1 : 0);
