// Audit round 5 (sort / top / ORDER BY LIMIT): sort and top on edge data (ties,
// -0 / 0, NaN, ±Infinity, 1e308, 2^53, text by code point with emoji and
// U+FF5E, dates before 1970) against a stable JS sort, on tables and datasets;
// ORDER BY ... LIMIT / OFFSET key sequences against the same sort. The SQL
// answers were checked against DuckDB in vectro-qa-2026-11-05/sort. Also:
// datasets have the documented ...Async methods; a dataset queried with an
// async call and then dropped doesn't crash Node when it is garbage collected;
// split-table topAsync keeps table order inside ties.
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

let failures = 0, checks = 0;
const show = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
const same = (a, b, msg) => { checks++; const x = show(a), y = show(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };

let seed = 5;
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const TXT = ['a', 'b', 'B', '', 'é', 'é', '😀', '～', 'İ', 'a ', ' a', 'ab', 'aB'];
const NUM = [0, -0, 1, 1, 2, 3, 3, NaN, -1, 2.5, Infinity, -Infinity, 1e308, -1e308, 5e-324, 2 ** 53, 0.1 + 0.2, 0.3];
function mk(n) {
  const t = { id: [], o: [], o2: [], s: [], d: [] };
  for (let i = 0; i < n; i++) { t.id.push(i); t.o.push(pick(NUM)); t.o2.push(Math.floor(rnd() * 4)); t.s.push(pick(TXT)); t.d.push(rnd() < 0.1 ? new Date(NaN) : new Date(Date.UTC(1960 + Math.floor(rnd() * 80), 0, 1))); }
  return t;
}
const T = { t: mk(600), o: mk(1), big: mk(60000) };

const miss = (x) => x === '' || (typeof x === 'number' && Number.isNaN(x)) || (x instanceof Date && Number.isNaN(+x));
const cp = (a, b) => { const x = [...a].map((c) => c.codePointAt(0)), y = [...b].map((c) => c.codePointAt(0)); for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i]; return x.length - y.length; };
// stable; missing last in both directions (SQL: unless NULLS FIRST); -0 equals 0
function order(t, by, nullsFirst = false) {
  const keys = by.map((k) => ({ col: k.replace(/^-/, ''), desc: k.startsWith('-') }));
  return t.id.map((_, i) => i).sort((i, j) => {
    for (const { col, desc } of keys) {
      const a = t[col][i], b = t[col][j];
      if (miss(a) || miss(b)) { if (miss(a) && miss(b)) continue; return miss(a) !== nullsFirst ? 1 : -1; }
      const c = typeof a === 'string' ? cp(a, b) : +a === +b ? 0 : +a < +b ? -1 : 1;
      if (c) return desc ? -c : c;
    }
    return i - j;
  });
}
const keyOf = (v) => (miss(v) ? null : v instanceof Date ? v.toISOString() : v === 0 ? 0 : v);

const BYS = [['o'], ['-o'], ['s'], ['-s'], ['d'], ['-d'], ['o2', '-o'], ['-o2', 's', 'o']];
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-sort-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const V = {};
  for (const [name, t] of Object.entries(T)) {
    const tab = e.table({ id: Float64Array.from(t.id), o: Float64Array.from(t.o), o2: Float64Array.from(t.o2), s: t.s, d: t.d });
    if (source !== 'dataset') { V[name] = tab; continue; }
    const path = join(dir, `${name}.vectro`);
    e.writeDataset(path, tab, { rowsPerChunk: Math.max(3, Math.ceil(t.id.length / 7)) });
    V[name] = e.openDataset(path);
  }
  for (const [name, t] of Object.entries(T)) for (const by of BYS) {
    const want = order(t, by), arg = by.length > 1 ? by : by[0], src = V[name];
    if (name !== 'big') same((source === 'async' ? await src.sortAsync(arg) : src.sort(arg)).toRows().map((r) => r.id), want, `${tag} ${name} sort ${by}`);
    for (const k of [1, 5, 300]) {
      const got = source === 'async' ? await src.topAsync(k, arg) : src.top(k, arg);
      same(got.toRows().map((r) => r.id), want.slice(0, k), `${tag} ${name} top ${k} ${by}`);
    }
    // SQL: the keys of the rows, in order (ties may be cut at any row)
    for (const nf of [false, true]) for (const lim of [' LIMIT 7', ' LIMIT 4 OFFSET 3', '']) {
      if (name === 'big' && !lim) continue;
      const ord = by.map((k) => (k[0] === '-' ? k.slice(1) + ' DESC' : k) + (nf ? ' NULLS FIRST' : '')).join(', ');
      const q = `SELECT ${by.map((k) => k.replace('-', '')).join(', ')} FROM ${name} ORDER BY ${ord}${lim}`;
      const rows = source === 'async' ? await sql(q, V).toRowsAsync() : sql(q, V).toRows();
      const idx = order(t, by, nf), [lo, hi] = lim.includes('OFFSET') ? [3, 7] : lim ? [0, 7] : [0, idx.length];
      same(rows.map((r) => Object.values(r).map(keyOf)), idx.slice(lo, hi).map((i) => by.map((k) => keyOf(t[k.replace('-', '')][i]))), `${tag} ${q}`);
    }
  }
  await e.close();
}

// datasets: the documented ...Async methods, and no crash when a dataset used asynchronously is dropped
// O4: LIMIT / OFFSET take constant expressions, round as DuckDB (LIMIT 2.5 is 3), and OFFSET works alone
{
  const { sql } = await import('../src/sql.mjs');
  const e = new Engine(), t = e.table({ v: [5, 3, 1, 4, 2] });
  const v = (q) => { try { return sql(q, { t }).toRows().map((r) => r.v); } catch (err) { return 'error: ' + err.message; } };
  same(v('SELECT v FROM t LIMIT 2.5'), [5, 3, 1], 'LIMIT 2.5 is 3 rows');
  same(v('SELECT v FROM t ORDER BY v OFFSET 3'), [4, 5], 'OFFSET without LIMIT');
  same(v('SELECT v FROM t ORDER BY v LIMIT 2 + 1 OFFSET 1'), [2, 3, 4], 'LIMIT 2 + 1');
  same(v('SELECT v FROM t UNION SELECT v FROM t ORDER BY v OFFSET 4'), [5], 'OFFSET without LIMIT after a UNION');
  await e.close();
}

if (MODE === 'node') {
  const e = new Engine(), path = join(dir, 'async.vectro');
  e.writeDataset(path, { g: ['a', 'b', 'a'], v: [3, 1, 2] });
  const d = e.openDataset(path);
  const missing = ['sort', 'top', 'sum', 'mean', 'min', 'max', 'join'].filter((m) => typeof d[m + 'Async'] !== 'function');
  same(missing, [], 'datasets have the ...Async version of every query method');
  same(typeof d.groupBy('g').aggAsync, 'function', 'dataset groupBy().aggAsync');
  if (!missing.length) {
    same((await d.sortAsync('-v')).toRows().map((r) => r.v), [3, 2, 1], 'dataset sortAsync');
    same((await d.topAsync(1, 'v')).toRows().map((r) => r.v), [1], 'dataset topAsync');
    same([await d.sumAsync('v'), await d.minAsync('v'), await d.maxAsync('v')], [6, 1, 3], 'dataset sumAsync / minAsync / maxAsync');
  }
  await e.close();
  const script = join(dir, 'drop.mjs'), own = join(dir, 'drop.vectro'); // (a file: a script given with -e can't start worker threads, X1; its own dataset: on Windows a file open here can't be replaced)
  writeFileSync(script, `
    import { Engine } from ${JSON.stringify(new URL('../src/node.mjs', import.meta.url).href)};
    const e = new Engine();
    e.writeDataset(${JSON.stringify(own)}, { v: Float64Array.from({ length: 5000 }, (_, i) => i % 5) }, { rowsPerChunk: 1000 });
    for (let i = 0; i < 3; i++) { let d = e.openDataset(${JSON.stringify(own)}); await d.toRowsAsync(5); d = null; }
    for (let i = 0; i < 5; i++) { gc(); await new Promise((r) => setTimeout(r, 50)); }
    await e.close(); console.log('survived');`);
  const child = spawnSync(process.execPath, ['--expose-gc', script], { encoding: 'utf8' });
  same([child.stdout.trim(), (child.stderr.match(/FileHandle[^\n]*|Error: [^\n]*/) ?? [''])[0].slice(0, 120)], ['survived', ''], 'a dropped dataset used asynchronously does not crash at garbage collection');

  const { SplitEngine } = await import('../src/node.mjs');
  const N = 20000, id = Float64Array.from({ length: N }, (_, i) => i), o = Float64Array.from({ length: N }, (_, i) => [Infinity, -Infinity, 1, NaN][Math.floor(i / 1000) % 4]);
  const se = new SplitEngine({ workers: 3 }), st = await se.tableAsync({ id, o }), plain = new Engine();
  for (const by of ['o', '-o']) same((await st.topAsync(3000, by)).toRows().map((r) => r.id), plain.table({ id, o }).top(3000, by).toRows().map((r) => r.id), `split topAsync(3000, ${by}) keeps table order in ties`);
  await se.close(); await plain.close();
}
rmSync(dir, { recursive: true, force: true });

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 sort checks passed`);
process.exit(failures ? 1 : 0);
