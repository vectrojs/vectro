// Audit round 5 (joins): every SQL join kind on edge data (empty and one-row
// sides, all-missing keys, -0 / 0, NaN, ±Infinity, 2^53, unicode text keys,
// one key everywhere), checked against a plain-JS nested-loop join, on tables
// and datasets, sync and async. The answers were checked against DuckDB in
// vectro-qa-2026-11-05/joins. Also: COALESCE and FULL JOIN USING keep dates,
// b.* keeps b's own column names, SELECT * after ON keeps both keys.
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const same = (a, b, msg) => { checks++; const x = JSON.stringify(a), y = JSON.stringify(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };

let seed = 12345;
const rnd = () => ((seed = (seed * 1103515245 + 12345) >>> 0) / 2 ** 32);
const pick = (a) => a[Math.floor(rnd() * a.length)];
const NUMK = [0, 1, 2, -1, 2147483647, -2147483648, 4294967296];
const FLTK = [0, -0, 1, 1.5, NaN, Infinity, -Infinity, 2 ** 53, 2 ** 53 + 2, 1e300, 5e-324, 0.1 + 0.2, 0.3];
const TXTK = ['a', 'b', '', 'A', 'é', 'é', '😀', 'İ', 'i', ' a', 'x'.repeat(300), '0'];
function mk(n, same) {
  const t = { id: [], k: [], f: [], s: [], g: [] };
  for (let i = 0; i < n; i++) {
    t.id.push(i);
    t.k.push(same ? 7 : rnd() < 0.08 ? NaN : pick(NUMK));
    t.f.push(same ? 0.5 : pick(FLTK));
    t.s.push(same ? 'z' : pick(TXTK));
    t.g.push(pick(['g1', 'g2', '']));
  }
  return t;
}
const T = { a: mk(40), b: mk(35), o: mk(1), sa: mk(60, true), sb: mk(50, true) };
T.n = { id: [0, 1], k: [NaN, NaN], f: [NaN, NaN], s: ['', ''], g: ['', ''] };

// the reference: missing (NaN, '') never matches, -0 equals 0
const present = (v) => v !== '' && !Number.isNaN(v);
function reference(L, R, kind, keys) {
  const out = [], hitR = new Set();
  for (let i = 0; i < L.id.length; i++) {
    let hit = false;
    for (let j = 0; j < R.id.length; j++) {
      if (keys.every(([x, y]) => present(L[x][i]) && present(R[y][j]) && L[x][i] === R[y][j])) { out.push([i, j]); hit = true; hitR.add(j); }
    }
    if (!hit && (kind === 'LEFT JOIN' || kind === 'FULL JOIN')) out.push([i, null]);
  }
  if (kind === 'RIGHT JOIN' || kind === 'FULL JOIN') for (let j = 0; j < R.id.length; j++) if (!hitR.has(j)) out.push([null, j]);
  return out;
}
const pairs = (rows) => rows.map((r) => [Number.isNaN(r.lid) ? null : r.lid, Number.isNaN(r.rid) ? null : r.rid]);
const sorted = (p) => p.map((x) => JSON.stringify(x)).sort();

const ONS = [['l.k = r.k', [['k', 'k']]], ['l.f = r.f', [['f', 'f']]], ['l.s = r.s', [['s', 's']]], ['l.k = r.f', [['k', 'f']]],
  ['l.k = r.k AND l.s = r.s', [['k', 'k'], ['s', 's']]], ['r.g = l.g AND l.f = r.f', [['g', 'g'], ['f', 'f']]]];
const PAIRS = [['a', 'b'], ['a', 'a'], ['o', 'a'], ['a', 'o'], ['n', 'a'], ['a', 'n'], ['n', 'n'], ['sa', 'sb']];
const KINDS = ['JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'];

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-joins-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const V = {};
  for (const [name, t] of Object.entries(T)) {
    const tab = e.table({ id: Float64Array.from(t.id), k: Float64Array.from(t.k), f: Float64Array.from(t.f), s: t.s, g: t.g });
    if (source !== 'dataset') { V[name] = tab; continue; }
    const path = join(dir, `${name}-${checks}.vectro`);
    e.writeDataset(path, tab, { rowsPerChunk: Math.max(3, Math.ceil(t.id.length / 5)) });
    V[name] = e.openDataset(path);
  }
  const run = (q) => (source === 'async' ? sql(q, V).toRowsAsync() : sql(q, V).toRows());
  for (const [x, y] of PAIRS) for (const kind of KINDS) for (const [on, keys] of ONS) {
    const q = `SELECT l.id AS lid, r.id AS rid FROM ${x} l ${kind} ${y} r ON ${on}`;
    same(sorted(pairs(await run(q))), sorted(reference(T[x], T[y], kind, keys)), `${tag} ${q}`);
  }
  // an empty side (a filter that keeps nothing keeps the column types)
  for (const kind of KINDS) {
    const q = `SELECT s, l.id AS lid, r.id AS rid FROM (SELECT * FROM a WHERE id < 0) l ${kind} b r USING (s)`;
    const want = kind === 'RIGHT JOIN' || kind === 'FULL JOIN' ? T.b.id.map((j) => [T.b.s[j], null, j]) : [];
    same(sorted((await run(q)).map((r) => [r.s, Number.isNaN(r.lid) ? null : r.lid, r.rid])), sorted(want), `${tag} empty left ${kind} USING (s)`);
  }
  // CROSS JOIN
  const cross = (await run('SELECT COUNT(*) AS n, SUM(l.id) AS s1, SUM(r.id) AS s2 FROM a l CROSS JOIN b r'))[0];
  same([cross.n, cross.s1, cross.s2], [40 * 35, 35 * (39 * 40) / 2, 40 * (34 * 35) / 2], `${tag} CROSS JOIN`);
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

// dates through COALESCE and FULL JOIN USING; b.* names; SELECT * after ON
{
  const e = new Engine();
  const day = (s) => new Date(s);
  const da = e.table({ d: ['2024-01-01', '2024-01-02'].map(day), x: [1, 2] }), db = e.table({ d: ['2024-01-02', '1969-12-31'].map(day), y: [10, 20] });
  const iso = (rows, c) => rows.map((r) => (r[c] instanceof Date ? r[c].toISOString() : r[c]));
  same(iso(sql('SELECT COALESCE(d, d) AS d FROM da ORDER BY 1', { da }).toRows(), 'd'), ['2024-01-01T00:00:00.000Z', '2024-01-02T00:00:00.000Z'], 'COALESCE of dates is a date');
  same(iso(sql('SELECT d FROM da FULL JOIN db USING (d) ORDER BY d', { da, db }).toRows(), 'd'), ['1969-12-31T00:00:00.000Z', '2024-01-01T00:00:00.000Z', '2024-01-02T00:00:00.000Z'], 'FULL JOIN USING a date key keeps dates');
  same(iso(sql('SELECT COALESCE(da.d, db.d) AS d FROM da FULL JOIN db ON da.d = db.d ORDER BY 1', { da, db }).toRows(), 'd'), ['1969-12-31T00:00:00.000Z', '2024-01-01T00:00:00.000Z', '2024-01-02T00:00:00.000Z'], 'COALESCE of two date columns');
  const a = e.table({ id: [1, 2, 3], k: [1, 2, 2], s: ['x', 'y', 'z'] }), b = e.table({ id: [10, 20], k: [2, 3], s: ['p', 'q'] });
  same(Object.keys(sql('SELECT b.* FROM a JOIN b ON a.k = b.k', { a, b }).toRows()[0]), ['id', 'k', 's'], 'b.* keeps b\'s column names');
  same(sql('SELECT * FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.id', { a, b }).toRows().map((r) => Object.values(r).map((v) => (Number.isNaN(v) ? null : v))),
    [[1, 1, 'x', null, null, ''], [2, 2, 'y', 10, 2, 'p'], [3, 2, 'z', 10, 2, 'p']], 'SELECT * after ON keeps the right key');
  same(sql('SELECT * FROM a FULL JOIN b USING (k) ORDER BY k', { a, b }).toRows().map((r) => r.k), [1, 2, 2, 3], 'USING key appears once, from either side');
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 join checks passed`);
process.exit(failures ? 1 : 0);
