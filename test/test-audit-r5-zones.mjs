// Audit round 5 (datasets and zone maps): datasets whose chunks and 65,536-row
// blocks begin and end on edge values (NaN-only chunks, -0 / 0, ±Infinity, one
// value, labels found only in some chunks or only on a chunk's first row, an
// all-missing text chunk, the min on a chunk's last row), filtered exactly on
// those values with params, &&, ||, !, isMissing, in(); count / sum / min / max,
// the first rows and top(3) must equal plain JS and the same query on the table
// in memory (vectro-qa-2026-11-05/zones: 2,902 checks, all equal, sync, async and
// 1 thread). Also: an empty dataset, one written from a filtered view, written
// over; and a dataset handle that stays open while its file is written over
// must give the old answers or an error, never the new file's bytes read with
// the old layout (it mixed old and new rows).
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs'; import { tmpdir } from 'node:os'; import { join } from 'node:path';
const OPEN = /can't replace .* while it is open/; // (Windows can't replace a file that is open: vectro's plain error)
if (MODE !== 'node') { console.log('datasets are Node only: skipped'); process.exit(0); }
const OPTS = process.env.OPTS ?? 'default';
const e = new Engine(OPTS === 'serial' ? { threads: 1 } : {});
const dir = mkdtempSync(join(tmpdir(), 'r5z-'));
let seed = 3; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
// layout per chunk: each chunk of `ch` rows gets one "shape" per column
function build(n, ch) {
  const t = { id: [], o: [], z: [], c: [], d: [], r: [], u: [] };
  const shapes = [(i) => i, () => NaN, (i, k) => (k % 2 ? -0 : 0), () => Infinity, () => -Infinity, (i) => 1e308 - i, (i) => (i % 3) - 1, () => 5];
  for (let i = 0; i < n; i++) {
    const k = Math.floor(i / ch), j = i % ch;
    t.id.push(i);
    t.o.push(shapes[k % shapes.length](i, j)); // a different shape per chunk: NaN-only, -0 / 0, ±Infinity, one value
    t.z.push(Math.floor(i / 7)); // sorted with runs that cross chunk edges
    t.c.push(k % 5 === 4 ? '' : `L${k % 4}${j === 0 ? 'first' : ''}`); // labels only in some chunks; an all-missing chunk; a label only on a chunk's first row
    t.d.push(k % 6 === 5 ? NaN : Date.UTC(1960, 0, 1) + i * 86400e3);
    t.r.push(Math.floor(rnd() * 1000));
    t.u.push(j === ch - 1 ? -1 : j); // the min of each chunk sits on its last row
  }
  return t;
}
const SETS = [['small chunks', 2000, 97], ['one-row chunks', 40, 1], ['big chunks with blocks', 200_000, 140_000]];
let pass = 0, fail = 0, err = 0; const bad = [];
const ok = (got, want, msg) => { const g = JSON.stringify(got, rep), w = JSON.stringify(want, rep); if (g === w) pass++; else { fail++; bad.push(`${msg}\n     got  ${g.slice(0, 220)}\n     want ${w.slice(0, 220)}`); } };
const rep = (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : Object.is(v, -0) ? 0 : v);
const miss = (x) => x === '' || Number.isNaN(x);
for (const [label, n, ch] of SETS) {
  const t = build(n, ch);
  const cols = { id: Float64Array.from(t.id), o: Float64Array.from(t.o), z: Float64Array.from(t.z), c: t.c, d: t.d.map((x) => new Date(x)), r: Float64Array.from(t.r), u: Float64Array.from(t.u) };
  const tab = e.table(cols), p = join(dir, label.replace(/ /g, '_') + '.vectro');
  e.writeDataset(p, tab, { rowsPerChunk: ch });
  const ds = e.openDataset(p);
  // edge values: every chunk's first / last value of each column
  const edges = { z: new Set(), u: new Set(), d: new Set(), o: new Set([0, -0, 5, 1e308, 1e308 - 1, -1, 1]) };
  for (let k = 0; k * ch < n; k++) for (const c of ['z', 'u', 'd']) { const a = t[c][k * ch], b = t[c][Math.min(n, (k + 1) * ch) - 1]; for (const v of [a, b]) if (!Number.isNaN(v) && edges[c].size < 40) edges[c].add(v); }
  const F = [];
  for (const [c, vs] of Object.entries(edges)) for (const v of vs) {
    const P = { v };
    for (const op of ['>=', '>', '<=', '<', '==', '!=']) F.push([`${c} ${op} v`, P, (r) => ({ '>=': r[c] >= v, '>': r[c] > v, '<=': r[c] <= v, '<': r[c] < v, '==': r[c] === v, '!=': r[c] !== v }[op])]);
    F.push([`${c} >= v && ${c} <= v`, P, (r) => r[c] >= v && r[c] <= v], [`!(${c} > v)`, P, (r) => !(r[c] > v)], [`${c} >= v && r < 500`, P, (r) => r[c] >= v && r.r < 500]);
  }
  for (const [x, P, f] of [['o > lim', { lim: 1e300 }, (r) => r.o > 1e300], ['o < lim', { lim: -1e300 }, (r) => r.o < -1e300], ['o == inf', { inf: Infinity }, (r) => r.o === Infinity], ['o == ninf', { ninf: -Infinity }, (r) => r.o === -Infinity],
    ['o >= ninf', { ninf: -Infinity }, (r) => r.o >= -Infinity], ['isMissing(o)', {}, (r) => Number.isNaN(r.o)], ['!isMissing(o) && o == 0', {}, (r) => r.o === 0], ['o != o', {}, (r) => r.o !== r.o], ['o == nanp', { nanp: NaN }, () => false],
    ["c == 'L1'", {}, (r) => r.c === 'L1'], ["c == 'L2first'", {}, (r) => r.c === 'L2first'], ["c != 'L1'", {}, (r) => r.c !== 'L1'], ["c == 'nope'", {}, () => false], ['isMissing(c)', {}, (r) => r.c === ''], ["in(c, 'L0', 'L3first', 'nope')", {}, (r) => ['L0', 'L3first'].includes(r.c)],
    ["c == 'L1' && z >= 100", {}, (r) => r.c === 'L1' && r.z >= 100], ["d >= '1961-01-01'", {}, (r) => r.d >= Date.UTC(1961, 0, 1)], ["d < '1960-01-01'", {}, (r) => r.d < Date.UTC(1960, 0, 1)], ['isMissing(d)', {}, (r) => Number.isNaN(r.d)],
    ['z >= v || u < 0', { v: 300 }, (r) => r.z >= 300 || r.u < 0], ['u < 0', {}, (r) => r.u < 0], ['z + 0 >= 500', {}, (r) => r.z >= 500]]) F.push([x, P, f]);
  const rows = t.id.map((_, i) => ({ id: t.id[i], o: t.o[i], z: t.z[i], c: t.c[i], d: t.d[i], r: t.r[i], u: t.u[i] }));
  const sum = (rs, c) => rs.reduce((s, r) => (miss(r[c]) ? s : s + r[c]), 0), mn = (rs, c) => { const p = rs.filter((r) => !miss(r[c])).map((r) => r[c]); return p.length ? p.reduce((a, b) => (b < a ? b : a)) : NaN; }, mx = (rs, c) => { const p = rs.filter((r) => !miss(r[c])).map((r) => r[c]); return p.length ? p.reduce((a, b) => (b > a ? b : a)) : NaN; };
  let skippedSome = 0;
  for (const [x, P, f] of F) {
    const keep = rows.filter(f);
    const want = { n: keep.length, sr: sum(keep, 'r'), lo: mn(keep, 'z'), hi: mx(keep, 'u'), first: keep.slice(0, 3).map((r) => r.id), top: [...keep].sort((a, b) => b.r - a.r || a.id - b.id).slice(0, 3).map((r) => r.id) };
    for (const [src, q] of [['dataset', ds], ['table', tab]]) {
      try {
        const v = q.filter(x, P);
        const a = OPTS === 'async' ? await v.aggAsync({ n: 'count()', sr: 'sum(r)', lo: 'min(z)', hi: 'max(u)' }) : v.agg({ n: 'count()', sr: 'sum(r)', lo: 'min(z)', hi: 'max(u)' });
        if (src === 'dataset' && ds.lastRun?.skipped) skippedSome++;
        const first = (src === 'dataset' ? v.limit(3) : v.head(3)).toRows().map((r) => r.id);
        const top = v.top(3, ['-r', 'id']).toRows().map((r) => r.id);
        ok({ ...a, first, top }, want, `${label} ${src}: filter(${x}) ${JSON.stringify(P, rep)}`);
      } catch (er) { err++; bad.push(`${label} ${src}: filter(${x}) ERROR ${er.message.slice(0, 120)}`); }
    }
  }
  // SQL on the dataset, a few shapes
  for (const [w, f] of [['z >= 500 AND z <= 520', (r) => r.z >= 500 && r.z <= 520], ["c = 'L2'", (r) => r.c === 'L2'], ['o > 1e300', (r) => r.o > 1e300], ['o = 0', (r) => r.o === 0], ['o IS NULL', (r) => Number.isNaN(r.o)], ['u < 0', (r) => r.u < 0], ["d >= DATE '1961-06-01'", (r) => r.d >= Date.UTC(1961, 5, 1)], ['NOT (z > 100)', (r) => r.z <= 100]]) {
    const keep = rows.filter(f);
    for (const [src, q] of [['dataset', ds], ['table', tab]]) {
      try { const r = sql(`SELECT COUNT(*) AS n, SUM(r) AS s, MIN(z) AS lo FROM t WHERE ${w}`, { t: q }); const got = OPTS === 'async' ? await r.toRowsAsync() : r.toRows(); ok(got[0], { n: keep.length, s: keep.length ? sum(keep, 'r') : NaN, lo: mn(keep, 'z') }, `${label} ${src} SQL WHERE ${w}`); }
      catch (er) { err++; bad.push(`${label} ${src} SQL WHERE ${w} ERROR ${er.message.slice(0, 120)}`); }
    }
  }
  if (!skippedSome) { fail++; bad.push(`${label}: the dataset never skipped a chunk or block`); }
  ds.close(); tab.dispose();
}
// an empty dataset; a dataset written from a filtered view; reopened twice; written over
{
  const t = e.table({ a: Float64Array.from([1, 2, 3, NaN]), s: ['x', 'y', '', 'x'] });
  const p0 = join(dir, 'empty.vectro');
  try { e.writeDataset(p0, t.filter('a > 100')); const d0 = e.openDataset(p0); ok([d0.count(), d0.filter('a > 0').count(), d0.agg({ s: 'sum(a)', m: 'min(a)' }), d0.groupBy('s').agg({ n: 'count()' }).toRows()], [0, 0, { s: 0, m: NaN }, []], 'empty dataset'); d0.close(); } catch (er) { err++; bad.push('empty dataset ERROR ' + er.message.slice(0, 120)); }
  const p1 = join(dir, 'view.vectro');
  try { e.writeDataset(p1, t.filter("s == 'x'")); const d1 = e.openDataset(p1), d2 = e.openDataset(p1); ok([d1.toRows(), d2.count()], [[{ a: 1, s: 'x' }, { a: NaN, s: 'x' }], 2], 'dataset from a filtered view, opened twice'); d1.close(); d2.close(); } catch (er) { err++; bad.push('view dataset ERROR ' + er.message.slice(0, 120)); }
  try { e.writeDataset(p1, t); const d3 = e.openDataset(p1); ok(d3.count(), 4, 'a dataset written over'); d3.close(); } catch (er) { err++; bad.push('overwrite ERROR ' + er.message.slice(0, 120)); }
  // (Windows can't replace a file that is open: the plain error is the answer there)
  try { const d4 = e.openDataset(p1); try { e.writeDataset(p1, t.filter('a > 1')); ok([d4.count(), e.openDataset(p1).count()], [4, 2], 'written over while open'); } catch (er) { if (OPEN.test(er.message)) pass++; else throw er; } finally { d4.close(); } } catch (er) { err++; bad.push('overwrite while open: ' + er.message.slice(0, 120)); }
}
// a handle left open while its file is written over: the old answers or an error, never a mix
{
  const p = join(dir, 'over.vectro'), N = 100000;
  const mk = (k) => e.table({ a: Float64Array.from({ length: N }, (_, i) => i * k), r: Float64Array.from({ length: N }, (_, i) => (i * 7919 * k) % 1000) });
  e.writeDataset(p, mk(1), { rowsPerChunk: 10000 });
  const d = e.openDataset(p);
  const before = [d.filter('r > 500').agg({ s: 'sum(a)', n: 'count()' }), d.toRows(3)];
  let written = true; try { e.writeDataset(p, mk(3), { rowsPerChunk: 10000 }); } catch (er) { if (!OPEN.test(er.message)) throw er; written = false; } // (Windows: the plain error)
  let after; if (!written) { pass++; d.close(); } else try { after = [d.filter('r > 500').agg({ s: 'sum(a)', n: 'count()' }), d.toRows(3)]; } catch (er) { after = null; }
  if (written) { if (after === null) pass++; else ok(after, before, 'an open dataset whose file is written over gives its old answers (or an error)'); d.close(); }
}
let failures = fail + err, checks = pass + fail + err;
for (const b of bad.slice(0, 30)) console.log('FAIL ' + b);
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 zone-map checks passed`);
await e.close(); rmSync(dir, { recursive: true, force: true });
process.exit(failures ? 1 : 0);
