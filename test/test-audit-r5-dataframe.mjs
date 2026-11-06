// Audit round 5 (dataframe API): random chains of 1-6 steps (filter, derive,
// select, sort, top, head / limit, slice, join, groupBy().agg, agg) on the edge
// data kit (NaN, -0, ±Infinity, 1e308, 2^53 + 2, int32 limits, '' and emoji text,
// dates before 1970, missing keys, repeated join keys), run eagerly and lazily on
// a table, a compressed table and a dataset, checked against a plain-JS model
// written from the docs (vectro-qa-2026-11-05/df ran 400 chains in every mode).
// Also: on a dataset, a derive / select whose column isn't used later followed by
// limit or top lost columns the query still needed; a join keeps the left
// table's columns first in lazy queries too; a dataset's top() by a renamed text
// column from a join; group order (numbers ascending with NaN last, text in
// dictionary order).
import { Engine, MODE } from './test-mode.mjs';
import { mkdtempSync, rmSync } from 'node:fs'; import { tmpdir } from 'node:os'; import { join } from 'node:path';
const OPTS = process.env.OPTS ?? 'default';
let seed = 99;
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const NUM = [0, -0, 1, -1, 2, 2.5, -2.5, 3, 7, NaN, NaN, Infinity, -Infinity, 1e308, -1e308, 5e-324, 2 ** 53 + 2, 2147483647, -2147483648, 0.1, 123.456];
const VAL = [0, -0, 1, 2, 3.5, -4, 10, 100, NaN, 0.25, Infinity, -Infinity, 1e6];
const TXT = ['a', 'b', 'é', 'e\u0301', '😀', '', 'ab', 'B', 'İ', 'z'];
const DAY = 86400e3;
function mk(n) {
  const t = { id: [], n1: [], n2: [], v: [], s: [], d: [] };
  for (let i = 0; i < n; i++) { t.id.push(i); t.n1.push(pick(NUM)); t.n2.push(rnd() < 0.1 ? NaN : Math.floor(rnd() * 6)); t.v.push(pick(VAL)); t.s.push(pick(TXT)); t.d.push(rnd() < 0.1 ? NaN : Date.UTC(1950 + Math.floor(rnd() * 90), Math.floor(rnd() * 12), 1 + Math.floor(rnd() * 28))); }
  return t;
}
const SIZES = { k1: 300, e0: 0, e1: 1, e2: 2, big: 20000 };
const DATA = Object.fromEntries(Object.entries(SIZES).map(([k, n]) => [k, mk(n)]));
const LOOK = { n2: [0, 1, 1, 3, NaN, 5, 9], s: ['a', 'é', 'é', '', '😀', 'q', 'B'], w: [10, 11, 12, 13, 14, 15, 16] }; // repeated keys, a missing key, keys not in the left table

// ---- the JS model: rows are objects; numbers NaN = missing; text '' = missing; dates are ms (NaN missing)
const miss = (x) => x === '' || (typeof x === 'number' && Number.isNaN(x));
const tru = (x) => x !== 0; // a value used as a condition: true unless 0 (NaN counts as true)
const b = (x) => (x ? 1 : 0);
const yearOf = (ms) => (Number.isNaN(ms) ? NaN : new Date(ms).getUTCFullYear());
const monthStart = (ms) => { if (Number.isNaN(ms)) return NaN; const d = new Date(ms); return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1); };
const likeRe = (p) => new RegExp('^' + p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '[\\s\\S]*').replace(/_/g, '[\\s\\S]') + '$', 'u');
const FILTERS = [
  ['n1 > 0', (r) => r.n1 > 0], ['n1 <= 2', (r) => r.n1 <= 2], ['n2 == 3', (r) => r.n2 === 3], ['n2 != 3', (r) => r.n2 !== 3], ['!(n1 > 0)', (r) => !(r.n1 > 0)],
  ['isMissing(n1)', (r) => Number.isNaN(r.n1)], ['!isMissing(n1) && n1 < 1e300', (r) => !Number.isNaN(r.n1) && r.n1 < 1e300], ["s == 'a'", (r) => r.s === 'a'], ["s != 'a'", (r) => r.s !== 'a'],
  ["in(s, 'a', 'é', '😀')", (r) => ['a', 'é', '😀'].includes(r.s)], ["startsWith(s, 'a')", (r) => r.s.startsWith('a')], ["like(s, '%b%')", (r) => likeRe('%b%').test(r.s)], ["ilike(s, 'b')", (r) => r.s.toLowerCase() === 'b'],
  ['isMissing(s)', (r) => r.s === ''], ['n1 > n2 || n2 == 0', (r) => r.n1 > r.n2 || r.n2 === 0], ["d > date('2000-01-01')", (r) => r.d > Date.UTC(2000, 0, 1)], ['year(d) == 1999', (r) => yearOf(r.d) === 1999],
  ['abs(n1) < 10', (r) => Math.abs(r.n1) < 10], ['floor(n1) == n1', (r) => Math.floor(r.n1) === r.n1], ['n2', (r) => tru(r.n2)], ['v * 0 == 0', (r) => r.v * 0 === 0], ['v != v', (r) => r.v !== r.v],
  ['n2 >= lo', (r) => r.n2 >= 2, { lo: 2 }], ['coalesce(v, 0) > 1', (r) => (miss(r.v) ? 0 : r.v) > 1], ["s == 'a' || n2 == 1", (r) => r.s === 'a' || r.n2 === 1],
];
const DERIVES = [
  ['n1 * 2 + n2', (r) => r.n1 * 2 + r.n2], ['coalesce(n1, n2, 0)', (r) => (!miss(r.n1) ? r.n1 : !miss(r.n2) ? r.n2 : 0)], ['abs(v) - floor(n2 / 2)', (r) => Math.abs(r.v) - Math.floor(r.n2 / 2)],
  ['n2 > 2 ? v : -v', (r) => (r.n2 > 2 ? r.v : -r.v)], ['max(n2, 3)', (r) => Math.max(r.n2, 3)], ['min(v, n2)', (r) => Math.min(r.v, r.n2)], ['sqrt(n2)', (r) => Math.sqrt(r.n2)], ['n1 > 0', (r) => b(r.n1 > 0)],
  ['v / n2', (r) => r.v / r.n2], ['year(d)', (r) => yearOf(r.d)], ['isMissing(s) + isMissing(v)', (r) => b(r.s === '') + b(Number.isNaN(r.v))], ['n2 && v', (r) => b(tru(r.n2) && tru(r.v))],
  ['!n2', (r) => b(!tru(r.n2))], ["s == 'é' ? 1 : 2", (r) => (r.s === 'é' ? 1 : 2)], ['n2 * k', (r) => r.n2 * 1.5, { k: 1.5 }], ['ceil(v)', (r) => Math.ceil(r.v)],
];
const cp = (a, z) => { const x = [...a].map((c) => c.codePointAt(0)), y = [...z].map((c) => c.codePointAt(0)); for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i]; return x.length - y.length; };
const sortRows = (rows, by) => { const ks = by.map((k) => ({ c: k.replace(/^-/, ''), desc: k[0] === '-' })); return rows.map((r, i) => [r, i]).sort(([a, i], [z, j]) => { for (const { c, desc } of ks) { const x = a[c], y = z[c]; if (miss(x) || miss(y)) { if (miss(x) && miss(y)) continue; return miss(x) ? 1 : -1; } const o = typeof x === 'string' ? cp(x, y) : x === y ? 0 : x < y ? -1 : 1; if (o) return desc ? -o : o; } return i - j; }).map(([r]) => r); };
const AGGS = { c: ['count()', (rs) => rs.length], sv: ['sum(v)', (rs) => rs.reduce((s, r) => (miss(r.v) ? s : s + r.v), 0)], mv: ['mean(v)', (rs) => { const p = rs.filter((r) => !miss(r.v)); return p.length ? p.reduce((s, r) => s + r.v, 0) / p.length : NaN; }],
  lo: ['min(n1)', (rs) => { const p = rs.filter((r) => !miss(r.n1)).map((r) => r.n1); return p.length ? Math.min(...p) : NaN; }], hi: ['max(n1)', (rs) => { const p = rs.filter((r) => !miss(r.n1)).map((r) => r.n1); return p.length ? Math.max(...p) : NaN; }],
  cx: ['count(n1)', (rs) => rs.filter((r) => !miss(r.n1)).length], cc: ['count(n1 > 0)', (rs) => rs.filter((r) => r.n1 > 0).length], ds: ['countDistinct(s)', (rs) => new Set(rs.map((r) => r.s).filter((s) => s !== '')).size],
  dn: ['countDistinct(n1)', (rs) => new Set(rs.map((r) => r.n1).filter((x) => !miss(x)).map((x) => (x === 0 ? 0 : x))).size], se: ['sum(v * (n2 > 1))', (rs) => rs.reduce((s, r) => { const x = r.v * b(r.n2 > 1); return miss(x) ? s : s + x; }, 0)] };

// a random chain: steps applied to the model and recorded for Vectro
function chain() {
  const steps = [], len = 1 + Math.floor(rnd() * 6);
  let derived = 0, cols = ['id', 'n1', 'n2', 'v', 's', 'd'], joined = false;
  for (let i = 0; i < len; i++) {
    const kind = pick(['filter', 'filter', 'derive', 'derive', 'sort', 'top', 'head', 'slice', 'select', 'join', 'group', 'agg']);
    if (kind === 'filter') { const f = pick(FILTERS); if (f[0].split(/[^a-z0-9]/).some((w) => ['n1', 'n2', 'v', 's', 'd'].includes(w) && !cols.includes(w))) continue; steps.push({ kind, f }); }
    else if (kind === 'derive') { const f = pick(DERIVES); if (f[0].split(/[^a-z0-9]/).some((w) => ['n1', 'n2', 'v', 's', 'd'].includes(w) && !cols.includes(w))) continue; const name = `x${derived++}`; steps.push({ kind, name, f }); cols.push(name); }
    else if (kind === 'sort' || kind === 'top') { const by = Array.from({ length: 1 + Math.floor(rnd() * 3) }, () => (rnd() < 0.5 ? '-' : '') + pick(cols)); steps.push({ kind, by: [...new Set(by)], k: pick([0, 1, 3, 10, 500]) }); }
    else if (kind === 'head') steps.push({ kind, k: pick([0, 1, 5, 200]) });
    else if (kind === 'slice') steps.push({ kind, a: pick([0, 1, 3, 50]), z: pick([undefined, 2, 10, 400]) });
    else if (kind === 'select') { const keep = cols.filter((c) => c === 'id' || rnd() < 0.7); steps.push({ kind, keep: [...keep] }); cols = [...keep]; }
    else if (kind === 'join' && !joined && cols.includes('n2') && cols.includes('s')) { const on = pick(['n2', 's']); steps.push({ kind, on, how: pick(['inner', 'left']) }); cols = [...cols, ...(on === 'n2' ? ['s_right', 'w'] : ['n2_right', 'w'])]; joined = true; }
    else if (kind === 'group' || kind === 'agg') {
      const keys = kind === 'group' ? pick([['n2'], ['s'], ['n2', 's'], ['d']]).filter((k) => cols.includes(k)) : [];
      if (kind === 'group' && !keys.length) continue;
      const spec = Object.fromEntries(Object.entries(AGGS).filter(([, [ex]]) => !ex.match(/\b(n1|n2|v|s)\b/g)?.some((w) => !cols.includes(w)) && rnd() < 0.5));
      if (!Object.keys(spec).length) spec.c = AGGS.c;
      steps.push({ kind, keys, spec }); break;
    }
  }
  return steps;
}
function model(rows, steps) {
  for (const st of steps) {
    if (st.kind === 'filter') rows = rows.filter(st.f[1]);
    else if (st.kind === 'derive') rows = rows.map((r) => ({ ...r, [st.name]: st.f[1](r) }));
    else if (st.kind === 'sort') rows = sortRows(rows, st.by);
    else if (st.kind === 'top') rows = sortRows(rows, st.by).slice(0, st.k);
    else if (st.kind === 'head') rows = rows.slice(0, st.k);
    else if (st.kind === 'slice') rows = rows.slice(st.a, st.z);
    else if (st.kind === 'select') rows = rows.map((r) => Object.fromEntries(st.keep.map((c) => [c, r[c]])));
    else if (st.kind === 'join') {
      const out = [], other = st.on === 'n2' ? 's' : 'n2';
      for (const r of rows) {
        const m = LOOK[st.on].map((k, j) => j).filter((j) => !miss(r[st.on]) && !miss(LOOK[st.on][j]) && (LOOK[st.on][j] === r[st.on] || (LOOK[st.on][j] === 0 && r[st.on] === 0)));
        for (const j of m) out.push({ ...r, [`${other}_right`]: LOOK[other][j], w: LOOK.w[j] });
        if (!m.length && st.how === 'left') out.push({ ...r, [`${other}_right`]: other === 's' ? '' : NaN, w: NaN });
      }
      rows = out;
    } else if (st.kind === 'group') {
      const groups = new Map();
      for (const r of rows) { const k = JSON.stringify(st.keys.map((c) => (miss(r[c]) ? null : r[c] === 0 ? 0 : r[c]))); if (!groups.has(k)) groups.set(k, []); groups.get(k).push(r); }
      rows = [...groups.values()].map((rs) => ({ ...Object.fromEntries(st.keys.map((c) => [c, rs[0][c] === 0 ? 0 : rs[0][c]])), ...Object.fromEntries(Object.entries(st.spec).map(([n, [, f]]) => [n, f(rs)])) }));
    } else if (st.kind === 'agg') rows = [Object.fromEntries(Object.entries(st.spec).map(([n, [, f]]) => [n, f(rows)]))];
  }
  return rows;
}
const vjoinT = {};
function vectro(src, steps, lazy, look) {
  let q = lazy ? src.lazy() : src;
  for (const st of steps) {
    if (st.kind === 'filter') q = q.filter(st.f[0], st.f[2]);
    else if (st.kind === 'derive') q = q.derive(st.name, st.f[0], st.f[2]);
    else if (st.kind === 'sort') q = q.sort(st.by.length > 1 ? st.by : st.by[0]);
    else if (st.kind === 'top') q = q.top(st.k, st.by.length > 1 ? st.by : st.by[0]);
    else if (st.kind === 'head') q = lazy || !q.head ? q.limit(st.k) : q.head(st.k); // (a dataset has limit, not head / slice)
    else if (st.kind === 'slice') q = lazy || !q.slice ? q.limit(st.z === undefined ? 1e12 : Math.max(0, st.z - st.a), st.a) : q.slice(st.a, st.z);
    else if (st.kind === 'select') q = q.select(...st.keep);
    else if (st.kind === 'join') q = q.join(look[st.on], { on: st.on, how: st.how });
    else if (st.kind === 'group') q = q.groupBy(st.keys.length > 1 ? st.keys : st.keys[0]).agg(Object.fromEntries(Object.entries(st.spec).map(([n, [ex]]) => [n, ex])));
    else if (st.kind === 'agg') return { agg: q.agg(Object.fromEntries(Object.entries(st.spec).map(([n, [ex]]) => [n, ex]))) };
  }
  return { q };
}
const norm = (v) => (v instanceof Date ? (Number.isNaN(+v) ? null : +v) : v === '' || v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)) ? null : Object.is(v, -0) ? 0 : v);
const close = (x, y) => (typeof x === 'number' && typeof y === 'number' ? x === y || (Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= 1e-9 * Math.max(1, Math.abs(x), Math.abs(y))) : x === y);
const rowKey = (r, cols) => JSON.stringify(cols.map((c) => { const v = norm(r[c]); return typeof v === 'number' ? +v.toPrecision(8) : v; }));
const J = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
function sameRows(got, want, ordered) {
  if (got.length !== want.length) return `rows ${got.length} vs ${want.length}`;
  if (!want.length) return null;
  const cols = Object.keys(want[0]), gcols = Object.keys(got[0] ?? {});
  if (cols.some((c) => !gcols.includes(c)) || gcols.length !== cols.length) return `columns ${gcols} vs ${cols}`;
  let g = got, w = want;
  if (!ordered) { g = [...got].sort((a, z) => (rowKey(a, cols) < rowKey(z, cols) ? -1 : 1)); w = [...want].sort((a, z) => (rowKey(a, cols) < rowKey(z, cols) ? -1 : 1)); }
  for (let i = 0; i < w.length; i++) for (const c of cols) if (!close(norm(g[i][c]), norm(w[i][c]))) return `row ${i} col ${c}: ${J(norm(g[i][c]))} vs ${J(norm(w[i][c]))} (${J(Object.fromEntries(cols.map((k) => [k, norm(g[i][k])])))} vs ${J(Object.fromEntries(cols.map((k) => [k, norm(w[i][k])])))})`;
  return null;
}
const describe = (steps) => steps.map((s) => s.kind === 'filter' ? `filter(${s.f[0]})` : s.kind === 'derive' ? `derive(${s.name}, ${s.f[0]})` : s.kind === 'sort' ? `sort(${s.by})` : s.kind === 'top' ? `top(${s.k}, ${s.by})` : s.kind === 'head' ? `head(${s.k})` : s.kind === 'slice' ? `slice(${s.a}, ${s.z})` : s.kind === 'select' ? `select(${s.keep})` : s.kind === 'join' ? `join(${s.on}, ${s.how})` : s.kind === 'group' ? `groupBy(${s.keys}).agg(${Object.values(s.spec).map((x) => x[0])})` : `agg(${Object.values(s.spec).map((x) => x[0])})`).join('.');

const e = new Engine(OPTS === 'serial' ? { threads: 1 } : {});
const dir = mkdtempSync(join(tmpdir(), 'r5df-'));
const SRC = {};
for (const [name, t] of Object.entries(DATA)) {
  const cols = { id: Float64Array.from(t.id), n1: Float64Array.from(t.n1), n2: Float64Array.from(t.n2), v: Float64Array.from(t.v), s: t.s, d: t.d.map((x) => new Date(x)) };
  SRC[`${name}/table`] = e.table(cols);
  SRC[`${name}/packed`] = e.table(cols, { compress: true });
  if (!t.id.length) { SRC[`${name}/table`] = SRC['k1/table'].filter('id < 0').collect(); SRC[`${name}/packed`] = SRC['k1/packed'].filter('id < 0').collect(); } // (an empty text column from [] is a number column: J3)
  if (e.writeDataset && t.id.length) { const p = join(dir, name + '.vectro'); e.writeDataset(p, SRC[`${name}/table`], { rowsPerChunk: Math.max(2, Math.ceil(t.id.length / 7)) }); SRC[`${name}/dataset`] = e.openDataset(p); }
}
const look = { n2: e.table({ n2: Float64Array.from(LOOK.n2), s: LOOK.s, w: Float64Array.from(LOOK.w) }), s: e.table({ s: LOOK.s, n2: Float64Array.from(LOOK.n2), w: Float64Array.from(LOOK.w) }) };
const ROWS = Object.fromEntries(Object.entries(DATA).map(([k, t]) => [k, t.id.map((_, i) => ({ id: t.id[i], n1: t.n1[i], n2: t.n2[i], v: t.v[i], s: t.s[i], d: t.d[i] }))]));
const N = 120;
let pass = 0, fail = 0, err = 0; const bad = new Map();
const note = (k, msg) => { if (!bad.has(k)) bad.set(k, []); bad.get(k).push(msg); };
for (let c = 0; c < N; c++) {
  const steps = chain(), d = describe(steps);
  for (const size of Object.keys(DATA)) {
    if (size === 'big' && c % 10) continue;
    const want = model(ROWS[size], steps);
    const ordered = !steps.some((s) => s.kind === 'group');
    for (const kind of ['table', 'packed', 'dataset']) for (const lazy of [false, true]) {
      const src = SRC[`${size}/${kind}`]; if (!src) continue;
      const tag = `${kind}${lazy ? ' lazy' : ''}`;
      let got;
      try {
        const r = vectro(src, steps, lazy, look);
        if (r.agg) got = [OPTS === 'async' && r.agg.then ? await r.agg : r.agg];
        else got = OPTS === 'async' ? await r.q.toRowsAsync() : r.q.toRows();
        if (got[0] && typeof got[0].then === 'function') got = [await got[0]];
      } catch (er) { err++; note(`${tag}: ERROR ${er.message.split('\n')[0].slice(0, 120)}`, `${size}: ${d}`); continue; }
      const why = sameRows(got, want, ordered);
      if (!why) pass++; else { fail++; note(`${tag}: ${why.replace(/[-\d.e+]+|"[^"]*"/g, '#').slice(0, 60)}`, `${size}: ${d}  ->  ${why.slice(0, 300)}`); }
    }
  }
}
let failures = fail + err, checks = pass + fail + err;
for (const [k, v] of [...bad].sort((a, z) => z[1].length - a[1].length).slice(0, 15)) { console.log(`FAIL ${v.length} x ${k}`); for (const x of v.slice(0, 2)) console.log('    ' + x.slice(0, 400)); }
const same = (got, want, msg) => { checks++; const g = JSON.stringify(got), w = JSON.stringify(want); if (g !== w) { failures++; console.log(`FAIL ${msg}\n  got  ${g.slice(0, 300)}\n  want ${w.slice(0, 300)}`); } };
const tryRun = (f) => { try { return f(); } catch (er) { return 'error: ' + er.message.slice(0, 100); } };
{
  const t = e.table({ id: [1, 2, 3, 4], a: [5, 6, 7, 8], b: [1, 0, 1, 0], s: ['x', 'y', 'x', 'z'] });
  const lk = e.table({ b: [0, 1], s: ['zero', 'one'] });
  const srcs = [['table lazy', t.lazy()]];
  if (e.writeDataset) { const p = join(dir, 'small.vectro'); e.writeDataset(p, t, { rowsPerChunk: 2 }); srcs.push(['dataset', e.openDataset(p)]); }
  for (const [n, q] of srcs) {
    same(tryRun(() => q.derive('x', 'a + b').limit(2).agg({ s: 'sum(a)' })), { s: 11 }, `${n}: derive(unused).limit(2).agg(sum(a))`);
    same(tryRun(() => q.select('id', 'a', 'b').limit(2).select('id', 'a').toRows()), [{ id: 1, a: 5 }, { id: 2, a: 6 }], `${n}: select().limit().select()`);
    same(tryRun(() => q.select('id', 'a').top(1, '-a').agg({ n: 'count()' })), { n: 1 }, `${n}: select().top().agg(count())`);
    same(tryRun(() => Object.keys(q.join(lk, { on: 'b' }).toRows()[0])), ['id', 'a', 'b', 's', 's_right'], `${n}: join keeps the left table's columns first, in order`);
    same(tryRun(() => q.join(lk, { on: 'b' }).top(2, 's_right').toRows().map((r) => r.id)), [1, 3], `${n}: join(...).top(2, renamed text column)`);
  }
  // group order: numbers ascending, -0 with 0, NaN last; text in dictionary (first seen) order
  const g = e.table({ k: [3, NaN, -0, 1, 0, 3], s: ['US', 'EU', 'APAC', 'EU', 'US', 'EU'], v: [1, 2, 3, 4, 5, 6] });
  same(g.groupBy('k').agg({ n: 'count()' }).toRows().map((r) => [Number.isNaN(r.k) ? 'NaN' : r.k, r.n]), [[0, 2], [1, 1], [3, 2], ['NaN', 1]], 'number groups ascending, -0 with 0, NaN last');
  same(g.groupBy('s').agg({ v: 'sum(v)' }).toRows().map((r) => r.s), ['US', 'EU', 'APAC'], 'text groups in dictionary order');
  same(g.filter('v > 100').groupBy('s').agg({ n: 'count()', v: 'sum(v)' }).toRows(), [], 'groupBy of an empty filter has no groups');
  same(g.filter('v > 100').agg({ n: 'count()', v: 'sum(v)', m: 'mean(v)' }), { n: 0, v: 0, m: NaN }, 'agg of an empty filter: count 0, sum 0, mean NaN');
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 dataframe checks passed`);
await e.close(); rmSync(dir, { recursive: true, force: true });
process.exit(failures ? 1 : 0);
