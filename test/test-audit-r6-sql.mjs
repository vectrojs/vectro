// Audit round 6 (run 1, SQL): edge cases in what 0.2.0 added, checked against a
// plain-JS nested-loop join (the answers were checked against DuckDB in
// vectro-qa-2026-11-07/run1). Outer joins ON a constant (ON TRUE, ON FALSE,
// ON 1 = 1, a condition AND FALSE) failed inside the engine; two comparisons
// with an expression on the left side (ON a.v < b.v AND b.v < a.v + 2) failed
// with "no column __jl0"; STDDEV / VARIANCE of a whole table whose squares
// overflow gave NaN instead of Infinity (GROUP BY gave Infinity). With a
// dataset on the left, a text key made by an expression (ON UPPER(a.s) =
// UPPER(b.s), ON a.s IS NOT DISTINCT FROM b.s) lost the matches of labels
// first seen after the first chunk (silently wrong answers).
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const str = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isFinite(x) ? String(x) : x)); // (Infinity and NaN apart)
const same = (a, b, msg) => { checks++; const x = str(a), y = str(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };
const fails = (f, msg) => { checks++; try { f(); failures++; console.log(`FAIL ${msg}: no error`); } catch (e) { if (/^vectro/.test(e.message)) return; failures++; console.log(`FAIL ${msg}: ${e.message}`); } };

const engine = new Engine();
const L = { id: [0, 1, 2, 3], v: [1, 2.5, NaN, -1] }, R = { id: [10, 11, 12], v: [2, 3, NaN] };
const a = engine.table({ id: Float64Array.from(L.id), v: Float64Array.from(L.v) });
const b = engine.table({ id: Float64Array.from(R.id), v: Float64Array.from(R.v) });
const empty = engine.table({ id: new Float64Array(0), v: new Float64Array(0) });
const T = { a, b, e: empty };

// nested loop: inner pairs, then the unmatched rows of the kept side(s)
function loop(lt, rt, how, test) {
  const out = [], seenR = new Set();
  for (let i = 0; i < lt.id.length; i++) {
    let hit = false;
    for (let j = 0; j < rt.id.length; j++) if (test(lt, i, rt, j)) { out.push([lt.id[i], rt.id[j]]); hit = true; seenR.add(j); }
    if (!hit && (how === 'LEFT' || how === 'FULL')) out.push([lt.id[i], null]);
  }
  if (how === 'RIGHT' || how === 'FULL') for (let j = 0; j < rt.id.length; j++) if (!seenR.has(j)) out.push([null, rt.id[j]]);
  return out;
}
const rows = (q) => sql(q, T).toRows().map((r) => [r.x, r.y].map((v) => (v == null || Number.isNaN(v) ? null : v)));
const sorted = (x) => x.map((r) => JSON.stringify(r)).sort();
const Edata = { id: [], v: [] };

// 1. outer joins ON a constant
const CONST = [['TRUE', () => true], ['FALSE', () => false], ['1 = 1', () => true], ['1 = 0', () => false], ['l.v < r.v AND FALSE', () => false], ['NULL', () => false]];
for (const how of ['JOIN', 'LEFT', 'RIGHT', 'FULL']) {
  for (const [on, f] of CONST) {
    for (const [ln, rn, lt, rt] of [['a', 'b', L, R], ['a', 'e', L, Edata], ['e', 'b', Edata, R]]) {
      const q = `SELECT l.id AS x, r.id AS y FROM ${ln} l ${how === 'JOIN' ? '' : how + ' '}JOIN ${rn} r ON ${on}`;
      let got;
      try { got = sorted(rows(q)); } catch (e) { got = e.message; }
      same(got, sorted(loop(lt, rt, how, f)), q);
    }
  }
}

// 2. two comparisons with an expression on one side
const lt2 = (x, y) => x < y;
const COND = [
  ['l.v < r.v AND r.v < l.v + 2', (l, i, r, j) => lt2(l.v[i], r.v[j]) && lt2(r.v[j], l.v[i] + 2)],
  ['l.v + 1 > r.v AND l.v * 2 <= r.v + 3', (l, i, r, j) => l.v[i] + 1 > r.v[j] && l.v[i] * 2 <= r.v[j] + 3],
  ['l.v - 1 < r.v AND l.v + 1 > r.v AND l.id < 3', (l, i, r, j) => l.v[i] - 1 < r.v[j] && l.v[i] + 1 > r.v[j] && l.id[i] < 3],
  ['l.v BETWEEN r.v - 1 AND r.v + 1', (l, i, r, j) => l.v[i] >= r.v[j] - 1 && l.v[i] <= r.v[j] + 1],
  ['l.v + 0 BETWEEN r.v - 1 AND r.v', (l, i, r, j) => l.v[i] >= r.v[j] - 1 && l.v[i] <= r.v[j]],
  ['r.v BETWEEN l.v - 1 AND l.v + 1', (l, i, r, j) => r.v[j] >= l.v[i] - 1 && r.v[j] <= l.v[i] + 1], // (a band join, as users write it)
  ['l.v - 1 <= r.v AND r.v <= l.v + 1', (l, i, r, j) => l.v[i] - 1 <= r.v[j] && r.v[j] <= l.v[i] + 1],
];
for (const how of ['JOIN', 'LEFT', 'RIGHT', 'FULL']) {
  for (const [on, f] of COND) {
    const q = `SELECT l.id AS x, r.id AS y FROM a l ${how === 'JOIN' ? '' : how + ' '}JOIN b r ON ${on}`;
    let got;
    try { got = sorted(rows(q)); } catch (e) { got = e.message; }
    same(got, sorted(loop(L, R, how, f)), q);
  }
}
// self join, as found: a1.f < a2.f AND a2.f < a1.f + 2
{
  const q = 'SELECT l.id AS x, r.id AS y FROM a l LEFT JOIN a r ON l.v < r.v AND r.v < l.v + 2';
  let got;
  try { got = sorted(rows(q)); } catch (e) { got = e.message; }
  same(got, sorted(loop(L, L, 'LEFT', (l, i, r, j) => l.v[i] < r.v[j] && r.v[j] < l.v[i] + 2)), q);
}

// 3. STDDEV / VARIANCE: squares that overflow are Infinity, with or without GROUP BY
{
  const t = engine.table({ g: ['a', 'a', 'a', 'b', 'b'], v: Float64Array.from([1e300, -1e300, 5, 1e200, 3e200]) });
  const r = sql('SELECT STDDEV(v) AS s, STDDEV_POP(v) AS p, VARIANCE(v) AS x, VAR_SAMP(v) AS y, VAR_POP(v) AS z FROM t', { t }).toRows()[0];
  same(Object.values(r), [Infinity, Infinity, Infinity, Infinity, Infinity], 'STDDEV of the whole table, squares overflow');
  const r2 = sql('SELECT STDDEV(v) AS a, VAR_POP(v) AS b, SUM(v * v) AS c, AVG(v) AS m FROM t', { t }).toRows()[0]; // (as found: SUM and AVG beside)
  same([r2.a, r2.b, r2.c], [Infinity, Infinity, Infinity], 'STDDEV beside SUM and AVG, squares overflow');
  const g = sql('SELECT g, STDDEV(v) AS s, VAR_POP(v) AS z FROM t GROUP BY g ORDER BY g', { t }).toRows();
  same(g.map((x) => [x.s, x.z]), [[Infinity, Infinity], [Infinity, Infinity]], 'STDDEV per group, squares overflow');
  const u = engine.table({ v: Float64Array.from([1, 2, 3, 4]) });
  same(sql('SELECT STDDEV_POP(v) AS s, VAR_SAMP(v) AS x FROM u', { u }).toRows()[0], { s: Math.sqrt(1.25), x: 5 / 3 }, 'STDDEV of 1..4 unchanged');
  const w = engine.table({ v: Float64Array.from([1, Infinity, 3]) });
  same(Number.isNaN(sql('SELECT STDDEV(v) AS s FROM w', { w }).toRows()[0].s), true, 'STDDEV with an infinite value is missing (NaN), as before');
}

// 5. BETWEEN two columns of the right table (the second bound is checked as the pairs are made), with
// missing and infinite bounds, every join kind
{
  const P = { id: [0, 1, 2, 3, 4, 5, 6], t: [5, -1, NaN, Infinity, 3, 3, -Infinity] };
  const W = { id: [10, 11, 12, 13, 14, 15], lo: [0, 3, NaN, -Infinity, 4, 6], hi: [5, 3, 9, 0, Infinity, 2] };
  const p = engine.table({ id: Float64Array.from(P.id), t: Float64Array.from(P.t) }), w = engine.table({ id: Float64Array.from(W.id), lo: Float64Array.from(W.lo), hi: Float64Array.from(W.hi) });
  const ONS = [['l.t BETWEEN r.lo AND r.hi', (x, l, h) => x >= l && x <= h], ['l.t >= r.lo AND l.t < r.hi', (x, l, h) => x >= l && x < h], ['l.t > r.lo AND l.t <= r.hi', (x, l, h) => x > l && x <= h],
    ['r.lo <= l.t AND r.hi >= l.t', (x, l, h) => l <= x && h >= x], ['l.t <= r.hi AND l.t >= r.lo', (x, l, h) => x <= h && x >= l], ['l.t NOT BETWEEN r.lo AND r.hi', (x, l, h) => x < l || x > h],
    ['l.t >= r.lo AND l.t <= r.hi AND l.id < 5', (x, l, h, i) => x >= l && x <= h && i < 5]];
  for (const how of ['JOIN', 'LEFT', 'RIGHT', 'FULL']) {
    for (const [on, f] of ONS) {
      const q = `SELECT l.id AS x, r.id AS y FROM p l ${how === 'JOIN' ? '' : how + ' '}JOIN w r ON ${on}`;
      let got;
      try { got = sorted(sql(q, { p, w }).toRows().map((r) => [r.x, r.y].map((v) => (v == null || Number.isNaN(v) ? null : v)))); } catch (e) { got = e.message; }
      same(got, sorted(loop(P, W, how, (l, i, r, j) => f(l.t[i], r.lo[j], r.hi[j], l.id[i]))), q);
    }
  }
}

// 4. a dataset on the left, text keys made by an expression: labels first seen in a later chunk
if (engine.writeDataset) {
  const dir = mkdtempSync(join(tmpdir(), 'r6sql-'));
  try {
    const n = 60, ids = Float64Array.from({ length: n }, (_, i) => i), labels = Array.from({ length: n }, (_, i) => (i % 9 === 4 ? '' : `k${i % 23}`));
    const lt = engine.table({ id: ids, s: labels });
    const rt = engine.table({ rid: Float64Array.from({ length: 30 }, (_, i) => 100 + i), s: Array.from({ length: 30 }, (_, i) => (i === 29 ? '' : `K${i % 25}`)) });
    engine.writeDataset(join(dir, 'l.vectro'), lt, { rowsPerChunk: 7 });
    const ld = engine.openDataset(join(dir, 'l.vectro'));
    for (const on of ['UPPER(l.s) = r.s', 'UPPER(l.s) = UPPER(r.s)', 'LOWER(r.s) = l.s', 'LOWER(r.s) IS NOT DISTINCT FROM l.s', "COALESCE(l.s, 'none') = COALESCE(LOWER(r.s), 'none')"]) {
      for (const how of ['JOIN', 'LEFT JOIN']) {
        const q = `SELECT l.id AS x, r.rid AS y FROM l ${how} r ON ${on}`;
        let got;
        try { got = sorted(sql(q, { l: ld, r: rt }).toRows().map((x) => [x.x, Number.isNaN(x.y) ? null : x.y])); } catch (e) { got = e.message; }
        same(got, sorted(sql(q, { l: lt, r: rt }).toRows().map((x) => [x.x, Number.isNaN(x.y) ? null : x.y])), `dataset: ${q}`);
      }
    }
    const want = sql('SELECT COUNT(*) AS n FROM l JOIN r ON UPPER(l.s) = r.s', { l: lt, r: rt }).toRows()[0].n;
    same(want, 64, 'UPPER key on the table: 64 pairs');
    ld.close?.();
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

fails(() => sql('SELECT l.id AS x FROM a l LEFT JOIN b r ON l.v < r.v AND r.v < l.v + 2 AND UPPER(\'x\') = \'X\'', T).toRows(), 'outer join with a text function: a vectro error');

// R6-1.6 / 1.7 / 1.8: text = text in an outer ON, CAST of 0x / 0b / 0o text, LIMIT NULL / CAST / (SELECT ...)
{
  const s1 = engine.table({ k: [1, 2, 4], s: ['x', 'y', ''] }), s2 = engine.table({ k: [3, 2, 5], s: ['x', 'z', ''] }), nt = engine.table({ n: [2] });
  const v = (q) => sql(q, { s1, s2, nt, a }).toRows().map((r) => Object.values(r).map((x) => (x == null || Number.isNaN(x) ? null : x)));
  same(sorted(v('SELECT s1.k, s2.k AS bk FROM s1 FULL JOIN s2 ON s1.k = s2.k OR s1.s = s2.s')), sorted([[1, 3], [2, 2], [4, null], [null, 5]]), 'FULL JOIN ON keys OR text = text (missing text matches nothing)');
  same(v("SELECT CAST('0x10' AS DOUBLE) AS a, CAST('0b11' AS INTEGER) AS b, CAST(' 12 ' AS INT) AS c FROM nt"), [[null, null, 12]], "CAST of '0x10' / '0b11' is missing, as DuckDB refuses them");
  same(v('SELECT id FROM a ORDER BY id LIMIT NULL'), [[0], [1], [2], [3]], 'LIMIT NULL: no limit');
  same(v("SELECT id FROM a ORDER BY id LIMIT CAST('2' AS INT)"), [[0], [1]], 'LIMIT CAST(...)');
  same(v('SELECT id FROM a ORDER BY id LIMIT (SELECT n FROM nt) OFFSET (SELECT n - 1 FROM nt)'), [[1], [2]], 'LIMIT / OFFSET (SELECT ...)');
  fails(() => sql('SELECT id FROM a LIMIT (SELECT id FROM a)', { a }).toRows(), 'LIMIT (SELECT ...) with more than one row');
}
// R6-1.9: keys plus a range: only the pairs the range keeps (answers as a nested loop)
{
  const N = 3000, M = 60;
  const ka = engine.table({ g: Float64Array.from({ length: N }, (_, i) => i % 7), t: Float64Array.from({ length: N }, (_, i) => (i * 37) % 600) });
  const kb = engine.table({ g: Float64Array.from({ length: M }, (_, i) => i % 7), lo: Float64Array.from({ length: M }, (_, i) => i * 10), hi: Float64Array.from({ length: M }, (_, i) => (i % 9 === 0 ? NaN : i * 10 + 10)) });
  const A = ka.toColumns(), B = kb.toColumns();
  for (const how of ['JOIN', 'LEFT JOIN']) {
    let want = 0;
    for (let i = 0; i < N; i++) { let m = 0; for (let j = 0; j < M; j++) if (A.g[i] === B.g[j] && A.t[i] >= B.lo[j] && A.t[i] <= B.hi[j]) m++; want += m || (how === 'LEFT JOIN' ? 1 : 0); }
    same(sql(`SELECT COUNT(*) AS n FROM ka ${how} kb ON ka.g = kb.g AND ka.t BETWEEN kb.lo AND kb.hi`, { ka, kb }).toRows()[0].n, want, `${how}: keys and BETWEEN`);
  }
}
await engine.close();
console.log(`${checks - failures}/${checks} audit r6 SQL checks passed`);
if (failures) process.exit(1);
