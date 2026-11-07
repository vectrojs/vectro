// Audit round 6 (run 1, SQL): edge cases in what 0.2.0 added, checked against a
// plain-JS nested-loop join (the answers were checked against DuckDB in
// vectro-qa-2026-11-07/run1). Outer joins ON a constant (ON TRUE, ON FALSE,
// ON 1 = 1, a condition AND FALSE) failed inside the engine; two comparisons
// with an expression on the left side (ON a.v < b.v AND b.v < a.v + 2) failed
// with "no column __jl0"; STDDEV / VARIANCE of a whole table whose squares
// overflow gave NaN instead of Infinity (GROUP BY gave Infinity).
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';

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

fails(() => sql('SELECT l.id AS x FROM a l LEFT JOIN b r ON l.v < r.v AND r.v < l.v + 2 AND UPPER(\'x\') = \'X\'', T).toRows(), 'outer join with a text function: a vectro error');

await engine.close();
console.log(`${checks - failures}/${checks} audit r6 SQL checks passed`);
if (failures) process.exit(1);
