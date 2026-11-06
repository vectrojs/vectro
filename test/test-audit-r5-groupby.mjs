// Audit round 5 (GROUP BY): COUNT, COUNT(DISTINCT), SUM, AVG, MIN, MAX per group
// on edge data (missing keys, -0 / 0, ±Infinity, 1e308, 5e-324, 2^53, unicode
// and 200-character labels, an empty table, one row, all missing), checked
// against a plain-JS group-by on tables and datasets (small chunks), sync and
// async. Answers were checked against DuckDB in vectro-qa-2026-11-05/groupby.
// Also: a dataset chunk whose sum is NaN (Infinity and -Infinity in it) still
// makes the total NaN; CAST('inf' AS DOUBLE) is Infinity.
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const show = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
const close = (x, y) => (typeof x === 'number' && typeof y === 'number' ? (Number.isNaN(x) && Number.isNaN(y)) || x === y || (Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= 1e-9 * Math.max(Math.abs(x), Math.abs(y))) : x === y);
const same = (a, b, msg) => {
  checks++;
  const ok = a.length === b.length && a.every((r, i) => r.length === b[i].length && r.every((v, j) => close(v, b[i][j])));
  if (!ok) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${show(a).slice(0, 300)}\n  want ${show(b).slice(0, 300)}`); }
};

let seed = 99;
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const G = ['a', 'b', '', 'A', 'é', 'é', '😀', 'İ', ' a', 'x'.repeat(200)];
const KEYS = [0, -0, 1, 2, NaN, Infinity, -Infinity, 1.5, 2 ** 53, 0.1 + 0.2, 0.3];
const VALS = [1, 2.5, -3, 0, -0, 100, 0.125, 1e6, NaN];
const XTRM = [1e308, 5e-324, Infinity, 2 ** 53, 1, NaN, 1e-300];
function mk(n) {
  const t = { g: [], h: [], k: [], k2: [], v: [], x: [] };
  for (let i = 0; i < n; i++) { t.g.push(pick(G)); t.h.push(pick(['p', 'q', ''])); t.k.push(pick(KEYS)); t.k2.push(Math.floor(rnd() * 5)); t.v.push(pick(VALS)); t.x.push(pick(XTRM)); }
  return t;
}
const T = { t: mk(3000), o: mk(1) };
T.n = mk(50);
for (const c of ['k', 'v', 'x']) T.n[c] = T.n[c].map(() => NaN);
T.n.h = T.n.h.map((_, i) => (i ? '' : 'only'));

// the reference: missing is NaN or ''; -0 is 0; a group of missing keys is one group
const miss = (v) => v === '' || Number.isNaN(v);
const keyOf = (v) => (miss(v) ? 'null' : typeof v === 'string' ? 's' + v : 'n' + (v === 0 ? 0 : v));
function reference(t, keys, col) {
  const n = t.g.length, groups = new Map();
  for (let i = 0; i < n; i++) {
    const id = keys.map((k) => keyOf(t[k][i])).join('|');
    if (!groups.has(id)) groups.set(id, { key: keys.map((k) => (miss(t[k][i]) ? null : t[k][i] === 0 ? 0 : t[k][i])), vals: [] });
    if (!miss(t[col][i])) groups.get(id).vals.push(t[col][i]);
  }
  if (!keys.length && !groups.size) groups.set('', { key: [], vals: [] });
  return [...groups.values()].map(({ key, vals }) => {
    const sum = vals.length ? vals.reduce((a, b) => a + b, 0) : NaN;
    return [...key, n ? 0 : 0, vals.length, new Set(vals.map((v) => (v === 0 ? 0 : v))).size, sum, vals.length ? sum / vals.length : NaN,
      vals.length ? Math.min(...vals) : NaN, vals.length ? Math.max(...vals) : NaN].map((v) => (Number.isNaN(v) ? null : v));
  });
}
const sortRows = (rows) => rows.sort((a, b) => { const x = show(a), y = show(b); return x < y ? -1 : x > y ? 1 : 0; });
const rowsOf = (rows) => rows.map((r) => Object.values(r).map((v) => (v === '' || Number.isNaN(v) ? null : Object.is(v, -0) ? 0 : v)));

const KEYSETS = [[], ['g'], ['h'], ['k'], ['k2'], ['g', 'h'], ['k', 'h'], ['h', 'k2']];
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-groupby-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const V = {};
  for (const [name, t] of Object.entries(T)) {
    const tab = e.table({ g: t.g, h: t.h, k: Float64Array.from(t.k), k2: Float64Array.from(t.k2), v: Float64Array.from(t.v), x: Float64Array.from(t.x) });
    if (source !== 'dataset') { V[name] = tab; continue; }
    const path = join(dir, `${name}.vectro`);
    e.writeDataset(path, tab, { rowsPerChunk: Math.max(3, Math.ceil(t.g.length / 7)) });
    V[name] = e.openDataset(path);
  }
  V.e = sql('SELECT * FROM t WHERE k2 < 0', V).collect();
  const E = Object.fromEntries(Object.keys(T.t).map((c) => [c, []]));
  const run = (q) => (source === 'async' ? sql(q, V).toRowsAsync() : sql(q, V).toRows());
  for (const [name, t] of [...Object.entries(T), ['e', E]]) for (const keys of KEYSETS) for (const col of ['v', 'x', 'k', 'k2']) {
    const sel = keys.map((k) => `${k} AS "${k}_"`).join(', ') + (keys.length ? ', ' : '');
    const q = `SELECT ${sel}0 AS zero, COUNT(${col}) AS c, COUNT(DISTINCT ${col}) AS cd, SUM(${col}) AS s, AVG(${col}) AS a, MIN(${col}) AS mn, MAX(${col}) AS mx FROM ${name}${keys.length ? ' GROUP BY ' + keys.join(', ') : ''}`;
    same(sortRows(rowsOf(await run(q))), sortRows(reference(t, keys, col)), `${tag} ${q}`);
  }
  // COUNT(*) per group and in all
  same(rowsOf(await run('SELECT COUNT(*) AS n FROM e')), [[0]], `${tag} COUNT(*) of an empty table`);
  same(rowsOf(await run('SELECT COUNT(*) AS n FROM n GROUP BY h ORDER BY h')), [[1], [49]], `${tag} COUNT(*) with a missing-label group`);
  same(rowsOf(await run('SELECT COUNT(*) AS n FROM (SELECT g, h FROM t GROUP BY g, h) s')), [[new Set(T.t.g.map((g, i) => keyOf(g) + '|' + keyOf(T.t.h[i]))).size]], `${tag} number of (g, h) groups`);
  await e.close();
}

// a dataset chunk holding Infinity and -Infinity sums to NaN, so the total is NaN, as on a table
{
  const e = new Engine();
  if (e.writeDataset) {
    const t = e.table({ k: Float64Array.from([Infinity, -Infinity, 1, 2]), g: ['a', 'a', 'a', 'a'] });
    const path = join(dir, 'inf.vectro');
    e.writeDataset(path, t, { rowsPerChunk: 2 });
    const d = e.openDataset(path);
    for (const q of ['SELECT SUM(k) AS s, AVG(k) AS a FROM x', 'SELECT g, SUM(k) AS s, AVG(k) AS a FROM x GROUP BY g']) {
      same(rowsOf(sql(q, { x: d }).toRows()), rowsOf(sql(q, { x: t }).toRows()), `dataset vs table: ${q}`);
    }
    same([[d.groupBy('g').agg({ s: 'sum(k)' }).toRows()[0].s]], [[NaN]], 'dataset groupBy sum with a NaN chunk');
  }
  same(rowsOf(sql("SELECT CAST('inf' AS DOUBLE) AS a, CAST('-inf' AS DOUBLE) AS b, CAST('Infinity' AS DOUBLE) AS c", { one: e.table({ x: [1] }) }).toRows()), [[Infinity, -Infinity, Infinity]], "CAST('inf' AS DOUBLE)");
  // G3: FILTER (WHERE ...) on aggregates; an unknown aggregate says so
  const f = e.table({ g: ['a', 'a', 'b', 'b'], v: [1, 2, 3, 4] });
  same(rowsOf(sql('SELECT g, COUNT(*) FILTER (WHERE v > 1) AS c, SUM(v) FILTER (WHERE v < 4) AS s, MEDIAN(v) FILTER (WHERE v > 1) AS m FROM f GROUP BY g ORDER BY g', { f }).toRows()), [['a', 1, 3, 2], ['b', 2, 3, 3.5]], 'aggregates with FILTER (WHERE ...)');
  same(rowsOf(sql('SELECT COUNT(*) FILTER (WHERE v > 9) AS c FROM f', { f }).toRows()), [[0]], 'COUNT(*) FILTER that keeps nothing is 0');
  let msg = ''; try { sql('SELECT g, STDDEV(v) AS s FROM f GROUP BY g', { f }).toRows(); } catch (err) { msg = err.message; }
  same([[/unknown function STDDEV\(\)/.test(msg)]], [[true]], 'an unknown aggregate in a GROUP BY query says it is unknown');
  // S2: NULL in a text IN list
  same(rowsOf(sql("SELECT g FROM f WHERE g IN ('a', NULL)", { f }).toRows()), [['a'], ['a']], "text IN ('a', NULL)");
  same(rowsOf(sql("SELECT g FROM f WHERE g NOT IN ('a', NULL)", { f }).toRows()), [], "text NOT IN ('a', NULL) keeps nothing");
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 group-by checks passed`);
process.exit(failures ? 1 : 0);
