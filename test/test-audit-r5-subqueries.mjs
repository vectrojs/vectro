// Audit round 5 (subqueries and CTEs): IN / NOT IN / EXISTS / NOT EXISTS,
// uncorrelated and correlated scalar subqueries (COUNT is 0, not NULL, where
// nothing matches), CTEs used twice, on edge data (NULL keys in the subquery,
// duplicate keys, -0 / 0, an empty side, text keys), checked against plain JS
// on tables and datasets, sync and async. The answers were checked against
// DuckDB in vectro-qa-2026-11-05/subq. Also: a scalar subquery with several
// rows is an error (it was a hidden cross join); NULL in a text IN list.
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const show = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
const close = (x, y) => (typeof x === 'number' && typeof y === 'number' ? x === y || (Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= 1e-9 * Math.max(Math.abs(x), Math.abs(y))) : x === y);
const same = (a, b, msg) => {
  checks++;
  if (!(a.length === b.length && a.every((v, i) => close(v, b[i])))) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${show(a).slice(0, 300)}\n  want ${show(b).slice(0, 300)}`); }
};

let seed = 3;
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
function mk(n, keys) {
  const t = { id: [], k: [], s: [], v: [] };
  for (let i = 0; i < n; i++) { t.id.push(i); t.k.push(pick(keys)); t.s.push(pick(['a', 'b', '', 'é', '😀'])); t.v.push(pick([1, 2.5, -3, 0, 100, NaN])); }
  return t;
}
const T = { t: mk(200, [0, -0, 1, 2, 3, NaN, 7]), u: mk(80, [0, 1, 1, 2, NaN, 9]), w: mk(40, [1, 2, 3]) };
T.w.s = T.w.s.map((s) => s || 'a');
const E = { id: [], k: [], s: [], v: [] };

const miss = (x) => x === '' || Number.isNaN(x);
const ids = (rows) => rows.map((r) => r.id).sort((a, b) => a - b);
const where = (t, f) => t.id.filter((_, i) => f(i));
// x IN (list): true if some present value equals; NULL (unknown) if no match but the list has a missing value or x is missing
const inList = (x, list) => (miss(x) ? (list.length ? null : false) : list.some((y) => !miss(y) && y === x) ? true : list.some(miss) ? null : false);
const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-subq-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const V = {};
  for (const [name, t] of Object.entries(T)) {
    const tab = e.table({ id: Float64Array.from(t.id), k: Float64Array.from(t.k), s: t.s, v: Float64Array.from(t.v) });
    if (source !== 'dataset') { V[name] = tab; continue; }
    const path = join(dir, `${name}.vectro`);
    e.writeDataset(path, tab, { rowsPerChunk: Math.max(3, Math.ceil(t.id.length / 7)) });
    V[name] = e.openDataset(path);
  }
  V.e = sql('SELECT * FROM t WHERE id < 0', V).collect();
  const D = { ...T, e: E };
  const run = async (q) => (source === 'async' ? sql(q, V).toRowsAsync() : sql(q, V).toRows());
  for (const [x, y] of [['t', 'u'], ['t', 'w'], ['t', 'e'], ['u', 't'], ['e', 'u']]) {
    const X = D[x], Y = D[y];
    for (const col of ['k', 's']) {
      same(ids(await run(`SELECT id FROM ${x} WHERE ${col} IN (SELECT ${col} FROM ${y})`)), where(X, (i) => inList(X[col][i], Y[col]) === true), `${tag} ${x}.${col} IN ${y}`);
      same(ids(await run(`SELECT id FROM ${x} WHERE ${col} NOT IN (SELECT ${col} FROM ${y})`)), where(X, (i) => inList(X[col][i], Y[col]) === false), `${tag} ${x}.${col} NOT IN ${y}`);
      const present = Y[col].filter((v) => !miss(v));
      same(ids(await run(`SELECT id FROM ${x} WHERE ${col} NOT IN (SELECT ${col} FROM ${y} WHERE ${col} IS NOT NULL)`)), where(X, (i) => inList(X[col][i], present) === false), `${tag} ${x}.${col} NOT IN ${y} (no NULLs)`);
      const has = (i) => Y[col].some((v) => !miss(v) && !miss(X[col][i]) && v === X[col][i]);
      same(ids(await run(`SELECT id FROM ${x} a WHERE EXISTS (SELECT 1 FROM ${y} b WHERE b.${col} = a.${col})`)), where(X, has), `${tag} ${x} EXISTS ${y} on ${col}`);
      same(ids(await run(`SELECT id FROM ${x} a WHERE NOT EXISTS (SELECT 1 FROM ${y} b WHERE b.${col} = a.${col})`)), where(X, (i) => !has(i)), `${tag} ${x} NOT EXISTS ${y} on ${col}`);
      // correlated COUNT and SUM
      const matches = (i) => Y.id.map((_, j) => j).filter((j) => !miss(Y[col][j]) && !miss(X[col][i]) && Y[col][j] === X[col][i]);
      const got = (await run(`SELECT id, (SELECT COUNT(*) FROM ${y} b WHERE b.${col} = a.${col}) AS n, (SELECT SUM(b.v) FROM ${y} b WHERE b.${col} = a.${col}) AS s FROM ${x} a`)).sort((a, b) => a.id - b.id);
      same(got.map((r) => r.n), X.id.map((_, i) => matches(i).length), `${tag} ${x} correlated COUNT(*) on ${col}`);
      same(got.map((r) => (Number.isNaN(r.s) ? null : r.s)), X.id.map((_, i) => { const vs = matches(i).map((j) => Y.v[j]).filter((v) => !miss(v)); return vs.length ? vs.reduce((a, b) => a + b, 0) : null; }), `${tag} ${x} correlated SUM on ${col}`);
    }
    // uncorrelated scalar
    const yv = Y.v.filter((v) => !miss(v)), avg = yv.length ? yv.reduce((a, b) => a + b, 0) / yv.length : NaN;
    same(ids(await run(`SELECT id FROM ${x} WHERE v > (SELECT AVG(v) FROM ${y})`)), where(X, (i) => !miss(X.v[i]) && X.v[i] > avg), `${tag} ${x}.v > (SELECT AVG(v) FROM ${y})`);
    same((await run(`SELECT id, (SELECT v FROM ${y} WHERE id < 0) AS none FROM ${x}`)).map((r) => Number.isNaN(r.none)), X.id.map(() => true), `${tag} empty scalar subquery is NULL`);
  }
  // CTE used twice, and a CTE named like a table
  const kc = new Map();
  for (const k of T.t.k) if (!miss(k)) kc.set(k === 0 ? 0 : k, (kc.get(k === 0 ? 0 : k) ?? 0) + 1);
  const pairs = (await run('WITH a AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) SELECT a1.k AS k, a1.n AS n1, a2.n AS n2 FROM a a1 JOIN a a2 ON a1.k = a2.k')).map((r) => [r.k === 0 ? 0 : r.k, r.n1, r.n2]).sort((a, b) => a[0] - b[0]);
  same(pairs.flat(), [...kc].sort((a, b) => a[0] - b[0]).flatMap(([k, n]) => [k, n, n]), `${tag} CTE joined with itself`);
  same((await run('WITH u AS (SELECT id FROM t WHERE id < 10) SELECT COUNT(*) AS n FROM u')).map((r) => r.n), [10], `${tag} CTE named like a table wins`);
  for (const x of Object.values(V)) if (x?.isDataset) x.close(); // (Windows: open files can't be deleted below)
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

{
  const e = new Engine();
  const t = e.table({ id: [1, 2], s: ['a', 'b'], k: [1, 2], price: [5, 7] }), u = e.table({ v: [10, 20, 30] });
  const err = (q) => { try { sql(q, { t, u }).toRows(); return 'no error'; } catch (x) { return /more than one row/i.test(x.message) ? 'error' : x.message; } };
  same([err('SELECT id, (SELECT v FROM u) AS many FROM t')], ['error'], 'a scalar subquery with 3 rows in SELECT is an error');
  same([err('SELECT SUM(price) AS total FROM t WHERE price < (SELECT v FROM u)')], ['error'], 'a scalar subquery with 3 rows in WHERE is an error');
  same([err('SELECT (SELECT v FROM u) AS s')], ['error'], 'a scalar subquery with 3 rows without FROM is an error');
  same(sql('SELECT id, (SELECT v FROM u ORDER BY v LIMIT 1) AS s FROM t', { t, u }).toRows().map((r) => r.s), [10, 10], 'LIMIT 1 makes it one row');
  const q = (text) => { try { return sql(text, { t }).toRows().map((r) => r.id); } catch (x) { return [x.message]; } };
  same(q("SELECT id FROM t WHERE s IN ('a', NULL) ORDER BY id"), [1], "text IN ('a', NULL)");
  same(q("SELECT id FROM t WHERE s NOT IN ('a', NULL) ORDER BY id"), [], "text NOT IN ('a', NULL) keeps nothing");
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 subquery checks passed`);
process.exit(failures ? 1 : 0);
