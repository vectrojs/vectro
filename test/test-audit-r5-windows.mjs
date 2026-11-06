// Audit round 5 (windows): ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG / LEAD,
// running and ROWS-frame SUM / COUNT / MIN / MAX, and RANGE frames, on edge data
// (missing partition keys, ties, -0 / 0, ±Infinity and NaN in the order column,
// one-row partitions, an empty table), checked against a plain-JS window
// evaluator, on tables and datasets, sync and async. The answers were checked
// against DuckDB in vectro-qa-2026-11-05/windows. Also: RANGE frames with
// NULLS FIRST and infinite order values, GROUP BY the same key twice.
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

let seed = 7;
const rnd = () => { let t = (seed = (seed + 0x6d2b79f5) | 0); t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
function mk(n) {
  const t = { id: [], p: [], o: [], v: [] };
  for (let i = 0; i < n; i++) {
    t.id.push(i); t.p.push(pick(['a', 'b', '', '😀']));
    t.o.push(pick([0, -0, 1, 1, 2, 3, 3, 10, NaN, -1, 2.5, Infinity, -Infinity]));
    t.v.push(pick([1, 2.5, -3, 0, 100, NaN, 0.125]));
  }
  return t;
}
const T = { t: mk(300), o: mk(1), e: { id: [], p: [], o: [], v: [] } };

// the reference: partitions by p (missing is one partition), order by o (missing last, or first), then id
const miss = (v) => v === '' || Number.isNaN(v);
const nul = (v) => (miss(v) ? null : v);
function windows(t, { part, desc, nullsFirst }) {
  const n = t.id.length, out = Array.from({ length: n }, () => ({}));
  const parts = new Map();
  for (let i = 0; i < n; i++) { const k = part ? (miss(t.p[i]) ? '\0' : t.p[i]) : ''; if (!parts.has(k)) parts.set(k, []); parts.get(k).push(i); }
  const cmpO = (a, b) => {
    const x = t.o[a], y = t.o[b];
    if (miss(x) || miss(y)) return miss(x) && miss(y) ? 0 : miss(x) === nullsFirst ? -1 : 1;
    return x === y ? 0 : (x < y) !== desc ? -1 : 1;
  };
  for (const rows of parts.values()) {
    rows.sort((a, b) => cmpO(a, b) || a - b);
    const m = rows.length;
    let rank = 1, dense = 0;
    for (let j = 0; j < m; j++) {
      const i = rows[j], r = out[i];
      if (j === 0 || cmpO(rows[j - 1], i) !== 0) { rank = j + 1; dense++; }
      let end = j; while (end + 1 < m && cmpO(rows[end + 1], i) === 0) end++;
      const vals = (a, b) => rows.slice(Math.max(a, 0), Math.min(b, m - 1) + 1).map((x) => t.v[x]).filter((x) => !miss(x));
      const sum = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) : null);
      const run = vals(0, end), near = vals(j - 1, j + 1);
      Object.assign(r, {
        rn: j + 1, rank, dense, nt: Math.floor((j * 4) / m) + 1 > 0 ? ntile(j, m, 4) : 0,
        lag: j > 0 ? nul(t.v[rows[j - 1]]) : null, lead2: j + 2 < m ? nul(t.v[rows[j + 2]]) : -1,
        rsum: sum(run), rcount: run.length, nsum: sum(near), nmin: near.length ? Math.min(...near) : null, nmax: near.length ? Math.max(...near) : null,
      });
    }
  }
  return out;
}
function ntile(j, m, k) { const small = Math.floor(m / k), big = m % k; return j < big * (small + 1) ? Math.floor(j / (small + 1)) + 1 : big + Math.floor((j - big * (small + 1)) / small) + 1; }

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-windows-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const V = {};
  for (const [name, t] of Object.entries(T)) {
    const tab = e.table({ id: Float64Array.from(t.id.length ? t.id : [0]), p: t.p.length ? t.p : ['x'], o: Float64Array.from(t.o.length ? t.o : [0]), v: Float64Array.from(t.v.length ? t.v : [0]) });
    const src = t.id.length ? tab : sql('SELECT * FROM x WHERE id < 0', { x: tab }).collect();
    if (source !== 'dataset' || !t.id.length) { V[name] = src; continue; }
    const path = join(dir, `${name}.vectro`);
    e.writeDataset(path, src, { rowsPerChunk: Math.max(3, Math.ceil(t.id.length / 7)) });
    V[name] = e.openDataset(path);
  }
  const run = (q) => (source === 'async' ? sql(q, V).toRowsAsync() : sql(q, V).toRows());
  for (const [name, t] of Object.entries(T)) for (const part of [false, true]) for (const desc of [false, true]) for (const nullsFirst of [false, true]) {
    const ref = windows(t, { part, desc, nullsFirst });
    const ord = `o${desc ? ' DESC' : ''} NULLS ${nullsFirst ? 'FIRST' : 'LAST'}`, P = part ? 'PARTITION BY p ' : '';
    const q = `SELECT id, ROW_NUMBER() OVER (${P}ORDER BY ${ord}, id) AS rn, RANK() OVER (${P}ORDER BY ${ord}) AS rank, DENSE_RANK() OVER (${P}ORDER BY ${ord}) AS dense,
      NTILE(4) OVER (${P}ORDER BY ${ord}, id) AS nt, LAG(v) OVER (${P}ORDER BY ${ord}, id) AS lag, LEAD(v, 2, -1) OVER (${P}ORDER BY ${ord}, id) AS lead2,
      SUM(v) OVER (${P}ORDER BY ${ord}) AS rsum, COUNT(v) OVER (${P}ORDER BY ${ord}) AS rcount,
      SUM(v) OVER (${P}ORDER BY ${ord}, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nsum,
      MIN(v) OVER (${P}ORDER BY ${ord}, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nmin,
      MAX(v) OVER (${P}ORDER BY ${ord}, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nmax FROM ${name}`;
    const got = (await run(q)).sort((a, b) => a.id - b.id);
    for (const col of ['rn', 'rank', 'dense', 'nt', 'lag', 'lead2', 'rsum', 'rcount', 'nsum', 'nmin', 'nmax'])
      same(got.map((r) => nul(r[col])), ref.map((r) => r[col]), `${tag} ${name} ${P}${ord}: ${col}`);
  }
  for (const x of Object.values(V)) if (x?.isDataset) x.close(); // (Windows: open files can't be deleted below)
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

// RANGE with an offset, NULLS FIRST and infinite order values (DuckDB's answers)
{
  const e = new Engine();
  const t = e.table({ id: [0, 1, 2, 3, 4, 5], o: Float64Array.from([-Infinity, 0, Infinity, NaN, NaN, 1]) });
  const count = (ord) => sql(`SELECT id, COUNT(*) OVER (ORDER BY o ${ord} RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM t ORDER BY id`, { t }).toRows().map((r) => r.c);
  same(count('NULLS FIRST'), [1, 1, 1, 2, 2, 2], 'RANGE 1 PRECEDING, NULLS FIRST, with ±Infinity');
  same(count('DESC NULLS FIRST'), [1, 2, 1, 2, 2, 1], 'RANGE 1 PRECEDING, DESC NULLS FIRST, with ±Infinity');
  same(count('NULLS LAST'), [1, 1, 1, 2, 2, 2], 'RANGE 1 PRECEDING, NULLS LAST, with ±Infinity');
  const g = e.table({ q: [1, 1, 2], o: [5, 6, 5] });
  let rows;
  try { rows = sql('SELECT q, o, COUNT(*) AS n FROM g GROUP BY q, o, q ORDER BY q, o', { g }).toRows().map((r) => r.n); } catch (err) { rows = [err.message]; }
  same(rows, [1, 1, 1], 'GROUP BY the same key twice');
  // W3: LAG / LEAD of text with a text default (a number default is written as text, as DuckDB casts it)
  const w = e.table({ g: ['a', 'a', 'b', 'b'], i: [1, 2, 3, 4], s: ['x', 'y', 'z', ''] });
  const lag = sql("SELECT LAG(s, 1, 'none') OVER (PARTITION BY g ORDER BY i) AS p, LEAD(s, 1, 'end') OVER (ORDER BY i) AS q, LAG(s, 2, 7) OVER (ORDER BY i) AS r FROM w ORDER BY i", { w }).toRows();
  same(lag.flatMap((r) => [r.p, r.q, r.r]), ['none', 'y', '7', 'x', 'z', '7', 'none', '', 'x', 'z', 'end', 'y'], 'LAG / LEAD of text with a text default');
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 window checks passed`);
process.exit(failures ? 1 : 0);
