// Audit round 5 (percentiles): MEDIAN, PERCENTILE_CONT / PERCENTILE_DISC (ASC and
// DESC), QUANTILE_CONT / QUANTILE_DISC per group, on groups of one value, all
// missing, all the same, even and odd counts, ties, ±Infinity, ±1e308, -0 / 0,
// checked against plain JS on tables and datasets, sync and async; the answers
// were checked against DuckDB in vectro-qa-2026-11-05/pct. Also: interpolation
// with ±Infinity (it gave NaN: -Inf + (5 - -Inf) / 2) and ±1e308 (it overflowed);
// PERCENTILE_DISC ... ORDER BY x DESC (it took 1 - p, one value off); MEDIAN(DISTINCT
// x) (DISTINCT was ignored); percentiles of text (MEDIAN gave the mean of label
// codes, and missing text counted as a value).
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const norm = (v) => (v === '' || v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)) ? null : Object.is(v, -0) ? 0 : v);
const close = (a, b) => (typeof a === 'number' && typeof b === 'number' ? a === b || (Number.isFinite(a) && Number.isFinite(b) && Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b))) : a === b);
const same = (got, want, msg) => {
  checks++;
  const g = got.map(norm), w = want.map(norm), bad = g.length !== w.length ? -1 : g.findIndex((v, i) => !close(v, w[i]));
  if (g.length === w.length && bad < 0) return;
  failures++;
  if (failures <= 40) console.log(`FAIL ${msg}${bad >= 0 ? ` (first difference in group ${GROUPS[bad]?.[0]})` : ''}\n  got  ${JSON.stringify(bad >= 0 ? g.slice(bad, bad + 3) : g.slice(0, 5), (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v))}\n  want ${JSON.stringify(bad >= 0 ? w.slice(bad, bad + 3) : w.slice(0, 5), (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v))}`);
};

const GROUPS = [['a-one', [5]], ['b-nan', [NaN, NaN]], ['c-same', [3, 3, 3, 3]], ['d-even', [1, 2, 3, 4]], ['e-odd', [7, 1, 4]], ['f-two', [10, 20]], ['g-inf', [Infinity, 1, 2, -Infinity]],
  ['h-pinf', [Infinity, Infinity, 1]], ['i-ninf', [-Infinity, 5]], ['j-zero', [-0, 0, -0]], ['k-ties', [1, 1, 1, 2, 2, 9]], ['l-mixnan', [NaN, 4, NaN, 2]], ['m-big', [1e308, 1e308, -1e308]],
  ['n-501', Array.from({ length: 501 }, (_, i) => ((i * 37) % 101) / 4)]];
const TXT = ['b', 'c', 'a', 'é', 'z', '😀', 'b'];
const id = [], g = [], x = [], s = [];
for (const [name, xs] of GROUPS) for (const v of xs) { id.push(id.length); g.push(name); x.push(v); s.push(Number.isNaN(v) ? '' : TXT[id.length % TXT.length]); }
const present = (vs) => vs.filter((v) => !(typeof v === 'number' ? Number.isNaN(v) : v === ''));
const asc = (vs) => [...vs].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
const cpAsc = (vs) => [...vs].sort((a, b) => { const x = [...a].map((c) => c.codePointAt(0)), y = [...b].map((c) => c.codePointAt(0)); for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i]; return x.length - y.length; });
// continuous: linear between the two values around p * (n - 1), written so that ±Infinity and ±1e308 don't give NaN or overflow
const cont = (vs, p) => { const v = asc(present(vs)); if (!v.length) return NaN; const pos = p * (v.length - 1), lo = Math.floor(pos), f = pos - lo; return f === 0 || v[lo] === v[lo + 1] ? v[lo] : v[lo] * (1 - f) + v[lo + 1] * f; };
// discrete: the first value (in the ORDER BY direction) whose cumulative share reaches p
const disc = (vs, p, desc = false, sorter = asc) => { const v = sorter(present(vs)); if (desc) v.reverse(); return v.length ? v[Math.max(0, Math.ceil(p * v.length) - 1)] : NaN; };
const byGroup = GROUPS.map(([name]) => [name, x.filter((_, i) => g[i] === name), s.filter((_, i) => g[i] === name)]);
const P = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1, 1 / 3];

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-pct-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  let t = e.table({ id: Float64Array.from(id), g, x: Float64Array.from(x), s });
  if (source === 'dataset') { e.writeDataset(join(dir, 't.vectro'), t, { rowsPerChunk: 7 }); t = e.openDataset(join(dir, 't.vectro')); }
  const per = async (f) => { try { const q = sql(`SELECT g, ${f} AS q FROM t GROUP BY g ORDER BY g`, { t }); return (source === 'async' ? await q.toRowsAsync() : q.toRows()).map((r) => r.q); } catch (err) { return [err.message]; } };
  same(await per('MEDIAN(x)'), byGroup.map(([, xs]) => cont(xs, 0.5)), `${tag} MEDIAN(x)`);
  for (const p of P) {
    same(await per(`PERCENTILE_CONT(${p}) WITHIN GROUP (ORDER BY x)`), byGroup.map(([, xs]) => cont(xs, p)), `${tag} PERCENTILE_CONT(${p})`);
    same(await per(`PERCENTILE_CONT(${p}) WITHIN GROUP (ORDER BY x DESC)`), byGroup.map(([, xs]) => cont(xs, 1 - p)), `${tag} PERCENTILE_CONT(${p}) ... DESC`);
    same(await per(`PERCENTILE_DISC(${p}) WITHIN GROUP (ORDER BY x)`), byGroup.map(([, xs]) => disc(xs, p)), `${tag} PERCENTILE_DISC(${p})`);
    same(await per(`PERCENTILE_DISC(${p}) WITHIN GROUP (ORDER BY x DESC)`), byGroup.map(([, xs]) => disc(xs, p, true)), `${tag} PERCENTILE_DISC(${p}) ... DESC`);
    same(await per(`QUANTILE_CONT(x, ${p})`), byGroup.map(([, xs]) => cont(xs, p)), `${tag} QUANTILE_CONT(x, ${p})`);
    same(await per(`QUANTILE_DISC(x, ${p})`), byGroup.map(([, xs]) => disc(xs, p)), `${tag} QUANTILE_DISC(x, ${p})`);
  }
  same(await per('MEDIAN(DISTINCT x)'), byGroup.map(([, xs]) => cont([...new Set(xs.map((v) => (v === 0 ? 0 : v)))], 0.5)), `${tag} MEDIAN(DISTINCT x)`);
  // text: the discrete percentile of the labels present (DuckDB's MEDIAN of text is the lower middle one)
  same(await per('MEDIAN(s)'), byGroup.map(([, , ss]) => disc(ss, 0.5, false, cpAsc)), `${tag} MEDIAN(s)`);
  for (const p of [0.25, 0.9]) {
    same(await per(`PERCENTILE_DISC(${p}) WITHIN GROUP (ORDER BY s)`), byGroup.map(([, , ss]) => disc(ss, p, false, cpAsc)), `${tag} PERCENTILE_DISC(${p}) of text`);
    same(await per(`PERCENTILE_DISC(${p}) WITHIN GROUP (ORDER BY s DESC)`), byGroup.map(([, , ss]) => disc(ss, p, true, cpAsc)), `${tag} PERCENTILE_DISC(${p}) of text DESC`);
  }
  // the whole table, and an empty one
  const one = async (q) => { try { const r = sql(q, { t }); return (source === 'async' ? await r.toRowsAsync() : r.toRows()).map((row) => row.q); } catch (err) { return [err.message]; } };
  same(await one('SELECT MEDIAN(x) AS q FROM t'), [cont(x, 0.5)], `${tag} MEDIAN over the whole table`);
  same(await one('SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x) AS q FROM t WHERE id < 0'), [NaN], `${tag} PERCENTILE_CONT of no rows is missing`);
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 percentile checks passed`);
process.exit(failures ? 1 : 0);
