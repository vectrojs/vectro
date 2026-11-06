// Audit round 5 (expressions): arithmetic, %, ABS / SQRT / FLOOR / CEIL / ROUND,
// LEAST / GREATEST / COALESCE / NULLIF, both CASE forms and three-valued logic on
// every pair of edge numbers (0, -0, ±Infinity, 1e308, 5e-324, 2^53 + 2, int32
// limits, x.5 values, NULL), checked against plain JS on tables and datasets,
// sync and async. The answers were checked against DuckDB in
// vectro-qa-2026-11-05/expr. Also: CAST of text and constants to whole numbers
// rounds half away from zero, of columns half to even, and gives a missing value
// out of range; DECIMAL(p, s)
// rounds to s digits; CAST to VARCHAR keeps every digit and writes booleans as
// true / false; ROUND with negative digits; and nested or many-argument LEAST /
// GREATEST / COALESCE / ROUND / % plan in milliseconds (they grew 2-4 times per level).
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

let failures = 0, checks = 0;
const norm = (v) => (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)) ? null : typeof v === 'boolean' ? +v : Object.is(v, -0) ? 0 : v);
const close = (x, y) => (typeof x === 'number' && typeof y === 'number' ? x === y || (Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= 1e-12 * Math.max(Math.abs(x), Math.abs(y))) : x === y);
const show = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : v));
const same = (got, want, msg) => {
  checks++;
  const g = got.map(norm), w = want.map(norm);
  const bad = g.length !== w.length ? -1 : g.findIndex((v, i) => !close(v, w[i]));
  if (g.length === w.length && bad < 0) return;
  failures++;
  if (failures <= 60) console.log(`FAIL ${msg}${bad >= 0 && g.length === PAIRS.length ? ` (first difference at a, b = ${show(PAIRS[bad])})` : ''}\n  got  ${show(bad >= 0 ? g.slice(bad, bad + 4) : g.slice(0, 6))}\n  want ${show(bad >= 0 ? w.slice(bad, bad + 4) : w.slice(0, 6))}`);
};

const N = [0, -0, 1, -1, 2, -2, 3, 7, -7, 2.5, -2.5, 0.5, -0.5, 1.5, 0.125, 1.005, 9.5, 0.1, 0.2, 0.3, 123.456, NaN,
  Infinity, -Infinity, 1e308, -1e308, 5e-324, 2 ** 53 + 2, 2 ** 52 + 1, 2147483647, -2147483648, 2147483648, 1e15 + 0.5];
const PAIRS = [];
for (const x of N) for (const y of N) PAIRS.push([x, y]);
const A = PAIRS.map((p) => p[0]), B = PAIRS.map((p) => p[1]);
const miss = Number.isNaN;
// SQL rounding: half away from zero
const rnd = (x, d = 0) => { const f = 10 ** d; if (!Number.isFinite(x) || Math.abs(x * f) >= 2 ** 52) return x; return (Math.sign(x) * Math.round(Math.abs(x) * f)) / f; };
const cmp3 = (x, y, f) => (miss(x) || miss(y) ? null : f(x, y));
const and3 = (p, q) => (p === false || q === false ? false : p === null || q === null ? null : true);
const or3 = (p, q) => (p === true || q === true ? true : p === null || q === null ? null : false);
const not3 = (p) => (p === null ? null : !p);
const gt0 = (x) => cmp3(x, 0, (u, v) => u > v);
const firstOf = (...xs) => xs.find((x) => !miss(x)) ?? NaN;
const least = (...xs) => { const p = xs.filter((x) => !miss(x)); return p.length ? Math.min(...p) : NaN; };
const greatest = (...xs) => { const p = xs.filter((x) => !miss(x)); return p.length ? Math.max(...p) : NaN; };
const sel = (f) => PAIRS.map(([a, b]) => f(a, b));
const keep = (f) => PAIRS.map((p, i) => (f(p[0], p[1]) === true ? i : -1)).filter((i) => i >= 0);

const COLS = {
  'a + b': (a, b) => a + b, 'a - b': (a, b) => a - b, 'a * b': (a, b) => a * b, 'a / b': (a, b) => a / b, '-a': (a) => -a,
  'a % b': (a, b) => a % b, // IEEE fmod: the sign of the dividend, as in DuckDB and PostgreSQL
  'ABS(a)': Math.abs, 'SQRT(a)': (a) => (a < 0 ? NaN : Math.sqrt(a)), 'FLOOR(a)': Math.floor, 'CEIL(a)': Math.ceil,
  'ROUND(a)': (a) => rnd(a), 'ROUND(a, 1)': (a) => rnd(a, 1), 'ROUND(a, 2)': (a) => rnd(a, 2), 'ROUND(a, -1)': (a) => rnd(a, -1), 'ROUND(a, -2)': (a) => rnd(a, -2),
  'LEAST(a, b)': least, 'GREATEST(a, b)': greatest, 'LEAST(a, b, 1)': (a, b) => least(a, b, 1), 'GREATEST(a, NULL, b)': greatest,
  'COALESCE(a, b, -1)': (a, b) => firstOf(a, b, -1), 'IFNULL(a, b)': (a, b) => firstOf(a, b),
  'NULLIF(a, b)': (a, b) => (miss(a) || (!miss(b) && a === b) ? NaN : a), 'NULLIF(a, 0)': (a) => (a === 0 ? NaN : a),
  'CASE WHEN a > b THEN 1 WHEN a < b THEN -1 WHEN a = b THEN 0 END': (a, b) => (miss(a) || miss(b) ? NaN : a > b ? 1 : a < b ? -1 : 0),
  'CASE WHEN a > 0 THEN a ELSE b END': (a, b) => (a > 0 ? a : b),
  'CASE a WHEN 0 THEN 10 WHEN 1 THEN 11 WHEN b THEN 12 ELSE 13 END': (a, b) => (a === 0 ? 10 : a === 1 ? 11 : !miss(b) && a === b ? 12 : 13),
  'CASE WHEN a IS NULL THEN b END': (a, b) => (miss(a) ? b : NaN), 'CASE WHEN a > 0 THEN NULL ELSE a END': (a) => (a > 0 ? NaN : a),
  'a + b * 2 - a / 4': (a, b) => a + b * 2 - a / 4, '(a - b) * (a + b)': (a, b) => (a - b) * (a + b),
};
const WHERES = {
  '(a > 0) AND (b > 0)': (a, b) => and3(gt0(a), gt0(b)), '(a > 0) OR (b > 0)': (a, b) => or3(gt0(a), gt0(b)),
  'NOT (a > 0 AND b > 0)': (a, b) => not3(and3(gt0(a), gt0(b))), 'NOT (a > b)': (a, b) => not3(cmp3(a, b, (u, v) => u > v)),
  'a IS DISTINCT FROM b': (a, b) => (miss(a) || miss(b) ? miss(a) !== miss(b) : a !== b), 'a IS NOT DISTINCT FROM b': (a, b) => (miss(a) || miss(b) ? miss(a) === miss(b) : a === b),
  'a IN (0, 1, b)': (a, b) => (miss(a) ? null : a === 0 || a === 1 || (!miss(b) && a === b) ? true : miss(b) ? null : false),
  'a NOT IN (0, NULL)': () => null, 'a BETWEEN b AND 3': (a, b) => and3(cmp3(a, b, (u, v) => u >= v), cmp3(a, 3, (u, v) => u <= v)),
  'a <> b': (a, b) => cmp3(a, b, (u, v) => u !== v), 'NULLIF(a, b) IS NULL': (a, b) => miss(a) || (!miss(b) && a === b),
  'a % 3 = 1': (a) => cmp3(a % 3, 1, (u, v) => u === v),
};

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-expr-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  let p = e.table({ id: Float64Array.from(PAIRS, (_, i) => i), a: Float64Array.from(A), b: Float64Array.from(B) });
  if (source === 'dataset') { e.writeDataset(join(dir, 'p.vectro'), p, { rowsPerChunk: 97 }); p = e.openDataset(join(dir, 'p.vectro')); }
  const run = async (q) => (source === 'async' ? sql(q, { p }).toRowsAsync() : sql(q, { p }).toRows());
  for (const [x, f] of Object.entries(COLS)) {
    let rows; try { rows = (await run(`SELECT id, ${x} AS x FROM p`)).sort((u, v) => u.id - v.id).map((r) => r.x); } catch (err) { rows = [err.message]; }
    same(rows, sel(f), `${tag} ${x}`);
  }
  for (const [w, f] of Object.entries(WHERES)) {
    let rows; try { rows = (await run(`SELECT id FROM p WHERE ${w}`)).map((r) => r.id).sort((u, v) => u - v); } catch (err) { rows = [err.message]; }
    same(rows, keep(f), `${tag} WHERE ${w}`);
  }
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

// casts
{
  const e = new Engine();
  const one = e.table({ id: [1] });
  const val = (x) => { try { return sql(`SELECT ${x} AS x FROM one`, { one }).toRows()[0].x; } catch (err) { return 'error: ' + err.message; } };
  const INT = { INTEGER: 2 ** 31, SMALLINT: 2 ** 15, TINYINT: 2 ** 7, BIGINT: 2 ** 63 };
  for (const [ty, lim] of Object.entries(INT)) {
    const xs = [2.5, -2.5, 0.5, -0.5, 1.5, 2.4999, 3.7, -3.7, 0, 126.5, 32767.4, 2147483647.4, 2147483647.5, 1e10, 1e19, 1e300];
    same(xs.map((x) => val(`CAST(${x} AS ${ty})`)), xs.map((x) => { const r = rnd(x); return r >= -lim && r < lim ? r : NaN; }), `CAST(number AS ${ty}) rounds half away from zero, missing out of range`);
    const ts = ['2.5', '-2.5', '.5', ' 12 ', '1e3', '9999999999', '-2147483649', 'abc', ''];
    same(ts.map((s) => val(`CAST('${s}' AS ${ty})`)), ts.map((s) => { const r = s.trim() === '' ? NaN : rnd(Number(s)); return r >= -lim && r < lim ? r : NaN; }), `CAST(text AS ${ty})`);
  }
  for (const v of ['\'nan\'', '\'inf\'', '\'-Infinity\'']) same([val(`CAST(CAST(${v} AS DOUBLE) AS INTEGER)`)], [NaN], `CAST(${v} AS INTEGER) is missing`);
  const t = e.table({ a: Float64Array.from([2.5, 3.7, -3.7, 0.5, 1e10, NaN, Infinity]) });
  // (a column is DOUBLE: half to even, as DuckDB and PostgreSQL; the constants above are DECIMALs there)
  same(sql('SELECT CAST(a AS INTEGER) AS x FROM t', { t }).toRows().map((r) => r.x), [2, 4, -4, 0, NaN, NaN, NaN], 'CAST(column AS INTEGER)');
  same(sql('SELECT a::BIGINT AS x FROM t', { t }).toRows().map((r) => r.x), [2, 4, -4, 0, 1e10, NaN, NaN], 'column::BIGINT');
  const h = e.table({ a: Float64Array.from([3.5, -2.5, -3.5, 2.4999999999999996, -0.49999999999999994, 4503599627370497]) });
  same(sql('SELECT CAST(a AS BIGINT) AS x FROM h', { t: h, h }).toRows().map((r) => r.x), [4, -2, -4, 2, -0, 4503599627370497], 'CAST(column AS BIGINT): half to even');
  const ds = [0.125, -0.125, 2.5, 123.456, 1.999, 7, 12345678.994];
  same(ds.map((x) => val(`CAST(${x} AS DECIMAL(10,2))`)), [0.13, -0.13, 2.5, 123.46, 2, 7, 12345678.99], 'CAST(x AS DECIMAL(10,2)) rounds to 2 digits');
  same([val('CAST(123456789.5 AS DECIMAL(10,2))')], [NaN], 'CAST(x AS DECIMAL(10,2)) is missing when it does not fit');
  same([0.1, 1e15 + 0.5, 16777217].map((x) => val(`CAST(${x} AS REAL)`)), [0.1, 1e15 + 0.5, 16777217].map(Math.fround), 'CAST(x AS REAL) is a 32-bit float');
  const vs = [2.5, 0.1, 123.456, 1e15 + 0.5, 2 ** 53 + 2, -7, 0, 5e-324, 0.1 + 0.2, 1 / 3];
  same(vs.map((x) => val(`CAST(${x} AS VARCHAR)`)), vs.map(String), 'CAST(number AS VARCHAR) keeps every digit');
  same(['1 > 0', '1 < 0', 'TRUE', 'NULL = 1'].map((x) => val(`CAST(${x} AS VARCHAR)`)), ['true', 'false', 'true', ''], 'CAST(boolean AS VARCHAR) is true / false');
  same(['ROUND(123.456, -1)', 'ROUND(-155, -1)', 'ROUND(1e308, 2)', 'ROUND(0.49999999999999994)', 'ROUND(-0.49999999999999994)', 'ROUND(5e-324, 2)'].map(val), [120, -160, 1e308, 0, 0, 0], 'ROUND edge values');
  same(['7 % 0.1', '9007199254740994 % 3', '5 % (1.0 / 0)', '-1 % 5e-324', '1e308 % 2.5'].map(val), [7 % 0.1, 9007199254740994 % 3, 5, -0, 1e308 % 2.5], '% on edge values');
  await e.close();
}

// planning time: nested or many-argument functions grew 2-4 times per level and ran out of memory. Many-argument
// LEAST / GREATEST and nested COALESCE / IFNULL / LEAST must answer; ROUND / NULLIF / % nested many levels deep may
// instead stop at once with "too large once written out"; none may hang or run out of memory
if (MODE === 'node') {
  const SH = [
    ['GREATEST of 16 columns', `GREATEST(${Array.from({ length: 16 }, (_, i) => `a + ${i}`).join(', ')})`, '16,null,12'], ['LEAST of 16 columns', `LEAST(${Array.from({ length: 16 }, (_, i) => `a - ${i}`).join(', ')})`, '-14,null,-18'],
    ['GREATEST of 300 columns', `GREATEST(${Array.from({ length: 300 }, (_, i) => `a + ${i}`).join(', ')})`, '300,null,296'],
    ['16 nested COALESCE', `${'COALESCE('.repeat(16)}a${', 0)'.repeat(16)}`, '1,0,-3'], ['16 nested IFNULL', `${'IFNULL('.repeat(16)}a${', 0)'.repeat(16)}`, '1,0,-3'],
    ['16 nested LEAST', `${'LEAST('.repeat(16)}a${', 5)'.repeat(16)}`, '1,5,-3'], ['16 nested ROUND', `${'ROUND('.repeat(16)}a${', 1)'.repeat(16)}`, '1,null,-3', true],
    ['24 nested NULLIF', `${'NULLIF('.repeat(24)}a${', 7)'.repeat(24)}`, '1,null,-3', true], ['16 nested %', `${'('.repeat(16)}a${' % 7)'.repeat(16)}`, '1,null,-3', true],
  ];
  for (const [label, x, want, mayRefuse] of SH) {
    const r = spawnSync(process.execPath, ['--max-old-space-size=512', '--input-type=module', '-e', `
      import { Engine } from ${JSON.stringify(new URL('../src/node.mjs', import.meta.url).href)}; import { sql } from ${JSON.stringify(new URL('../src/sql.mjs', import.meta.url).href)};
      const e = new Engine({ threads: 1 }), t = e.table({ a: [1, NaN, -3] });
      try { console.log(sql(process.env.Q, { t }).toRows().map((r) => (Number.isNaN(r.x) ? 'null' : r.x)).join(',')); } catch (err) { console.log(/too large once written out/.test(err.message) ? 'too large' : err.message); }
      await e.close();`],
      { encoding: 'utf8', timeout: 5000, env: { ...process.env, Q: `SELECT ${x} AS x FROM t` } });
    const got = r.stdout.trim();
    same([mayRefuse && got === 'too large' ? want : got], [want], `${label} answers within 5 s${mayRefuse ? ' (or refuses at once)' : ''} (${r.signal ? 'killed after 5 s' : r.status ? 'exit ' + r.status : 'ok'})`);
  }
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 expression checks passed`);
process.exit(failures ? 1 : 0);
