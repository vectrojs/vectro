// Dataframe and loading bugs a QA audit found (after 0.1.3), each checked
// against plain JavaScript: expressions that read no column, top() with a
// constant first key, -0 in sort keys, a -0 constant in compiled kernels, lazy
// descending sorts on a derived text column, LIKE's _ and emoji, text columns
// used as numbers, free text in float32 tables, datasets of free text only,
// impossible dates, very long free text, text sorted by code point.
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); };
const throwsLike = async (fn, re, msg) => { try { await fn(); check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const dir = mkdtempSync(join(tmpdir(), 'vectro-audit-df-'));
const engines = [new Engine(), new Engine({ threads: 1 }), new Engine({ jit: false })];
const names = ['default', 'threads 1', 'jit false'];

// 1. an expression that reads no column (constants, params): folded, not "a reduction needs at least one array"
for (const [k, e] of engines.entries()) {
  const x = [80, 77, 5], at = names[k];
  for (const type of [Float64Array, Float32Array]) {
    const t = e.table({ x }, { type }), tag = `${at} ${type.name}`;
    same([t.sum('1'), t.count('100'), t.count('NaN'), t.min('p', { p: 4 }), t.max('2 * 3')], [3, 3, 0, 4, 6], `${tag}: sum / count / min / max of constants`);
    same(t.agg({ w: 'sum(weight)', m: 'mean(p)' }, { weight: 2, p: 3 }), { w: 6, m: 3 }, `${tag}: agg of params`);
    same(t.agg({ n: 'countDistinct(x)', k: 'count(100)' }), { n: 3, k: 3 }, `${tag}: countDistinct next to count(100)`);
    same(t.lazy().agg({ s: 'sum(1)' }), { s: 3 }, `${tag}: lazy sum(1)`);
    same(t.filter('x > 50').agg({ s: 'sum(1)', p: 'sum(p)' }, { p: 2 }), { s: 2, p: 4 }, `${tag}: constants under a filter`);
    for (const on of [1, 0]) {
      const f = t.filter('on == 1', { on }), keep = on ? x : [];
      same([f.count(), f.collect().length, f.toRows().map((r) => r.x)], [keep.length, keep.length, keep], `${tag}: filter of params only (on = ${on})`);
      same(f.sort('x').toRows().map((r) => r.x), [...keep].sort((a, b) => a - b), `${tag}: params-only filter, then sort`);
      same([f.head(2).toRows().map((r) => r.x), f.slice(1).toRows().map((r) => r.x)], [keep.slice(0, 2), keep.slice(1)], `${tag}: params-only filter, then head / slice`);
      same(f.derive('y', 'x + 1').toRows().map((r) => r.y), keep.map((v) => v + 1), `${tag}: params-only filter, then derive`);
      same(f.join(e.table({ x: [5, 80], z: [1, 2] }, { type }), { on: 'x' }).toRows(), keep.filter((v) => v === 80 || v === 5).map((v) => ({ x: v, z: v === 5 ? 1 : 2 })), `${tag}: params-only filter, then join`);
    }
    same(e.table({ x: [] }, { type }).sum('1'), 0, `${tag}: sum(1) of no rows`);
  }
}

// 2. top(k, by) with a first key that reads no column, on more than 50,000 rows
{
  const e = engines[0], n = 50_001;
  const t = e.table({ w: Float64Array.from({ length: n }, (_, i) => n - i) });
  same(t.top(2, ['(1)', 'w']).toRows(), [{ w: 1 }, { w: 2 }], 'top with a constant first key');
  same(t.top(3, '(2 * 3)').toRows(), [{ w: n }, { w: n - 1 }, { w: n - 2 }], 'top with only a constant key: table order');
  same(t.lazy().derive('tier', '1').top(2, ['tier', 'w']).toRows(), [{ w: 1, tier: 1 }, { w: 2, tier: 1 }], 'lazy top on a derived constant');
  same(t.lazy().derive('tier', '1').sort(['tier', '-w']).limit(2).toRows(), [{ w: n, tier: 1 }, { w: n - 1, tier: 1 }], 'lazy sort + limit on a derived constant');
}

// 3. -0 and 0 are equal sort keys: stable, and the next key decides (sort, top, SQL ORDER BY)
for (const [k, e] of engines.entries()) {
  for (const n of [100, 6_000, 60_000]) {
    const x = Float64Array.from({ length: n }, (_, i) => (i % 5 === 0 ? (i % 10 ? 0 : -0) : (i % 997) / 7));
    const y = Float64Array.from({ length: n }, (_, i) => (i * 7919) % 13);
    const id = Float64Array.from({ length: n }, (_, i) => i);
    const t = e.table({ x, y, id });
    const order = (desc) => Array.from(id).sort((a, b) => ((desc ? x[b] - x[a] : x[a] - x[b]) || y[a] - y[b] || a - b)); // (x[a] - x[b] is 0 for -0 and 0)
    const tag = `${names[k]} n=${n}`;
    same(Array.from(t.sort(['x', 'y']).toColumns().id), order(false), `${tag}: sort x, y with -0 and 0 in x`);
    same(Array.from(t.sort(['-x', 'y']).toColumns().id), order(true), `${tag}: sort -x, y`);
    same(Array.from(t.sort('x').toColumns().id), Array.from(id).sort((a, b) => x[a] - x[b] || a - b), `${tag}: sort x is stable`);
    same(Array.from(t.top(50, ['x', 'y']).toColumns().id), order(false).slice(0, 50), `${tag}: top x, y`);
    same(sql('SELECT id FROM t ORDER BY x, y', { t }).toRows().map((r) => r.id), order(false), `${tag}: SQL ORDER BY x, y`);
  }
}

// 4. a -0 constant stays -0 in compiled kernels (x / (-0) is -Infinity)
for (const [k, e] of engines.entries()) {
  for (const type of [Float64Array, Float32Array]) {
    const x = [1, 2, -3, 4, 5, 6, 7, 8, 9];
    const t = e.table({ x }, { type }), col = (expr) => Array.from(t.derive('v', expr).column('v'));
    const tag = `${names[k]} ${type.name}`;
    same(col('x / (-0)').map(String), x.map((v) => String(v / -0)), `${tag}: x / (-0)`);
    check(col('-0').every((v) => Object.is(v, -0)), `${tag}: derive '-0' gives -0`);
    same(col('x * (-0)').map((v) => Object.is(v, -0)), x.map((v) => Object.is(v * -0, -0)), `${tag}: x * (-0)`);
    same(col('x / 0 + x / (-0) * 0').map(String), x.map((v) => String(v / 0 + (v / -0) * 0)), `${tag}: 0 and -0 in one kernel`);
    same(t.sum('1 / (x * 0 - 0)'), x.reduce((s, v) => s + 1 / (v * 0 - 0), 0), `${tag}: sum with -0 arithmetic`);
  }
}

// 5. a lazily derived copy of a text column sorts by label, descending too (missing last)
{
  const e = engines[0];
  const c = ['EU', '', 'US', 'a', 'EU', 'b'];
  const t = e.table({ c, i: [0, 1, 2, 3, 4, 5] });
  const want = (desc) => c.map((s, i) => [s, i]).sort(([a, i], [b, j]) => (a === '' ? 1 : b === '' ? -1 : 0) || (a < b ? -1 : a > b ? 1 : 0) * (desc ? -1 : 1) || i - j).map(([s]) => s);
  same(t.lazy().derive('d', 'c').sort('-d').toRows().map((r) => r.d), want(true), 'lazy derived text, sort -d');
  same(t.lazy().derive('d', 'c').top(6, '-d').toRows().map((r) => r.d), want(true), 'lazy derived text, top -d');
  same(t.lazy().derive('d', 'c').sort('d').toRows().map((r) => r.d), want(false), 'lazy derived text, sort d');
  same(t.derive('d', 'c').sort('-d').toRows().map((r) => r.d), want(true), 'eager derived text, sort -d');
}

// 6. LIKE's _ is one character (code point), an emoji too
{
  const e = engines[0];
  const s = ['😀', 'é', 'ab', '𝔘x', 'x', '😀😀', ''];
  const t = e.table({ s });
  const like = (p) => s.filter((v) => new RegExp(`^${p.replace(/%/g, '.*').replace(/_/g, '.')}$`, 'su').test(v));
  for (const p of ['_', '__', '%_', '_x', '😀_', '%😀']) {
    same(t.filter(`like(s, '${p}')`).toRows().map((r) => r.s), like(p), `like(s, '${p}')`);
    same(sql(`SELECT s FROM t WHERE s LIKE '${p}'`, { t }).toRows().map((r) => r.s), like(p), `SQL LIKE '${p}'`);
  }
  same(t.filter("ilike(s, '_X')").toRows().map((r) => r.s), ['𝔘x'], 'ilike _ (as before)');
  same(t.filter("like(s, 'a.')").count(), 0, "like: . is a plain character");
}

// 8. a text (category) column used as a number is an error; comparing it with text still works
{
  const e = engines[0];
  const t = e.table({ city: ['Paris', 'Oslo', 'Rome', 'Oslo'], price: [10, 20, 30, 40] });
  for (const [what, f] of [['filter city > 0', () => t.filter('city > 0').count()], ['sum(city * 2)', () => t.agg({ v: 'sum(city * 2)' })],
    ['price + city', () => t.derive('v', 'price + city')], ['-city', () => t.sort('-(city)')], ['filter(city)', () => t.filter('city')],
    ['!city', () => t.filter('!city').count()], ['sum(city)', () => t.sum('city')], ['min(city)', () => t.min('city')], ['abs(city)', () => t.derive('v', 'abs(city)')],
    ['city ? 1 : 0', () => t.derive('v', 'city ? 1 : 0')], ['SQL price + city', () => sql('SELECT price + city AS v FROM t', { t }).toRows()]]) {
    await throwsLike(f, /city is a text column/, `text as a number: ${what}`);
  }
  await throwsLike(() => t.filter("city >= 'Oslo'").count(), /can only be compared with == or !=/, 'text < text keeps its own error');
  same([t.filter("city == 'Oslo'").count(), t.filter("city != 'Oslo' && price > 10").count(), t.filter("startsWith(city, 'R') || like(city, 'P%')").count(), t.filter('isMissing(city)').count()], [2, 1, 2, 0], 'text compared with text, text functions');
  same(t.derive('c2', 'city').sort('-c2').toRows().map((r) => r.c2), ['Rome', 'Paris', 'Oslo', 'Oslo'], 'a copy of a text column');
  same(t.derive('v', "city == 'Oslo' ? price : 0").sum('v'), 60, 'text comparison in ? :');
  same([t.count('city'), t.agg({ d: 'countDistinct(city)' }).d], [4, 3], 'count / countDistinct of text');
  same(t.groupBy('city').agg({ s: 'sum(price)' }).toRows(), [{ city: 'Paris', s: 10 }, { city: 'Oslo', s: 60 }, { city: 'Rome', s: 30 }], 'groupBy text');
  same(sql("SELECT city, SUM(price) AS s FROM t WHERE city <> 'Rome' GROUP BY city ORDER BY city", { t }).toRows(), [{ city: 'Oslo', s: 60 }, { city: 'Paris', s: 10 }], 'SQL on text');
}

// 9. free text in a float32 table: loaded as a category (a guess), or a clear error (types: string)
{
  const e = engines[0];
  let csv = 'id,email\n';
  for (let i = 0; i < 200; i++) csv += `${i},user${i}@example.com\n`;
  const rows = e.parseCSV(csv, { type: Float32Array }).toRows();
  check(rows.length === 200 && rows.every((r, i) => r.id === i && r.email === `user${i}@example.com`), 'float32 table: free text comes back whole');
  await throwsLike(() => e.parseCSV(csv, { type: Float32Array, types: { email: 'string' } }), /Float32Array table can't hold/, 'float32 table, types string: an error');
  same(e.parseCSV(csv, { types: { email: 'string' } }).toRows()[150], { id: 150, email: 'user150@example.com' }, 'float64 table: free text as before');
}

// 10. writeDataset and csvToDataset of a table whose columns are all free text
{
  const e = engines[0];
  let csv = 'email\n';
  for (let i = 0; i < 60; i++) csv += `u${i}@x.com\n`;
  const want = Array.from({ length: 60 }, (_, i) => ({ email: `u${i}@x.com` }));
  writeFileSync(join(dir, 'emails.csv'), csv);
  same(e.writeDataset(join(dir, 'a.vectro'), e.parseCSV(csv)).rows, 60, 'writeDataset of free text only');
  same(e.csvToDataset(join(dir, 'emails.csv'), join(dir, 'b.vectro')).rows, 60, 'csvToDataset of free text only');
  for (const f of ['a', 'b']) {
    const ds = e.openDataset(join(dir, `${f}.vectro`));
    same(ds.lazy().toRows(), want, `dataset ${f} of free text reads back`);
    await ds.close();
  }
}

// 11. dates that don't exist (2024-02-31) aren't rolled into the next month
{
  const e = engines[0];
  const text = ['2024-02-31', '2023-02-29', '2024-04-31', '2024-06-15'];
  same(e.parseCSV(`d\n${text.join('\n')}\n`).toRows().map((r) => r.d), text, 'guessed: impossible dates leave the column as text');
  const days = ['2024-02-31', '2023-02-29', '2024-04-31', '1900-02-29', '2024-06-15', '2024-02-29', '2000-02-29', '2023-12-31'];
  const ok = (s) => { const d = new Date(`${s}T00:00:00Z`); return d.toISOString().slice(0, 10) === s; };
  same(e.parseCSV(`d\n${days.join('\n')}\n`, { types: { d: 'date' } }).toRows().map((r) => r.d && r.d.toISOString().slice(0, 10)), days.map((s) => (ok(s) ? s : null)), 'typed date: impossible dates are missing');
  same(e.parseCSV('d\n2024-02-29\n2023-02-28\n1900-02-28\n').toRows().map((r) => r.d instanceof Date), [true, true, true], 'real dates still guessed as dates');
  same(e.table({ d: ['2024-02-30', '2024-03-01'] }, { dates: ['d'] }).toRows().map((r) => r.d && r.d.toISOString()), [null, '2024-03-01T00:00:00.000Z'], 'table dates: impossible date missing');
}

// 12. a free-text column with one value over 512 KB loads (as a category)
{
  const e = engines[0];
  let csv = 'id,note\n';
  const note = (i) => `note ${i}${i === 30 ? ' ' + 'y'.repeat(600_000) : ''}`;
  for (let i = 0; i < 60; i++) csv += `${i},"${note(i)}"\n`;
  const rows = e.parseCSV(csv).toRows();
  check(rows.length === 60 && rows.every((r, i) => r.id === i && r.note === note(i)), 'long free text: every value whole');
  await throwsLike(() => e.parseCSV(csv, { types: { note: 'string' } }), /longer than 524287 bytes/, 'long text typed string: a clear error');
}

// 13. text sorts by code point (as DuckDB), not UTF-16 units; missing text last
{
  const e = engines[0];
  const s = ['😀', 'ﬁ', 'Ｚ', 'a', '', 'z', '𝔘', 'é', 'ab', '￿', 'Z'];
  const t = e.table({ s });
  const cp = (a, b) => { const x = [...a].map((c) => c.codePointAt(0)), y = [...b].map((c) => c.codePointAt(0)); for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i]; return x.length - y.length; };
  const asc = s.filter((v) => v).sort(cp);
  same(t.sort('s').toRows().map((r) => r.s), [...asc, ''], 'text sort by code point');
  same(t.sort('-s').toRows().map((r) => r.s), [...asc.reverse(), ''], 'text sort descending by code point');
  same(sql('SELECT s FROM t ORDER BY s', { t }).toRows().map((r) => r.s), [...s.filter((v) => v).sort(cp), ''], 'SQL ORDER BY text by code point');
}

for (const e of engines) await e.close();
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} audit checks passed`);
process.exit(failures ? 1 : 0);
