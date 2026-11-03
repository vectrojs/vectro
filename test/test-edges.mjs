// Edge cases a pre-launch audit found (0.1.3), each checked against plain
// JavaScript: stray quotes in CSV fields, text filters matching labels spread
// over many codes, CSV types that change after the sampled rows, select after
// filter, lazy count of a derived condition, CSVs wider than one parse task,
// JSON lines with arrays and odd key names, SQL GROUP BY a condition, repeated
// header names, isMissing on text.
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, SplitEngine, sql } from '../src/node.mjs';
import { openTables } from '../src/serve/index.mjs';
import { cleanNames } from '../src/csv.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); };
const throwsLike = async (fn, re, msg) => { try { await fn(); check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const dir = mkdtempSync(join(tmpdir(), 'vectro-edges-'));
let seed = 3;
const rnd = () => ((seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 4294967296);
const pick = (a) => a[Math.floor(rnd() * a.length)];
const engines = [new Engine(), new Engine({ threads: 1 }), new Engine({ threads: 4 })];

// 1. a quote inside an unquoted field (27" wide, O"Brien) is a plain character: no row lost or moved
{
  for (const e of engines) same(e.parseCSV('a,b\n12" ruler,1\nx,2\ny,3\nz,4\n').toRows(), [{ a: '12" ruler', b: 1 }, { a: 'x', b: 2 }, { a: 'y', b: 3 }, { a: 'z', b: 4 }], 'stray quote: small file');
  let csv = 'id,name,price\n';
  for (let i = 0; i < 1000; i++) csv += `${i},${i % 97 === 5 ? 'Monitor 27" wide' : `item${i % 10}`},${i}\n`;
  for (const e of engines) for (const chunkBytes of [undefined, 500, 37]) {
    const rows = e.parseCSV(csv, { chunkBytes }).toRows();
    check(rows.length === 1000 && rows.every((r, i) => r.id === i && r.price === i), `stray quote, 1000 rows, chunkBytes ${chunkBytes}: ${rows.length} rows`);
  }
  // random fields: plain, plain with stray quotes, quoted with "" and newlines; every chunk size
  const field = () => {
    const r = rnd();
    if (r < 0.4) { const v = pick(['a', 'x y', '12', 'é', '😀']); return [v, v]; }
    if (r < 0.7) { const v = pick(['27" wide', 'O"Brien', 'a"', 'x""y', 'b"c"d']); return [v, v]; }
    const v = pick(['q,1', 'two\nlines', 'say "hi"', '', 'a\n"b"\n']); return [`"${v.replaceAll('"', '""')}"`, v];
  };
  let bad = 0;
  for (let t = 0; t < 120; t++) {
    const nCols = 1 + Math.floor(rnd() * 4), nRows = 1 + Math.floor(rnd() * 200), want = [];
    let text = Array.from({ length: nCols }, (_, i) => `c${i}`).join(',') + '\n';
    for (let r = 0; r < nRows; r++) { const fs = Array.from({ length: nCols }, field); want.push(fs.map((f) => f[1])); text += fs.map((f) => f[0]).join(',') + '\n'; }
    const types = Object.fromEntries(Array.from({ length: nCols }, (_, i) => [`c${i}`, 'category']));
    for (const e of engines) for (const chunkBytes of [1, 64, undefined]) {
      const got = e.parseCSV(text, { chunkBytes, types }).toRows().map((row) => Object.values(row));
      if (JSON.stringify(got) !== JSON.stringify(want)) bad++;
    }
  }
  check(bad === 0, `stray quotes fuzz: ${bad} of ${120 * 9} loads differ`);
  // readCSV, split tables and csvToDataset cut files by the same rules
  const path = join(dir, 'stray.csv');
  writeFileSync(path, csv);
  same(engines[0].readCSV(path, { chunkBytes: 500 }).count(), 1000, 'stray quote: readCSV');
  same((await engines[0].readCSVAsync(path, { chunkBytes: 500 })).count(), 1000, 'stray quote: readCSVAsync');
  const s = new SplitEngine({ workers: 3 });
  same((await (await s.parseCSVAsync(csv)).aggAsync({ n: 'count()', p: 'sum(price)' })), { n: 1000, p: 499500 }, 'stray quote: split table');
  await s.close();
  engines[0].csvToDataset(path, join(dir, 'stray.vectro'), { pieceBytes: 700 });
  same(engines[0].openDataset(join(dir, 'stray.vectro')).agg({ n: 'count()', p: 'sum(price)' }), { n: 1000, p: 499500 }, 'stray quote: csvToDataset');
}

// 2. text filters whose matching labels are spread over many codes (3,000 SKUs)
{
  const n = 60_000, sku = Array.from({ length: n }, (_, i) => `SKU-${(i * 7919) % 3000}`), v = Float64Array.from({ length: n }, (_, i) => i % 13);
  const want = (pred) => { let c = 0, s = 0; for (let i = 0; i < n; i++) if (pred(sku[i])) { c++; s += v[i]; } return { c, s }; };
  const cases = [["endsWith(sku, '7')", (x) => x.endsWith('7')], ["contains(sku, '5')", (x) => x.includes('5')], ["like(sku, '%3')", (x) => x.endsWith('3')],
    ["ilike(sku, 'sku-%9')", (x) => x.endsWith('9')], [`in(sku, ${Array.from({ length: 400 }, (_, i) => `'SKU-${i * 7}'`).join(', ')})`, (x) => +x.slice(4) % 7 === 0 && +x.slice(4) < 2800]];
  for (const e of engines) for (const compress of [false, true]) {
    const t = e.table({ sku, v }, { compress });
    for (const [f, pred] of cases) {
      const w = want(pred);
      same(t.filter(f).agg({ c: 'count()', s: 'sum(v)' }), w, `${f} (compress ${compress})`);
      same(t.filter(f).groupBy('v').agg({ c: 'count()' }).toRows().reduce((a, r) => a + r.c, 0), w.c, `${f} then groupBy`);
      same(t.lazy().filter(f).groupBy('v').agg({ c: 'count()' }).toRows().reduce((a, r) => a + r.c, 0), w.c, `lazy ${f} then groupBy`);
      same(t.filter(`!(${f})`).count(), n - w.c, `NOT ${f}`);
    }
    same(sql("SELECT COUNT(*) AS c FROM t WHERE sku LIKE '%7'", { t }).toRows(), [{ c: want((x) => x.endsWith('7')).c }], 'SQL LIKE over many labels');
    same(t.filter("endsWith(sku, '7')").columnNames, ['sku', 'v'], 'its 0 / 1 column stays hidden');
    same(Object.keys(t.filter("endsWith(sku, '7')").toRows(1)[0]), ['sku', 'v'], 'hidden in rows too');
    t.dispose();
  }
}

// 3. a CSV column whose type changes after the sampled rows is loaded as text, not NaN
{
  let csv = 'id,code,note,d\n';
  for (let i = 0; i < 1200; i++) csv += `${i},${i >= 1100 ? `A${i}` : i},${i >= 1100 ? 'refund requested' : ''},${i >= 1100 ? '2024-01-01 9am' : '2024-01-02'}\n`;
  for (const e of engines) {
    const rows = e.parseCSV(csv).toRows();
    same([rows[5].code, rows[1100].code, rows[1150].note, rows[1100].d], ['5', 'A1100', 'refund requested', '2024-01-01 9am'], 'later text keeps its value');
    same(e.parseCSV(csv, { types: { code: 'number' } }).toRows()[1100].code, NaN, 'an asked-for number type stays a number');
  }
  const s = new SplitEngine({ workers: 2 });
  await throwsLike(() => s.parseCSVAsync(csv), /"code".*looked like numbers.*types: \{"code":"category"/, 'split table: a clear error');
  await s.close();
  writeFileSync(join(dir, 'late.csv'), csv);
  await throwsLike(() => engines[0].csvToDataset(join(dir, 'late.csv'), join(dir, 'late.vectro'), { pieceBytes: 2000 }), /looked like/, 'csvToDataset: a clear error');
}

// 4-5. select after filter keeps only the columns asked for; lazy count of a derived condition = eager
{
  const e = engines[0];
  const t = e.table({ r: ['a', 'b', 'c', 'd'], v: [1, 2, 3, 4], price: [50, 150, 80, 90] });
  const s = t.filter('v > 1').select('r');
  same([s.columnNames, s.toRows(), Object.keys(s.toColumns()), s.collect().columnNames], [['r'], [{ r: 'b' }, { r: 'c' }, { r: 'd' }], ['r'], ['r']], 'select after filter');
  await throwsLike(() => s.select('v'), /no column "v"/, 'a filter-only column can\'t be selected');
  const d = t.derive('big', 'price > 100');
  same(t.lazy().derive('big', 'price > 100').agg({ n: 'count(big)', s: 'sum(big)' }), d.agg({ n: 'count(big)', s: 'sum(big)' }), 'lazy count(derived condition) = eager');
  same(t.lazy().derive('big', 'price > 100').derive('k', 'v > 2').groupBy('k').agg({ n: 'count(big)' }).toRows(), d.derive('k', 'v > 2').groupBy('k').agg({ n: 'count(big)' }).toRows(), 'lazy grouped count(derived condition) = eager');
}

// 6. a CSV wider than one parse task (MAX_ARGS arrays) on every thread
{
  const C = 300, R = 1500;
  const val = (r, c) => (c % 3 === 0 ? `cat${(r * c) % 7}` : c % 3 === 1 ? (r * 31 + c) % 1000 : ((r + c) % 97) / 4);
  let csv = Array.from({ length: C }, (_, c) => `c${c}`).join(',') + '\n';
  for (let r = 0; r < R; r++) csv += Array.from({ length: C }, (_, c) => val(r, c)).join(',') + '\n';
  for (const e of engines) {
    const rows = e.parseCSV(csv, { chunkBytes: 50_000 }).toRows();
    check(rows.length === R && rows.every((row, r) => Array.from({ length: C }, (_, c) => row[`c${c}`] === val(r, c)).every(Boolean)), `wide CSV, ${C} columns: every cell`);
  }
}

// 7-8. JSON lines: an array in some rows keeps every row; odd key names are cleaned, not skipped
{
  const d = mkdtempSync(join(dir, 'json-'));
  writeFileSync(join(d, 'events.jsonl'), ['{"user":"a","ms":12}', '{"user":"b","ms":40,"tags":[3,7]}', '{"user":"c","ms":5}', '{"user":"d","ms":9,"tags":[1,2]}'].join('\n'));
  writeFileSync(join(d, 'logs.jsonl'), ['{"@timestamp":"2024-01-01","user-agent":"curl","user id":7}', '{"@timestamp":"2024-01-02","user-agent":"chrome","user id":8}'].join('\n'));
  const cat = await openTables({ root: d, engine: engines[0] });
  same(cat.named().events?.toRows().map((r) => r.user), ['a', 'b', 'c', 'd'], 'JSON lines: rows without the array kept');
  same(cat.named().logs?.toRows(), [{ _timestamp: '2024-01-01', user_agent: 'curl', user_id: 7 }, { _timestamp: '2024-01-02', user_agent: 'chrome', user_id: 8 }], 'JSON lines: odd keys cleaned');
  await cat.close();
}

// 9. SQL GROUP BY a condition: true, false and NULL groups
{
  const t = engines[0].table({ a: [5, -3, NaN, 7, NaN] });
  same(sql('SELECT a > 0 AS pos, COUNT(*) AS n FROM t GROUP BY 1 ORDER BY 1', { t }).toRows(), [{ pos: 0, n: 1 }, { pos: 1, n: 2 }, { pos: NaN, n: 2 }], 'GROUP BY a condition');
}

// 10. repeated or cleaned header names never collide; isMissing on text
{
  same(cleanNames(['a', 'a', 'a_2']), ['a', 'a_3', 'a_2'], 'a repeated name skips names the header has');
  same(cleanNames(['Order Date', 'Order-Date', '1st', 'NaN']), ['Order_Date', 'Order_Date_2', '_1st', 'NaN_'], 'cleaned names stay apart');
  same(engines[0].parseCSV('a,a,a_2\n1,2,3\n').toRows(), [{ a: 1, a_3: 2, a_2: 3 }], 'no column lost');
  for (const compress of [false, true]) {
    const t = engines[0].table({ r: ['a', '', 'b', ''] }, { compress });
    same([t.filter('isMissing(r)').count(), t.filter('!isMissing(r)').count(), engines[0].table({ r: ['a', 'b'] }).filter('isMissing(r)').count()], [2, 2, 0], `isMissing on text (compress ${compress})`);
  }
}

for (const e of engines) await e.close();
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} edge-case checks passed`);
process.exit(failures ? 1 : 0);
