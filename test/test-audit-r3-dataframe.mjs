// Dataframe and loading bugs from round 3 of the pre-launch audit, each checked against plain
// JavaScript: compress() and a text filter's hidden 0 / 1 column, text columns compared with
// each other (and with numbers), joins showing hidden columns, JSON lines with late text and late
// keys, blank CSV rows at the end of a dataset piece, Float32Array free text in datasets and split
// tables, select() after filter() keeping the user's names free, CSV categories with "" escapes.
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
const dir = mkdtempSync(join(tmpdir(), 'vectro-r3-'));
const configs = [{}, { threads: 1 }, { wasm: false }, { jit: false }];
const engines = configs.map((c) => new Engine(c));

// 1. compress() leaves a text filter's hidden 0 / 1 column alone (it is cached; packing it freed it)
{
  const n = 20000;
  let csv = 'sku,v\n';
  for (let i = 0; i < n; i++) csv += `SKU-${(i * 7919) % 3000},${i}\n`;
  const want = csv.trim().split('\n').slice(1).filter((l) => l.split(',')[0].includes('5')).length;
  const flows = {
    derive: (t) => t.derive('w', 'v * 2').compress(),
    filter: (t) => t.filter('v >= 0').compress(),
    again: (t) => t.compress(),
    async: (t) => t.compressAsync(),
  };
  for (const [ci, cfg] of configs.entries()) for (const [name, f] of Object.entries(flows)) {
    const e = new Engine(cfg);
    const t = e.parseCSV(csv, { compress: true, types: { sku: 'category' } });
    const before = t.filter("contains(sku, '5')").count();
    await f(t);
    e.table({ junk: new Float64Array(n).fill(7) }); // (a later allocation: would reuse freed memory)
    const after = t.filter("contains(sku, '5')").count();
    same([before, after], [want, want], `cfg${ci} compress after a text filter (${name})`);
    let err = null;
    try { t.dispose(); t.dispose(); } catch (x) { err = x.message; }
    same(err, null, `cfg${ci} dispose after compress (${name})`);
    await e.close();
  }
  // a plain table: compress after the filter, then the same filter again, and on a fresh view
  for (const e of engines) {
    const sku = Array.from({ length: n }, (_, i) => `SKU-${(i * 7919) % 3000}`);
    const t = e.table({ sku, v: Float64Array.from({ length: n }, (_, i) => i) });
    t.filter("contains(sku, '5')").count();
    t.compress();
    same([t.filter("contains(sku, '5')").count(), t.filter('v < 10000').filter("contains(sku, '5')").count()], [want, sku.slice(0, 10000).filter((s) => s.includes('5')).length], 'plain table: text filter after compress');
    t.dispose();
  }
}

// 2. text columns compared with each other compare labels; text with numbers is an error
{
  const ship = ['Paris', 'Berlin', 'Oslo', 'Rome', ''], bill = ['Berlin', 'Berlin', 'Rome', 'Oslo', 'Oslo'];
  for (const e of engines) for (const compress of [false, true]) {
    const t = e.table({ ship, bill, p: [0, 1, 2, 3, 4] }, { compress });
    same(t.filter('ship == bill').toRows().map((r) => r.p), [1], `ship == bill (compress ${compress})`);
    same(t.filter('ship != bill').toRows().map((r) => r.p), [0, 2, 3, 4], `ship != bill (compress ${compress})`);
    same(t.filter('bill == ship && p >= 0').count(), 1, 'bill == ship, the other way round');
    same(t.derive('k', 'ship == bill ? 1 : 0').toRows().map((r) => r.k), [0, 1, 0, 0, 0], 'ship == bill in derive');
    same(t.lazy().filter('ship == bill').toRows().map((r) => r.p), [1], 'lazy ship == bill');
    same(sql('SELECT p FROM t WHERE ship = bill', { t }).toRows().map((r) => r.p), [1], 'SQL ship = bill');
    // a copy of a column shares its labels: codes compare directly
    const u = t.derive('s2', 'ship');
    same(u.filter('ship == s2').count(), 5, 'a column and its copy');
    await throwsLike(() => t.filter('ship == 1'), /ship is text, compared with the number 1/, 'text == number');
    await throwsLike(() => t.filter('ship != p'), /ship is text, compared with "p", a number/, 'text != number column');
    await throwsLike(() => t.filter('1 == bill'), /bill is text/, 'number == text');
    await throwsLike(() => t.filter('ship == x', { x: 1 }), /ship is text/, 'text == param');
    await throwsLike(() => t.derive('k', 'coalesce(ship, bill)'), /coalesce\(\) mixes two text columns/, 'coalesce of two text columns');
    await throwsLike(() => t.derive('k', 'p > 1 ? ship : bill'), /\? : mixes two text columns/, '?: of two text columns');
    await throwsLike(() => t.derive('k', 'p > 1 ? ship : 0'), /\? : mixes a text column and numbers/, '?: of text and a number');
    await throwsLike(() => t.derive('k', 'coalesce(ship, p)'), /coalesce\(\) mixes a text column and numbers/, 'coalesce of text and a number');
    // one text column through ? : and coalesce stays text; coalesce skips missing text ('')
    same(t.derive('k', 'p > 1 ? ship : ship').toRows().map((r) => r.k), ship, '?: of one text column stays text');
    same(u.derive('k', 'coalesce(ship, s2)').toRows().map((r) => r.k), ship, 'coalesce of one text column stays text');
    same(e.table({ a: ['', 'x', ''], b: ['y', 'z', ''] }).derive('b2', 'a').derive('k', 'coalesce(a, b2)').toRows().map((r) => r.k), ['', 'x', ''], 'coalesce of text: same labels');
    t.dispose();
  }
  // in a dataset (each chunk has its own dictionaries)
  const e = engines[0];
  const big = { ship: [], bill: [], p: [] };
  for (let i = 0; i < 5000; i++) { big.ship.push(['Paris', 'Berlin', 'Oslo'][i % 3]); big.bill.push(['Oslo', 'Berlin', 'Rome', 'Paris'][i % 4]); big.p.push(i); }
  const want = big.p.filter((i) => big.ship[i] === big.bill[i]).length;
  e.writeDataset(join(dir, 'sb.vectro'), e.table(big), { rowsPerChunk: 700 });
  const ds = e.openDataset(join(dir, 'sb.vectro'));
  same(ds.lazy().filter('ship == bill').count(), want, 'dataset ship == bill');
  await ds.close();
}

// 3. joins never show a hidden column (a text filter's 0 / 1), on either side
{
  for (const e of engines) {
    const n = 6000;
    const customers = e.table({ cid: Float64Array.from({ length: n }, (_, i) => i), name: Array.from({ length: n }, (_, i) => `cust-${i}`) });
    const orders = e.table({ cid: Float64Array.from({ length: 3 * n }, (_, i) => i % n), amt: Float64Array.from({ length: 3 * n }, (_, i) => i), tag: Array.from({ length: 3 * n }, (_, i) => `t${i % 4000}`) });
    customers.filter("contains(name, '7')").count();
    orders.filter("contains(tag, '7')").count();
    for (const how of ['inner', 'left']) {
      const j = orders.join(customers, { on: 'cid', how });
      same(j.columnNames, ['cid', 'amt', 'tag', 'name'], `${how} join columnNames`);
      same(Object.keys(j.toRows(1)[0]), ['cid', 'amt', 'tag', 'name'], `${how} join rows`);
    }
    const dup = e.table({ cid: Float64Array.from({ length: 2 * n }, (_, i) => i % n), name: Array.from({ length: 2 * n }, (_, i) => `cust-${i}`) }); // (repeated right keys: the pair join)
    dup.filter("contains(name, '7')").count();
    same(orders.join(dup, { on: 'cid' }).columnNames, ['cid', 'amt', 'tag', 'name'], 'pair join columnNames');
  }
}

// 4. JSON lines: text after the first 1,000 lines, keys first seen there, cleaned names
{
  const L = (arr) => arr.map((o) => JSON.stringify(o)).join('\n') + '\n';
  const rows = Array.from({ length: 1200 }, (_, i) => (i < 1100 ? { id: i, code: i, late: i < 1050 ? null : 5 } : { id: i, code: `A${i}`, extra: 'x', late: 'w' }));
  writeFileSync(join(dir, 'events.jsonl'), L(rows));
  writeFileSync(join(dir, 'vec.jsonl'), L(rows.map((o, i) => (i === 0 ? { ...o, emb: [1, 2] } : o))));
  writeFileSync(join(dir, 'names.jsonl'), L([{ 'a-b': 1, a_b: 2 }]));
  writeFileSync(join(dir, 'typed.jsonl'), L(rows.map(({ id, code }) => ({ id, code }))));
  const cat = await openTables({ root: dir });
  for (const name of ['events', 'vec']) {
    const t = cat.named()[name];
    same(t.columnNames, ['id', 'code', 'late', 'extra'], `${name}: every key is a column`);
    const r = t.toRows();
    same([r[5].code, r[1100].code, r[1100].extra, r[5].extra, r[1060].late, r[1199].late, r[5].late], ['5', 'A1100', 'x', '', '5', 'w', ''], `${name}: late text kept`);
  }
  same(cat.named().names.toRows(), [{ a_b_2: 1, a_b: 2 }], 'a clean key keeps its name');
  await cat.close();
  same(cleanNames(['a-b', 'a_b']), ['a_b_2', 'a_b'], 'cleanNames: an already clean name keeps it');
  same(cleanNames(['x y', 'x_y', 'x y']), ['x_y_2', 'x_y', 'x_y_3'], 'cleanNames: the others get suffixes');
}

// 5. csvToDataset: a blank line at the end of a piece is a row, as readCSV loads it
{
  const e = engines[0];
  for (const eol of ['\n', '\r\n']) {
    let csv = 'a,b' + eol;
    for (let i = 0; i < 2000; i++) csv += i % 10 === 9 ? eol : `${i},x${i % 7}${eol}`;
    csv += eol + eol; // (blank lines at the end of the file: no rows)
    writeFileSync(join(dir, 'blank.csv'), csv);
    const want = e.readCSV(join(dir, 'blank.csv')).toRows();
    for (const pieceBytes of [64, 300, 4096, 1 << 20]) {
      e.csvToDataset(join(dir, 'blank.csv'), join(dir, 'blank.vectro'), { pieceBytes });
      const ds = e.openDataset(join(dir, 'blank.vectro'));
      same(ds.toRows(), want, `csvToDataset blank rows, pieceBytes ${pieceBytes}, eol ${JSON.stringify(eol)}`);
      await ds.close();
    }
    const ds = await e.csvToDatasetAsync(join(dir, 'blank.csv'), join(dir, 'blank2.vectro'), { pieceBytes: 300 }).then(() => e.openDataset(join(dir, 'blank2.vectro')));
    same(ds.toRows().length, want.length, 'csvToDatasetAsync blank rows');
    await ds.close();
  }
}

// 6. Float32Array and a guessed free-text column: datasets and split tables load it as a category
{
  let csv = 'id,email\n';
  for (let i = 0; i < 200; i++) csv += `${i},user${i}@example.com\n`;
  const e = engines[0];
  const want = e.parseCSV(csv, { type: Float32Array }).toRows();
  writeFileSync(join(dir, 'f32.csv'), csv);
  for (const pieceBytes of [100, 1 << 20]) {
    e.csvToDataset(join(dir, 'f32.csv'), join(dir, 'f32.vectro'), { type: Float32Array, pieceBytes });
    const ds = e.openDataset(join(dir, 'f32.vectro'));
    same(ds.toRows(), want, `csvToDataset Float32 free text, pieceBytes ${pieceBytes}`);
    await ds.close();
  }
  await throwsLike(() => e.csvToDataset(join(dir, 'f32.csv'), join(dir, 'f32b.vectro'), { type: Float32Array, types: { email: 'string' } }), /free-text column/, 'asked-for free text in Float32 still an error');
  const s = new SplitEngine({ workers: 3 });
  for (const type of [Float32Array, Float64Array, undefined]) {
    const st = await s.parseCSVAsync(csv, { type });
    same(await st.toRowsAsync(), want, `split parseCSVAsync type ${type?.name}`);
    same(await st.aggAsync({ s: 'sum(id)' }), { s: 19900 }, `split sum, type ${type?.name}`);
    await st.dispose();
  }
  await s.close();
}

// 7. filter().select(): the filter's other columns are kept under internal names
{
  for (const e of engines) for (const compress of [false, true]) {
    const t = e.table({ r: ['a', 'b', 'c', 'd'], v: [1, 2, 3, 4], w: [9, 8, 7, 6] }, { compress });
    const s = t.filter('v > 1 && w < 9').select('r');
    same(s.columnNames, ['r'], 'select: columnNames');
    same(s.derive('v', '1').toRows(), [{ r: 'b', v: 1 }, { r: 'c', v: 1 }, { r: 'd', v: 1 }], 'select: the name is free for derive');
    same(s.derive('v', '1').filter('v == 1').count(), 3, 'select: the new column is the one read');
    await throwsLike(() => s.sort('v'), /unknown column "v"/, 'select: sort by an unselected column');
    await throwsLike(() => s.groupBy('v').agg({ n: 'count()' }), /no column "v"|unknown column "v"/, 'select: group by an unselected column');
    same(s.filter("r != 'c'").toRows(), [{ r: 'b' }, { r: 'd' }], 'select then filter');
    same(s.select('r').toRows(), [{ r: 'b' }, { r: 'c' }, { r: 'd' }], 'select twice');
    same(t.filter('v > 1').select('r', 'v').filter('v < 4').select('r').toRows(), [{ r: 'b' }, { r: 'c' }], 'select, filter, select');
    same(s.collect().toRows(), [{ r: 'b' }, { r: 'c' }, { r: 'd' }], 'select then collect');
    // the table compressed after the select: the view follows its columns
    const t2 = e.table({ r: ['a', 'b', 'c', 'd'], v: [1, 2, 3, 4] });
    const s2 = t2.filter('v > 1').select('r');
    t2.compress();
    e.table({ junk: new Float64Array(10000).fill(0) });
    same(s2.toRows(), [{ r: 'b' }, { r: 'c' }, { r: 'd' }], 'select view after the table is compressed');
    same(s2.count(), 3, 'select view count after compress');
  }
}

// 8. CSV categories: a field with "" escapes is keyed by its text
{
  for (const e of engines) {
    for (const type of ['string', 'category']) {
      same(e.parseCSV('v,n\n5"",1\n"5""",2\n', { types: { v: type } }).toRows().map((r) => r.v), ['5""', '5"'], `escaped quotes (${type})`);
      same(e.parseCSV('v,n\n"5""",2\n5"",1\n', { types: { v: type } }).toRows().map((r) => r.v), ['5"', '5""'], `escaped quotes the other way (${type})`);
    }
    // the same text quoted with escapes and plain: one label
    const t = e.parseCSV('v,n\n"say ""hi""",1\nsay "hi",2\n"say ""hi""",3\nx,4\n"x",5\n', { types: { v: 'category' } });
    same(t.toRows().map((r) => r.v), ['say "hi"', 'say "hi"', 'say "hi"', 'x', 'x'], 'escaped and plain: same text');
    same([t.filter(`v == 'say "hi"'`).count(), t.groupBy('v').agg({ n: 'count()' }).count()], [3, 2], 'escaped and plain: one label');
  }
  // many chunks
  let csv = 'v\n';
  const vals = [];
  for (let i = 0; i < 3000; i++) { const k = i % 4; vals.push(['a"', 'a""', 'b', '"q"'][k]); csv += ['"a"""', 'a""', 'b', '"""q"""'][k] + '\n'; }
  for (const e of engines) same(e.parseCSV(csv, { types: { v: 'category' }, chunkBytes: 97 }).toRows().map((r) => r.v), vals, 'escaped quotes over many chunks');
}

for (const e of engines) await e.close();
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-3 dataframe checks passed`);
process.exit(failures ? 1 : 0);
