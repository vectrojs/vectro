// Audit round 6 (run 3, getting data in and out): CSV headers named like
// Object.prototype properties (constructor, toString, valueOf, ...) made
// every CSV loader refuse the file, because the `types` lookup found the
// inherited function (vectro-qa-2026-11-07/run3).
import { Engine, MODE } from './test-mode.mjs';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const str = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isFinite(x) ? String(x) : x));
const same = (a, b, msg) => { checks++; const x = str(a), y = str(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };
const fails = (f, re, msg) => { checks++; try { f(); failures++; console.log(`FAIL ${msg}: no error`); } catch (e) { if (re.test(e.message)) return; failures++; console.log(`FAIL ${msg}: ${e.message}`); } };
const attempt = (f) => { try { return f(); } catch (e) { return 'ERR ' + e.message; } };

const engine = new Engine();
const dir = mkdtempSync(join(tmpdir(), 'vectro-r6-load-'));
const node = typeof engine.readCSV === 'function' && typeof engine.csvToDataset === 'function'; // (Node: files)

// CSV headers named like Object.prototype properties
for (const h of ['constructor', 'toString', 'valueOf', 'hasOwnProperty', '__defineGetter__']) {
  const csv = `${h},b\n1,x\n2,y\n`;
  const want = [{ [h]: 1, b: 'x' }, { [h]: 2, b: 'y' }];
  same(attempt(() => engine.parseCSV(csv).toRows()), want, `parseCSV, a column named ${h}`);
  same(attempt(() => engine.parseCSV(csv, { types: { b: 'category' } }).toRows()), want, `parseCSV with types, a column named ${h}`);
  same(attempt(() => engine.parseCSV(csv, { columns: [h] }).toRows()), want.map((r) => ({ [h]: r[h] })), `parseCSV columns: [${h}]`);
  same(attempt(() => engine.parseCSV(csv, { type: Float32Array }).toRows()), want, `parseCSV Float32, a column named ${h}`);
  if (node) {
    const p = join(dir, `${h}.csv`);
    writeFileSync(p, csv);
    same(attempt(() => engine.readCSV(p).toRows()), want, `readCSV, a column named ${h}`);
    same(attempt(() => { engine.csvToDataset(p, p + '.vectro'); const ds = engine.openDataset(p + '.vectro'); try { return ds.toRows(); } finally { ds.close(); } }), want, `csvToDataset, a column named ${h}`);
  }
}
// a wrong type is still refused, an own `types` entry still wins
fails(() => engine.parseCSV('a\n1\n', { types: { a: 'int' } }), /type for "a" must be/, 'types: int');
same(engine.parseCSV('constructor\n01\n', { types: { constructor: 'category' } }).toRows(), [{ constructor: '01' }], 'types: { constructor: category }');

// Two writers on one dataset path in this process: the second one's rows were lost without a word
// (two writeDatasetAsync appends at once kept only one of them). Now the second writer is refused.
if (node) {
  const p = join(dir, 'two.vectro');
  engine.writeDataset(p, { v: [1, 2] });
  const w = engine.createDataset(p, { append: true });
  w.append({ v: [3] });
  fails(() => engine.createDataset(p, { append: true }), /already being written/, 'a second writer while one is open');
  fails(() => engine.writeDataset(p, { v: [9] }), /already being written/, 'writeDataset while a writer is open');
  w.close();
  engine.writeDataset(p, { v: [4] }, { append: true }); // (free again after close)
  const got = await Promise.allSettled([engine.writeDatasetAsync(p, { v: [10] }, { append: true }), engine.writeDatasetAsync(p, { v: [20] }, { append: true })]);
  const ok = got.filter((g) => g.status === 'fulfilled').length;
  const ds = engine.openDataset(p);
  same(ds.length, 4 + ok, 'concurrent async appends: every append that succeeded is in the file');
  same(got.filter((g) => g.status === 'rejected').every((g) => /already being written/.test(g.reason.message)), true, 'concurrent async appends: the other one is refused');
  ds.close();
  const a = engine.createDataset(p);
  a.abort();
  same(attempt(() => engine.writeDataset(p, { v: [5] }).rows), 1, 'free again after abort');
  const failed = attempt(() => engine.writeDataset(p, { v: [1], s: ['x'] }, { append: true }));
  same(/differ/.test(failed), true, 'a failed append is refused');
  same(attempt(() => engine.writeDataset(p, { v: [6] }, { append: true }).rows), 2, 'free again after a failed append');
}

// A dataset writer refused batches that lose nothing in the dataset's column kinds: an empty batch
// or a column of only nulls (plain [] / [null] are number columns), and text into a free-text column
// (plain strings are categories). A feed whose optional field is null for a whole batch stopped.
if (node) {
  const rowsOf = (p) => { const ds = engine.openDataset(p); try { return [ds.columnNames, ds.toRows()]; } finally { ds.close(); } };
  const p = join(dir, 'kinds.vectro');
  const w = engine.createDataset(p, { rowsPerChunk: 3 });
  w.append({ k: ['a', 'b'], v: [1, 2], d: [new Date(0), new Date(86400000)] });
  w.append({ k: [], v: [], d: [] });
  w.append({ k: [null, null], v: [3, null], d: [null, null] });
  w.append({ k: ['c'], v: [null], d: [new Date(172800000)] });
  w.append(engine.table({ k: ['z'], v: [9], d: [new Date(0)] }).filter('v > 100'));
  w.close();
  same(rowsOf(p), [['k', 'v', 'd'], [
    { k: 'a', v: 1, d: new Date(0) }, { k: 'b', v: 2, d: new Date(86400000) },
    { k: '', v: 3, d: null }, { k: '', v: NaN, d: null }, { k: 'c', v: NaN, d: new Date(172800000) }]].map((x) => JSON.parse(str(x))), 'writer: empty and all-null batches take the dataset\'s kinds');
  const ds = engine.openDataset(p);
  same(ds.groupBy('k').agg({ n: 'count()' }).sort('k').toRows(), [{ k: 'a', n: 1 }, { k: 'b', n: 1 }, { k: 'c', n: 1 }, { k: '', n: 2 }], 'writer: all-null text is missing text (sorted last)');
  ds.close();
  fails(() => engine.writeDataset(p, { k: ['x'], v: ['oops'], d: [null] }, { append: true }), /"v" is category here but number/, 'text into a number column is still refused');
  fails(() => engine.writeDataset(p, { k: [1], v: [1], d: [null] }, { append: true }), /"k" is number here but category/, 'numbers into a text column are still refused');
  // free text (from a CSV) and categories, either way round
  let csv = 'email,n\n';
  for (let i = 0; i < 60; i++) csv += `u${i}@x.com,${i}\n`;
  const c = join(dir, 'emails.csv'), e = join(dir, 'emails.vectro');
  writeFileSync(c, csv);
  engine.csvToDataset(c, e);
  same(attempt(() => engine.writeDataset(e, { email: ['new@x.com', null, 'ünï 🙂'], n: [100, 101, 102] }, { append: true }).rows), 63, 'plain strings appended to a free-text column');
  same(attempt(() => engine.writeDataset(e, engine.parseCSV(csv, { types: { email: 'category' } }), { append: true }).rows), 123, 'a category appended to a free-text column');
  same(rowsOf(e)[1].slice(60, 63), [{ email: 'new@x.com', n: 100 }, { email: '', n: 101 }, { email: 'ünï 🙂', n: 102 }], 'free text appended reads back');
  same(rowsOf(e)[1].slice(-1), [{ email: 'u59@x.com', n: 59 }], 'category appended to free text reads back');
  const k = join(dir, 'cat.vectro');
  engine.writeDataset(k, { email: ['a@x', 'b@x'], n: [1, 2] });
  same(attempt(() => engine.writeDataset(k, engine.parseCSV(csv), { append: true }).rows), 62, 'free text appended to a category column');
  same(rowsOf(k)[1].slice(-2), [{ email: 'u58@x.com', n: 58 }, { email: 'u59@x.com', n: 59 }], 'free text appended to a category reads back');
}

// An Arrow *file* (.arrow / .feather, "ARROW1" first) cut short exactly after a message loaded as
// fewer rows (or none) without an error: it was read as a stream. A file needs its footer.
{
  const t = engine.table({ id: [1, 2, 3], g: ['a', 'b', 'a'] });
  const whole = t.toArrow({ file: true }), stream = t.toArrow();
  let wrong = 0, cut = 0;
  for (let at = 0; at < whole.length; at++) {
    try { const x = engine.fromArrow(whole.subarray(0, at)); cut++; if (x.length !== 3) wrong++; } catch (e) { if (!/^vectro/.test(e.message)) wrong++; }
  }
  same([wrong, cut], [0, 0], 'a cut Arrow file: always an error');
  same(attempt(() => engine.fromArrow(whole).toRows().length), 3, 'a whole Arrow file reads');
  same(attempt(() => engine.fromArrow(stream).toRows().length), 3, 'an Arrow stream reads');
  same(/footer/.test(attempt(() => engine.fromArrow(whole.subarray(0, whole.length - 1)))), true, 'a cut Arrow file names the missing footer');
}

// Damaged files: errors that say so (raw "Offset is outside the bounds of the DataView", "position
// NaN", JSON syntax errors before). Bytes changed inside the data itself can't be seen: there are no
// checksums (written up in vectro-qa-2026-11-07/AREA-3.md).
{
  const t = engine.table({ id: Array.from({ length: 50 }, (_, i) => i), g: Array.from({ length: 50 }, (_, i) => 'g' + (i % 3)), ts: Array.from({ length: 50 }, (_, i) => new Date(i * 1000)) });
  for (const file of [false, true]) {
    const b = t.toArrow({ file });
    const raw = new Set();
    for (let i = 0; i < b.length; i++) for (const x of [0xff, 0x80, 0x01]) {
      const c = b.slice(); c[i] ^= x;
      try { engine.fromArrow(c).toRows(); } catch (e) { if (!/^vectro/.test(e.message) && !/^Invalid (array|typed array) length/.test(e.message)) raw.add(e.message.replace(/\d+/g, 'N')); }
    }
    same([...raw], [], `damaged Arrow ${file ? 'file' : 'stream'}: every error is Vectro's`);
  }
  if (node) {
    const p = join(dir, 'damaged.vectro');
    engine.writeDataset(p, t, { rowsPerChunk: 20 });
    const good = readFileSync(p);
    const raw = new Set();
    const at = (i) => { const c = Buffer.from(good); c[i] ^= 0xff; writeFileSync(p, c); try { const ds = engine.openDataset(p); try { ds.toRows(); } finally { ds.close(); } } catch (e) { if (!/^vectro/.test(e.message)) raw.add(e.message.replace(/\d+/g, 'N').slice(0, 80)); } };
    const footer = good.length - 24 - Number(new DataView(good.buffer, good.byteOffset).getFloat64(good.length - 16, true));
    for (let i = footer; i < good.length; i++) at(i); // (the footer and the tail after it)
    same([...raw], [], 'damaged dataset footer: every error is Vectro\'s');
  }
}

// A stream's UTF-8 BOM cut over pushes (or TCP reads): [EF] then [BB BF] stayed in the first value
// (a CSV stream's first date was missing). A BOM in one piece was already dropped.
if (node) {
  const { SplitEngine } = await import('../src/node.mjs');
  const net = await import('node:net');
  const split = new SplitEngine({ workers: 1 });
  const row = Buffer.from('\uFEFF2024-01-01T00:00:00Z,EU,1\n2024-01-02T00:00:00Z,US,2\n');
  const want = [{ ts: new Date('2024-01-01T00:00:00Z'), region: 'EU', price: 1 }, { ts: new Date('2024-01-02T00:00:00Z'), region: 'US', price: 2 }];
  for (const cuts of [[1], [2], [1, 2], [3], [1, 1, 1, 1]]) {
    const s = await split.streamAsync({ format: 'csv', columns: { ts: 'date', region: 'category', price: 'number' } });
    let at = 0;
    for (const c of cuts) { await s.push(row.subarray(at, at + c)); at += c; }
    await s.push(row.subarray(at));
    await s.flushAsync();
    same(await s.toRowsAsync(), want, `CSV stream, a BOM pushed in pieces of ${cuts}`);
  }
  for (const cuts of [[1], [2], [1, 1]]) {
    const srv = net.createServer(async (sock) => {
      let at = 0;
      for (const c of cuts) { sock.write(row.subarray(at, at + c)); at += c; await new Promise((r) => setTimeout(r, 30)); }
      sock.end(row.subarray(at));
    });
    await new Promise((r) => srv.listen(0, '127.0.0.1', r));
    const s = await split.streamAsync({ format: 'csv', columns: { ts: 'date', region: 'category', price: 'number' } });
    await s.connect(0, { host: '127.0.0.1', port: srv.address().port });
    for (let k = 0; k < 100 && (await s.flushAsync()).rows < 2; k++) await new Promise((r) => setTimeout(r, 20));
    same(await s.toRowsAsync(), want, `CSV stream over TCP, a BOM in pieces of ${cuts}`);
    srv.close();
  }
  await split.close();
}

// Wrong CSV options: csvToDataset with pieceBytes: 0 never ended; type: Int32Array stored 2024 dates
// as 1969 without a word; columns: [], types: null failed inside the engine; sampleRows: 0 guessed
// types from no rows (and the error then said "in the first 1000 rows").
{
  const csv = 'a,b,c\n1,x,2024-01-01\n2,y,2024-01-02\n';
  const bad = [
    [{ type: Int32Array }, /type must be Float64Array or Float32Array/],
    [{ columns: [] }, /columns must be an array/],
    [{ columns: 'a' }, /columns must be an array/],
    [{ types: null }, /types must be an object/],
    [{ types: 'number' }, /types must be an object/],
    [{ types: ['number'] }, /types must be an object/],
    [{ sampleRows: 0 }, /sampleRows must be/],
    [{ sampleRows: -1 }, /sampleRows must be/],
    [{ sampleRows: NaN }, /sampleRows must be/],
  ];
  for (const [o, re] of bad) fails(() => engine.parseCSV(csv, o), re, `parseCSV ${str(o)}`);
  same(attempt(() => engine.parseCSV(csv, { sampleRows: 1, type: Float32Array, columns: ['a', 'b'], types: {} }).toRows()), [{ a: 1, b: 'x' }, { a: 2, b: 'y' }], 'good options still load');
  if (node) {
    const p = join(dir, 'opts.csv');
    writeFileSync(p, csv);
    for (const [o, re] of [...bad, [{ pieceBytes: 0 }, /pieceBytes must be/], [{ pieceBytes: -5 }, /pieceBytes must be/], [{ pieceBytes: NaN }, /pieceBytes must be/]]) {
      fails(() => engine.csvToDataset(p, join(dir, 'opts.vectro'), o), re, `csvToDataset ${str(o)}`);
    }
    same(attempt(() => engine.csvToDataset(p, join(dir, 'opts.vectro'), { pieceBytes: 2.5 }).rows), 2, 'csvToDataset pieceBytes 2.5 (whole bytes)');
    let err = null;
    try { await engine.csvToDatasetAsync(p, join(dir, 'opts.vectro'), { pieceBytes: 0 }); } catch (e) { err = e.message; }
    same(/pieceBytes must be/.test(err), true, 'csvToDatasetAsync pieceBytes: 0');
    const { SplitEngine } = await import('../src/node.mjs');
    const split = new SplitEngine({ workers: 1 });
    for (const [o, re] of bad) { let m = null; try { await split.parseCSVAsync(csv, o); } catch (e) { m = e.message; } same(re.test(m), true, `split parseCSVAsync ${str(o)}`); }
    await split.close();
  }
}

rmSync(dir, { recursive: true, force: true });
// R6-3.9 / 3.12 / 3.13: mixed line ends, an unterminated quote, a column named __proto__; R6-3.10 / 3.11: CSV streams
{
  const rows = (text) => engine.parseCSV(text).toRows();
  same(rows('a,b\r\n1,x\n2,y\r3,z\r\n'), [{ a: 1, b: 'x' }, { a: 2, b: 'y' }, { a: 3, b: 'z' }], 'mixed line ends: a lone CR ends a row');
  same(rows('a,b\r1,x\r\n2,y\n'), [{ a: 1, b: 'x' }, { a: 2, b: 'y' }], 'first line ends in CR, then CRLF and LF');
  same(rows('a,b\n1,"x\ry"\n'), [{ a: 1, b: 'x\ry' }], 'a CR inside quotes stays in the value');
  let msg = ''; try { rows('a,b\n1,"open\n2,x\n'); } catch (err) { msg = err.message; }
  same(/ends inside a quoted field/.test(msg), true, 'an unterminated quote is an error, not one swallowed value');
  same(Object.keys(rows('__proto__,b\n1,2\n')[0]), ['_proto_', 'b'], 'a CSV column named __proto__ is _proto_');
  if (MODE === 'node') {
    const { SplitEngine } = await import('../src/node.mjs');
    const se = new SplitEngine({ workers: 2 });
    const h = await se.streamAsync({ columns: { region: 'category', n: 'number' }, header: true });
    await h.push('region,n\nEU,1\nUS,'); await h.push('2\n');
    same(await (await h.snapshotAsync()).toRowsAsync(), [{ region: 'EU', n: 1 }, { region: 'US', n: 2 }], 'stream header: true leaves out the first line');
    const c = await se.streamAsync({ columns: { x: 'number' } });
    await c.push('1\r2\r'); await c.push('3\r');
    same(await (await c.snapshotAsync()).toRowsAsync(), [{ x: 1 }, { x: 2 }, { x: 3 }], 'a CSV stream with CR-only line ends');
    let m2 = ''; try { await se.streamAsync({ columns: { x: 'number' }, hedaer: true }); } catch (err) { m2 = err.message; }
    same(/doesn't take "hedaer"/.test(m2), true, 'streamAsync refuses an unknown option');
    await se.close();
  }
}
await engine.close();
console.log(`audit r6 loading: ${checks - failures}/${checks} checks passed`);
if (failures) process.exit(1);
