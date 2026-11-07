// Audit round 6 (run 3, getting data in and out): CSV headers named like
// Object.prototype properties (constructor, toString, valueOf, ...) made
// every CSV loader refuse the file, because the `types` lookup found the
// inherited function (vectro-qa-2026-11-07/run3).
import { Engine } from './test-mode.mjs';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
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

rmSync(dir, { recursive: true, force: true });
await engine.close();
console.log(`audit r6 loading: ${checks - failures}/${checks} checks passed`);
if (failures) process.exit(1);
