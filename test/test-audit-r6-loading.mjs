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

rmSync(dir, { recursive: true, force: true });
await engine.close();
console.log(`audit r6 loading: ${checks - failures}/${checks} checks passed`);
if (failures) process.exit(1);
