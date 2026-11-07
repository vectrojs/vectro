// Audit round 6 (run 2, dataframe API): the join options that 0.2.0 added
// (range, where, cross, how: 'right' | 'full'), checked against a plain-JS
// nested loop (vectro-qa-2026-11-07/run2). A right / full join on a text key
// with the same name on both sides showed the wrong label in the rows only
// the right table has (the right table's code read in the left table's
// dictionary). With a dataset on the left, right / full joins added the
// right rows without a match once per chunk; they are refused now.
import { Engine } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const str = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isFinite(x) ? String(x) : x));
const same = (a, b, msg) => { checks++; const x = str(a), y = str(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };
const fails = (f, msg) => { checks++; try { f(); failures++; console.log(`FAIL ${msg}: no error`); } catch (e) { if (/^vectro/.test(e.message)) return; failures++; console.log(`FAIL ${msg}: ${e.message}`); } };
const sorted = (rows) => rows.map((r) => str(r)).sort();

const engine = new Engine();
const a = engine.table({ k: ['x', 'y', 'z'], v: [1, 2, 3] });
const b = engine.table({ k: ['w', 'y', 'q', 'x'], w: [10, 20, 30, 40] });

// right / full join on a text key named the same on both sides: right-only rows keep their own label
const right = [{ k: 'x', v: 1, w: 40 }, { k: 'y', v: 2, w: 20 }, { k: 'w', v: NaN, w: 10 }, { k: 'q', v: NaN, w: 30 }];
const full = [{ k: 'x', v: 1, w: 40 }, { k: 'y', v: 2, w: 20 }, { k: 'z', v: 3, w: NaN }, { k: 'w', v: NaN, w: 10 }, { k: 'q', v: NaN, w: 30 }];
same(a.join(b, { on: 'k', how: 'right' }).toRows(), right, 'right join, text key');
same(a.join(b, { on: 'k', how: 'full' }).toRows(), full, 'full join, text key');
same(a.lazy().join(b, { on: 'k', how: 'full' }).toRows(), full, 'lazy full join, text key');
same(sorted(await a.lazy().join(b, { on: 'k', how: 'full' }).toRowsAsync()), sorted(full), 'async full join, text key');
same(a.join(b, { on: 'k', how: 'right', where: 'v < 5' }).toRows(), right, 'right join with where, text key');
same(a.join(b, { on: 'k', how: 'full', range: { left: 'v', right: 'w', op: '<' } }).toRows(), full, 'full join with range, text key');
same(engine.table({ k: ['x', 'y', 'z'], v: [1, 2, 3] }, { compress: true }).join(b, { on: 'k', how: 'full' }).toRows(), full, 'full join, compressed left');
// a label only the right table has, and one missing on the right
const c = engine.table({ k: ['名前', '', 'Größe', 'é'], w: [1, 2, 3, 4] });
same(a.join(c, { on: 'k', how: 'right' }).toRows(), [{ k: '名前', v: NaN, w: 1 }, { k: '', v: NaN, w: 2 }, { k: 'Größe', v: NaN, w: 3 }, { k: 'é', v: NaN, w: 4 }], 'right join, labels only on the right');
// numbers and two keys
const n1 = engine.table({ k: [1, 2, 3], s: ['a', 'b', 'c'], v: [1, 2, 3] }), n2 = engine.table({ k: [5, 2, 3], s: ['e', 'b', 'x'], w: [1, 2, 3] });
same(sorted(n1.join(n2, { on: ['k', 's'], how: 'full' }).toRows()), sorted([{ k: 2, s: 'b', v: 2, w: 2 }, { k: 1, s: 'a', v: 1, w: NaN }, { k: 3, s: 'c', v: 3, w: NaN }, { k: 5, s: 'e', v: NaN, w: 1 }, { k: 3, s: 'x', v: NaN, w: 3 }]), 'full join, two keys');

// datasets on the left: right / full give each right row without a match once (or are refused)
if (engine.writeDataset) {
  const dir = mkdtempSync(join(tmpdir(), 'r6df-'));
  try {
    engine.writeDataset(join(dir, 'a.vectro'), a, { rowsPerChunk: 1 });
    const ds = engine.openDataset(join(dir, 'a.vectro'));
    for (const how of ['right', 'full']) {
      const want = sorted(how === 'right' ? right : full);
      for (const opts of [{ on: 'k', how }, { on: 'k', how, where: 'v < 5' }, { on: [], how, cross: true, where: 'k == k_right' }]) {
        fails(() => ds.join(b, opts).toRows(), `dataset ${str(opts)}: refused`);
        checks++; try { await ds.lazy().join(b, opts).toRowsAsync(); failures++; console.log(`FAIL dataset async ${str(opts)}: no error`); } catch (e) { if (!/^vectro/.test(e.message)) { failures++; console.log(`FAIL dataset async ${str(opts)}: ${e.message}`); } }
      }
      // SQL does them on datasets (keys only)
      same(sorted(sql(`SELECT k, v, w FROM ds ${how.toUpperCase()} JOIN b USING (k)`, { ds, b }).toRows()), want, `dataset SQL ${how} join`);
    }
    // inner and left joins, and a table on the left with the dataset on the right, still work
    same(sorted(ds.join(b, { on: 'k', how: 'left' }).toRows()), sorted([full[0], full[1], full[2]]), 'dataset left join');
    same(sorted(a.lazy().join(ds.lazy(), { on: 'k', how: 'full' }).toRows()).length, 3, 'table full join a dataset (its rows read in)');
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

await engine.close();
console.log(`${checks - failures}/${checks} audit r6 dataframe checks passed`);
if (failures) process.exit(1);
