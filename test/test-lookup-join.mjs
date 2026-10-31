// Join + GROUP BY right columns on datasets (the lookup path) give what the
// same query gives in memory (the usual join path), on threads and one thread.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, sql } from '../src/node.mjs';
import { writeDataset } from '../src/dataset.mjs';
import { Table } from '../src/table.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 30) console.log('FAIL', msg); } };
const norm = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' ? (v !== v ? 'NaN' : Number(v.toPrecision(12))) : v));
const same = (a, b, msg) => { const x = norm(a), y = norm(b); check(x === y, `${msg}\n  got  ${x.slice(0, 400)}\n  want ${y.slice(0, 400)}`); };
let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const dir = mkdtempSync(join(tmpdir(), 'vectro-lk-'));
const N = 50_000;
const o = {
  cust: Float64Array.from({ length: N }, (_, i) => (i % 97 === 0 ? NaN : i % 89 === 0 ? 2.5 : Math.floor(rnd() * 3300) - 100)), // NaN, fractions, keys the right lacks (<0, >=3000)
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  price: Float64Array.from({ length: N }, () => (rnd() < 0.05 ? NaN : Math.round(rnd() * 10000) / 100)),
  region: Array.from({ length: N }, () => ['EU', 'US', 'APAC'][Math.floor(rnd() * 3)]),
};
const M = 3000;
const right = {
  cust: Float64Array.from({ length: M }, (_, i) => (i === 17 ? NaN : i)),
  tier: Float64Array.from({ length: M }, (_, i) => (i % 50 === 0 ? NaN : 1 + (i % 3))),
  seg: Array.from({ length: M }, (_, i) => ['gold', 'silver', 'bronze', 'tin'][i % 4]),
  score: Float64Array.from({ length: M }, (_, i) => (i * 7919) % 1000 / 10), // decimals: hash numbering
  qty: Float64Array.from({ length: M }, (_, i) => i % 5), // clashes with a left name: qty_right
  big: Float64Array.from({ length: M }, (_, i) => i * 1e9), // groups by a sparse key
};
const QUERIES = [
  'SELECT c.tier, COUNT(*) AS n, SUM(o.qty) AS q FROM o JOIN c ON o.cust = c.cust WHERE o.qty = 9 GROUP BY c.tier ORDER BY c.tier',
  'SELECT c.tier, COUNT(*) AS n, SUM(o.price) AS s, AVG(o.price) AS a, MIN(o.price) AS lo, MAX(o.price) AS hi, COUNT(o.price) AS np FROM o JOIN c ON o.cust = c.cust GROUP BY c.tier ORDER BY c.tier',
  'SELECT c.seg, COUNT(*) AS n, SUM(o.qty * o.price) AS r FROM o JOIN c ON o.cust = c.cust WHERE o.region = \'EU\' GROUP BY c.seg ORDER BY c.seg',
  'SELECT c.seg, c.tier, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust GROUP BY c.seg, c.tier ORDER BY c.seg, c.tier',
  'SELECT c.score, COUNT(*) AS n, SUM(o.qty) AS q FROM o JOIN c ON o.cust = c.cust GROUP BY c.score ORDER BY c.score',
  'SELECT c.big, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust WHERE o.qty > 7 GROUP BY c.big ORDER BY n DESC, c.big LIMIT 10',
  'SELECT c.cust AS k, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust GROUP BY c.cust ORDER BY n DESC, k LIMIT 10',
  'SELECT c.qty, COUNT(*) AS n, SUM(o.qty) AS q FROM o JOIN c ON o.cust = c.cust GROUP BY c.qty ORDER BY c.qty',
  'SELECT c.tier, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust WHERE c.tier = 2 AND o.qty < 3 GROUP BY c.tier',
  'SELECT c.tier, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust WHERE o.qty > 100 GROUP BY c.tier',
  'SELECT c.tier, o.region, COUNT(*) AS n FROM o JOIN c ON o.cust = c.cust GROUP BY c.tier, o.region ORDER BY 1, 2', // (a left key: the usual path)
  'SELECT c.tier, COUNT(DISTINCT o.region) AS d FROM o JOIN c ON o.cust = c.cust GROUP BY c.tier ORDER BY 1', // (distinct: the usual path)
  'SELECT c.tier, COUNT(*) AS n FROM o LEFT JOIN c ON o.cust = c.cust GROUP BY c.tier ORDER BY 1', // (left join: the usual path)
  'SELECT c.tier, SUM(o.qty) AS q FROM o JOIN d AS c ON o.cust = c.cust GROUP BY c.tier ORDER BY 1', // (repeated right keys: the usual path)
  'SELECT c.tier, SUM(o.qty) AS q FROM o JOIN s AS c ON o.cust = c.cust GROUP BY c.tier ORDER BY 1', // (sparse right keys: the usual path)
];
let hits = 0;
const orig = Table.prototype._lookupGroupAggAsync, origS = Table.prototype._lookupGroupAgg;
Table.prototype._lookupGroupAggAsync = async function (...a) { const r = await orig.apply(this, a); if (r) hits++; return r; };
Table.prototype._lookupGroupAgg = function (...a) { const r = origS.apply(this, a); if (r) hits++; return r; };
for (const [label, opts] of [['threads', {}], ['single thread', { threads: 1 }]]) {
  for (const bitPack of [true, false]) {
    const e = new Engine(opts);
    const t = e.table(o), c = e.table(right);
    const dup = e.table({ cust: Float64Array.from({ length: 2 * M }, (_, i) => i % M), tier: Float64Array.from({ length: 2 * M }, (_, i) => i % 4) });
    const sp = e.table({ cust: Float64Array.from({ length: M }, (_, i) => i * 1e7), tier: Float64Array.from({ length: M }, (_, i) => i % 4) });
    const path = join(dir, `o-${label}-${bitPack}.vectro`);
    writeDataset(e, path, t, { rowsPerChunk: 4096, bitPack });
    const ds = e.openDataset(path);
    for (const q of QUERIES) {
      const want = sql(q, { o: t, c, d: dup, s: sp }).toRows();
      same(sql(q, { o: ds, c, d: dup, s: sp }).toRows(), want, `${label} pack=${bitPack} sync: ${q}`);
      same(await sql(q, { o: ds, c, d: dup, s: sp }).toRowsAsync(), want, `${label} pack=${bitPack} async: ${q}`);
    }
    // lazy API, a derived left value and a filter above the join
    const lz = (src) => src.lazy().derive('rev', 'price * qty').join(c, { on: 'cust' }).groupBy('seg').agg({ r: 'sum(rev)', n: 'count()' }).toRows();
    same(lz(ds), lz(t), `${label} pack=${bitPack} lazy derived`);
    await ds.close?.();
    e.close?.();
  }
}
check(hits > 0, `the lookup path ran (${hits} times)`);
rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks (lookup hits ${hits})` : `all ${checks} lookup-join checks passed (lookup hits ${hits})`);
process.exit(failures ? 1 : 0);
