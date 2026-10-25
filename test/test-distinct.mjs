// Distinct counts: countDistinct (exact) and approxDistinct (exact on a table,
// a HyperLogLog sketch where results merge: split tables, streams, windows)
// against plain JavaScript sets.
import { Engine, SplitEngine, DistinctSketch } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 17;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const near = (got, want, tol) => Math.abs(got - want) <= tol * Math.max(want, 1) + 2;

function data(n, space) {
  const cols = { region: [], shop: [], cust: [], price: [], qty: [] };
  for (let i = 0; i < n; i++) {
    cols.region.push(pick(['EU', 'US', 'APAC', 'LATAM']));
    cols.shop.push(`s${Math.floor(rnd() * 40)}`);
    cols.cust.push(rnd() < 0.03 ? NaN : rnd() < 0.03 ? -0 : Math.floor(rnd() * space) * 0.5);
    cols.price.push(Math.floor(rnd() * 200));
    cols.qty.push(1 + Math.floor(rnd() * 5));
  }
  return cols;
}
const keyOf = (v) => (typeof v === 'string' ? `s${v}` : Object.is(v, -0) ? 0 : v);
function distinct(cols, valueOf, keep, groupKeys) {
  const n = cols.region.length, groups = new Map();
  for (let i = 0; i < n; i++) {
    if (!keep(i)) continue;
    const g = groupKeys.map((k) => cols[k][i]).join('|');
    if (!groups.has(g)) groups.set(g, new Set());
    const v = valueOf(i);
    if (v === v) groups.get(g).add(keyOf(v));
  }
  return groups;
}
const CASES = [
  // [aggregation arg, js value, filter text, js filter]
  ['cust', (c, i) => c.cust[i], null, () => true],
  ['region', (c, i) => c.region[i], null, () => true],
  ['price * qty', (c, i) => c.price[i] * c.qty[i], 'qty > 2', (c, i) => c.qty[i] > 2],
  ['cust', (c, i) => c.cust[i], "region == 'EU'", (c, i) => c.region[i] === 'EU'],
];

// ---- tables (exact, both names) -------------------------------------------------
for (const [label, make, opts] of [
  ['threads', () => new Engine(), {}], ['2 threads', () => new Engine({ threads: 2 }), {}], ['single thread', () => new Engine({ threads: 1 }), {}],
  ['interpreter', () => new PortableEngine({ jit: false }), {}], ['float32', () => new Engine(), { type: Float32Array }],
]) {
  const e = make();
  for (const [n, space] of [[0, 10], [2000, 300], [300_000, 120_000]]) {
    if (label === 'interpreter' && n > 10_000) continue;
    const cols = data(n, space);
    const t = e.table(cols, opts);
    for (const [arg, js, ftext, fjs] of CASES) {
      if (n === 0 && (arg === 'region' || ftext?.includes("'"))) continue; // (an empty text column looks like numbers)
      const v = ftext ? t.filter(ftext) : t;
      const whole = distinct(cols, (i) => js(cols, i), (i) => fjs(cols, i), []);
      const want = whole.get('')?.size ?? 0;
      const got = v.agg({ c: `countDistinct(${arg})`, a: `approxDistinct(${arg})`, n: 'count()' });
      checks++; if (got.c !== want || got.a !== want) fail(`${label} n=${n} countDistinct(${arg}) ${ftext ?? ''}: ${got.c}/${got.a}, want ${want}`);
      if (!n) continue;
      for (const keys of [['region'], ['region', 'shop']]) {
        const groups = distinct(cols, (i) => js(cols, i), (i) => fjs(cols, i), keys);
        const rows = v.groupBy(keys.length === 1 ? keys[0] : keys).agg({ c: `countDistinct(${arg})`, s: 'sum(qty)' }).toRows();
        checks++;
        const bad = rows.length !== groups.size ? `${rows.length} groups, want ${groups.size}` : rows.find((r) => r.c !== groups.get(keys.map((k) => r[k]).join('|'))?.size);
        if (bad) fail(`${label} n=${n} groupBy(${keys}) countDistinct(${arg}) ${ftext ?? ''}: ${typeof bad === 'string' ? bad : JSON.stringify(bad)}`);
      }
    }
    t.dispose();
  }
  await e.close();
}

// ---- split tables and streams ------------------------------------------------------
{
  const e = new SplitEngine({ workers: 3 });
  const cols = data(60_000, 20_000);
  const t = await e.tableAsync(cols);
  for (const [arg, js, ftext, fjs] of CASES) {
    const v = ftext ? t.filter(ftext) : t;
    const want = distinct(cols, (i) => js(cols, i), (i) => fjs(cols, i), []).get('')?.size ?? 0;
    const got = await v.aggAsync({ c: `countDistinct(${arg})`, a: `approxDistinct(${arg})` });
    checks++; if (got.c !== want) fail(`split countDistinct(${arg}) ${ftext ?? ''}: ${got.c}, want ${want}`);
    checks++; if (!near(got.a, want, 0.05)) fail(`split approxDistinct(${arg}) ${ftext ?? ''}: ${got.a}, want ~${want}`);
    const groups = distinct(cols, (i) => js(cols, i), (i) => fjs(cols, i), ['region']);
    const rows = (await v.groupBy('region').aggAsync({ c: `countDistinct(${arg})`, a: `approxDistinct(${arg})` })).toRows();
    checks++;
    if (rows.length !== groups.size || rows.some((r) => r.c !== groups.get(r.region).size || !near(r.a, groups.get(r.region).size, 0.06))) fail(`split groupBy countDistinct(${arg}): ${JSON.stringify(rows)}`);
  }
  // a stream: exact over the rows kept, sketches in a live window
  const s = await e.streamAsync({ format: 'csv', columns: { ts: 'number', region: 'category', cust: 'number' } });
  let csv = '';
  const sent = [];
  for (let i = 0; i < 40_000; i++) {
    const row = [1_700_000_000_000 + i * 25, pick(['EU', 'US', 'APAC']), Math.floor(rnd() * 8000)];
    sent.push(row);
    csv += row.join(',') + '\n';
  }
  await s.push(csv);
  await s.flushAsync();
  const per = new Map();
  for (const [, r, c] of sent) { if (!per.has(r)) per.set(r, new Set()); per.get(r).add(c); }
  const g = (await s.groupBy('region').aggAsync({ c: 'countDistinct(cust)' })).toRows();
  checks++; if (g.some((r) => r.c !== per.get(r.region).size)) fail(`stream countDistinct: ${JSON.stringify(g)}`);
  const w = await s.windowAsync({ by: 'region', every: 1000, last: 2_000_000, timeColumn: 'ts', agg: { n: 'count()', u: 'approxDistinct(cust)', v: 'approxDistinct(cust * 2)' } });
  const totals = await w.readAsync();
  checks++; if (totals.length !== 3 || totals.some((r) => !near(r.u, per.get(r.region).size, 0.05) || !near(r.v, per.get(r.region).size, 0.05))) fail(`window approxDistinct: ${JSON.stringify(totals)}`);
  // per slice (series): each second's distinct customers
  const series = await w.readAsync({ series: true });
  const bySlice = new Map();
  for (const [ts, r, c] of sent) { const k = `${Math.floor(ts / 1000) * 1000}|${r}`; if (!bySlice.has(k)) bySlice.set(k, new Set()); bySlice.get(k).add(c); }
  const off = series.filter((r) => !near(r.u, bySlice.get(`${r.t}|${r.region}`)?.size ?? -99, 0.05)); // (two values can share a register: ±1 on small slices)
  checks++; if (series.length !== bySlice.size || off.length) fail(`window series approxDistinct: ${series.length} vs ${bySlice.size}; ${JSON.stringify(off.slice(0, 3))} want ${off.slice(0, 3).map((r) => bySlice.get(`${r.t}|${r.region}`)?.size)}`);
  let msg = '';
  try { await s.windowAsync({ every: 1000, last: 10_000, timeColumn: 'ts', agg: { c: 'countDistinct(cust)' } }); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('approxDistinct')) fail(`window countDistinct accepted: ${msg}`);
  await e.close();
}

// ---- the sketch on its own --------------------------------------------------------------
{
  const a = new DistinctSketch(), b = new DistinctSketch();
  for (let i = 0; i < 200_000; i++) (i % 2 ? a : b).add(i % 5 ? i : `id${i}`);
  const merged = DistinctSketch.fromBytes(a.toBytes()).merge(b);
  checks++; if (!near(merged.estimate(), 200_000, 0.05)) fail(`sketch merge: ${merged.estimate()}`);
  const c = new DistinctSketch();
  for (const v of [1, 1, -0, 0, NaN, 'x', 'x', null]) c.add(v);
  checks++; if (c.estimate() !== 2 + 1) fail(`sketch small: ${c.estimate()}`); // 1, 0, 'x'
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} distinct-count checks passed`);
process.exit(failures ? 1 : 0);
