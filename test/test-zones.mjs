// Zone maps: filtered queries on time-ordered data skip blocks, and every
// answer still equals plain JavaScript (and the same query without skipping).
import { Engine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const close = (a, b) => JSON.stringify(a, (k, x) => (typeof x === 'number' ? (Number.isFinite(x) ? Number(x.toPrecision(10)) : String(x)) : x)) === JSON.stringify(b, (k, x) => (typeof x === 'number' ? (Number.isFinite(x) ? Number(x.toPrecision(10)) : String(x)) : x));

let seed = 41;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const N = 300_000;
const REG = ['APAC', 'EU', 'LATAM', 'US'];
const rows = Array.from({ length: N }, (_, i) => ({
  ts: 1_000_000 + i * 2 + Math.floor(rnd() * 3),          // rising, with small jitter
  price: rnd() < 0.01 ? NaN : Math.round(rnd() * 10000) / 100,
  qty: 1 + Math.floor(rnd() * 9),
  region: REG[Math.floor((i / N) * 4)],                      // sorted in four runs
  seq: i % 1000 === 0 ? -5 : i,                              // a few negative values
}));
const columns = (type) => ({
  ts: rows.map((r) => r.ts), price: rows.map((r) => r.price), qty: rows.map((r) => r.qty), region: rows.map((r) => r.region), seq: rows.map((r) => r.seq),
});
const T0 = 1_000_000, T1 = T0 + 2 * N;
// [filter, params, JS version, whether zone maps should skip rows]
const FILTERS = [
  ['ts >= t', { t: T1 - 20_000 }, (r, p) => r.ts >= p.t, true],
  ['ts > t && price > 50', { t: T1 - 60_000 }, (r, p) => r.ts > p.t && r.price > 50, true],
  ['t <= ts && ts < u', { t: T0 + 100_000, u: T0 + 140_000 }, (r, p) => p.t <= r.ts && r.ts < p.u, true],
  ['ts < 1000500', {}, (r) => r.ts < 1000500, true],
  ["region == 'LATAM' && qty >= 5", {}, (r) => r.region === 'LATAM' && r.qty >= 5, true],
  ['ts == t', { t: T0 + 2 * 123_456 }, (r, p) => r.ts === p.t, true],
  ['ts >= t || price > 99', { t: T1 - 20_000 }, (r, p) => r.ts >= p.t || r.price > 99, false],  // OR: no skipping
  ['price > 99.5', {}, (r) => r.price > 99.5, false],                                          // random column: no skipping
  ['ts > t', { t: T1 + 10 }, () => false, true],                                                 // nothing matches
  ['seq < -1 && ts > t', { t: T0 }, (r, p) => r.seq < -1 && r.ts > p.t, false],
  ['-ts <= -t', { t: T1 - 30_000 }, (r, p) => -r.ts <= -p.t, false],
];
const ref = (keep) => {
  const sel = rows.filter(keep);
  const prices = sel.map((r) => r.price);
  return {
    agg: { n: sel.length, rev: sel.reduce((a, r) => a + (r.price * r.qty === r.price * r.qty ? r.price * r.qty : 0), 0), lo: sel.length ? Math.min(...sel.map((r) => r.qty)) : NaN, hi: sel.length ? Math.max(...sel.map((r) => r.qty)) : NaN, avg: sel.length ? sel.reduce((a, r) => a + r.qty, 0) / sel.length : NaN },
    groups: REG.map((g) => ({ region: g, n: sel.filter((r) => r.region === g).length })).filter((g) => g.n),
    first: sel.slice(0, 5).map((r) => r.ts),
    count: sel.length, prices,
  };
};

for (const [name, make] of [['threads', () => new Engine()], ['interpreter', () => new PortableEngine({ jit: false })]]) {
  const engine = make();
  const t = engine.table(columns());
  for (const [f, p, js, skips] of FILTERS) {
    const L = `${name} ${f}`;
    const want = ref((r) => js(r, p));
    const before = engine._zoneSkips ?? 0;
    const v = t.filter(f, p);
    const got = {
      agg: v.agg({ n: 'count()', rev: 'sum(price * qty)', lo: 'min(qty)', hi: 'max(qty)', avg: 'mean(qty)' }),
      groups: v.groupBy('region').agg({ n: 'count()' }).toRows(),
      first: v.toRows(5).map((r) => r.ts),
      count: v.count(),
    };
    checks++;
    if (!close(got.agg, want.agg)) fail(`${L} agg ${JSON.stringify(got.agg)} vs ${JSON.stringify(want.agg)}`);
    checks++; if (!close(got.groups, want.groups)) fail(`${L} groups ${JSON.stringify(got.groups)} vs ${JSON.stringify(want.groups)}`);
    checks++; if (!close(got.first, want.first)) fail(`${L} first rows ${got.first} vs ${want.first}`);
    checks++; if (got.count !== want.count) fail(`${L} count ${got.count} vs ${want.count}`);
    const c = v.collect();
    checks++; if (c.length !== want.count) fail(`${L} collect ${c.length}`);
    c.dispose();
    checks++;
    const skipped = (engine._zoneSkips ?? 0) > before;
    if (skipped !== skips) fail(`${L}: zone maps ${skipped ? 'skipped' : 'did not skip'} rows`);
  }
  t.dispose();

  // float32: a boundary that isn't exact in float32 (filter and zones must round alike)
  const vals = Float32Array.from({ length: 200_000 }, (_, i) => i / 10);
  const f = engine.table({ x: vals }, { type: Float32Array });
  for (const lim of [100.1, 7777.7, 19999.9]) {
    for (const [op, js] of [['>=', (a, b) => a >= b], ['>', (a, b) => a > b], ['<=', (a, b) => a <= b], ['<', (a, b) => a < b], ['==', (a, b) => a === b]]) {
      const want = vals.filter((x) => js(x, Math.fround(lim))).length;
      checks++;
      const got = f.filter(`x ${op} ${lim}`).count();
      if (got !== want) fail(`${name} float32 x ${op} ${lim}: ${got} vs ${want}`);
    }
  }
  f.dispose();
  await engine.close();
}
// streams: zone maps kept as batches arrive, rebuilt when the buffer compacts
{
  const { SplitEngine } = await import('../src/node.mjs');
  for (const workers of [1, 2]) {
    const split = new SplitEngine({ workers });
    const keepRows = 150_000, perWorker = Math.ceil(keepRows / workers);
    const s = await split.streamAsync({ columns: { ts: 'number', price: 'number', region: 'category' }, keepRows });
    const all = Array.from({ length: 400_000 }, (_, i) => ({ ts: 5_000_000 + i, price: Math.round(rnd() * 1000) / 10, region: REG[i % 4] }));
    const chunks = [];
    for (let i = 0; i < all.length; i += 5_000) chunks.push(all.slice(i, i + 5_000));
    const byWorker = Array.from({ length: workers }, () => []);
    chunks.forEach((c, k) => byWorker[k % workers].push(...c));
    for (const c of chunks) await s.push(c.map((r) => `${r.ts},${r.price},${r.region}`).join('\n') + '\n');
    await s.flushAsync();
    const kept = byWorker.flatMap((list) => list.slice(-perWorker));
    for (const [f, p, js] of [
      ['ts >= t', { t: 5_000_000 + 390_000 }, (r, q) => r.ts >= q.t],
      ['ts >= a && ts < b', { a: 5_000_000 + 300_000, b: 5_000_000 + 320_000 }, (r, q) => r.ts >= q.a && r.ts < q.b],
      ["ts < t && region == 'EU'", { t: 5_000_000 + 280_000 }, (r, q) => r.ts < q.t && r.region === 'EU'],
      ['ts < t', { t: 1 }, () => false],
    ]) {
      const sel = kept.filter((r) => js(r, p));
      const want = { n: sel.length, s: Number(sel.reduce((a, r) => a + r.price, 0).toPrecision(10)) };
      const got = await s.filter(f, p).aggAsync({ n: 'count()', s: 'sum(price)' });
      checks++;
      if (got.n !== want.n || Number(got.s.toPrecision(10)) !== want.s) fail(`stream w=${workers} ${f}: ${JSON.stringify(got)} vs ${JSON.stringify(want)}`);
    }
    await split.close();
  }
}

// tables made from other tables' rows (sort, collect, top) get new zone maps,
// not copies of the old ones (a stale copy skipped blocks that match)
{
  const e = new Engine();
  const n = 200_000;
  const t = e.table({ ts: Float64Array.from({ length: n }, (_, i) => i), v: Float64Array.from({ length: n }, (_, i) => (i * 7919) % 1000) });
  t.filter('ts < 1000').count(); // builds zone maps on ts
  const sorted = t.sort('-ts'), kept = t.filter('v >= 500').collect(), best = t.top(150_000, '-ts');
  const want = (rows) => rows.filter((r) => r.ts < 1000).length;
  for (const [label, x] of [['sort', sorted], ['collect', kept], ['top', best]]) {
    checks++;
    const got = x.filter('ts < 1000').count();
    if (got !== want(x.toRows())) fail(`${label}: stale zone maps (${got} rows)`);
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} zone-map checks passed`);
process.exit(failures ? 1 : 0);
