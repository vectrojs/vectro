// Group-by with many groups, partitioned by key range (see partition.mjs):
// the same groups, in the same order, with the same totals as plain
// JavaScript (sums added in row order, so exactly), however the keys are
// spread: partitions split again (and again) when they hold too many groups,
// several key words, every kind of key value, packed columns, filters and
// parameters, on any number of threads; and the hash path where this one
// doesn't apply (more key words than it takes, too little memory).
import { Engine } from '../src/node.mjs';
import { estimateGroups } from '../src/partition.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 2024; // (mulberry32: every value of a range shows up)
const rnd = () => { seed = (seed + 0x6d2b79f5) | 0; let x = Math.imul(seed ^ (seed >>> 15), 1 | seed); x = (x + Math.imul(x ^ (x >>> 7), 61 | x)) ^ x; return ((x ^ (x >>> 14)) >>> 0) / 4294967296; };
const N = 120_000;
const SPECIAL = [NaN, -0, 0, Infinity, -Infinity, 1e300, -1e300, 5e-324, -5e-324, 2 ** 53, -(2 ** 53), 2 ** 53 + 2, 0.1, -0.1, 1, -1];

// key columns: how their values are spread
const DIST = {
  ids: () => 1e9 + Math.floor(rnd() * 60_000) * 7919,                   // whole numbers, wide range
  heavy: () => (rnd() < 0.5 ? 42.5 : Math.floor(rnd() * 1e6) / 4),      // one key in half the rows
  clusters: () => 2 ** [10, 30, 50][Math.floor(rnd() * 3)] + Math.floor(rnd() * 20_000), // far apart
  bimodal: () => (rnd() < 0.5 ? rnd() : 1e6 + rnd()),                   // fractions, two lumps
  specials: () => (rnd() < 0.3 ? SPECIAL[Math.floor(rnd() * SPECIAL.length)] : Math.floor(rnd() * 40_000) - 20_000.5),
  two: () => (rnd() < 0.5 ? -7.25 : 3.5),                               // two values (fractional)
  small: () => Math.floor(rnd() * 5) - 2,                               // whole, tiny range
  nanInts: () => (rnd() < 0.05 ? NaN : Math.floor(rnd() * 90_000)),     // whole numbers and NaN
};
const cols = {};
for (const [name, gen] of Object.entries(DIST)) cols[name] = Float64Array.from({ length: N }, gen);
cols.sorted = Float64Array.from({ length: N }, (_, i) => i * 0.5);       // every row its own group
cols.reversed = Float64Array.from({ length: N }, (_, i) => (N - i) * -1.5);
cols.one = new Float64Array(N).fill(0.25);                               // one group
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
cols.region = Array.from({ length: N }, () => REG[Math.floor(rnd() * REG.length)]);
cols.day = Array.from({ length: N }, () => new Date(Date.UTC(2020, 0, 1) + Math.floor(rnd() * 3000) * 86_400_000 + Math.floor(rnd() * 86_400_000))); // (not whole seconds: not numbered by time steps)
cols.price = Float64Array.from({ length: N }, () => (rnd() < 0.03 ? NaN : Math.round(rnd() * 1e6) / 100));
cols.qty = Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9));
const regionRank = new Map();
for (const r of cols.region) if (!regionRank.has(r)) regionRank.set(r, regionRank.size); // (text keys: dictionary order)

// plain JavaScript: groups sorted by key (text in dictionary order, NaN
// last, -0 as 0), sums in row order
const keyOf = (name, i) => { const v = name === 'day' ? cols.day[i].getTime() : cols[name][i]; return name === 'region' ? v : Object.is(v, -0) ? 0 : v; };
function reference(keys, keep = () => true) {
  const groups = new Map();
  for (let i = 0; i < N; i++) {
    if (!keep(i)) continue;
    const tuple = keys.map((k) => keyOf(k, i));
    const id = tuple.map((v) => (typeof v === 'number' && v !== v ? 'NaN' : String(v))).join('|');
    let g = groups.get(id);
    if (!g) groups.set(id, (g = { tuple, n: 0, s: 0, sp: 0, np: 0, lo: Infinity, hi: -Infinity, big: 0 }));
    const p = cols.price[i], q = cols.qty[i];
    g.n++;
    if (p === p) { g.s += p * q; g.sp += p; g.np++; if (p < g.lo) g.lo = p; if (p > g.hi) g.hi = p; }
    if (p > 5000) g.big++;
  }
  const cmp = (x, y, k) => {
    if (k === 'region') return regionRank.get(x) - regionRank.get(y);
    if (x === y) return 0;
    if (x !== x) return y !== y ? 0 : 1;
    if (y !== y) return -1;
    return x < y ? -1 : 1;
  };
  return [...groups.values()].sort((a, b) => { for (let k = 0; k < keys.length; k++) { const c = cmp(a.tuple[k], b.tuple[k], keys[k]); if (c) return c; } return 0; });
}
const SPEC = { n: 'count()', s: 'sum(price * qty)', m: 'mean(price)', lo: 'min(price)', hi: 'max(price)', c: 'count(price)', big: 'count(price > 5000)' };
const same = (a, b) => Object.is(a, b) || (a !== a && b !== b);
const near = (a, b) => same(a, b) || Math.abs(a - b) <= 1e-12 * Math.max(1, Math.abs(b)); // (the hash path's per-thread totals: not in row order)
function compare(label, got, want, keys, eq = same) {
  checks++;
  if (got.length !== want.length) return fail(`${label}: ${got.length} groups, want ${want.length}`);
  for (let g = 0; g < want.length; g++) {
    const r = got[g], w = want[g];
    const keyBad = keys.findIndex((k, j) => (k === 'region' ? r[k] !== w.tuple[j] : k === 'day' ? r[k].getTime() !== w.tuple[j] : !same(r[k], w.tuple[j])));
    const vals = { n: w.n, s: w.s, m: w.np ? w.sp / w.np : NaN, lo: w.np ? w.lo : NaN, hi: w.np ? w.hi : NaN, c: w.np, big: w.big };
    const valBad = Object.keys(vals).find((k) => !eq(r[k], vals[k]));
    if (keyBad >= 0 || valBad) return fail(`${label}: group ${g} ${JSON.stringify(r)} want keys ${JSON.stringify(w.tuple)} ${JSON.stringify(vals)} (${keyBad >= 0 ? `key ${keys[keyBad]}` : valBad})`);
  }
}

const KEYSETS = [
  ['ids'], ['heavy'], ['clusters'], ['bimodal'], ['specials'], ['sorted'], ['reversed'], ['one'], ['nanInts'], ['day'],
  ['region', 'ids'], ['ids', 'region'], ['small', 'nanInts', 'region'],   // one word
  ['two', 'sorted'],                                                     // two words, the first with two values
  ['specials', 'bimodal'], ['region', 'specials', 'heavy', 'bimodal'],   // 3-4 words
  ['specials', 'bimodal', 'heavy', 'sorted', 'reversed'],                // 5 words: the hash path
];
for (const [label, opts] of [['threads', {}], ['one thread', { threads: 1 }], ['3 threads', { threads: 3 }]]) {
  const e = new Engine(opts);
  const t = e.table(cols);
  for (const [mode, setup, eq] of [
    ['', () => { e._partitionGroups = 0; }], // (even few groups)
    ['split again', () => { e._partitionGroups = 0; e._partitionScratch = 64; e._partitionP = 16; }], // partitions hold 64 groups: most split, some several times
    ['routed', () => {}, near], // as chosen: few groups (or more key words than this path takes) on the hash path
  ]) {
    setup();
    for (const keys of KEYSETS) compare(`${label} ${mode} ${keys}`, t.groupBy(keys).agg(SPEC).toRows(), reference(keys), keys, eq);
    e._partitionGroups = e._partitionScratch = e._partitionP = undefined;
  }
  // a filter (the matching rows are collected first) and the async API
  e._partitionGroups = 0;
  compare(`${label} filter`, (await t.filter('qty > 3').groupBy(['region', 'specials']).aggAsync(SPEC)).toRows(), reference(['region', 'specials'], (i) => cols.qty[i] > 3), ['region', 'specials']);
  e._partitionGroups = undefined;
  // the hash path, where this one would need too much memory: the same groups
  e._partitionBytes = 0;
  compare(`${label} no room`, t.groupBy(['ids', 'region']).agg(SPEC).toRows(), reference(['ids', 'region']), ['ids', 'region'], near);
  e._partitionBytes = undefined;
  await e.close();
}

{
  const e = new Engine();
  const t = e.table(cols);
  e._partitionGroups = 0; // (this path whatever the number of groups)
  // parameters in the aggregations, and more aggregations than one kernel of the hash path takes
  const many = Object.fromEntries(Array.from({ length: 12 }, (_, j) => [`a${j}`, `sum(qty * ${j} + price * k)`]));
  const g = t.groupBy('ids').agg({ ...many, mx: 'max(qty - k)' }, { k: 3 }).toRows(), want = new Map();
  for (let i = 0; i < N; i++) {
    const w = want.get(cols.ids[i]) ?? want.set(cols.ids[i], { a: Array(12).fill(0), mx: -Infinity }).get(cols.ids[i]);
    for (let j = 0; j < 12; j++) { const v = cols.qty[i] * j + cols.price[i] * 3; w.a[j] += v === v ? v : 0; }
    w.mx = Math.max(w.mx, cols.qty[i] - 3);
  }
  checks++;
  const bad = g.length !== want.size ? `${g.length} groups` : g.find((r) => { const w = want.get(r.ids); return !w || r.mx !== w.mx || w.a.some((x, j) => !same(r[`a${j}`], x)); });
  if (bad) fail(`12 sums with a parameter: ${JSON.stringify(bad)}`);
  // packed columns (compress): read packed by the kernels
  const p = e.table({ ids: cols.ids, nanInts: cols.nanInts, region: cols.region, price: cols.price, qty: cols.qty, specials: cols.specials });
  p.compress();
  for (const keys of [['ids'], ['region', 'nanInts'], ['specials', 'region']]) compare(`packed ${keys}`, p.groupBy(keys).agg(SPEC).toRows(), reference(keys), keys);
  // text codes with no label (NaN, as a join's rows without a match have them): last
  const x = e.table({ region: cols.region, ids: cols.ids, price: cols.price, qty: cols.qty }, { compress: false }); // (codes as numbers, to write NaN into)
  const codes = e.live(x.cols.get('region').data);
  for (let i = 0; i < N; i += 7) codes[i] = NaN;
  const got = x.groupBy(['region', 'ids']).agg({ n: 'count()' }).toRows();
  const wantN = new Map();
  for (let i = 0; i < N; i++) { const k = `${i % 7 === 0 ? 'NaN' : cols.region[i]}|${cols.ids[i]}`; wantN.set(k, (wantN.get(k) ?? 0) + 1); }
  checks++;
  const firstMissing = got.findIndex((r) => r.region == null);
  if (got.length !== wantN.size || got.some((r) => wantN.get(`${r.region ?? 'NaN'}|${r.ids}`) !== r.n) || firstMissing < 0 || got.slice(firstMissing).some((r) => r.region != null)) fail(`text codes with no label: ${got.length} groups, want ${wantN.size}`);
  // memory running out partway (the records, 16+ bytes a row, fail): the hash path instead, nothing left allocated
  {
    const alloc = e.alloc.bind(e), before = e.sizes.size;
    e.alloc = (Type, len) => { if (Type === Uint8Array && len >= 12 * N) throw new RangeError('vectro: out of memory (4GB limit)'); return alloc(Type, len); };
    const g = t.groupBy(['ids', 'region']).agg(SPEC);
    e.alloc = alloc;
    compare('out of memory', g.toRows(), reference(['ids', 'region']), ['ids', 'region'], near);
    g.dispose();
    checks++;
    if (e.sizes.size !== before) fail(`out of memory: ${e.sizes.size - before} arrays still allocated`);
  }
  // keys at nested scales: every split peels off one, deeper than the limit (then the hash path)
  {
    const M = 60_000, nest = () => { for (const b of [53, 37, 21, 5]) if (rnd() < 0.02) return Math.floor(rnd() * 2 ** b); return 0; };
    const k = { a: Float64Array.from({ length: M }, nest), b: Float64Array.from({ length: M }, nest), c: Float64Array.from({ length: M }, () => Math.floor(rnd() * 2 ** 37)) };
    const want = new Map();
    for (let i = 0; i < M; i++) { const id = `${k.a[i]}|${k.b[i]}|${k.c[i]}`; want.set(id, (want.get(id) ?? 0) + 1); }
    for (const scratch of [undefined, 16]) {
      e._partitionScratch = scratch;
      const g = e.table(k).groupBy(['a', 'b', 'c']).agg({ n: 'count()' }).toRows();
      checks++;
      const sorted = g.every((r, i) => i === 0 || g[i - 1].a < r.a || (g[i - 1].a === r.a && (g[i - 1].b < r.b || (g[i - 1].b === r.b && g[i - 1].c < r.c))));
      if (g.length !== want.size || !sorted || g.some((r) => want.get(`${r.a}|${r.b}|${r.c}`) !== r.n)) fail(`nested scales (scratch ${scratch}): ${g.length} groups, want ${want.size}, sorted ${sorted}`);
    }
    e._partitionScratch = undefined;
  }
  // tables just past the threshold, and no rows at all (forced on)
  e._partitionRows = e._partitionGroups = 0;
  compare('no rows', t.filter('qty > 100').groupBy('ids').agg(SPEC).toRows(), [], ['ids']);
  compare('one row', t.filter('sorted == 7').groupBy('ids').agg(SPEC).toRows(), reference(['ids'], (i) => cols.sorted[i] === 7), ['ids']);
  e._partitionRows = undefined;
  // the same sums on any number of threads (row order), unlike per-thread totals
  const sums = [];
  for (const threads of [1, 2, 5, 8]) {
    const e2 = new Engine({ threads });
    e2._partitionGroups = 0;
    sums.push(JSON.stringify(e2.table(cols).groupBy('heavy').agg({ s: 'sum(price * 1.1)' }).toRows()));
    await e2.close();
  }
  checks++;
  if (new Set(sums).size !== 1) fail('sums differ between thread counts');
  await e.close();
}
// the group estimate on runs of the same sorted keys (partial groups put
// together): not one key per run seen
{
  const e = new Engine();
  for (const runs of [2, 3, 8]) {
    const G = 100_000, k = new Float64Array(G * runs);
    for (let r = 0; r < runs; r++) for (let i = 0; i < G; i++) k[r * G + i] = i;
    const t = e.table({ k }, { compress: false }), est = estimateGroups(t, ['k']);
    checks++;
    if (!(est >= G / 4 && est <= G * runs)) fail(`estimate over ${runs} sorted runs of ${G} keys: ${est}`);
    t.dispose();
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} partitioned group-by checks passed`);
process.exit(failures ? 1 : 0);
