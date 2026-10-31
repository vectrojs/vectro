// countDistinct on wide ranges: the stride path and the partitioned path,
// against JavaScript sets, on several thread counts and forced splits.
import { Engine } from '../src/node.mjs';
let failures = 0, checks = 0;
const fail = (m) => { failures++; if (failures <= 20) console.log('FAIL', m); };
let seed = 7;
const rnd = () => { seed = (seed + 0x6d2b79f5) | 0; let x = Math.imul(seed ^ (seed >>> 15), 1 | seed); x = (x + Math.imul(x ^ (x >>> 7), 61 | x)) ^ x; return ((x ^ (x >>> 14)) >>> 0) / 4294967296; };
const N = 200_000;
const SPECIAL = [NaN, -0, 0, Infinity, -Infinity, 1e300, -1e300, 5e-324, 2 ** 53, -(2 ** 53), 0.1, -0.1];
const V = {
  stride: () => 1e9 + Math.floor(rnd() * 150_000) * 7919,
  strideNaN: () => (rnd() < 0.02 ? NaN : -5e9 + Math.floor(rnd() * 150_000) * 1000),
  offStride: null, // stride with one value off it, in a row the sample skips
  wide: () => Math.floor(rnd() * 2 ** 40),
  wideNaN: () => (rnd() < 0.05 ? NaN : rnd() < 0.05 ? -0 : Math.floor(rnd() * 2 ** 45) - 2 ** 44),
  floats: () => Math.round(rnd() * 1e7) / 100,
  specials: () => (rnd() < 0.3 ? SPECIAL[Math.floor(rnd() * SPECIAL.length)] : Math.floor(rnd() * 80_000) - 40_000.5),
  clusters: () => 2 ** [10, 30, 50][Math.floor(rnd() * 3)] + Math.floor(rnd() * 40_000),
  allNaN: () => NaN,
  huge: () => (rnd() < 0.5 ? 2 ** 60 : 0) + Math.floor(rnd() * 100_000) * 4096, // beyond 2^53 of lo: float keys
};
const cols = {};
for (const [k, g] of Object.entries(V)) if (g) cols[k] = Float64Array.from({ length: N }, g);
cols.offStride = Float64Array.from(cols.stride); cols.offStride[12345] += 1;
cols.region = Array.from({ length: N }, () => ['EU', 'US', 'APAC', 'LATAM', 'MEA'][Math.floor(rnd() * 5)]);
cols.bucket = Float64Array.from({ length: N }, () => Math.floor(rnd() * 300));       // many groups
cols.wideg = Float64Array.from({ length: N }, () => Math.floor(rnd() * 2000) * 1e7); // groups by the hash path
const keyOf = (v) => (Object.is(v, -0) ? 0 : v);
function ref(col, by) {
  const m = new Map();
  for (let i = 0; i < N; i++) { const g = by ? cols[by][i] : 0, v = cols[col][i]; if (!m.has(g)) m.set(g, new Set()); if (v === v) m.get(g).add(keyOf(v)); }
  let s = 0; for (const x of m.values()) s += x.size; return [m.size, s];
}
const SETUPS = [
  ['default', {}, () => {}],
  ['1 thread', { threads: 1 }, () => {}],
  ['3 threads', { threads: 3 }, () => {}],
  ['split again', {}, (e) => { e._partitionGroups = 0; e._partitionScratch = 64; e._partitionP = 16; }],
  ['no stride', {}, (e) => { e._noStride = true; }],
];
for (const [label, opts, setup] of SETUPS) {
  const e = new Engine(opts); setup(e);
  const t = e.table(cols, { compress: false }), tc = e.table(cols); // (plain and packed)
  for (const col of Object.keys(V)) for (const by of [null, 'region', 'bucket', 'wideg']) {
    const want = ref(col, by);
    for (const [tl, tt] of [['plain', t], ['packed', tc]]) {
      let got;
      if (!by) got = [1, tt.agg({ c: `countDistinct(${col})` }).c];
      else { const g = tt.groupBy(by).agg({ c: `countDistinct(${col})` }); got = [g.length, g.sum('c')]; g.dispose(); }
      checks++;
      if (got[0] !== want[0] || got[1] !== want[1]) fail(`${label} ${tl} countDistinct(${col}) by ${by}: ${got} want ${want}`);
    }
  }
  // filtered
  const f = t.filter('bucket < 100'), want = (() => { const s = new Set(); for (let i = 0; i < N; i++) if (cols.bucket[i] < 100 && cols.wide[i] === cols.wide[i]) s.add(cols.wide[i]); return s.size; })();
  checks++; const got = f.agg({ c: 'countDistinct(wide)' }).c; if (got !== want) fail(`${label} filtered: ${got} want ${want}`);
  e.close();
}
// float32 tables: values on a step, with one off it where v - lo rounds in
// float32 (in a row the step's sample skips), grouped and not, 1 thread and all
{
  let s = 77; const r = () => { s ^= s << 13; s ^= s >>> 17; s ^= s << 5; return (s >>> 0) / 2 ** 32; };
  for (const threads of [undefined, 1]) {
    const e = new Engine(threads ? { threads } : {});
    for (const [name, n, gen] of [
      ['even wide + odd trap', 2_000_000, (i) => (i === 0 ? -(2 ** 25) : i === 5 ? 0 : i === 1 ? 1 : 2 * Math.floor(r() * 1.5 * 2 ** 24) - 2 ** 25)],
      ['stride 6 trap', 2_000_000, (i) => (i === 0 ? -(2 ** 26) : i === 3 ? 9 : i === 5 ? 8 : 6 * Math.floor(r() * 2 ** 23) - 2 ** 26)],
      ['stride 1000 + NaN / -0', 500_000, (i) => (i % 97 === 0 ? NaN : i % 89 === 0 ? -0 : 1000 * Math.floor(r() * 30000))],
    ]) {
      const v = new Float64Array(n), g = new Float64Array(n);
      for (let i = 0; i < n; i++) { v[i] = Math.fround(gen(i)); g[i] = Math.floor(r() * 7); }
      const all = new Set(), per = Array.from({ length: 7 }, () => new Set());
      for (let i = 0; i < n; i++) if (v[i] === v[i]) { all.add(v[i] + 0); per[g[i]].add(v[i] + 0); }
      const t = e.table({ v, g }, { type: Float32Array });
      checks++; const got = t.agg({ c: 'countDistinct(v)' }).c; if (got !== all.size) fail(`f32 ${name} (threads ${threads}): ${got} want ${all.size}`);
      const res = t.groupBy('g').agg({ c: 'countDistinct(v)' }), gg = res.column('g'), cc = res.column('c');
      for (let j = 0; j < res.length; j++) { checks++; if (cc[j] !== per[gg[j]].size) fail(`f32 ${name} group ${gg[j]} (threads ${threads}): ${cc[j]} want ${per[gg[j]].size}`); }
      res.dispose(); t.dispose();
    }
    e.close();
  }
}
console.log(failures ? `${failures} of ${checks} checks FAILED` : `all ${checks} wide distinct checks passed`);
process.exit(failures ? 1 : 0);
