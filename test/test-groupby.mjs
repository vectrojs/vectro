// groupBy on any keys: text, any numbers (negative, fractions, big IDs, NaN,
// -0) and several keys at once, with and without filters, on every engine.
// Every result must equal plain JavaScript, in the documented order.
import { Engine, SplitEngine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 99;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];

const N = 30_000;
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const rows = Array.from({ length: N }, (_, i) => ({
  region: pick(REG),
  shop: `shop${Math.floor(rnd() * 300)}`,
  small: Math.floor(rnd() * 7),                                  // direct path
  id: 1e12 + Math.floor(rnd() * 2000) * 7919,                   // big IDs: hash path
  score: pick([-2.5, -1, -0, 0, 0.5, 3.25, NaN, 1e300, -Infinity]), // fractions, NaN, -0
  uniq: i * 3 + 0.5,                                             // a group per row
  price: Math.round(rnd() * 1e4) / 100,
  qty: 1 + Math.floor(rnd() * 9),
}));
const cols = Object.fromEntries(Object.keys(rows[0]).map((k) => [k, rows.map((r) => r[k])]));
const firstSeen = (name) => { const m = new Map(); for (const r of rows) if (!m.has(r[name])) m.set(r[name], m.size); return m; };
const textRank = { region: firstSeen('region'), shop: firstSeen('shop') };

function reference(keys, keep, spec) {
  const groups = new Map();
  for (const r of rows) {
    if (!keep(r)) continue;
    const tuple = keys.map((k) => (Object.is(r[k], -0) ? 0 : r[k]));
    const id = JSON.stringify(tuple.map((v) => (Number.isNaN(v) ? 'NaN' : v)));
    let g = groups.get(id);
    if (!g) groups.set(id, (g = { tuple, rows: [] }));
    g.rows.push(r);
  }
  const cmp = (x, y, k) => {
    if (textRank[k]) { x = textRank[k].get(x); y = textRank[k].get(y); }
    if (x === y) return 0;
    if (Number.isNaN(x)) return Number.isNaN(y) ? 0 : 1;
    if (Number.isNaN(y)) return -1;
    return x < y ? -1 : 1;
  };
  const list = [...groups.values()].sort((a, b) => { for (let k = 0; k < keys.length; k++) { const c = cmp(a.tuple[k], b.tuple[k], keys[k]); if (c) return c; } return 0; });
  return list.map((g) => {
    const out = {};
    keys.forEach((k, i) => { out[k] = g.tuple[i]; });
    for (const [name, fn] of Object.entries(spec)) out[name] = fn(g.rows);
    return out;
  });
}
const SPEC = { n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)', big: 'count(price > 50)' };
const JS = {
  n: (rs) => rs.length,
  rev: (rs) => rs.reduce((a, r) => a + r.price * r.qty, 0),
  avg: (rs) => rs.reduce((a, r) => a + r.price, 0) / rs.length,
  lo: (rs) => Math.min(...rs.map((r) => r.price)),
  hi: (rs) => Math.max(...rs.map((r) => r.qty)),
  big: (rs) => rs.filter((r) => r.price > 50).length,
};
const close = (a, b) => (typeof a === 'number' && typeof b === 'number' ? Object.is(a, b) || (a === 0 && b === 0) || Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)) || (Number.isNaN(a) && Number.isNaN(b)) : a === b);

const CASES = [
  [['region'], null, () => true],
  [['small'], null, () => true],
  [['id'], null, () => true],
  [['score'], null, () => true],
  [['uniq'], 'price > 90', (r) => r.price > 90],
  [['region', 'small'], null, () => true],                       // direct, combined
  [['region', 'shop'], "qty >= 5", (r) => r.qty >= 5],           // 5 x 300 combos
  [['shop', 'id'], null, () => true],                             // hash, two keys
  [['score', 'region', 'small'], "region != 'US'", (r) => r.region !== 'US'],
  [['id'], 'price > 1000', () => false],                          // no rows
];

const engines = [
  ['threads', () => new Engine()],
  ['single thread', () => new Engine({ threads: 1 })],
  ['interpreter', () => new PortableEngine({ jit: false })],
  ['split, 3 workers', () => new SplitEngine({ workers: 3 })],
];
for (const [label, make] of engines) {
  const e = make();
  const t = e.tableAsync ? await e.tableAsync(cols) : e.table(cols);
  for (const [forceScatter, partitioned] of label.startsWith('split') ? [[false, false]] : [[false, false], [true, false], [false, true]]) {
    if (forceScatter) e._maxGroupSlots = 64; // too few accumulator slots: the one-thread fallback
    if (partitioned) e._partitionRows = e._partitionGroups = 0; // the partitioned path on these small tables too (see partition.mjs)
    for (const [keys, filter, keep] of CASES) {
      const want = reference(keys, keep, JS);
      const v = filter ? t.filter(filter) : t;
      let got;
      try { got = (await v.groupBy(keys.length === 1 ? keys[0] : keys).aggAsync(SPEC)).toRows(); } catch (err) { fail(`${label} ${keys} ${filter}: ${err.message}`); continue; }
      checks++;
      const how = forceScatter ? ' (fallback)' : partitioned ? ' (partitioned)' : '';
      if (got.length !== want.length) { fail(`${label}${how} ${keys} ${filter}: ${got.length} groups, want ${want.length}`); continue; }
      const bad = want.findIndex((w, i) => Object.keys(w).some((k) => !close(got[i][k], w[k])));
      if (bad >= 0) fail(`${label}${how} ${keys} ${filter} group ${bad}: ${JSON.stringify(got[bad])} vs ${JSON.stringify(want[bad])}`);
    }
    e._maxGroupSlots = e._partitionRows = e._partitionGroups = undefined;
  }
  await t.dispose?.();
  await e.close();
}
// Big tables: partitioned by key range (see partition.mjs), and on the hash
// path, where every thread inserts into one shared hash table at once. Few
// keys (threads race for the same slots), many keys, a table that starts
// too small (grows and runs again), two keys, float32.
{
  const M = 400_000;
  const few = Float64Array.from({ length: M }, (_, i) => [1.5, -0, 0, NaN, 2 ** 40][i % 5]);
  const many = Float64Array.from({ length: M }, (_, i) => 1e12 + ((i * 7919) % 300_007) * 0.5);
  const other = Float64Array.from({ length: M }, (_, i) => (i * 31) % 17);
  const val = Float64Array.from({ length: M }, (_, i) => (i % 101) - 50);
  const ref = (keyOf) => {
    const m = new Map();
    for (let i = 0; i < M; i++) { const k = keyOf(i); const g = m.get(k) ?? m.set(k, [0, 0]).get(k); g[0]++; g[1] += val[i]; }
    return m;
  };
  const norm = (x) => (Object.is(x, -0) ? 0 : Number.isNaN(x) ? 'NaN' : x);
  const cases = [
    ['few keys, contention', ['few'], (i) => String(norm(few[i])), (r) => String(norm(r.few))],
    ['300K keys', ['many'], (i) => String(many[i]), (r) => String(r.many)],
    ['300K keys, table starts tiny', ['many'], (i) => String(many[i]), (r) => String(r.many), 16],
    ['two keys', ['other', 'many'], (i) => `${other[i]}|${many[i]}`, (r) => `${r.other}|${r.many}`],
  ];
  for (const [label, make] of [['threads', () => new Engine()], ['threads, float32', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['2 threads', () => new Engine({ threads: 2 })], ['4 threads', () => new Engine({ threads: 4 })]]) {
    const e = make();
    const f32 = label.includes('float32');
    const t = e.table({ few, many: f32 ? many.map((x) => (x - 1e12) * 2) : many, other, val }, f32 ? { type: Float32Array } : {});
    for (const [ranged, hash] of [[false, false], [false, true], [true, true]]) {
      e._partitionRows = hash ? Infinity : undefined;
      e._partitionGroups = hash ? undefined : 0; // (the few-keys case too)
      e._bigGroupSlots = ranged ? 1 : undefined; // ranged: each thread adds up its own range of groups
      for (const [name, keys, keyOf, rowKey, start] of cases) {
        const want = ref(f32 && keys.includes('many') ? (i) => keyOf(i).replace(String(many[i]), String((many[i] - 1e12) * 2)) : keyOf);
        e._hashLimit = start;
        const g = t.groupBy(keys.length === 1 ? keys[0] : keys).agg({ n: 'count()', s: 'sum(val)' }).toRows();
        e._hashLimit = undefined;
        checks++;
        const bad = g.length !== want.size ? `${g.length} groups, want ${want.size}` : g.find((r) => { const w = want.get(rowKey(r)); return !w || w[0] !== r.n || w[1] !== r.s; });
        if (bad) fail(`${label}${ranged ? ' (ranged)' : hash ? ' (hash)' : ''} ${name}: ${typeof bad === 'string' ? bad : JSON.stringify(bad)}`);
      }
      // a filter on the direct path with many groups, ranged too
      const g = t.filter('val > 0').groupBy('other').agg({ n: 'count()' }).toRows();
      let want = 0;
      for (let i = 0; i < M; i++) if (val[i] > 0) want++;
      checks++; if (g.reduce((a, r) => a + r.n, 0) !== want) fail(`${label}${ranged ? ' (ranged)' : ''} filtered direct groups`);
    }
    e._bigGroupSlots = e._partitionRows = e._partitionGroups = undefined;
    await e.close();
  }
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} group-by checks passed`);
process.exit(failures ? 1 : 0);
