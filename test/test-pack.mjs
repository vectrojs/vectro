// Compressed columns: packing never changes a value, saves memory where it
// can, and every query gives the same answer on a compressed table. (The
// whole suite also runs with VECTRO_COMPRESS=1: every table packed.)
import { Engine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';
import { chooseEncoding, decodeOne, KINDS } from '../src/pack.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 13;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

// encodings round-trip exactly (or aren't used)
const GENS = {
  small: [() => 1 + Math.floor(rnd() * 9), 'u8'],
  codes: [() => Math.floor(rnd() * 3000), 'u16'],
  negatives: [() => Math.floor(rnd() * 200) - 100, 'u8'],
  cents: [() => Math.round(rnd() * 1e5) / 100, 'i32'],
  centsMissing: [() => (rnd() < 0.1 ? NaN : Math.round(rnd() * 1e4) / 100), 'u16'],
  seconds: [() => 1.7e12 + Math.floor(rnd() * 1e8) * 1000, 'i32'],
  days: [() => 1.7e12 + Math.floor(rnd() * 3000) * 86_400_000, 'u16'],
  tenths: [() => Math.round(rnd() * 500) / 10, 'u16'],
  random: [() => rnd(), null],
  huge: [() => Math.floor(rnd() * 2 ** 45), null],
  negZero: [() => (rnd() < 0.5 ? -0 : 1), null],
};
for (const f32 of [false, true]) {
  for (const [name, [gen, kind]] of Object.entries(GENS)) {
    const n = 5000, R = f32 ? Math.fround : (x) => x;
    const values = Float64Array.from({ length: n }, () => R(gen()));
    const enc = chooseEncoding(values, n, f32);
    checks++;
    if (!f32 && (enc?.kind ?? null) !== kind) fail(`${name}: encoding ${enc?.kind ?? 'none'}, expected ${kind ?? 'none'}`);
    if (!enc) continue;
    const back = values.every((v) => {
      const code = Number.isNaN(v) ? KINDS[enc.kind].nan : enc.div ? Math.round(v * enc.div) : Math.round((v - enc.base) / enc.mul);
      return Object.is(decodeOne(enc, code, f32), v);
    });
    checks++; if (!back) fail(`${name} f32=${f32}: values change`);
  }
}

// compressed tables answer every query like plain ones
const N = 200_000;
const REG = ['EU', 'US', 'APAC', 'LATAM'];
const cols = {
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 5000)),
  price: Float64Array.from({ length: N }, () => (rnd() < 0.02 ? NaN : Math.round(rnd() * 100000) / 100)),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  region: Array.from({ length: N }, () => REG[Math.floor(rnd() * 4)]),
  ts: Array.from({ length: N }, (_, i) => new Date(Date.UTC(2024, 0, 1) + i * 1000)),
  noise: Float64Array.from({ length: N }, () => rnd()),
};
const QUERIES = [
  (t) => t.agg({ s: 'sum(price * qty)', n: 'count(price)', m: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' }),
  (t) => t.filter("region == 'EU' && price > 500 && qty >= 3").agg({ s: 'sum(price)', c: 'count()' }),
  (t) => t.filter("ts >= '2024-01-02T12:00:00Z'").agg({ c: 'count()', y: 'sum(hour(ts))' }),
  (t) => t.groupBy('region').agg({ s: 'sum(price * qty)', a: 'mean(price)' }).toRows(),
  (t) => t.groupBy(['cust']).agg({ n: 'count()' }).toRows(20),
  (t) => t.filter('qty == 9').collect().toRows(30),
  (t) => t.top(15, ['-price', 'cust']).toRows(),
  (t) => t.sort('ts').toRows(5),
  (t) => { const d = t.derive('rev', 'price * qty'); try { return d.filter('rev > 1000').agg({ s: 'sum(rev)' }); } finally { d.dispose(); } },
  (t) => t.lazy().filter('price > 100').groupBy('region').agg({ c: 'countDistinct(cust)' }).toRows(),
  (t) => t.toRows(3),
];
// (group sums add per thread, in an order that varies run to run: compare 12 digits)
const norm = (x) => JSON.stringify(x, (k, v) => (typeof v === 'number' ? (v !== v ? 'NaN' : Number(v.toPrecision(12))) : v));
for (const [label, make] of [['threads', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['interpreter', () => new PortableEngine({ jit: false })]]) {
  const e = make();
  const plain = e.table(cols, { compress: false });
  const packed = e.table(cols, { compress: true });
  const mem = packed.memoryBytes();
  checks++; if (!(mem.bytes < mem.uncompressed / 2)) fail(`${label}: ${mem.bytes} bytes of ${mem.uncompressed}`);
  checks++; if ([...packed.cols].filter(([, c]) => c.enc).length !== 5) fail(`${label}: packed ${[...packed.cols].filter(([, c]) => c.enc).map(([n]) => n)}`);
  QUERIES.forEach((q, i) => {
    checks++;
    const a = norm(q(plain)), b = norm(q(packed));
    if (a !== b) fail(`${label} query ${i}: ${b.slice(0, 160)} vs ${a.slice(0, 160)}`);
  });
  // in place: a view made before compress() sees packed columns; compressing twice is a no-op
  const t = e.table(cols, { compress: false });
  const view = t.filter("region == 'US'");
  const before = view.agg({ s: 'sum(price)' });
  t.compress(); t.compress();
  checks++; if (norm(view.agg({ s: 'sum(price)' })) !== norm(before)) fail(`${label}: view after compress`);
  [plain, packed, t].forEach((x) => x.dispose());
  await e.close();
}

// CSV loads compressed on request
{
  const e = new Engine();
  const csv = 'id,price,qty,region\n' + Array.from({ length: 1000 }, (_, i) => `${i},${(i % 97) / 4},${1 + (i % 5)},${REG[i % 4]}`).join('\n');
  const t = e.parseCSV(csv, { compress: true });
  checks++; if (![...t.cols.values()].every((c) => c.enc)) fail('CSV compress');
  checks++; if (t.filter("region == 'EU'").sum('price') !== e.parseCSV(csv).filter("region == 'EU'").sum('price')) fail('CSV compressed answers');
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} compression checks passed`);
process.exit(failures ? 1 : 0);
