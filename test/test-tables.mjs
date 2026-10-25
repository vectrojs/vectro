// Table tests: every operation is checked against plain JS on the same rows,
// on float64 and float32 tables, small (1 thread) and large (all threads).
import { Engine, MODE } from './test-mode.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 20) console.log('FAIL', msg); };
const close = (got, want, scale = Math.abs(want)) => {
  checks++;
  return got === want || (Number.isNaN(got) && Number.isNaN(want)) || Math.abs(got - want) <= 1e-9 * Math.max(1, scale);
};
const expect = (label, got, want, scale) => { if (!close(got, want, scale)) fail(`${label}: got ${got}, want ${want}`); };

let seed = 99;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REGIONS = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];

const e = new Engine();
for (const type of [Float64Array, Float32Array]) {
  const R = type === Float32Array ? Math.fround : (x) => x;
  for (const n of [1, 7, 1000, 250_003]) {
    const tag = `${type.name} n=${n}`;
    const rows = Array.from({ length: n }, (_, i) => ({
      price: R(Math.round(rnd() * 100000) / 100),
      qty: Math.floor(rnd() * 10),
      region: REGIONS[Math.floor(rnd() * REGIONS.length)],
      customer: Math.floor(rnd() * 500),
      id: i,
    }));
    const t = e.tableFromRows(rows, { type });
    const fromCols = e.table({ price: rows.map((r) => r.price), qty: rows.map((r) => r.qty), region: rows.map((r) => r.region) }, { type });

    // aggregations, unfiltered
    const rev = (r) => R(R(r.price) * R(r.qty));
    expect(`${tag} count()`, t.count(), n);
    expect(`${tag} sum(price*qty)`, t.sum('price * qty'), rows.reduce((s, r) => s + rev(r), 0), rows.reduce((s, r) => s + Math.abs(rev(r)), 0));
    expect(`${tag} fromColumns sum`, fromCols.sum('price'), rows.reduce((s, r) => s + r.price, 0), rows.reduce((s, r) => s + r.price, 0));
    expect(`${tag} max(qty)`, t.max('qty'), rows.reduce((m, r) => Math.max(m, r.qty), -Infinity));

    // lazy filter with text and a param, several aggregations at once
    const match = (r) => r.region === 'EU' && r.price > 250 && r.qty >= 2;
    const f = t.filter("region == 'EU'").filter('price > minPrice && qty >= 2', { minPrice: 250 });
    const m = rows.filter(match);
    const a = f.agg({ n: 'count()', revenue: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)', big: 'count(qty > 5)' });
    expect(`${tag} filter count`, a.n, m.length);
    expect(`${tag} filter revenue`, a.revenue, m.reduce((s, r) => s + rev(r), 0), m.reduce((s, r) => s + Math.abs(rev(r)), 0));
    expect(`${tag} filter mean`, a.avg, m.length ? m.reduce((s, r) => s + r.price, 0) / m.length : NaN, 1e6);
    expect(`${tag} filter min`, a.lo, m.length ? m.reduce((x, r) => Math.min(x, r.price), Infinity) : NaN); // min of nothing: NaN (SQL NULL)
    expect(`${tag} filter max`, a.hi, m.length ? m.reduce((x, r) => Math.max(x, r.qty), -Infinity) : NaN);
    expect(`${tag} filter count(qty>5)`, a.big, m.filter((r) => r.qty > 5).length);
    expect(`${tag} unknown text matches nothing`, t.filter("region == 'MARS'").count(), 0);
    expect(`${tag} != text`, t.filter("region != 'EU'").count(), rows.filter((r) => r.region !== 'EU').length);

    // collect: the filtered rows, in order, with every column
    const c = f.collect();
    checks++;
    if (c.length !== m.length) fail(`${tag} collect length ${c.length} vs ${m.length}`);
    else {
      const got = c.toRows();
      for (let i = 0; i < m.length; i++) {
        const w = m[i], g = got[i];
        if (g.id !== w.id || g.price !== w.price || g.qty !== w.qty || g.region !== w.region || g.customer !== w.customer) {
          fail(`${tag} collect row ${i}: ${JSON.stringify(g)} vs ${JSON.stringify(w)}`); break;
        }
      }
    }
    expect(`${tag} collected table aggregates`, c.sum('qty'), m.reduce((s, r) => s + r.qty, 0));
    c.dispose();

    // derive
    const d = t.derive('revenue', 'price * qty * (1 - 0.1)');
    let ok = true;
    for (let i = 0; i < n; i++) if (d.column('revenue')[i] !== R(R(rows[i].price * rows[i].qty) * R(0.9))) { ok = false; break; }
    checks++;
    if (!ok) fail(`${tag} derive`);
    expect(`${tag} filter on derived`, d.filter('revenue > 1000').count(), rows.filter((r) => R(R(r.price * r.qty) * R(0.9)) > 1000).length);

    // groupBy a text column (with a filter) and an integer column
    const byRegion = t.filter('qty > 0').groupBy('region').agg({ revenue: 'sum(price * qty)', orders: 'count()', avg: 'mean(price)', top: 'max(price)' });
    const ref = new Map();
    for (const r of rows) {
      if (!(r.qty > 0)) continue;
      const g = ref.get(r.region) ?? { revenue: 0, orders: 0, total: 0, top: -Infinity };
      g.revenue += rev(r); g.orders++; g.total += r.price; g.top = Math.max(g.top, r.price);
      ref.set(r.region, g);
    }
    const gotRows = byRegion.toRows();
    checks++;
    if (gotRows.length !== ref.size) fail(`${tag} groupBy region: ${gotRows.length} groups vs ${ref.size}`);
    for (const g of gotRows) {
      const w = ref.get(g.region);
      if (!w) { fail(`${tag} groupBy region: unexpected group ${g.region}`); continue; }
      expect(`${tag} group ${g.region} revenue`, g.revenue, w.revenue);
      expect(`${tag} group ${g.region} orders`, g.orders, w.orders);
      expect(`${tag} group ${g.region} avg`, g.avg, w.total / w.orders);
      expect(`${tag} group ${g.region} top`, g.top, w.top);
    }
    const byCustomer = t.groupBy('customer').agg({ spend: 'sum(price)', orders: 'count()' });
    const refC = new Map();
    for (const r of rows) { const g = refC.get(r.customer) ?? { spend: 0, orders: 0 }; g.spend += r.price; g.orders++; refC.set(r.customer, g); }
    checks++;
    if (byCustomer.length !== refC.size) fail(`${tag} groupBy customer: ${byCustomer.length} vs ${refC.size}`);
    for (const g of byCustomer.toRows()) {
      const w = refC.get(g.customer);
      expect(`${tag} customer ${g.customer} spend`, g.spend, w?.spend);
      expect(`${tag} customer ${g.customer} orders`, g.orders, w?.orders);
    }

    byRegion.dispose(); byCustomer.dispose(); d.dispose(); t.dispose(); fromCols.dispose();
  }
}

// empty tables
{
  const t = e.table({ price: [], qty: [] });
  expect('empty count', t.count(), 0);
  expect('empty sum', t.sum('price'), 0);
  expect('empty filtered count', t.filter('price > 1').count(), 0);
  expect('empty collect', t.filter('price > 1').collect().length, 0);
  expect('empty groupBy', t.groupBy('qty').agg({ n: 'count()' }).length, 0);
}

// errors
{
  const t = e.table({ price: [1, 2, 3], region: ['EU', 'US', 'EU'] });
  const expectThrow = (label, fn) => { checks++; try { fn(); fail(`no error for ${label}`); } catch {} };
  expectThrow('unknown column', () => t.sum('cost'));
  expectThrow('text vs number column', () => t.filter("price == 'EU'"));
  expectThrow('text ordering', () => t.filter("region < 'EU'").count());
  expectThrow('bad aggregation', () => t.agg({ x: 'price * 2' }));
  expectThrow('groupBy unknown key', () => t.groupBy(['region', 'cost']));
  expectThrow('groupBy repeated key', () => t.groupBy(['region', 'region']));
  expectThrow('aggregation named like a key', () => t.groupBy('region').agg({ region: 'count()' }));
  expectThrow('bad column name', () => e.table({ 'my col': [1] }));
  expectThrow('param clashes with column', () => t.filter('price > region', { region: 1 }));
}

await e.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `[${MODE}] all ${checks} table checks passed`);
process.exit(failures ? 1 : 0);
