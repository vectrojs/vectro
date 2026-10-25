// sort / top on tables, split tables and streams, against plain JavaScript.
import { Engine, SplitEngine } from '../src/node.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

let seed = 31;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK'];
function data(n, { nanShare = 0.02 } = {}) {
  const id = [], price = [], qty = [], region = [];
  for (let i = 0; i < n; i++) {
    id.push(i);
    price.push(rnd() < nanShare ? NaN : Math.round(rnd() * 2000) / 4); // many ties
    qty.push(1 + Math.floor(rnd() * 5));
    region.push(REG[Math.floor(rnd() * REG.length)]);
  }
  return { id, price, qty, region };
}

// keys as text (for Vectro) and as JS (for the reference); text keys by label
const KEYS = [
  ['price', (r) => r.price],
  ['-price', (r) => -r.price],
  ['-(price * qty)', (r) => -(r.price * r.qty)],
  ['abs(price - 250)', (r) => Math.abs(r.price - 250)],
  [['region', '-price'], null],
  [['-region', 'qty', 'price'], null],
  [['qty', '-region'], null],
];
const labelKey = (k) => { const m = /^(-?)(\w+)$/.exec(k); return m && m[2] === 'region' ? { col: 'region', desc: m[1] === '-' } : null; };
const jsKey = (k) => { // one key -> (row) => number | string, desc flag for strings
  const lab = labelKey(k);
  if (lab) return { label: true, f: (r) => r.region, desc: lab.desc };
  const e = KEYS.find(([t]) => t === k)?.[1] ?? { qty: (r) => r.qty, price: (r) => r.price, '-price': (r) => -r.price, id: (r) => r.id, '-id': (r) => -r.id }[k];
  return { label: false, f: e };
};
function reference(rows, by) {
  const ks = (Array.isArray(by) ? by : [by]).map(jsKey);
  return rows.map((r, i) => ({ r, i })).sort((a, b) => {
    for (const k of ks) {
      const x = k.f(a.r), y = k.f(b.r);
      if (k.label) { if (x !== y) return (x < y ? -1 : 1) * (k.desc ? -1 : 1); continue; }
      const xn = x !== x, yn = y !== y;
      if (xn || yn) { if (xn && yn) continue; return xn ? 1 : -1; }
      if (x !== y) return x < y ? -1 : 1;
    }
    return a.i - b.i;
  }).map(({ r }) => r);
}
const rowsOf = (cols) => cols.id.map((_, i) => ({ id: cols.id[i], price: cols.price[i], qty: cols.qty[i], region: cols.region[i] }));
const norm = (rows) => rows.map((r) => ({ ...r, price: Number.isNaN(r.price) ? 'NaN' : r.price }));

const engine = new Engine();
for (const n of [0, 1, 7, 5_000, 120_000]) {
  const cols = data(n);
  const all = rowsOf(cols);
  const t = engine.table(cols);
  for (const [by] of KEYS) {
    const L = `n=${n} by=${JSON.stringify(by)}`;
    const want = reference(all, by);
    checks++;
    const got = await t.sortAsync(by);
    if (!same(norm(got.toRows()), norm(want))) fail(`${L} sort`);
    got.dispose();
    // top equals the head of sort, for many k
    for (const k of [0, 1, 10, 1_000, n, n + 5]) {
      checks++;
      const tp = t.top(k, by);
      if (!same(norm(tp.toRows()), norm(want.slice(0, k)))) fail(`${L} top(${k})`);
      tp.dispose();
    }
    // with a filter (an empty table can't know region holds text)
    if (!n) continue;
    const f = "region != 'US' && qty >= 2";
    const fwant = reference(all.filter((r) => r.region !== 'US' && r.qty >= 2), by);
    checks++;
    const ft = await t.filter(f).topAsync(25, by);
    if (!same(norm(ft.toRows()), norm(fwant.slice(0, 25)))) fail(`${L} filtered top`);
    ft.dispose();
  }
  t.dispose();
}

// mostly missing values: fewer than k rows have a value
{
  const cols = data(80_000, { nanShare: 0.9995 });
  const t = engine.table(cols);
  const want = reference(rowsOf(cols), '-price');
  checks++;
  if (!same(norm(t.top(200, '-price').toRows()), norm(want.slice(0, 200)))) fail('mostly NaN top');
  t.dispose();
}

// the biggest customers: group, then top
{
  const n = 200_000, cust = [], spend = [];
  for (let i = 0; i < n; i++) { cust.push(Math.floor(rnd() * 5000)); spend.push(Math.round(rnd() * 1e4) / 100); }
  const t = engine.table({ cust, spend });
  const g = t.groupBy('cust').agg({ total: 'sum(spend)', orders: 'count()' });
  const totals = new Map();
  cust.forEach((c, i) => { const v = totals.get(c) ?? { cust: c, total: 0, orders: 0 }; v.total += spend[i]; v.orders++; totals.set(c, v); });
  const want = [...totals.values()].sort((a, b) => b.total - a.total || a.cust - b.cust).slice(0, 10).map((r) => r.cust);
  checks++;
  const got = g.top(10, ['-total', 'cust']).toRows().map((r) => r.cust);
  if (!same(got, want)) fail(`biggest customers ${got} vs ${want}`);
  g.dispose(); t.dispose();
}

// free text and empty keys are refused
{
  const t = engine.parseCSV('id,note\n1,"a, b"\n2,c\n', { types: { note: 'string' } });
  for (const [by, text] of [['note', 'free-text'], [[], 'at least one key']]) {
    let msg = '';
    try { t.sort(by); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`bad key ${JSON.stringify(by)} accepted: ${msg}`);
  }
}

// split tables and streams: unique keys, so the order is fully defined
for (const workers of [1, 3]) {
  const split = new SplitEngine({ workers });
  const cols = data(60_000);
  const all = rowsOf(cols);
  const st = await split.tableAsync(cols);
  for (const by of [['-price', 'id'], ['region', 'price', 'id'], ['-(price * qty)', '-id']]) {
    const want = reference(all, by).slice(0, 50);
    for (const view of [st, st.filter('qty > 2')]) {
      const w = view === st ? want : reference(all.filter((r) => r.qty > 2), by).slice(0, 50);
      checks++;
      const got = await view.topAsync(50, by);
      if (!same(norm(got.toRows()), norm(w))) fail(`split w=${workers} ${JSON.stringify(by)} ${view === st ? '' : 'filtered'}`);
    }
  }
  const stream = await split.streamAsync({ columns: { id: 'number', price: 'number', qty: 'number', region: 'category' } });
  await stream.push(all.map((r) => `${r.id},${Number.isNaN(r.price) ? '' : r.price},${r.qty},${r.region}`).join('\n') + '\n');
  await stream.flushAsync();
  checks++;
  const got = await stream.topAsync(20, ['-price', 'id']);
  if (!same(norm(got.toRows()), norm(reference(all, ['-price', 'id']).slice(0, 20)))) fail(`stream w=${workers} top`);
  await split.close();
}
await engine.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} sort checks passed`);
process.exit(failures ? 1 : 0);
