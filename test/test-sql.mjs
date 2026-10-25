// SQL against plain JavaScript on random tables (missing prices, dates, a
// join), on every engine; and clear errors for what isn't supported.
import { Engine, sql } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const N = 20_000;
const O = Array.from({ length: N }, () => ({
  cust: Math.floor(rnd() * 60), price: rnd() < 0.05 ? NaN : Math.round(rnd() * 4000) / 4, qty: 1 + Math.floor(rnd() * 5),
  region: pick(['EU', 'US', 'APAC', 'LATAM']), ts: new Date(Date.UTC(2023, 0, 1) + Math.floor(rnd() * 700) * 86_400_000 + Math.floor(rnd() * 24) * 3_600_000),
}));
const C = Array.from({ length: 50 }, (_, i) => ({ cust: i, name: `c${i}`, tier: 1 + (i % 3) }));
const col = (rows, k) => rows.map((r) => r[k]);
const present = (x) => x === x;
const sum = (xs) => xs.filter(present).reduce((a, b) => a + b, 0);
const avg = (xs) => { const p = xs.filter(present); return p.length ? sum(p) / p.length : NaN; };
const groupRows = (rows, keyOf) => { const m = new Map(); for (const r of rows) { const k = keyOf(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); } return m; };
const byKey = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const month = (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
const joined = O.flatMap((o) => C.filter((c) => c.cust === o.cust).map((c) => ({ ...o, name: c.name, tier: c.tier })));

const CASES = [
  ['SELECT COUNT(*) AS n, COUNT(price) AS np, SUM(price * qty) AS rev, AVG(price) AS ap, MIN(price) AS lo, MAX(qty) AS hi FROM orders',
    () => [{ n: N, np: O.filter((r) => present(r.price)).length, rev: sum(O.map((r) => r.price * r.qty)), ap: avg(col(O, 'price')), lo: Math.min(...O.map((r) => r.price).filter(present)), hi: 5 }]],
  ["SELECT region, COUNT(*) AS n, SUM(qty) AS q FROM orders WHERE price > 500 GROUP BY region ORDER BY region",
    () => [...groupRows(O.filter((r) => r.price > 500), (r) => r.region)].sort((a, b) => byKey(a[0], b[0])).map(([region, rs]) => ({ region, n: rs.length, q: sum(col(rs, 'qty')) }))],
  ['SELECT cust, SUM(price) AS s FROM orders GROUP BY cust HAVING COUNT(*) > 340 ORDER BY s DESC, cust LIMIT 5',
    () => [...groupRows(O, (r) => r.cust)].filter(([, rs]) => rs.length > 340).map(([cust, rs]) => ({ cust, s: sum(col(rs, 'price')) })).sort((a, b) => b.s - a.s || a.cust - b.cust).slice(0, 5)],
  ["SELECT c.name, c.tier, SUM(o.price * o.qty) AS spend FROM orders o JOIN customers c ON o.cust = c.cust WHERE c.tier = 2 AND o.region IN ('EU', 'US') GROUP BY c.name, c.tier ORDER BY spend DESC LIMIT 7",
    () => [...groupRows(joined.filter((r) => r.tier === 2 && (r.region === 'EU' || r.region === 'US')), (r) => r.name)].map(([name, rs]) => ({ name, tier: 2, spend: sum(rs.map((r) => r.price * r.qty)) })).sort((a, b) => b.spend - a.spend).slice(0, 7)],
  ['SELECT o.cust, c.name FROM orders o LEFT JOIN customers c ON o.cust = c.cust WHERE o.cust >= 55 AND o.qty = 5 ORDER BY o.ts LIMIT 4',
    () => O.filter((r) => r.cust >= 55 && r.qty === 5).sort((a, b) => a.ts - b.ts).slice(0, 4).map((r) => ({ cust: r.cust, name: C.find((c) => c.cust === r.cust)?.name ?? '' }))],
  ["SELECT DATE_TRUNC('month', ts) AS m, COUNT(*) AS n FROM orders WHERE ts >= DATE '2024-01-01' AND ts < TIMESTAMP '2024-07-01 00:00:00' GROUP BY 1 ORDER BY m",
    () => [...groupRows(O.filter((r) => r.ts >= new Date('2024-01-01') && r.ts < new Date('2024-07-01')), (r) => month(r.ts))].sort((a, b) => a[0] - b[0]).map(([m, rs]) => ({ m: new Date(m), n: rs.length }))],
  ['SELECT EXTRACT(YEAR FROM ts) AS y, EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM orders GROUP BY y, h ORDER BY n DESC, y, h LIMIT 3',
    () => [...groupRows(O, (r) => `${r.ts.getUTCFullYear()}|${r.ts.getUTCHours()}`)].map(([k, rs]) => ({ y: +k.split('|')[0], h: +k.split('|')[1], n: rs.length })).sort((a, b) => b.n - a.n || a.y - b.y || a.h - b.h).slice(0, 3)],
  ['SELECT cust, price, qty, CASE WHEN price >= 900 THEN 3 WHEN price >= 500 THEN 2 ELSE 1 END AS band FROM orders WHERE price IS NOT NULL ORDER BY price DESC, cust LIMIT 6',
    () => O.filter((r) => present(r.price)).sort((a, b) => b.price - a.price || a.cust - b.cust).slice(0, 6).map((r) => ({ cust: r.cust, price: r.price, qty: r.qty, band: r.price >= 900 ? 3 : r.price >= 500 ? 2 : 1 }))],
  ['SELECT COUNT(*) AS n FROM orders WHERE price IS NULL', () => [{ n: O.filter((r) => !present(r.price)).length }]],
  ['SELECT COUNT(DISTINCT cust) AS c, APPROX_COUNT_DISTINCT(region) AS r FROM orders WHERE qty BETWEEN 2 AND 3', () => [{ c: new Set(O.filter((r) => r.qty >= 2 && r.qty <= 3).map((r) => r.cust)).size, r: 4 }]],
  ['SELECT DISTINCT region FROM orders ORDER BY region DESC', () => [...new Set(col(O, 'region'))].sort().reverse().map((region) => ({ region }))],
  ["SELECT region, ROUND(AVG(price), 1) AS ap, COALESCE(MAX(price), 0) AS mx FROM orders WHERE region LIKE '%A%' GROUP BY region ORDER BY 1",
    () => [...groupRows(O.filter((r) => r.region.includes('A')), (r) => r.region)].sort((a, b) => byKey(a[0], b[0])).map(([region, rs]) => ({ region, ap: Math.round(avg(col(rs, 'price')) * 10) / 10, mx: Math.max(...col(rs, 'price').filter(present)) }))],
  ['SELECT cust, qty FROM orders WHERE NOT (qty < 5) AND cust % 7 = 3 ORDER BY cust DESC, qty LIMIT 4 OFFSET 2',
    () => O.filter((r) => r.qty >= 5 && r.cust % 7 === 3).map((r) => ({ cust: r.cust, qty: r.qty })).sort((a, b) => b.cust - a.cust || a.qty - b.qty).slice(2, 6)],
  ['SELECT tier, COUNT(*) AS customers FROM customers GROUP BY tier ORDER BY tier', () => [1, 2, 3].map((tier) => ({ tier, customers: C.filter((c) => c.tier === tier).length }))],
  ["SELECT SUM(CASE WHEN region = 'EU' THEN qty ELSE 0 END) AS eu, SUM(qty) - SUM(CASE WHEN region = 'EU' THEN qty ELSE 0 END) AS other FROM orders",
    () => [{ eu: sum(O.filter((r) => r.region === 'EU').map((r) => r.qty)), other: sum(O.filter((r) => r.region !== 'EU').map((r) => r.qty)) }]],
  ['SELECT name, tier FROM customers WHERE tier <> 1 AND name LIKE \'c1_\' ORDER BY name', () => C.filter((c) => c.tier !== 1 && /^c1.$/.test(c.name)).sort((a, b) => byKey(a.name, b.name)).map(({ name, tier }) => ({ name, tier }))],
];
const norm = (v) => (v instanceof Date ? v.getTime() : typeof v === 'number' ? (v !== v ? 'NaN' : Number(v.toPrecision(12))) : v);
for (const [label, make] of [['threads', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['interpreter', () => new PortableEngine({ jit: false })]]) {
  const e = make();
  const tables = {
    orders: e.table({ cust: col(O, 'cust'), price: col(O, 'price'), qty: col(O, 'qty'), region: col(O, 'region'), ts: col(O, 'ts') }),
    customers: e.table({ cust: col(C, 'cust'), name: col(C, 'name'), tier: col(C, 'tier') }),
  };
  for (const [query, ref] of CASES) {
    checks++;
    let got;
    try { got = sql(query, tables).toRows(); } catch (err) { fail(`${label}: ${query}\n  ${err.message}`); continue; }
    const want = ref();
    const ok = got.length === want.length && want.every((w, i) => Object.keys(w).length === Object.keys(got[i]).length && Object.keys(w).every((k) => norm(got[i][k]) === norm(w[k])));
    if (!ok) fail(`${label}: ${query}\n  got  ${JSON.stringify(got.slice(0, 3))} (${got.length})\n  want ${JSON.stringify(want.slice(0, 3))} (${want.length})`);
  }
  // async gives the same
  checks++;
  const q = sql(CASES[3][0], tables);
  if (JSON.stringify(await q.toRowsAsync()) !== JSON.stringify(q.toRows())) fail(`${label} async`);
  await e.close();
}

// errors say what to do
{
  const e = new Engine();
  const tables = { orders: e.table({ cust: [1], price: [2], region: ['EU'] }), customers: e.table({ cust: [1], name: ['a'] }), prices: e.table({ cust: [1], price: [3] }) };
  for (const [query, text] of [
    ['SELECT nope FROM orders', 'no column "nope"'],
    ['SELECT * FROM missing', 'no table "missing"'],
    ['SELECT region, price FROM orders GROUP BY region', 'must be in GROUP BY'],
    ['SELECT cust FROM orders WHERE SUM(price) > 1', 'HAVING'],
    ['SELECT price FROM orders o JOIN prices p ON o.cust = p.cust', 'ambiguous'],
    ['SELECT * FROM orders, customers', 'JOIN ... ON'],
    ['SELECT region || \'x\' FROM orders', 'text'],
    ["SELECT * FROM orders WHERE region = 'EU", 'not closed'],
    ['SELECT UPPER(region) FROM orders', 'unknown function UPPER'],
    ['SELECT * FROM orders LIMIT', 'LIMIT needs a number'],
  ]) {
    let msg = '';
    try { sql(query, tables).toRows(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${query}: ${msg}`);
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} SQL checks passed`);
process.exit(failures ? 1 : 0);
