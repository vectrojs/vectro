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
// subqueries, set operations, window functions, percentiles (references in plain JS)
const sorted = (xs) => xs.filter(present).sort((a, b) => a - b);
const cont = (xs, p) => { const s = sorted(xs), m = s.length; if (!m) return NaN; const r = p * (m - 1), lo = Math.floor(r); return s[lo] + (r - lo) * (s[Math.min(m - 1, lo + 1)] - s[lo]); };
const disc = (xs, p) => { const s = sorted(xs); return s.length ? s[Math.max(0, Math.ceil(p * s.length) - 1)] : NaN; };
const day = (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
const monthly = () => [...groupRows(O, (r) => month(r.ts))].sort((a, b) => a[0] - b[0]).map(([m, rs]) => ({ m, n: rs.length }));
const custs = (rows) => [...new Set(col(rows, 'cust'))].sort((a, b) => a - b);
CASES.push(
  ['WITH s AS (SELECT cust, SUM(qty) AS q FROM orders GROUP BY cust) SELECT cust, q FROM s WHERE q > 900 ORDER BY q DESC, cust LIMIT 5',
    () => [...groupRows(O, (r) => r.cust)].map(([cust, rs]) => ({ cust, q: sum(col(rs, 'qty')) })).filter((r) => r.q > 900).sort((a, b) => b.q - a.q || a.cust - b.cust).slice(0, 5)],
  ['WITH t(c, total) AS (SELECT cust, SUM(qty) FROM orders GROUP BY cust), u AS (SELECT * FROM t WHERE c < 10) SELECT MAX(total) AS m, COUNT(*) AS k FROM u',
    () => { const g = [...groupRows(O.filter((r) => r.cust < 10), (r) => r.cust)].map(([, rs]) => sum(col(rs, 'qty'))); return [{ m: Math.max(...g), k: g.length }]; }],
  ['SELECT c.name, t.n FROM (SELECT cust, COUNT(*) AS n FROM orders GROUP BY cust) AS t JOIN customers c ON t.cust = c.cust ORDER BY t.n DESC, c.name LIMIT 4',
    () => [...groupRows(O.filter((r) => r.cust < 50), (r) => r.cust)].map(([cust, rs]) => ({ name: `c${cust}`, n: rs.length })).sort((a, b) => b.n - a.n || byKey(a.name, b.name)).slice(0, 4)],
  ['SELECT COUNT(*) AS n FROM (SELECT cust FROM orders WHERE qty = 5 UNION ALL SELECT cust FROM customers) u', () => [{ n: O.filter((r) => r.qty === 5).length + C.length }]],
  ['SELECT cust FROM orders WHERE price > 990 UNION SELECT cust FROM customers WHERE tier = 3 ORDER BY cust',
    () => custs([...O.filter((r) => r.price > 990), ...C.filter((c) => c.tier === 3)]).map((cust) => ({ cust }))],
  ['SELECT cust FROM orders WHERE price > 990 INTERSECT SELECT cust FROM customers WHERE tier = 3 ORDER BY 1',
    () => custs(O.filter((r) => r.price > 990)).filter((c) => c < 50 && c % 3 === 2).map((cust) => ({ cust }))],
  ['SELECT cust FROM orders EXCEPT (SELECT cust FROM customers) ORDER BY cust DESC', () => custs(O).filter((c) => c >= 50).reverse().map((cust) => ({ cust }))],
  ["SELECT region AS r FROM orders WHERE cust = 1 UNION SELECT name FROM customers WHERE cust < 3 ORDER BY r",
    () => [...new Set([...O.filter((r) => r.cust === 1).map((r) => r.region), 'c0', 'c1', 'c2'])].sort(byKey).map((r) => ({ r }))],
  ['SELECT COUNT(*) AS n FROM orders WHERE cust IN (SELECT cust FROM customers WHERE tier = 2)', () => [{ n: O.filter((r) => r.cust < 50 && r.cust % 3 === 1).length }]],
  ['SELECT COUNT(*) AS n FROM orders WHERE cust NOT IN (SELECT cust FROM customers)', () => [{ n: O.filter((r) => r.cust >= 50).length }]],
  ["SELECT COUNT(*) AS n FROM orders WHERE region IN (SELECT region FROM orders WHERE price > 999 AND cust < 20)",
    () => { const rs = new Set(O.filter((r) => r.price > 999 && r.cust < 20).map((r) => r.region)); return [{ n: O.filter((r) => rs.has(r.region)).length }]; }],
  ['SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.cust AND o.price > 995) ORDER BY name',
    () => C.filter((c) => O.some((o) => o.cust === c.cust && o.price > 995)).map(({ name }) => ({ name })).sort((a, b) => byKey(a.name, b.name))],
  ['SELECT COUNT(*) AS n FROM customers c WHERE NOT EXISTS (SELECT * FROM orders o WHERE c.cust = o.cust AND o.price > 999)',
    () => [{ n: C.filter((c) => !O.some((o) => o.cust === c.cust && o.price > 999)).length }]],
  ['SELECT COUNT(*) AS n FROM customers c WHERE c.tier IN (SELECT o.qty FROM orders o WHERE o.cust = c.cust AND o.price > 990)',
    () => [{ n: C.filter((c) => O.some((o) => o.cust === c.cust && o.price > 990 && o.qty === c.tier)).length }]],
  ['SELECT COUNT(*) AS n FROM orders WHERE price > (SELECT AVG(price) FROM orders)', () => { const a = avg(col(O, 'price')); return [{ n: O.filter((r) => r.price > a).length }]; }],
  ['SELECT c.name, (SELECT MAX(o.price) FROM orders o WHERE o.cust = c.cust) AS mx, (SELECT COUNT(*) FROM orders o WHERE o.cust = c.cust AND o.qty = 5) AS n5 FROM customers c ORDER BY c.cust LIMIT 5',
    () => C.slice(0, 5).map((c) => ({ name: c.name, mx: Math.max(...O.filter((o) => o.cust === c.cust).map((o) => o.price).filter(present)), n5: O.filter((o) => o.cust === c.cust && o.qty === 5).length }))],
  ['SELECT COUNT(*) AS n FROM orders o WHERE o.price > (SELECT AVG(p.price) FROM orders p WHERE p.region = o.region)',
    () => { const a = new Map([...groupRows(O, (r) => r.region)].map(([k, rs]) => [k, avg(col(rs, 'price'))])); return [{ n: O.filter((r) => r.price > a.get(r.region)).length }]; }],
  ['SELECT cust, price, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY price DESC, ts) AS rn FROM orders WHERE cust < 3 AND price IS NOT NULL ORDER BY cust, rn LIMIT 8',
    () => { const rs = O.filter((r) => r.cust < 3 && present(r.price)).sort((a, b) => a.cust - b.cust || b.price - a.price || a.ts - b.ts); let k = 0; return rs.map((r, i) => ({ cust: r.cust, price: r.price, rn: (k = i && rs[i - 1].cust === r.cust ? k + 1 : 1) })).slice(0, 8); }],
  ['SELECT region, cust, s FROM (SELECT region, cust, SUM(qty) AS s, RANK() OVER (PARTITION BY region ORDER BY SUM(qty) DESC) AS r FROM orders GROUP BY region, cust) t WHERE r <= 2 ORDER BY region, s DESC, cust',
    () => [...groupRows(O, (r) => r.region)].sort((a, b) => byKey(a[0], b[0])).flatMap(([region, rs]) => {
      const g = [...groupRows(rs, (r) => r.cust)].map(([cust, xs]) => ({ region, cust, s: sum(col(xs, 'qty')) }));
      return g.filter((x) => g.filter((y) => y.s > x.s).length + 1 <= 2).sort((a, b) => b.s - a.s || a.cust - b.cust);
    })],
  ["SELECT d, n, SUM(n) OVER (ORDER BY d) AS running, n - LAG(n) OVER (ORDER BY d) AS change, AVG(n) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma FROM (SELECT DATE_TRUNC('month', ts) AS d, COUNT(*) AS n FROM orders GROUP BY 1) m ORDER BY d",
    () => { const ms = monthly(); let run = 0; return ms.map(({ m, n }, i) => ({ d: new Date(m), n, running: (run += n), change: i ? n - ms[i - 1].n : NaN, ma: avg(ms.slice(Math.max(0, i - 2), i + 1).map((x) => x.n)) })); }],
  ["SELECT d, SUM(n) OVER (ORDER BY d RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW) AS wk, MAX(n) OVER (ORDER BY d DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx FROM (SELECT DATE_TRUNC('day', ts) AS d, COUNT(*) AS n FROM orders GROUP BY 1) x ORDER BY d LIMIT 20 OFFSET 100",
    () => { const ds = [...groupRows(O, (r) => day(r.ts))].map(([d, rs]) => ({ d, n: rs.length })).sort((a, b) => a.d - b.d); return ds.map(({ d }, i) => ({ d: new Date(d), wk: sum(ds.filter((x) => x.d <= d && x.d >= d - 6 * 86_400_000).map((x) => x.n)), mx: Math.max(...ds.slice(Math.max(0, i - 1), i + 2).map((x) => x.n)) })).slice(100, 120); }],
  ['SELECT region, MEDIAN(price) AS med, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qty) AS p90, PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY price) AS p25, COUNT(*) AS n FROM orders GROUP BY region ORDER BY region',
    () => [...groupRows(O, (r) => r.region)].sort((a, b) => byKey(a[0], b[0])).map(([region, rs]) => ({ region, med: cont(col(rs, 'price'), 0.5), p90: cont(col(rs, 'qty'), 0.9), p25: disc(col(rs, 'price'), 0.25), n: rs.length }))],
  ['SELECT MEDIAN(qty) AS m, PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY price DESC) AS p, QUANTILE_CONT(price, 0.3) AS q3 FROM orders',
    () => [{ m: cont(col(O, 'qty'), 0.5), p: cont(col(O, 'price'), 0.9), q3: cont(col(O, 'price'), 0.3) }]],
  ['SELECT cust, COUNT(*) OVER (PARTITION BY tier) AS n, DENSE_RANK() OVER (ORDER BY tier) AS dr, NTILE(4) OVER (ORDER BY cust) AS q, FIRST_VALUE(cust) OVER (PARTITION BY tier ORDER BY cust) AS f, LAST_VALUE(cust) OVER (PARTITION BY tier ORDER BY cust ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS l, PERCENT_RANK() OVER (ORDER BY cust) AS pr, CUME_DIST() OVER (PARTITION BY tier ORDER BY cust) AS cd, LEAD(cust, 2, -1) OVER (PARTITION BY tier ORDER BY cust) AS ld, MIN(name) OVER () AS mn FROM customers ORDER BY cust',
    () => C.map((c, i) => { const same = C.filter((x) => x.tier === c.tier), pos = same.indexOf(c); return { cust: c.cust, n: same.length, dr: c.tier, q: i < 26 ? Math.floor(i / 13) + 1 : 3 + Math.floor((i - 26) / 12), f: same[0].cust, l: same.at(-1).cust, pr: i / 49, cd: (pos + 1) / same.length, ld: same[pos + 2]?.cust ?? -1, mn: 'c0' }; })],
  ['SELECT cust, qty, MEDIAN(price - 500) AS m, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY -price) AS d FROM orders GROUP BY cust, qty ORDER BY cust, qty',
    () => [...groupRows(O, (r) => r.cust * 10 + r.qty)].sort((a, b) => a[0] - b[0]).map(([k, rs]) => ({ cust: Math.floor(k / 10), qty: k % 10, m: cont(rs.map((r) => r.price - 500), 0.5), d: disc(rs.map((r) => -r.price), 0.5) }))],
  ["SELECT tier, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY name) AS mid, PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY name) AS top FROM customers GROUP BY tier ORDER BY tier",
    () => [1, 2, 3].map((tier) => { const ns = C.filter((c) => c.tier === tier).map((c) => c.name).sort(byKey); return { tier, mid: ns[Math.ceil(0.5 * ns.length) - 1], top: ns.at(-1) }; })],
  ['SELECT 1 + 2 AS x, 10 / 4 AS y', () => [{ x: 3, y: 2.5 }]],
  ['SELECT COUNT(*) AS n FROM customers a CROSS JOIN customers b WHERE a.tier = b.tier', () => [{ n: 17 * 17 + 17 * 17 + 16 * 16 }]],
  ['SELECT cust, price FROM orders WHERE cust = 7 ORDER BY price NULLS FIRST, ts OFFSET 14 ROWS FETCH FIRST 5 ROWS ONLY',
    () => O.filter((r) => r.cust === 7).sort((a, b) => (present(a.price) - present(b.price)) || (a.price - b.price) || a.ts - b.ts).slice(14, 19).map(({ cust, price }) => ({ cust, price }))],
);
// text functions and outer joins (missing text is '')
const P = [[1, '  Ann Smith', 'Seattle'], [2, 'bob jones ', 'Austin'], [3, 'Çelik Ayşe', 'Ankara'], [4, 'dan', 'San José'], [5, 'Eve-Marie', 'Paris'], [6, "Frank O'Neil", ''], [7, '東京 太郎', 'Tokyo'], [8, 'gus', 'Dublin']].map(([id, name, city]) => ({ id, name, city }));
const cps = (s) => Array.from(s);
const nul = (s) => s === '';
const sub = (s, a, n) => { const c = cps(s); const at = a > 0 ? a - 1 : a === 0 ? -1 : c.length + a; return c.slice(Math.max(0, at), Math.max(0, n === undefined ? c.length : at + n)).join(''); };
const lpad = (s, n, f) => { const c = cps(s); if (c.length >= n) return c.slice(0, n).join(''); let p = ''; while (cps(p).length < n - c.length) p += f; return cps(p).slice(0, n - c.length).join('') + s; };
const textOf = (v) => (v !== v ? '' : String(v));
const band = (p) => (p !== p ? 'unknown' : p >= 500 ? 'high' : p >= 100 ? 'mid' : 'low');
const ymOf = (d) => `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
const hi990 = () => [...groupRows(O.filter((r) => r.price > 990), (r) => r.cust)].map(([cust, rs]) => ({ cust, n: rs.length }));
CASES.push(
  ['SELECT UPPER(region) AS r, LOWER(region) AS l, LENGTH(region) AS n, COUNT(*) AS c FROM orders GROUP BY region ORDER BY r',
    () => [...groupRows(O, (r) => r.region)].sort((a, b) => byKey(a[0], b[0])).map(([g, rs]) => ({ r: g.toUpperCase(), l: g.toLowerCase(), n: g.length, c: rs.length }))],
  ["SELECT id, TRIM(name) AS t, UPPER(name) AS u, LENGTH(name) AS len, SUBSTR(name, 2, 3) AS s, LEFT(TRIM(name), 3) AS l, RIGHT(name, 2) AS r, REPLACE(name, ' ', '_') AS rp, REVERSE(city) AS rv, LPAD(city, 8, '.') AS lp, SPLIT_PART(TRIM(name), ' ', 2) AS sp, STRPOS(name, 'a') AS pos, CONCAT(city, '!', id) AS c, city || '/' || id AS cc, CONCAT_WS('-', city, name) AS ws, SUBSTRING(name FROM -3) AS e3, TRIM(LEADING ' ' FROM name) AS lt FROM people ORDER BY id",
    () => P.map(({ id, name, city }) => ({ id, t: name.trim(), u: name.toUpperCase(), len: cps(name).length, s: sub(name, 2, 3), l: cps(name.trim()).slice(0, 3).join(''), r: cps(name).slice(-2).join(''), rp: name.split(' ').join('_'), rv: cps(city).reverse().join(''), lp: nul(city) ? '' : lpad(city, 8, '.'), sp: name.trim().split(' ')[1] ?? '', pos: name.indexOf('a') < 0 ? 0 : cps(name.slice(0, name.indexOf('a'))).length + 1, c: `${city}!${id}`, cc: nul(city) ? '' : `${city}/${id}`, ws: nul(city) ? name : `${city}-${name}`, e3: cps(name).slice(-3).join(''), lt: name.replace(/^ +/, '') }))],
  ["SELECT COUNT(*) AS n FROM orders WHERE LOWER(region) = 'eu' OR STARTS_WITH(region, 'LA')", () => [{ n: O.filter((r) => r.region === 'EU' || r.region.startsWith('LA')).length }]],
  ["SELECT region || '/' || CAST(qty AS VARCHAR) AS k, COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY k LIMIT 7",
    () => [...groupRows(O, (r) => `${r.region}/${r.qty}`)].sort((a, b) => byKey(a[0], b[0])).slice(0, 7).map(([k, rs]) => ({ k, n: rs.length }))],
  ["SELECT CASE WHEN price >= 500 THEN 'high' WHEN price >= 100 THEN 'mid' WHEN price IS NULL THEN 'unknown' ELSE 'low' END AS band, COUNT(*) AS n FROM orders GROUP BY band ORDER BY band",
    () => [...groupRows(O, (r) => band(r.price))].sort((a, b) => byKey(a[0], b[0])).map(([b, rs]) => ({ band: b, n: rs.length }))],
  ["SELECT STRFTIME(ts, '%Y-%m') AS ym, COUNT(*) AS n, CAST(MIN(qty) AS VARCHAR) || '..' || CAST(MAX(qty) AS VARCHAR) AS q FROM orders GROUP BY ym ORDER BY ym LIMIT 5",
    () => [...groupRows(O, (r) => ymOf(r.ts))].sort((a, b) => byKey(a[0], b[0])).slice(0, 5).map(([ym, rs]) => ({ ym, n: rs.length, q: `${Math.min(...col(rs, 'qty'))}..${Math.max(...col(rs, 'qty'))}` }))],
  ["SELECT name FROM customers WHERE name >= 'c4' AND name < 'c5' ORDER BY name", () => C.map((c) => c.name).filter((s) => s >= 'c4' && s < 'c5').sort(byKey).map((name) => ({ name }))],
  ["SELECT COUNT(*) AS n FROM people WHERE name ILIKE '%an%' AND NOT REGEXP_MATCHES(city, '^S')", () => [{ n: P.filter((p) => p.name.toLowerCase().includes('an') && !nul(p.city) && !/^S/.test(p.city)).length }]],
  ["SELECT o.cust, COALESCE(c.name, 'none') AS name FROM orders o LEFT JOIN customers c ON o.cust = c.cust WHERE o.qty = 5 AND o.cust >= 48 ORDER BY o.ts LIMIT 6",
    () => O.filter((r) => r.qty === 5 && r.cust >= 48).sort((a, b) => a.ts - b.ts).slice(0, 6).map((r) => ({ cust: r.cust, name: r.cust < 50 ? `c${r.cust}` : 'none' }))],
  ['SELECT SUM(CAST(SUBSTR(name, 2) AS INTEGER)) AS s, COUNT(DISTINCT LEFT(name, 2)) AS d FROM customers', () => [{ s: 1225, d: new Set(C.map((c) => c.name.slice(0, 2))).size }]],
  ["SELECT region, COUNT(*) AS n FROM orders GROUP BY region HAVING LOWER(region) LIKE '%a%' ORDER BY region",
    () => [...groupRows(O, (r) => r.region)].filter(([g]) => g.toLowerCase().includes('a')).sort((a, b) => byKey(a[0], b[0])).map(([region, rs]) => ({ region, n: rs.length }))],
  ["SELECT name, CAST(RANK() OVER (ORDER BY tier DESC, name) AS VARCHAR) || '.' AS r FROM customers ORDER BY r LIMIT 5",
    () => { const s = [...C].sort((a, b) => b.tier - a.tier || byKey(a.name, b.name)); return s.map((c, i) => ({ name: c.name, r: `${i + 1}.` })).sort((a, b) => byKey(a.r, b.r)).slice(0, 5); }],
  ['SELECT c.cust, COUNT(o.cust) AS n FROM (SELECT * FROM orders WHERE price > 990) o RIGHT JOIN customers c ON o.cust = c.cust GROUP BY c.cust ORDER BY c.cust',
    () => C.map((c) => ({ cust: c.cust, n: O.filter((r) => r.price > 990 && r.cust === c.cust).length }))],
  ['SELECT COALESCE(o.cust, c.cust) AS k, o.n, c.name FROM (SELECT cust, COUNT(*) AS n FROM orders WHERE price > 990 GROUP BY cust) o FULL JOIN customers c ON o.cust = c.cust ORDER BY k',
    () => { const h = new Map(hi990().map((x) => [x.cust, x.n])); return [...new Set([...h.keys(), ...C.map((c) => c.cust)])].sort((a, b) => a - b).map((k) => ({ k, n: h.get(k) ?? NaN, name: k < 50 ? `c${k}` : '' })); }],
  ['SELECT cust, n, tier FROM (SELECT cust, COUNT(*) AS n FROM orders WHERE price > 995 GROUP BY cust) o FULL JOIN customers USING (cust) ORDER BY cust',
    () => { const h = new Map([...groupRows(O.filter((r) => r.price > 995), (r) => r.cust)].map(([k, rs]) => [k, rs.length])); return [...new Set([...h.keys(), ...C.map((c) => c.cust)])].sort((a, b) => a - b).map((cust) => ({ cust, n: h.get(cust) ?? NaN, tier: cust < 50 ? 1 + (cust % 3) : NaN })); }],
  ['SELECT o.cust, c.cust AS cc FROM orders o LEFT JOIN customers c ON o.cust = c.cust WHERE o.cust >= 49 AND o.qty = 1 AND o.price > 900 ORDER BY o.cust, o.ts',
    () => O.filter((r) => r.cust >= 49 && r.qty === 1 && r.price > 900).sort((a, b) => a.cust - b.cust || a.ts - b.ts).map((r) => ({ cust: r.cust, cc: r.cust < 50 ? r.cust : NaN }))],
  ['SELECT cust AS qty, qty AS cust FROM orders WHERE price > 999 ORDER BY qty, cust', () => O.filter((r) => r.price > 999).map((r) => ({ qty: r.cust, cust: r.qty })).sort((a, b) => a.qty - b.qty || a.cust - b.cust)],
  ['SELECT CAST(cust AS VARCHAR) AS k FROM customers WHERE cust < 3 UNION SELECT region FROM orders ORDER BY k', () => [...new Set(['0', '1', '2', ...O.map((r) => r.region)])].sort(byKey).map((k) => ({ k }))],
);
const norm = (v) => (v instanceof Date ? v.getTime() : typeof v === 'number' ? (v !== v ? 'NaN' : Number(v.toPrecision(12))) : v);
for (const [label, make] of [['threads', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['interpreter', () => new PortableEngine({ jit: false })]]) {
  const e = make();
  const tables = {
    orders: e.table({ cust: col(O, 'cust'), price: col(O, 'price'), qty: col(O, 'qty'), region: col(O, 'region'), ts: col(O, 'ts') }),
    customers: e.table({ cust: col(C, 'cust'), name: col(C, 'name'), tier: col(C, 'tier') }),
    people: e.table({ id: col(P, 'id'), name: col(P, 'name'), city: col(P, 'city') }),
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
    ['SELECT MIN(region) FROM orders', 'of text is not supported'],
    ["SELECT * FROM orders WHERE region = 'EU", 'not closed'],
    ['SELECT UPPER(region, 2) FROM orders', 'UPPER() takes 1'],
    ['SELECT NOPE(region) FROM orders', 'unknown function NOPE'],
    ['SELECT * FROM orders LIMIT', 'LIMIT needs a number'],
    ['SELECT cust FROM orders UNION SELECT cust, price FROM orders', 'same number of columns'],
    ['SELECT region FROM orders UNION SELECT cust FROM orders', 'text'],
    ['SELECT cust FROM orders WHERE ROW_NUMBER() OVER (ORDER BY cust) = 1', 'window functions'],
    ['SELECT ROW_NUMBER() AS r FROM orders', 'needs OVER'],
    ['SELECT cust FROM orders o WHERE price > (SELECT MAX(price) FROM prices p WHERE p.price > o.price)', 'outer query'],
    ['SELECT * FROM orders o FULL JOIN customers c ON o.cust = c.cust AND o.price > 1', 'key equalities'],
    ['SELECT CAST(region AS BLOB) FROM orders', 'CAST to BLOB'],
    ['SELECT * FROM (SELECT cust FROM orders)', 'needs a name'],
    ['SELECT PERCENTILE_CONT(0.5) FROM orders', 'WITHIN GROUP'],
    ['SELECT cust FROM orders INTERSECT ALL SELECT cust FROM customers', 'INTERSECT ALL'],
  ]) {
    let msg = '';
    try { sql(query, tables).toRows(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${query}: ${msg}`);
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} SQL checks passed`);
process.exit(failures ? 1 : 0);
