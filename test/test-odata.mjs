// The OData feed: $metadata, filters, $select / $orderby / $top / $skip,
// $count, $apply, paging with @odata.nextLink and errors; every answer must
// equal the same question asked in Vectro SQL.
import { Engine, sql } from '../src/node.mjs';
import { serve } from '../src/serve/http.mjs';
import { filterSql } from '../src/serve/odata.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 15) console.log('FAIL', msg); } };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const N = 3000;
const engine = new Engine();
const orders = engine.table({
  order_id: Array.from({ length: N }, (_, i) => i + 1),
  region: Array.from({ length: N }, () => ['Europe', 'Asia', "O'Hare", 'North America'][Math.floor(rnd() * 4)]),
  qty: Array.from({ length: N }, () => 1 + Math.floor(rnd() * 6)),
  price: Array.from({ length: N }, () => (rnd() < 0.05 ? NaN : Math.round(rnd() * 10000) / 100)),
  ts: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 500 * 86400) * 1000)),
  day: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 500) * 86_400_000)),
});
const tables = { orders };
const s = await serve({ tables, port: 0, engine, odataPage: 700, token: 'tok' });
const auth = { authorization: `Basic ${Buffer.from('anyone:tok').toString('base64')}` };
const get = async (path, headers = {}) => { const r = await fetch(`${s.url}/odata${path}`, { headers: { ...auth, ...headers } }); return { status: r.status, headers: r.headers, text: await r.text() }; };
const q = (params) => `/orders?${new URLSearchParams(params).toString()}`;
// what OData gives: dates as ISO text, days as YYYY-MM-DD, missing as null
const odataRows = (rows) => rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v instanceof Date ? (k === 'day' ? v.toISOString().slice(0, 10) : v.toISOString()) : typeof v === 'number' && v !== v ? null : v === '' ? null : v])));

// the service document and $metadata
{
  const doc = JSON.parse((await get('')).text);
  check(same(doc.value.map((x) => x.name), ['orders']), `service document: ${JSON.stringify(doc)}`);
  const md = await get('/$metadata');
  check(md.headers.get('odata-version') === '4.0' && /<EntityType Name="orders"><Key><PropertyRef Name="order_id"\/><\/Key>/.test(md.text), 'metadata: key');
  for (const [c, t] of [['order_id', 'Int64'], ['region', 'String'], ['qty', 'Int64'], ['price', 'Double'], ['ts', 'DateTimeOffset'], ['day', 'Date']]) check(md.text.includes(`<Property Name="${c}" Type="Edm.${t}"`), `metadata: ${c} is Edm.${t}`);
  check((await fetch(`${s.url}/odata/$metadata`)).status === 401, 'no token: 401');
}
// filters, select, order, top, skip, count: the same as SQL
const CASES = [
  [{ $filter: "region eq 'Europe' and qty gt 4", $orderby: 'order_id', $top: '20' }, "SELECT * FROM orders WHERE region = 'Europe' AND qty > 4 ORDER BY order_id LIMIT 20"],
  [{ $filter: "region eq 'O''Hare' or price lt 5.5", $select: 'order_id,price', $orderby: 'price desc,order_id', $top: '15', $skip: '3' }, "SELECT order_id, price FROM orders WHERE region = 'O''Hare' OR price < 5.5 ORDER BY price DESC, order_id LIMIT 15 OFFSET 3"],
  [{ $filter: 'ts ge 2024-06-01T00:00:00Z and ts lt 2024-07-01T00:00:00Z and day ge 2024-03-01', $orderby: 'ts', $top: '10' }, "SELECT * FROM orders WHERE ts >= TIMESTAMP '2024-06-01' AND ts < TIMESTAMP '2024-07-01' AND day >= DATE '2024-03-01' ORDER BY ts LIMIT 10"],
  [{ $filter: "contains(region, 'or') and not (qty in (1, 2)) and year(ts) eq 2025 and price eq null", $orderby: 'order_id' }, "SELECT * FROM orders WHERE STRPOS(region, 'or') > 0 AND NOT qty IN (1, 2) AND EXTRACT(YEAR FROM ts) = 2025 AND price IS NULL ORDER BY order_id"],
  [{ $filter: "startswith(tolower(region), 'nor') and qty mul 2 ge 8 and length(region) gt 5", $select: 'order_id,region,qty', $orderby: 'order_id desc', $top: '5' }, "SELECT order_id, region, qty FROM orders WHERE STARTS_WITH(LOWER(region), 'nor') AND qty * 2 >= 8 AND LENGTH(region) > 5 ORDER BY order_id DESC LIMIT 5"],
];
for (const [params, text] of CASES) {
  const r = await get(q(params));
  const body = JSON.parse(r.text);
  const want = odataRows(sql(text, tables).toRows());
  check(r.status === 200 && same(body.value, want), `${JSON.stringify(params)}\n  got  ${JSON.stringify(body.value?.slice(0, 2) ?? body)}\n  want ${JSON.stringify(want.slice(0, 2))}`);
}
// $count, /$count
{
  const r = JSON.parse((await get(q({ $filter: 'qty ge 5', $count: 'true', $top: '1' }))).text);
  const n = sql('SELECT COUNT(*) AS n FROM orders WHERE qty >= 5', tables).toRows()[0].n;
  check(r['@odata.count'] === n && r.value.length === 1 && !r['@odata.nextLink'], `$count=true: ${r['@odata.count']} vs ${n}`);
  check((await get(`/orders/$count?${new URLSearchParams({ $filter: 'qty ge 5' })}`)).text === String(n), '/$count');
}
// $apply
{
  const r = JSON.parse((await get(q({ $apply: "filter(qty gt 1)/groupby((region),aggregate(qty with sum as units,price with average as avg_price,$count as n,order_id with countdistinct as orders))", $orderby: 'units desc' }))).text);
  const want = odataRows(sql('SELECT region, SUM(qty) AS units, AVG(price) AS avg_price, COUNT(*) AS n, COUNT(DISTINCT order_id) AS orders FROM orders WHERE qty > 1 GROUP BY region ORDER BY units DESC', tables).toRows());
  check(same(r.value, want) && r['@odata.context'].endsWith('#orders(region,units,avg_price,n,orders)'), `$apply groupby: ${JSON.stringify(r).slice(0, 300)}`);
  const t = JSON.parse((await get(q({ $apply: 'aggregate(price with max as top,$count as n)' }))).text);
  const wt = sql('SELECT MAX(price) AS top, COUNT(*) AS n FROM orders', tables).toRows()[0];
  check(same(t.value, [wt]), `$apply aggregate: ${JSON.stringify(t)}`);
}
// paging: every row once, through @odata.nextLink
{
  let url = q({ $filter: 'qty ge 2', $select: 'order_id' }), pages = 0;
  const ids = [];
  while (url) {
    const r = JSON.parse((await get(url.replace(/^https?:\/\/[^/]+\/odata/, ''))).text);
    ids.push(...r.value.map((x) => x.order_id));
    url = r['@odata.nextLink'] ?? null;
    pages++;
  }
  const want = sql('SELECT order_id FROM orders WHERE qty >= 2', tables).toRows().map((x) => x.order_id);
  check(same(ids, want) && pages === Math.ceil(want.length / 700), `paging: ${ids.length} rows in ${pages} pages, want ${want.length}`);
  const small = await get(q({ $orderby: 'order_id', $top: '5' }), { prefer: 'odata.maxpagesize=2' });
  const b = JSON.parse(small.text);
  check(small.headers.get('preference-applied') === 'odata.maxpagesize=2' && b.value.length === 2 && /skiptoken=2/.test(b['@odata.nextLink']), 'Prefer: odata.maxpagesize');
  const last = JSON.parse((await get(q({ $orderby: 'order_id', $top: '5', $skiptoken: '4' }), { prefer: 'odata.maxpagesize=2' })).text);
  check(last.value.length === 1 && !last['@odata.nextLink'], 'the last page of a $top');
}
// errors
{
  const e1 = await get(q({ $filter: 'nope eq 1' }));
  check(e1.status === 400 && /no column "nope"/.test(JSON.parse(e1.text).error.message), 'unknown column: 400');
  check((await get('/missing')).status === 404, 'unknown table: 404');
  check((await get('/orders(5)')).status === 501, 'by key: 501');
  check((await get(q({ $filter: 'qty eq' }))).status === 400, 'bad filter: 400');
  let thrown = '';
  try { filterSql("substringof('a', region)", ['region']); } catch (err) { thrown = err.message; }
  check(/not supported/.test(thrown), 'unknown function');
}
await s.close();
await engine.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} OData checks passed`);
process.exit(failures ? 1 : 0);
