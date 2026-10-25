// One implementation of each query per system. Every function returns the
// answer, so the runner can check all systems agree.
import { N } from './data.mjs';

export const QUERIES = [
  ['q1', 'filtered revenue (price>100 && qty<5)'],
  ['q2', 'EU dashboard: count, avg price, max qty, revenue'],
  ['q3', 'group by region (8 groups): revenue, orders, avg'],
  ['q4', 'group by customer (100K groups): spend, orders'],
  ['q5', 'filter price>900 into a new table'],
  ['q6', 'new column: revenue = price*qty*(1-discount)'],
];

// ---- vectro -------------------------------------------------------------------
export async function vectro(cols) {
  const { Engine } = await import('../src/node.mjs');
  const e = new Engine();
  await new Promise((r) => setTimeout(r, 300));
  const s = performance.now();
  const t = e.table({ price: cols.price, qty: cols.qty, discount: cols.discount, region: cols.region, customer: cols.customer });
  const load = performance.now() - s;
  const rev = 'price * qty * (1 - discount)';
  return {
    load,
    q1: () => t.filter('price > 100 && qty < 5').sum(rev),
    q2: () => Object.values(t.filter("region == 'EU'").agg({ n: 'count()', avg: 'mean(price)', top: 'max(qty)', revenue: `sum(${rev})` })),
    q3: () => { const r = t.groupBy('region').agg({ revenue: `sum(${rev})`, orders: 'count()', avg: 'mean(price)' }); const out = r.toRows(); r.dispose(); return out; },
    q4: () => { const r = t.groupBy('customer').agg({ spend: 'sum(price * qty)', orders: 'count()' }); const n = r.length; r.dispose(); return n; },
    q5: () => { const r = t.filter('price > 900').collect(); const n = r.length; r.dispose(); return n; },
    q6: () => { const r = t.derive('revenue', rev); const n = r.column('revenue').length; r.dispose(); return n; },
    close: () => e.close(),
  };
}

// ---- Arquero ----------------------------------------------------------------------
export async function arquero(cols) {
  const aq = await import('arquero');
  const { op } = aq;
  const s = performance.now();
  const t = aq.table({ price: cols.price, qty: cols.qty, discount: cols.discount, region: cols.region, customer: cols.customer });
  const load = performance.now() - s;
  return {
    load,
    q1: () => t.filter((d) => d.price > 100 && d.qty < 5).rollup({ s: (d) => op.sum(d.price * d.qty * (1 - d.discount)) }).get('s', 0),
    q2: () => {
      const r = t.filter((d) => d.region === 'EU').rollup({ n: () => op.count(), avg: (d) => op.mean(d.price), top: (d) => op.max(d.qty), revenue: (d) => op.sum(d.price * d.qty * (1 - d.discount)) }).objects()[0];
      return [r.n, r.avg, r.top, r.revenue];
    },
    q3: () => t.groupby('region').rollup({ revenue: (d) => op.sum(d.price * d.qty * (1 - d.discount)), orders: () => op.count(), avg: (d) => op.mean(d.price) }).objects(),
    q4: () => t.groupby('customer').rollup({ spend: (d) => op.sum(d.price * d.qty), orders: () => op.count() }).numRows(),
    q5: () => t.filter((d) => d.price > 900).reify().numRows(),
    q6: () => t.derive({ revenue: (d) => d.price * d.qty * (1 - d.discount) }).numRows(),
    close: async () => {},
  };
}

// ---- DuckDB -----------------------------------------------------------------------
export async function duckdb(cols, csvPath) {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const inst = await DuckDBInstance.create(':memory:');
  const c = await inst.connect();
  const s = performance.now();
  await c.run(`CREATE TABLE orders AS SELECT * FROM read_csv('${csvPath}', header = true,
    columns = { 'price': 'DOUBLE', 'qty': 'DOUBLE', 'discount': 'DOUBLE', 'region': 'VARCHAR', 'customer': 'INTEGER' })`);
  const load = performance.now() - s;
  const rows = async (sql) => (await c.runAndReadAll(sql)).getRowObjects();
  const num = (v) => (typeof v === 'bigint' ? Number(v) : v);
  return {
    load,
    q1: async () => num((await rows('SELECT sum(price * qty * (1 - discount)) AS s FROM orders WHERE price > 100 AND qty < 5'))[0].s),
    q2: async () => Object.values((await rows(`SELECT count(*) AS n, avg(price) AS avg, max(qty) AS top, sum(price * qty * (1 - discount)) AS revenue FROM orders WHERE region = 'EU'`))[0]).map(num),
    q3: async () => (await rows('SELECT region, sum(price * qty * (1 - discount)) AS revenue, count(*) AS orders, avg(price) AS avg FROM orders GROUP BY region')).map((r) => ({ ...r, orders: num(r.orders) })),
    q4: async () => num((await rows('SELECT count(*) AS n FROM (SELECT customer, sum(price * qty) AS spend, count(*) AS orders FROM orders GROUP BY customer)'))[0].n),
    q5: async () => { await c.run('CREATE OR REPLACE TEMP TABLE f AS SELECT * FROM orders WHERE price > 900'); return num((await rows('SELECT count(*) AS n FROM f'))[0].n); },
    q6: async () => { await c.run('CREATE OR REPLACE TEMP TABLE d AS SELECT price * qty * (1 - discount) AS revenue FROM orders'); return N; },
    close: async () => { c.closeSync?.(); },
  };
}
