// Hasura live queries: the same orders, query and changes as bench/bench-live.mjs.
// Postgres 16 and Hasura run in Docker (see README.md); the query is a view
// (Hasura's GraphQL has no GROUP BY), watched as a GraphQL subscription over
// graphql-transport-ws. Hasura runs every live query again each refetch
// interval (HASURA_GRAPHQL_LIVE_QUERIES_MULTIPLEXED_REFETCH_INTERVAL, 1 s by
// default) and sends the whole answer when it changed.
//   change → received: from the UPDATE's start to the subscription message arriving
// Pauses between samples are random (25 ms up to one interval), so changes
// land at every point of Hasura's refetch cycle.
// Usage: node bench-hasura.mjs   (SIZES=10000,100000,1000000 CHANGES=1,100 SAMPLES=60 INTERVAL=1000 PG=postgres://postgres:bench@127.0.0.1:5442/shop HASURA=http://127.0.0.1:8085)
import pg from 'pg';
import { generator, rowOf, q, r2, wait, load } from './data.mjs';

const SIZES = (process.env.SIZES ?? '10000,100000,1000000').split(',').map(Number);
const CHANGES = (process.env.CHANGES ?? '1,100').split(',').map(Number);
const SAMPLES = Number(process.env.SAMPLES ?? 60);
const INTERVAL = Number(process.env.INTERVAL ?? 1000); // (only for the pauses and the label: set it on the Hasura container)
const PG = process.env.PG ?? 'postgres://postgres:bench@127.0.0.1:5442/shop';
const HASURA = process.env.HASURA ?? 'http://127.0.0.1:8085';
const SUB = 'subscription { product_revenue(order_by: [{ revenue: desc }, { product: asc }]) { product revenue n } }';

const db = new pg.Client(PG);
await db.connect();
const meta = async (body) => { const r = await fetch(`${HASURA}/v1/metadata`, { method: 'POST', body: JSON.stringify(body) }); return r.json(); };

async function fill(size) {
  const rand = generator(1);
  await db.query('DROP VIEW IF EXISTS product_revenue; DROP TABLE IF EXISTS orders; CREATE TABLE orders (id int PRIMARY KEY, region text, product text, price float8, qty int)');
  for (let i = 0; i < size; i += 50_000) {
    const rows = Array.from({ length: Math.min(50_000, size - i) }, (_, k) => rowOf(rand, i + k));
    await db.query('INSERT INTO orders SELECT * FROM unnest($1::int[], $2::text[], $3::text[], $4::float8[], $5::int[])', [rows.map((r) => r.id), rows.map((r) => r.region), rows.map((r) => r.product), rows.map((r) => r.price), rows.map((r) => r.qty)]);
  }
  await db.query('CREATE VIEW product_revenue AS SELECT product, SUM(price * qty) AS revenue, COUNT(*) AS n FROM orders GROUP BY product; ANALYZE orders');
  await meta({ type: 'pg_untrack_table', args: { source: 'default', table: { schema: 'public', name: 'product_revenue' } } });
  const t = await meta({ type: 'pg_track_table', args: { source: 'default', table: { schema: 'public', name: 'product_revenue' } } });
  if (t.message !== 'success') throw new Error(`track: ${JSON.stringify(t)}`);
  await meta({ type: 'reload_metadata', args: {} });
  return rand;
}

// a subscription: next() resolves with { at, bytes } of the next message
function subscribe() {
  const ws = new WebSocket(HASURA.replace('http', 'ws') + '/v1/graphql', 'graphql-transport-ws');
  let waiting = null, first;
  const firstP = new Promise((r) => { first = r; });
  ws.onopen = () => ws.send(JSON.stringify({ type: 'connection_init', payload: {} }));
  ws.onmessage = (ev) => {
    const at = performance.now(), m = JSON.parse(ev.data);
    if (m.type === 'connection_ack') ws.send(JSON.stringify({ id: '1', type: 'subscribe', payload: { query: SUB } }));
    else if (m.type === 'next') {
      if (m.payload.errors) throw new Error(JSON.stringify(m.payload.errors));
      const rows = m.payload.data.product_revenue.length;
      if (first) { first(rows); first = null; return; }
      const w = waiting; waiting = null; w?.({ at, bytes: ev.data.length, rows });
    } else if (m.type === 'error') throw new Error(JSON.stringify(m));
  };
  return { first: firstP, next: () => new Promise((r) => { waiting = r; setTimeout(() => r(null), INTERVAL + 10_000); }), close: () => ws.close() };
}

const results = [];
console.log(`load before: ${load()}; Hasura refetch interval ${INTERVAL} ms`);
for (const size of SIZES) {
  const rand = await fill(size);
  const sub = subscribe();
  console.log(`  ${size} rows loaded; first answer: ${await sub.first} products`);
  await wait(INTERVAL + 500);
  for (const n of CHANGES) {
    const total = [], bytes = [];
    let missing = 0;
    for (let i = 0; i < SAMPLES + 3; i++) {
      const ids = Array.from({ length: n }, () => Math.floor(rand() * size)), prices = ids.map(() => Math.round(1 + rand() * 999));
      const got = sub.next();
      const t0 = performance.now();
      await db.query('UPDATE orders o SET price = c.price FROM unnest($1::int[], $2::float8[]) AS c(id, price) WHERE o.id = c.id', [ids, prices]);
      const m = await got;
      if (i >= 3) { if (!m) missing++; else { total.push(m.at - t0); bytes.push(m.bytes); } }
      await wait(25 + rand() * INTERVAL);
    }
    const row = { rows: size, changed: n, interval_ms: INTERVAL, samples: total.length, missing, p50_ms: r2(q(total, 0.5)), p99_ms: r2(q(total, 0.99)), bytes_p50: q(bytes, 0.5) };
    results.push(row);
    console.log(JSON.stringify(row));
  }
  sub.close();
}
const plan = await db.query("EXPLAIN ANALYZE SELECT * FROM product_revenue ORDER BY revenue DESC, product");
console.log(`query time at the last size: ${plan.rows.at(-1)['QUERY PLAN']}`);
await db.end();
console.log(`load after: ${load()}`);
console.log(JSON.stringify(results));
