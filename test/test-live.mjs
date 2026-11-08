// Live sync: the keyed difference (diff.mjs), liveTable, and serve({ live })
// end to end with WebSocket clients: every client's answer must equal the
// query run again from scratch after each batch of random changes.
import { Engine, sql } from '../src/node.mjs';
import { serve, liveTable } from '../src/serve/index.mjs';
import { connectLive } from '../src/live/client.mjs';
import { diffAnswers, applyPatch, longestIncreasing } from '../src/live/diff.mjs';

let failures = 0, checks = 0;
const check = (name, ok, extra = '') => { checks++; if (!ok) { failures++; console.log(`FAIL ${name} ${extra}`); } };
const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
let seed = 7;
const rand = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
const pick = (n) => Math.floor(rand() * n);

// ---- diff ---------------------------------------------------------------------
{
  check('lis', eq([...longestIncreasing([3, 1, 2, 5, 4, 6])].sort(), [1, 2, 4, 5].sort()) || longestIncreasing([3, 1, 2, 5, 4, 6]).size === 4);
  check('lis empty', longestIncreasing([]).size === 0);
  const answer = (list) => ({ keys: list.map((r) => String(r[0])), rows: list, text: list.map((r) => JSON.stringify(r)) });
  for (let round = 0; round < 3000; round++) {
    const keys = [...new Set(Array.from({ length: pick(12) }, () => pick(15)))];
    const a = keys.map((k) => [k, pick(4)]);
    let b = a.filter(() => rand() > 0.2).map((r) => (rand() < 0.3 ? [r[0], pick(4)] : r));
    for (let i = 0; i < pick(4); i++) { const k = 100 + pick(20); if (!b.some((r) => r[0] === k)) b.splice(pick(b.length + 1), 0, [k, pick(4)]); }
    if (rand() < 0.5) b = b.sort((x, y) => x[1] - y[1] || x[0] - y[0]);
    const d = diffAnswers(answer(a), answer(b));
    const state = { keys: a.map((r) => String(r[0])), rows: a.map((r) => ({ k: r[0], v: r[1] })), index: new Map() };
    state.rows.forEach((r) => state.index.set(String(r.k), r));
    const objs = new Map(state.rows.map((r) => [String(r.k), r]));
    if (d) applyPatch(state, d, (v) => ({ k: v[0], v: v[1] }));
    const ok = eq(state.rows.map((r) => [r.k, r.v]), b) && eq(state.keys, b.map((r) => String(r[0])));
    if (!ok) { check('diff applies', false, JSON.stringify({ a, b, d })); break; }
    check('no diff when equal', (d === null) === eq(a, b));
    for (const r of state.rows) if (objs.has(String(r.k)) && eq(a.find((x) => x[0] === r.k), [r.k, r.v]) && objs.get(String(r.k)) !== r) { check('unchanged rows keep their objects', false); break; }
  }
  // one row rising in a sorted list of 20: one row sent, nothing else
  const a = Array.from({ length: 20 }, (_, i) => [i, 100 - i]);
  const b = [[15, 200], ...a.filter((r) => r[0] !== 15)];
  const d = diffAnswers(answer(a), answer(b));
  check('minimal move', d.del.length === 0 && d.set.length === 0 && d.put.length === 1 && eq(d.put[0], ['15', 0, [15, 200]]), JSON.stringify(d));
  const c = a.map((r) => (r[0] === 3 ? [3, 97.5] : r)); // (changed, same place)
  const d2 = diffAnswers(answer(a), answer(c));
  check('in-place update', d2.put.length === 0 && d2.set.length === 1, JSON.stringify(d2));
}

// ---- liveTable ------------------------------------------------------------------
const engine = new Engine();
{
  const t = liveTable({ key: 'id', columns: { id: 'number', name: 'text', x: 'number', at: 'date' } });
  t.insert([{ id: 1, name: 'a', x: 1, at: '2026-01-01' }, { id: 2, name: 'b', x: 2 }, { id: 3, name: 'c', x: 3 }]);
  let threw = false;
  try { t.insert([{ id: 2, name: 'z' }]); } catch { threw = true; }
  check('insert refuses a used key', threw && t.length === 3);
  t.upsert([{ id: 2, x: 20 }, { id: 4, name: 'd' }]);
  t.delete([1, 99]);
  check('rows after changes', eq([...t.rows()].sort((p, q) => p.id - q.id).map((r) => [r.id, r.name, r.x]), [[2, 'b', 20], [3, 'c', 3], [4, 'd', NaN]]));
  check('get', t.get(2).x === 20 && t.get(1) === undefined && t.has('3'));
  const tab = t.tableFor(engine);
  const r = await sql('SELECT name, x FROM t ORDER BY id', { t: tab }).toRowsAsync?.() ?? (await sql('SELECT name, x FROM t ORDER BY id', { t: tab }).collectAsync()).toRows();
  check('tableFor', eq(r.map((o) => o.name), ['b', 'c', 'd']), JSON.stringify(r));
  tab.dispose();
  let threw2 = false;
  try { liveTable({ key: 'nope', columns: { a: 'number' } }); } catch { threw2 = true; }
  check('bad key refused', threw2);
}

// ---- end to end -----------------------------------------------------------------
const columns = { id: 'number', region: 'text', product: 'text', price: 'number', qty: 'number' };
const orders = liveTable({ key: 'id', columns });
const regions = ['EU', 'US', 'APAC', 'LATAM'], products = Array.from({ length: 30 }, (_, i) => `p${i}`);
const rowOf = (id) => ({ id, region: regions[pick(4)], product: products[pick(30)], price: 1 + pick(100), qty: 1 + pick(5) });
let nextId = 1;
orders.insert(Array.from({ length: 500 }, () => rowOf(nextId++)));
const s = await serve({ engine, live: { orders }, port: 0, token: 'secret' });
const wsURL = `ws://127.0.0.1:${s.port}/live`;

const QUERIES = [
  ['SELECT region, SUM(price * qty) AS revenue, COUNT(*) AS n FROM orders GROUP BY region ORDER BY revenue DESC', 'region'],
  ['SELECT product, SUM(price * qty) AS revenue FROM orders GROUP BY product ORDER BY revenue DESC, product LIMIT 10', 'product'],
  ['SELECT id, product, price FROM orders WHERE price > 90 ORDER BY price DESC, id', null], // (key: the table's)
  ['SELECT region, product FROM orders WHERE qty = 5 ORDER BY region, product', null],       // (whole rows, with repeats)
];
const expected = async (text) => {
  const t = orders.tableFor(engine);
  try { const q = await sql(text, { orders: t }).collectAsync(); const n = q.length; const rows = q.toRows ? q.toRows() : await q.toRowsAsync(); q.dispose(); return { n, rows }; } finally { t.dispose(); }
};
const norm = (rows) => rows.map((r) => Object.values(r).map((v) => (typeof v === 'number' && v !== v ? null : v === '' ? null : v)));

{
  const clients = [0, 1, 2].map(() => connectLive(wsURL, { token: 'secret' }));
  const watches = clients.flatMap((c) => QUERIES.map(([q, key]) => c.watch(q, key ? { key } : {})));
  await Promise.all(watches.map((w) => w.first));
  check('first answers', watches.every((w) => w.rows.length > 0));
  check('watchers share runs', s.live.groups.size === QUERIES.length);
  const live = new Set(Array.from({ length: 500 }, (_, i) => i + 1));
  const lat = [];
  for (let round = 0; round < 60; round++) {
    const ops = 1 + pick(round % 10 === 0 ? 200 : 8);
    for (let k = 0; k < ops; k++) {
      const r = rand();
      if (r < 0.35) { const id = nextId++; orders.insert([rowOf(id)]); live.add(id); }
      else if (r < 0.6 && live.size) { const id = [...live][pick(live.size)]; orders.delete([id]); live.delete(id); }
      else if (live.size) { const id = [...live][pick(live.size)]; orders.upsert([{ id, price: 1 + pick(100), ...(rand() < 0.3 ? { region: regions[pick(4)] } : {}) }]); }
    }
    const want = await Promise.all(QUERIES.map(([q]) => expected(q)));
    const t0 = performance.now();
    let ok = false;
    while (performance.now() - t0 < 3000) {
      ok = watches.every((w, i) => eq(norm(w.rows), norm(want[i % QUERIES.length].rows)));
      if (ok) break;
      await wait(2);
    }
    lat.push(performance.now() - t0);
    if (!ok) {
      const bad = watches.findIndex((w, i) => !eq(norm(w.rows), norm(want[i % QUERIES.length].rows)));
      check(`round ${round}: every client matches a fresh run`, false, JSON.stringify({ q: QUERIES[bad % QUERIES.length][0], got: norm(watches[bad].rows).slice(0, 5), want: norm(want[bad % QUERIES.length].rows).slice(0, 5) }));
      break;
    }
    checks++;
  }
  check('patches were sent, not snapshots', s.live.stats.patches > 100 && s.live.stats.snapshots === watches.length, JSON.stringify(s.live.stats));

  // idle: nothing changes, nothing runs
  const runs = s.live.stats.rounds;
  await wait(150);
  check('idle costs nothing', s.live.stats.rounds === runs);

  // /sql sees the live table as it is now
  const res = await fetch(`http://127.0.0.1:${s.port}/sql?q=${encodeURIComponent('SELECT COUNT(*) AS n FROM orders')}`, { headers: { authorization: 'Bearer secret' } });
  const body = await res.json();
  check('/sql reads the live table', body.rows?.[0]?.[0] === orders.length, JSON.stringify(body).slice(0, 200));

  // a bad query: an error for that watch only
  const bad = clients[0].watch('SELECT nope FROM orders');
  let err = null;
  try { await bad.first; } catch (e) { err = e; }
  check('bad SQL gives an error', err && /nope/.test(err.message), String(err));
  const big = clients[0].watch('SELECT * FROM orders', {});
  await big.first;
  check('plain table watch', big.rows.length === orders.length);
  big.close();
  await wait(20);
  check('unwatch frees the group', s.live.groups.size === QUERIES.length);

  // reconnect: the server drops the connection; the client comes back and gets a fresh answer
  for (const c of s.live.clients) c.ws.socket.destroy();
  await wait(50);
  orders.upsert([{ id: [...live][0], price: 1000 }]);
  const want = await expected(QUERIES[2][0]);
  const t0 = performance.now();
  while (performance.now() - t0 < 4000 && !eq(norm(watches[2].rows), norm(want.rows))) await wait(10);
  check('reconnects and catches up', eq(norm(watches[2].rows), norm(want.rows)));
  for (const c of clients) c.close();
}

// a token is needed
{
  const ws = new WebSocket(wsURL);
  const closed = await new Promise((r) => { ws.onopen = () => r(false); ws.onerror = () => r(true); ws.onclose = () => r(true); });
  check('no token, no connection', closed);
}

// slow clients: with a tiny buffer limit every client is behind; each still ends with the right answer (fresh snapshots)
{
  s.live.maxBuffered = 0;
  const c = connectLive(wsURL, { token: 'secret' });
  const w = c.watch(QUERIES[1][0], { key: 'product' });
  await w.first;
  for (let i = 0; i < 300; i++) { orders.upsert([{ id: [...orders.slots.keys()][pick(orders.length)], price: 1 + pick(100) }]); if (i % 30 === 0) await wait(1); }
  const want = await expected(QUERIES[1][0]);
  const t0 = performance.now();
  while (performance.now() - t0 < 3000 && !eq(norm(w.rows), norm(want.rows))) await wait(5);
  check('slow client ends right', eq(norm(w.rows), norm(want.rows)));
  s.live.maxBuffered = 1 << 20;
  c.close();
}

await s.close();
await engine.close();
console.log(failures ? `live: ${failures} of ${checks} checks failed` : `live: all ${checks} checks pass`);
if (failures) process.exitCode = 1;
