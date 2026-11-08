// Firestore listeners (the local emulator): the same orders and changes as
// bench/bench-live.mjs. Firestore can't keep a GROUP BY up to date, so the
// writer keeps the totals itself, the usual Firestore way: each change is one
// batch that updates the order and adds the difference to its product's
// total (FieldValue.increment) in a product_revenue collection. The page side
// listens to product_revenue ordered by revenue (onSnapshot), which gives
// added / modified / removed with old and new places, as Vectro's patches do.
// The writer and the listener are two separate Firestore clients, so the
// listener sees the change only after the emulator has it.
//   change → received: from the batch commit's start to the listener's snapshot
// Usage: node bench-firestore.mjs   (SIZES=10000,100000 CHANGES=1,100 SAMPLES=200 EMULATOR=127.0.0.1:8086)
import { initializeApp } from 'firebase/app';
import { getFirestore, connectFirestoreEmulator, collection, doc, writeBatch, increment, onSnapshot, query, orderBy, terminate } from 'firebase/firestore';
import { generator, rowOf, q, r2, wait, load, PRODUCTS } from './data.mjs';

const SIZES = (process.env.SIZES ?? '10000,100000').split(',').map(Number);
const CHANGES = (process.env.CHANGES ?? '1,100').split(',').map(Number);
const SAMPLES = Number(process.env.SAMPLES ?? 200);
const [HOST, PORT] = (process.env.EMULATOR ?? '127.0.0.1:8086').split(':');
const PROJECT = 'vectro-live-bench';

const client = (name) => {
  const db = getFirestore(initializeApp({ projectId: PROJECT, apiKey: 'emulator' }, name));
  connectFirestoreEmulator(db, HOST, Number(PORT));
  return db;
};
const clear = () => fetch(`http://${HOST}:${PORT}/emulator/v1/projects/${PROJECT}/databases/(default)/documents`, { method: 'DELETE' });

const results = [];
console.log(`load before: ${load()}`);
for (const size of SIZES) {
  await clear();
  const writer = client(`writer-${size}`), reader = client(`reader-${size}`);
  const rand = generator(1);
  const orders = [], totals = new Map();
  for (let i = 0; i < size; i++) {
    const r = rowOf(rand, i);
    orders.push(r);
    const t = totals.get(r.product) ?? { revenue: 0, n: 0 };
    t.revenue += r.price * r.qty; t.n++;
    totals.set(r.product, t);
  }
  const t0 = performance.now();
  for (let i = 0; i < size; i += 500) { // (500 writes per batch: Firestore's limit)
    const b = writeBatch(writer);
    for (const r of orders.slice(i, i + 500)) b.set(doc(writer, 'orders', String(r.id)), r);
    await b.commit();
  }
  for (let p = 0; p < PRODUCTS; p += 500) {
    const b = writeBatch(writer);
    for (const [product, t] of [...totals].slice(p, p + 500)) b.set(doc(writer, 'product_revenue', product), { product, ...t });
    await b.commit();
  }
  console.log(`  ${size} orders loaded in ${Math.round(performance.now() - t0)} ms`);

  let waiting = null, first;
  const firstP = new Promise((r) => { first = r; });
  const stop = onSnapshot(query(collection(reader, 'product_revenue'), orderBy('revenue', 'desc')), (snap) => {
    const at = performance.now();
    if (first) { first(snap.size); first = null; return; }
    const changes = snap.docChanges().length;
    const w = waiting; waiting = null; w?.({ at, changes });
  });
  console.log(`  first answer: ${await firstP} products`);
  await wait(500);
  for (const n of CHANGES) {
    const total = [], docs = [];
    let missing = 0;
    for (let i = 0; i < SAMPLES + 10; i++) {
      const b = writeBatch(writer), delta = new Map();
      for (let k = 0; k < n; k++) {
        const r = orders[Math.floor(rand() * size)], price = Math.round(1 + rand() * 999);
        delta.set(r.product, (delta.get(r.product) ?? 0) + (price - r.price) * r.qty);
        r.price = price;
        b.update(doc(writer, 'orders', String(r.id)), { price });
      }
      for (const [product, d] of delta) b.update(doc(writer, 'product_revenue', product), { revenue: increment(d) });
      const got = new Promise((r) => { waiting = r; setTimeout(() => r(null), 5000); });
      const t1 = performance.now();
      await b.commit();
      const m = await got;
      if (i >= 10) { if (!m) missing++; else { total.push(m.at - t1); docs.push(m.changes); } }
      await wait(25);
    }
    const row = { rows: size, changed: n, samples: total.length, missing, p50_ms: r2(q(total, 0.5)), p99_ms: r2(q(total, 0.99)), docs_changed_p50: q(docs, 0.5) };
    results.push(row);
    console.log(JSON.stringify(row));
  }
  stop();
  await terminate(writer); await terminate(reader);
}
console.log(`load after: ${load()}`);
console.log(JSON.stringify(results));
process.exit(0); // (the SDK keeps timers alive)
