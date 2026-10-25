// Running totals over time windows on streams, against a brute-force
// reference over every row that arrived (rows dropped by keepRows still count).
import { SplitEngine } from '../src/node.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const close = (a, b) => JSON.stringify(a, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(10)) : x)) === JSON.stringify(b, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(10)) : x));

let seed = 99;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const AGG = { n: 'count()', big: 'count(qty > 5)', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' };

function makeRows(n, { spread = 1, late = 0.05 } = {}) {
  const rows = [];
  let t = 1_000_000;
  for (let i = 0; i < n; i++) {
    t += Math.floor(rnd() * 40 * spread);
    let ts = t;
    if (rnd() < late) ts = t - Math.floor(rnd() * 20_000 * spread); // arrives late
    if (rnd() < 0.01) ts = NaN; // no time
    rows.push({ ts, price: Math.round(rnd() * 1e5) / 100, qty: 1 + Math.floor(rnd() * 9), region: REG[Math.floor(rnd() * (i < n / 3 ? 2 : 5))] });
  }
  return rows;
}
const csv = (rows) => rows.map((r) => `${Number.isNaN(r.ts) ? '' : r.ts},${r.price},${r.qty},${r.region}`).join('\n') + '\n';

function reference(rows, { by, every, last }, series) {
  const slices = Math.ceil(last / every);
  const timed = rows.filter((r) => !Number.isNaN(r.ts));
  const newest = Math.max(...timed.map((r) => Math.floor(r.ts / every)));
  const groups = new Map();
  for (const r of timed) {
    const slice = Math.floor(r.ts / every);
    if (slice <= newest - slices) continue;
    const key = series ? `${slice}|${by ? r[by] : ''}` : (by ? r[by] : '');
    let g = groups.get(key);
    if (!g) groups.set(key, (g = { slice, label: by ? r[by] : null, n: 0, big: 0, rev: 0, sum: 0, lo: Infinity, hi: -Infinity }));
    g.n++; if (r.qty > 5) g.big++; g.rev += r.price * r.qty; g.sum += r.price; g.lo = Math.min(g.lo, r.price); g.hi = Math.max(g.hi, r.qty);
  }
  const out = [...groups.values()].map((g) => ({ ...(series ? { t: g.slice * every } : {}), ...(by ? { [by]: g.label } : {}), n: g.n, big: g.big, rev: g.rev, avg: g.sum / g.n, lo: g.lo, hi: g.hi }));
  const label = (r) => (by ? r[by] : '');
  return out.sort((a, b) => (series && a.t !== b.t ? a.t - b.t : label(a) < label(b) ? -1 : label(a) > label(b) ? 1 : 0));
}

const CASES = [
  { by: 'region', every: 1000, last: 60_000 },
  { by: 'region', every: 250, last: 5_000 },
  { by: null, every: 5000, last: 30_000 },
  { by: 'region', every: 1, last: 50_000, spread: 3000 }, // times spread too far for one group-by: fallback path
];
async function pushAll(s, text) {
  for (let i = 0; i < text.length; ) { const k = Math.floor(rnd() * 6000); await s.push(text.slice(i, i + k)); i += k; }
  await s.flushAsync();
}
const COLUMNS = { ts: 'number', price: 'number', qty: 'number', region: 'category' };

for (const workers of [1, 3]) {
  const engine = new SplitEngine({ workers });
  for (const c of CASES) {
    const L = `w=${workers} every=${c.every} last=${c.last} by=${c.by}`;
    const rows = makeRows(30_000, { spread: c.spread ?? 1 });
    // 1. window first; keepRows far below the row count: totals outlive the rows
    {
      const s = await engine.streamAsync({ columns: COLUMNS, keepRows: 2_000 });
      const w = await s.windowAsync({ by: c.by, every: c.every, last: c.last, timeColumn: 'ts', agg: AGG });
      await pushAll(s, csv(rows));
      for (const series of [false, true]) {
        checks++;
        const got = await w.readAsync({ series }), want = reference(rows, c, series);
        if (!close(got, want)) fail(`${L} series=${series}: got ${JSON.stringify(got).slice(0, 300)} want ${JSON.stringify(want).slice(0, 300)}`);
      }
      checks++;
      if ((await s.countAsync()) > 2_000 + workers) fail(`${L} rows were not dropped`); // keepRows is split per worker, rounded up
      await w.dispose();
      await s.dispose();
    }
    // 2. window opened after 9,000 rows arrived (all kept): they count too
    {
      const s = await engine.streamAsync({ columns: COLUMNS });
      await pushAll(s, csv(rows.slice(0, 9_000)));
      const w = await s.windowAsync({ by: c.by, every: c.every, last: c.last, timeColumn: 'ts', agg: AGG });
      await pushAll(s, csv(rows.slice(9_000)));
      checks++;
      const got = await w.readAsync(), want = reference(rows, c, false);
      if (!close(got, want)) fail(`${L} opened late: got ${JSON.stringify(got).slice(0, 300)} want ${JSON.stringify(want).slice(0, 300)}`);
      await s.dispose();
    }
  }
  // bad window options are refused
  const s = await engine.streamAsync({ columns: { ts: 'number', price: 'number', region: 'category' } });
  for (const [opts, text] of [[{ every: 1000, last: 5000, timeColumn: 'nope', agg: { n: 'count()' } }, 'timeColumn'], [{ by: 'price', every: 1000, last: 5000, timeColumn: 'ts', agg: { n: 'count()' } }, 'category'], [{ every: 0, last: 5000, timeColumn: 'ts', agg: { n: 'count()' } }, 'every > 0']]) {
    let msg = '';
    try { await s.windowAsync(opts); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`w=${workers} bad window options accepted (${text}): ${msg}`);
  }
  await engine.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} window checks passed`);
process.exit(failures ? 1 : 0);
