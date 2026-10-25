// Lazy queries: random chains of derive / filter / select / sort / limit /
// groupBy / join, run by the planner (optimized) and step by step with the
// table methods (eager). Rows must be the same, in the same order.
import { Engine, SplitEngine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = Number(process.env.SEED ?? 101);
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const norm = (v) => (v instanceof Date ? v.getTime() : typeof v === 'number' && v !== v ? 'NaN' : v);
const sameRows = (a, b, names) => a.length === b.length && a.every((r, i) => names.every((n) => norm(r[n]) === norm(b[i][n])));

function tables(e, n) {
  const orders = { cust: [], price: [], qty: [], region: [], ts: [] };
  for (let i = 0; i < n; i++) {
    orders.cust.push(Math.floor(rnd() * 40));
    orders.price.push(rnd() < 0.05 ? NaN : Math.round(rnd() * 1000) / 4);
    orders.qty.push(1 + Math.floor(rnd() * 6));
    orders.region.push(pick(['EU', 'US', 'APAC']));
    orders.ts.push(new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 400) * 86_400_000));
  }
  const customers = { cust: Array.from({ length: 50 }, (_, i) => i), tier: Array.from({ length: 50 }, () => 1 + Math.floor(rnd() * 3)), city: Array.from({ length: 50 }, () => pick(['Oslo', 'Rome', 'Lima'])) };
  return { orders: e.table(orders), customers: e.table(customers) };
}

// a random chain: steps applied to both a lazy query and an eager table
function randomChain() {
  const steps = [];
  const cols = ['cust', 'price', 'qty', 'region', 'ts'], nums = ['price', 'qty', 'cust'];
  let d = 0, grouped = false, joined = false;
  const count = 1 + Math.floor(rnd() * 6);
  for (let s = 0; s < count; s++) {
    const r = rnd();
    if (r < 0.25) { const name = `d${d++}`; steps.push(['derive', name, `${pick(nums)} ${pick(['*', '+', '-'])} ${pick([...nums, '2', '0.5'])}`]); nums.push(name); cols.push(name); }
    else if (r < 0.5) {
      const options = [`${pick(nums)} > ${Math.floor(rnd() * 100)}`, `${pick(nums)} < ${Math.floor(rnd() * 500)} && ${pick(nums)} >= 2`];
      if (cols.includes('region')) options.push(`region == '${pick(['EU', 'US'])}'`);
      if (cols.includes('ts')) options.push("ts >= '2024-06-01'", 'month(ts) == 3 || isMissing(price)');
      steps.push(['filter', pick(options)]);
    }
    else if (r < 0.6 && !grouped && !joined) { steps.push(['join']); joined = true; cols.push('tier', 'city'); nums.push('tier'); }
    else if (r < 0.7 && !grouped) {
      const key = pick(['region', 'cust', ...(joined ? ['city'] : []), ...cols.filter((c) => c.startsWith('d'))]);
      steps.push(['groupBy', key, { n: 'count()', s: `sum(${pick(nums.filter((x) => x !== key))})`, m: `max(${pick(nums.filter((x) => x !== key))})` }]);
      grouped = true; cols.length = 0; cols.push(key, 'n', 's', 'm'); nums.length = 0; nums.push('n', 's', 'm');
    } else if (r < 0.85) {
      const by = [pick(nums.map((x) => pick([x, `-${x}`]))), cols[0]];
      steps.push(rnd() < 0.5 ? ['sort', by] : ['top', 1 + Math.floor(rnd() * 30), by]);
    } else steps.push(['limit', Math.floor(rnd() * 50)]);
  }
  return steps;
}

for (const [label, make] of [['threads', () => new Engine()], ['single thread', () => new Engine({ threads: 1 })], ['interpreter', () => new PortableEngine({ jit: false })]]) {
  const e = make();
  const n = label === 'interpreter' ? 2000 : 30_000;
  const { orders, customers } = tables(e, n);
  for (let trial = 0; trial < 60; trial++) {
    const steps = randomChain();
    let q = orders.lazy(), t = orders;
    const made = [];
    const keep = (x) => { made.push(x); return x; };
    let names = ['cust', 'price', 'qty', 'region', 'ts'];
    try {
      for (const [op, a, b] of steps) {
        if (op === 'derive') { q = q.derive(a, b); t = keep(t.derive(a, b)); names = [...names, a]; }
        else if (op === 'filter') { q = q.filter(a); t = t.filter(a); }
        else if (op === 'join') { q = q.join(customers, { on: 'cust' }); t = keep(t.join(customers, { on: 'cust' })); names = [...names, 'tier', 'city']; }
        else if (op === 'groupBy') { q = q.groupBy(a).agg(b); t = keep(t.groupBy(a).agg(b)); names = [a, ...Object.keys(b)]; }
        else if (op === 'sort') { q = q.sort(a); t = keep(t.sort(a)); }
        else if (op === 'top') { q = q.top(a, b); t = keep(t.top(a, b)); }
        else if (op === 'limit') { q = q.limit(a); t = keep(t.head(a)); }
      }
      if (rnd() < 0.3 && names.length > 1) { const sel = names.slice(0, 2); q = q.select(...sel); names = sel; }
      const want = t.toRows(), got = q.toRows();
      checks++;
      if (!sameRows(got, want, names) || (got[0] && Object.keys(got[0]).sort().join() !== [...names].sort().join())) {
        fail(`${label} trial ${trial}: ${JSON.stringify(steps)}\n  got ${got.length} rows ${JSON.stringify(got.slice(0, 2), (k, v) => norm(v))}\n  want ${want.length} rows ${JSON.stringify(want.slice(0, 2), (k, v) => norm(v))}`);
      }
      // totals through the planner match totals on the eager table
      if (names.includes('qty') || names.includes('n')) {
        const col = names.includes('qty') ? 'qty' : 'n';
        checks++;
        const a = q.agg({ c: 'count()', s: `sum(${col})` }), b = t.agg({ c: 'count()', s: `sum(${col})` });
        if (a.c !== b.c || a.s !== b.s) fail(`${label} trial ${trial} totals: ${JSON.stringify(a)} vs ${JSON.stringify(b)}`);
      }
    } catch (err) { fail(`${label} trial ${trial}: ${JSON.stringify(steps)}: ${err.stack}`); }
    for (const x of made) x.dispose();
  }
  // the optimizer's work is visible in explain()
  const plan = orders.lazy().derive('rev', 'price * qty').filter('rev > 100').join(customers, { on: 'cust' }).filter('tier == 2').sort('-rev').limit(5).explain();
  checks++;
  if (!/filter \(\(price \* qty\) > 100\)/.test(plan) || !/right:[\s\S]*filter \(tier == 2\)/.test(plan) || !/top 5/.test(plan)) fail(`${label} explain:\n${plan}`);
  // async runs give the same answers
  const qa = orders.lazy().derive('rev', 'price * qty').filter("region == 'EU'").groupBy('cust').agg({ s: 'sum(rev)' }).sort('-s').limit(3);
  checks++; if (JSON.stringify(await qa.toRowsAsync()) !== JSON.stringify(qa.toRows())) fail(`${label} async`);
  await e.close();
}

// a lazy query doesn't leave tables behind, and doesn't free the ones it read
{
  const e = new Engine();
  const { orders, customers } = tables(e, 5000);
  const before = e.sizes?.size;
  for (let i = 0; i < 20; i++) orders.lazy().derive('rev', 'price * qty').filter('rev > 10').join(customers, { on: 'cust' }).groupBy('city').agg({ s: 'sum(rev)' }).sort('-s').toRows();
  checks++; if (e.sizes && e.sizes.size !== before) fail(`engine memory blocks: ${before} -> ${e.sizes.size}`);
  checks++; if (orders.count() !== 5000 || customers.count() !== 50) fail('source tables damaged');
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} lazy-query checks passed`);
process.exit(failures ? 1 : 0);
