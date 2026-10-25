// Correctness cases shared by Node and the browser check page. Everything is
// generated from fixed seeds, so every runtime sees the same rows and queries.
//
//   queryCases(engine)  the test-split.mjs queries plus quoted CSV; Node's
//                       answers are saved in expected.json and each browser
//                       engine must match them
//   fuzzCases(engine)   random expressions compiled to WebAssembly, checked
//                       against a plain JS interpreter in the same runtime
// `engine` is anything with tableAsync(columns) and parseCSVAsync(text):
// split engines, or a normal engine wrapped by asyncEngine().
import { parse } from '../../src/fuse.mjs';

export const asyncEngine = (e) => ({ tableAsync: async (cols) => e.table(cols), parseCSVAsync: (text) => e.parseCSVAsync(text) });

function lcg(seed) {
  return () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
}

// numbers rounded to 11 significant digits, so parallel sums compare equal
export const norm = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isInteger(x) ? Number(x.toPrecision(11)) : x));

const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
function data(n, rnd) {
  const price = new Float64Array(n), qty = [], cust = new Float64Array(n), region = [];
  for (let i = 0; i < n; i++) { price[i] = Math.round(rnd() * 1e5) / 100; qty.push(1 + Math.floor(rnd() * 9)); cust[i] = Math.floor(rnd() * 3000); region.push(REG[(i * 7 + Math.floor(rnd() * 3)) % 5]); }
  return { price, qty, cust, region };
}

const QUERIES = [
  ['agg filtered', (t) => t.filter("region == 'EU' && price > m", { m: 250 }).aggAsync({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)', big: 'count(qty > 6)' })],
  ['agg unfiltered', (t) => t.aggAsync({ s: 'sum(price)', a: 'mean(qty)', lo: 'min(qty)', hi: 'max(price)' })],
  ['shortcuts', async (t) => [await t.sumAsync('price * 2'), await t.meanAsync('price'), await t.minAsync('qty'), await t.maxAsync('qty'), await t.filter("region != 'US'").countAsync()]],
  ['filter matches nothing', (t) => t.filter("region == 'MARS'").aggAsync({ n: 'count()', s: 'sum(price)', a: 'mean(price)', lo: 'min(price)' })],
  ['groupBy text', async (t) => (await t.groupBy('region').aggAsync({ rev: 'sum(price * qty)', n: 'count()', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' })).toRows()],
  ['groupBy numeric, filtered', async (t) => (await t.filter('qty >= 3').groupBy('cust').aggAsync({ s: 'sum(price)', a: 'mean(qty)' })).toRows()],
  ['collect + agg', async (t) => { const c = await t.filter('price > 800').collectAsync(); const r = [c.length, await c.sumAsync('price'), await c.toRowsAsync(25)]; await c.dispose(); return r; }],
  ['derive + filter', async (t) => { const d = await t.deriveAsync('rev', 'price * qty'); const r = [await d.filter('rev > 4000').sumAsync('rev'), await d.filter('rev > 4000').toRowsAsync(10)]; await d.dispose(); return r; }],
  ['toRows filtered', (t) => t.filter("region == 'APAC' && qty == 9").toRowsAsync(40)],
];

// Returns { label: normalized answer }. onProgress(label) is called per case.
export async function queryCases(engine, onProgress = () => {}) {
  const out = {};
  const rnd = lcg(11);
  for (const n of [5, 9_999, 200_003]) {
    const t = await engine.tableAsync(data(n, rnd));
    out[`n=${n} length`] = norm(t.length);
    for (const [label, q] of QUERIES) { onProgress(`n=${n} ${label}`); out[`n=${n} ${label}`] = norm(await q(t)); }
    await t.dispose();
  }

  // CSV with quotes, commas and newlines inside fields
  const lines = ['id,price,region,note'];
  for (let i = 0; i < 250_000; i++) {
    const note = i % 7 === 0 ? `"multi\nline ${i}"` : i % 5 === 0 ? `"say ""hi"", ${i}"` : `plain ${i}`;
    lines.push(`${i},${(rnd() * 1000).toFixed(2)},${REG[i % 5]},${note}`);
  }
  onProgress('quoted CSV');
  const c = await engine.parseCSVAsync(new TextEncoder().encode(lines.join('\r\n')));
  out['csv rows'] = norm(c.length);
  out['csv first and last rows'] = norm([...(await c.toRowsAsync(30)), ...(await c.filter('id >= 249990').toRowsAsync())]);
  out['csv groupBy'] = norm((await c.groupBy('region').aggAsync({ n: 'count()', s: 'sum(price)', lo: 'min(id)', hi: 'max(id)' })).toRows());
  out['csv filtered sum'] = norm(await c.filter("region == 'US' && price < 500").sumAsync('price * 2'));
  await c.dispose();
  return out;
}

// ---- fuzz: random expressions vs a plain JS interpreter --------------------
const num = (v) => (typeof v === 'boolean' ? (v ? 1 : 0) : v);
const truthy = (v) => (typeof v === 'boolean' ? v : v != 0);
function evalRef(n, cols, k, i) {
  switch (n.t) {
    case 'num': return n.v;
    case 'var': return n.name === 'k' ? k : cols[n.name][i];
    case 'un': return n.op === '-' ? -num(evalRef(n.x, cols, k, i)) : !truthy(evalRef(n.x, cols, k, i));
    case 'cond': return truthy(evalRef(n.c, cols, k, i)) ? num(evalRef(n.x, cols, k, i)) : num(evalRef(n.y, cols, k, i));
    case 'call': return Math[n.fn](...n.args.map((x) => num(evalRef(x, cols, k, i))));
    case 'bin': {
      if (n.op === '&&') return truthy(evalRef(n.l, cols, k, i)) && truthy(evalRef(n.r, cols, k, i));
      if (n.op === '||') return truthy(evalRef(n.l, cols, k, i)) || truthy(evalRef(n.r, cols, k, i));
      const l = num(evalRef(n.l, cols, k, i)), r = num(evalRef(n.r, cols, k, i));
      switch (n.op) {
        case '+': return l + r; case '-': return l - r; case '*': return l * r; case '/': return l / r;
        case '<': return l < r; case '>': return l > r; case '<=': return l <= r; case '>=': return l >= r;
        case '==': return l == r; case '!=': return l != r;
      }
    }
  }
  throw new Error('bad node');
}
// SQL rules for missing values (NaN): sum, mean, min and max skip them, count(x)
// counts values present, count(condition) the rows where it holds, and min /
// max of nothing is NaN.
const isCondition = (n) => (n.t === 'bin' && ['<', '>', '<=', '>=', '==', '!=', '&&', '||'].includes(n.op)) || (n.t === 'un' && n.op === '!');
function refReduce(fn, ast, cols, k, n) {
  let s = 0, abs = 0, c = 0, m = fn === 'min' ? Infinity : -Infinity;
  for (let i = 0; i < n; i++) {
    const v = evalRef(ast, cols, k, i);
    if (fn === 'count') { if (isCondition(ast) ? truthy(v) : num(v) === num(v)) c++; continue; }
    const x = num(v);
    if (x !== x) continue;
    s += x; abs += Math.abs(x); c++;
    if (fn === 'min') m = Math.min(m, x);
    if (fn === 'max') m = Math.max(m, x);
  }
  if (fn === 'count') return { want: c, scale: 0 };
  if (fn === 'min' || fn === 'max') return { want: c ? m : NaN, scale: 0 };
  return { want: fn === 'mean' ? s / c : s, scale: fn === 'mean' ? abs / c : abs };
}
const same = (got, want, scale) =>
  got === want || (Number.isNaN(got) && Number.isNaN(want)) || (Number.isFinite(scale) && Math.abs(got - want) <= 1e-9 * scale + 1e-300);

// Returns { checks, failures: [text] }.
export async function fuzzCases(engine, { trials = 120, onProgress = () => {} } = {}) {
  const rnd = lcg(12345);
  const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
  const LITS = ['0', '1', '2', '0.5', '3', '10', '0.001', '2.5e1'];
  const randExpr = (depth) => {
    if (depth <= 0 || rnd() < 0.25) return pick(['a', 'b', 'c', 'a', 'k', ...LITS]);
    const e = () => randExpr(depth - 1);
    switch (Math.floor(rnd() * 8)) {
      case 0: case 1: return `(${e()} ${pick(['+', '-', '*', '/'])} ${e()})`;
      case 2: return `(${e()} ${pick(['<', '>', '<=', '>=', '==', '!='])} ${e()})`;
      case 3: return `(${e()} ${pick(['&&', '||'])} ${e()})`;
      case 4: return `${pick(['-', '!'])}${e()}`;
      case 5: return `${pick(['abs', 'sqrt', 'floor', 'ceil'])}(${e()})`;
      case 6: return `${pick(['min', 'max'])}(${e()}, ${e()})`;
      default: return `(${e()} ? ${e()} : ${e()})`;
    }
  };
  const failures = [];
  let checks = 0;
  for (const n of [1, 17, 1001, 40_003]) {
    const cols = { a: new Float64Array(n), b: new Float64Array(n), c: new Float64Array(n) };
    for (const col of Object.values(cols)) for (let i = 0; i < n; i++) col[i] = rnd() < 0.1 ? 0 : (rnd() - 0.4) * pick([1, 10, 1000]);
    const t = await engine.tableAsync(cols);
    onProgress(`fuzz n=${n}`);
    for (let trial = 0; trial < trials / 4; trial++) {
      const k = pick([2, -1.5, 0.25]);
      const item = () => { // every item must read a column
        for (;;) { const src = `${pick(['sum', 'min', 'max', 'count', 'mean'])}(${randExpr(3)})`; if (/\b[abc]\b/.test(src)) return src; }
      };
      const items = Array.from({ length: 1 + Math.floor(rnd() * 4) }, item);
      const spec = Object.fromEntries(items.map((src, j) => [`x${j}`, src]));
      const got = await t.aggAsync(spec, { k });
      items.forEach((src, j) => {
        checks++;
        const ast = parse(src);
        const { want, scale } = refReduce(ast.fn, ast.args[0], cols, k, n);
        if (!same(got[`x${j}`], want, scale) && failures.length < 20) failures.push(`n=${n} k=${k} ${src}: got ${got[`x${j}`]}, want ${want}`);
      });
    }
    await t.dispose();
  }
  return { checks, failures };
}
