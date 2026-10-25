// Self-test that runs inside the Workers runtime (workerd):
//   GET /env           what the runtime allows (WebAssembly compile, eval, ...)
//   GET /fuzz?seed=1   random expressions vs a reference evaluator
//   GET /tables        filters, aggregates, groupBy, collect, derive, CSV
//   GET /bench?q=..&engine=vectro|js&reps=..&rows=..   repeat a query (time it from outside:
//                      Workers freeze performance.now() while a request runs)
import { Engine, canCompileWasm } from '../../../src/portable.mjs';
import { parse } from '../../../src/fuse.mjs';
import wasmModule from '../../../dist/vectro.wasm';

let engine;
const getEngine = () => (engine ??= new Engine({ wasmModule }));

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const q = (k, d) => url.searchParams.get(k) ?? d;
    try {
      if (url.pathname === '/env') return Response.json(env());
      if (url.pathname === '/fuzz') return Response.json(fuzz(Number(q('seed', 1)), Number(q('trials', 300))));
      if (url.pathname === '/tables') return Response.json(tables());
      if (url.pathname === '/bench') return Response.json(bench(q('q', 'revenue'), q('engine', 'vectro'), Number(q('reps', 20)), Number(q('rows', 1_000_000))));
      return new Response('routes: /env /fuzz /tables /bench', { status: 404 });
    } catch (err) {
      return Response.json({ error: String(err?.stack ?? err) }, { status: 500 });
    }
  },
};

function env() {
  const tryIt = (f) => { try { f(); return 'allowed'; } catch (e) { return `blocked (${e.constructor.name}: ${e.message})`; } };
  const e = getEngine();
  return {
    compileWasmFromBytes: tryIt(() => new WebAssembly.Module(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]))),
    newFunction: tryIt(() => new Function('return 1')),
    eval: tryIt(() => (0, eval)('1')),
    SharedArrayBuffer: typeof SharedArrayBuffer,
    Buffer: typeof globalThis.Buffer,
    canCompileWasm: canCompileWasm(),
    engine: { jit: e.jit, threads: e.T, sharedMemory: e.isShared },
  };
}

// ---- random expressions vs a reference evaluator ------------------------------
const num = (v) => (typeof v === 'boolean' ? (v ? 1 : 0) : v);
const truthy = (v) => (typeof v === 'boolean' ? v : v != 0);
function evalRef(n, cols, i, k) {
  switch (n.t) {
    case 'num': return n.v;
    case 'var': return n.name === 'k' ? k : cols[n.name][i];
    case 'un': return n.op === '-' ? -num(evalRef(n.x, cols, i, k)) : !truthy(evalRef(n.x, cols, i, k));
    case 'cond': return truthy(evalRef(n.c, cols, i, k)) ? num(evalRef(n.x, cols, i, k)) : num(evalRef(n.y, cols, i, k));
    case 'call': return Math[n.fn](...n.args.map((a) => num(evalRef(a, cols, i, k))));
    case 'bin': {
      if (n.op === '&&') return truthy(evalRef(n.l, cols, i, k)) && truthy(evalRef(n.r, cols, i, k));
      if (n.op === '||') return truthy(evalRef(n.l, cols, i, k)) || truthy(evalRef(n.r, cols, i, k));
      const l = num(evalRef(n.l, cols, i, k)), r = num(evalRef(n.r, cols, i, k));
      return { '+': l + r, '-': l - r, '*': l * r, '/': l / r, '<': l < r, '>': l > r, '<=': l <= r, '>=': l >= r, '==': l == r, '!=': l != r }[n.op];
    }
  }
}
const same = (a, b, scale = 0) => Object.is(a, b) || a === b || (Number.isNaN(a) && Number.isNaN(b)) || Math.abs(a - b) <= 1e-9 * scale + 1e-300;

function fuzz(seed, trials) {
  const e = getEngine();
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
  const randExpr = (d) => {
    if (d <= 0 || rnd() < 0.25) return pick(['a', 'b', 'c', 'k', '0', '1', '2', '0.5', '10']);
    const x = () => randExpr(d - 1);
    switch (Math.floor(rnd() * 8)) {
      case 0: case 1: return `(${x()} ${pick(['+', '-', '*', '/'])} ${x()})`;
      case 2: return `(${x()} ${pick(['<', '>', '<=', '>=', '==', '!='])} ${x()})`;
      case 3: return `(${x()} ${pick(['&&', '||'])} ${x()})`;
      case 4: return `${pick(['-', '!'])}${x()}`;
      case 5: return `${pick(['abs', 'sqrt', 'floor', 'ceil'])}(${x()})`;
      case 6: return `${pick(['min', 'max'])}(${x()}, ${x()})`;
      default: return `(${x()} ? ${x()} : ${x()})`;
    }
  };
  let checks = 0;
  const failures = [];
  for (let trial = 0; trial < trials; trial++) {
    const n = pick([1, 3, 17, 1001, 9000]);
    const cols = {};
    for (const nm of ['a', 'b', 'c']) {
      const arr = e.alloc(Float64Array, n);
      cols[nm] = arr;
    }
    for (const nm in cols) {
      cols[nm] = e.live(cols[nm]);
      for (let i = 0; i < n; i++) cols[nm][i] = rnd() < 0.1 ? 0 : (rnd() - 0.4) * pick([1, 10, 1000]);
    }
    const k = pick([2, -1.5, 0.25]);
    const vars = { ...cols, k };
    if (rnd() < 0.3) {
      const src = randExpr(4);
      if (!/\b[abc]\b/.test(src)) { for (const x of Object.values(cols)) e.free(x); trial--; continue; }
      const out = e.eval(src, vars, e.alloc(Float64Array, n));
      const ast = parse(src);
      const live = Object.fromEntries(Object.entries(cols).map(([nm, x]) => [nm, e.live(x)]));
      for (let i = 0; i < n; i++) {
        checks++;
        const want = num(evalRef(ast, live, i, k));
        if (!same(out[i], want)) { failures.push(`map ${src} row ${i}: ${out[i]} vs ${want}`); break; }
      }
      e.free(out);
    } else {
      const items = Array.from({ length: 1 + Math.floor(rnd() * 3) }, () => `${pick(['sum', 'min', 'max', 'count', 'mean'])}(${randExpr(3)})`);
      if (!items.some((s) => /\b[abc]\b/.test(s))) { for (const x of Object.values(cols)) e.free(x); trial--; continue; }
      const got = e.eval(items, vars);
      const live = Object.fromEntries(Object.entries(cols).map(([nm, x]) => [nm, e.live(x)]));
      items.forEach((src, j) => {
        checks++;
        const call = parse(src), kind = call.fn;
        let s = 0, abs = 0, c = 0, m = kind === 'min' ? Infinity : -Infinity;
        for (let i = 0; i < n; i++) {
          const v = evalRef(call.args[0], live, i, k);
          if (kind === 'count') { if (truthy(v)) c++; continue; }
          const x = num(v);
          s += x; abs += Math.abs(x);
          if (kind === 'min') m = Math.min(m, x);
          if (kind === 'max') m = Math.max(m, x);
        }
        const want = kind === 'count' ? c : kind === 'min' || kind === 'max' ? m : kind === 'mean' ? s / n : s;
        const scale = kind === 'mean' ? abs / n : kind === 'sum' ? abs : 0;
        if (!same(got[j], want, scale)) failures.push(`${src}: ${got[j]} vs ${want}`);
      });
    }
    for (const x of Object.values(cols)) e.free(x);
  }
  return { seed, trials, checks, failures: failures.slice(0, 10), passed: failures.length === 0 };
}

// ---- tables and CSV -----------------------------------------------------------------
function tables() {
  const e = getEngine();
  const results = {};
  const check = (name, got, want, tol = 1e-9) => {
    const ok = typeof want === 'string' ? got === want : JSON.stringify(got) === JSON.stringify(want) || Math.abs(got - want) <= tol * Math.max(1, Math.abs(want));
    results[name] = ok ? 'ok' : `got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`;
  };
  const REG = ['EU', 'US', 'APAC'];
  const rows = Array.from({ length: 50_000 }, (_, i) => ({ price: ((i * 7919) % 100000) / 100, qty: 1 + (i % 9), region: REG[i % 3], customer: i % 500 }));
  const t = e.tableFromRows(rows);
  const m = rows.filter((r) => r.region === 'EU' && r.price > 250);
  const a = t.filter("region == 'EU' && price > 250").agg({ n: 'count()', rev: 'sum(price * qty)', avg: 'mean(price)', lo: 'min(price)', hi: 'max(qty)' });
  check('filter count', a.n, m.length);
  check('filter sum', a.rev, m.reduce((s, r) => s + r.price * r.qty, 0));
  check('filter mean', a.avg, m.reduce((s, r) => s + r.price, 0) / m.length);
  check('filter min', a.lo, Math.min(...m.map((r) => r.price)));
  check('filter max', a.hi, Math.max(...m.map((r) => r.qty)));
  const g = t.groupBy('region').agg({ rev: 'sum(price * qty)', n: 'count()' }).toRows();
  for (const reg of REG) {
    const sub = rows.filter((r) => r.region === reg);
    const row = g.find((x) => x.region === reg);
    check(`group ${reg} sum`, row.rev, sub.reduce((s, r) => s + r.price * r.qty, 0));
    check(`group ${reg} count`, row.n, sub.length);
  }
  const gc = t.groupBy('customer').agg({ n: 'count()' });
  check('group 500 customers', gc.length, 500);
  const c = t.filter('price > 900').collect();
  const want = rows.filter((r) => r.price > 900);
  check('collect rows', c.length, want.length);
  check('collect first row', JSON.stringify(c.toRows(1)[0]), JSON.stringify(want[0]));
  const d = t.derive('rev', 'price * qty');
  check('derive', d.sum('rev'), rows.reduce((s, r) => s + r.price * r.qty, 0));
  const csv = e.parseCSV('id,name,amount\r\n1,"Smith, J",10.5\r\n2,"say ""hi""",-3e2\r\n3,"two\nlines",\r\n4,plain,7');
  const cr = csv.toRows();
  check('csv rows', cr.length, 4);
  check('csv quoted comma', cr[0].name, 'Smith, J');
  check('csv escaped quotes', cr[1].name, 'say "hi"');
  check('csv newline in quotes', cr[2].name, 'two\nlines');
  check('csv number', cr[1].amount, -300);
  check('csv empty is NaN', String(cr[2].amount), 'NaN');
  for (const x of [t, gc, c, d, csv]) x.dispose();
  const failed = Object.entries(results).filter(([, v]) => v !== 'ok');
  return { checks: Object.keys(results).length, failed: Object.fromEntries(failed), passed: failed.length === 0 };
}

// ---- timing (from outside) ---------------------------------------------------------
let benchData;
function bench(query, which, reps, rows) {
  const e = getEngine();
  if (benchData?.N !== rows) {
    benchData?.t.dispose();
    const N = rows, REG = ['EU', 'US', 'APAC', 'LATAM'];
    const price = new Float64Array(N), qty = new Float64Array(N), region = new Array(N);
    let seed = 7;
    const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
    for (let i = 0; i < N; i++) { price[i] = Math.round(rnd() * 100000) / 100; qty[i] = 1 + Math.floor(rnd() * 9); region[i] = REG[Math.floor(rnd() * 4)]; }
    const codes = Float64Array.from(region, (r) => REG.indexOf(r));
    benchData = { N, price, qty, codes, t: e.table({ price, qty, region }) };
  }
  const { N, price, qty, codes, t } = benchData;
  const Q = {
    revenue: {
      vectro: () => t.filter('price > 100 && qty < 5').sum('price * qty'),
      js: () => { let s = 0; for (let i = 0; i < N; i++) if (price[i] > 100 && qty[i] < 5) s += price[i] * qty[i]; return s; },
    },
    dashboard: {
      vectro: () => Object.values(t.filter("region == 'EU'").agg({ n: 'count()', avg: 'mean(price)', top: 'max(qty)', rev: 'sum(price * qty)' })),
      js: () => { let n = 0, s = 0, top = -Infinity, rev = 0; for (let i = 0; i < N; i++) if (codes[i] === 0) { n++; s += price[i]; if (qty[i] > top) top = qty[i]; rev += price[i] * qty[i]; } return [n, s / n, top, rev]; },
    },
    group: {
      vectro: () => { const g = t.groupBy('region').agg({ rev: 'sum(price * qty)', n: 'count()' }); const r = g.toRows(); g.dispose(); return r.length; },
      js: () => { const r = new Float64Array(4), c = new Float64Array(4); for (let i = 0; i < N; i++) { r[codes[i]] += price[i] * qty[i]; c[codes[i]]++; } return 4; },
    },
    derive: {
      vectro: () => { const d = t.derive('rev', 'price * qty'); const n = d.length; d.dispose(); return n; },
      js: () => { const o = new Float64Array(N); for (let i = 0; i < N; i++) o[i] = price[i] * qty[i]; return o.length; },
    },
    none: { vectro: () => 0, js: () => 0 }, // measures request overhead
  };
  let r;
  for (let i = 0; i < reps; i++) r = Q[query][which]();
  return { query, engine: which, reps, rows: N, result: r };
}
