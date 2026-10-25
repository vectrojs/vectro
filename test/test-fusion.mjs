// Fuzz test for the expression compiler: random expressions are compiled to
// WASM SIMD and checked against an independent JS interpreter, on float64 and
// float32, at sizes that exercise the vector loop, the scalar tail and threads.
import { Engine, MODE } from './test-mode.mjs';
import { parse } from '../src/fuse.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };

// ---- reference interpreter ----------------------------------------------------
const num = (v) => (typeof v === 'boolean' ? (v ? 1 : 0) : v);
const truthy = (v) => (typeof v === 'boolean' ? v : v != 0);
function evalRef(n, env, i, R) {
  switch (n.t) {
    case 'num': return R(n.v);
    case 'var': return env.arrays[n.name] ? env.arrays[n.name][i] : R(env.scalars[n.name]);
    case 'un': return n.op === '-' ? -num(evalRef(n.x, env, i, R)) : !truthy(evalRef(n.x, env, i, R));
    case 'cond': return truthy(evalRef(n.c, env, i, R)) ? num(evalRef(n.x, env, i, R)) : num(evalRef(n.y, env, i, R));
    case 'call': {
      const a = n.args.map((x) => num(evalRef(x, env, i, R)));
      return R(Math[n.fn](...a));
    }
    case 'bin': {
      if (n.op === '&&') return truthy(evalRef(n.l, env, i, R)) && truthy(evalRef(n.r, env, i, R));
      if (n.op === '||') return truthy(evalRef(n.l, env, i, R)) || truthy(evalRef(n.r, env, i, R));
      const l = num(evalRef(n.l, env, i, R)), r = num(evalRef(n.r, env, i, R));
      switch (n.op) {
        case '+': return R(l + r);
        case '-': return R(l - r);
        case '*': return R(l * r);
        case '/': return R(l / r);
        case '<': return l < r;
        case '>': return l > r;
        case '<=': return l <= r;
        case '>=': return l >= r;
        case '==': return l == r;
        case '!=': return l != r;
      }
    }
  }
  throw new Error('bad node');
}

function refReduce(kind, ast, env, n, R) {
  let s = 0, abs = 0, c = 0, m = kind === 'min' ? Infinity : -Infinity;
  for (let i = 0; i < n; i++) {
    const v = evalRef(ast, env, i, R);
    if (kind === 'count') { if (truthy(v)) c++; continue; }
    const x = num(v);
    s += x; abs += Math.abs(x);
    if (kind === 'min') m = Math.min(m, x);
    if (kind === 'max') m = Math.max(m, x);
  }
  if (kind === 'count') return { want: c, scale: 0 };
  if (kind === 'min' || kind === 'max') return { want: m, scale: 0 };
  return { want: kind === 'mean' ? s / n : s, scale: kind === 'mean' ? abs / n : abs };
}

const same = (got, want, scale) =>
  got === want || (Number.isNaN(got) && Number.isNaN(want)) || (Number.isFinite(scale) && Math.abs(got - want) <= 1e-9 * scale + 1e-300);

// ---- random expressions ----------------------------------------------------------
let seed = Number(process.env.SEED ?? 12345); // SEED=n node test-fusion.mjs for other cases
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const LITS = ['0', '1', '2', '0.5', '3', '10', '0.001', '2.5e1'];
function randExpr(depth) {
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
}

// ---- run --------------------------------------------------------------------------
const engines = [new Engine(), new Engine({ minParallel: {} })]; // auto, and always-parallel
const SIZES = [1, 2, 3, 5, 8, 17, 1001, 40_003];

for (const Type of [Float64Array, Float32Array]) {
  const R = Type === Float32Array ? Math.fround : (x) => x;
  for (let trial = 0; trial < 250; trial++) {
    const n = pick(SIZES);
    const e = pick(engines);
    const arrays = {};
    for (const name of ['a', 'b', 'c']) {
      const arr = e.alloc(Type, n);
      for (let i = 0; i < n; i++) arr[i] = rnd() < 0.1 ? 0 : (rnd() - 0.4) * pick([1, 10, 1000]);
      arrays[name] = arr;
    }
    for (const nm in arrays) arrays[nm] = e.live(arrays[nm]); // single-thread memory may have grown
    const k = pick([2, -1.5, 0.25]);
    const used = (src, v) => new RegExp(`\\b${v}\\b`).test(src);

    if (rnd() < 0.3) {
      // elementwise map
      const src = randExpr(4);
      if (!['a', 'b', 'c'].some((v) => used(src, v))) { trial--; for (const x of Object.values(arrays)) e.free(x); continue; }
      const vars = { k };
      for (const v of ['a', 'b', 'c']) if (used(src, v)) vars[v] = arrays[v];
      const out0 = e.alloc(Type, n);
      for (const nm in arrays) arrays[nm] = e.live(arrays[nm]);
      const out = e.eval(src, vars, out0);
      const ast = parse(src), env = { arrays, scalars: { k } };
      for (let i = 0; i < n; i++) {
        checks++;
        const want = R(num(evalRef(ast, env, i, R)));
        if (!same(out[i], want, 0)) { fail(`${Type.name} n=${n} map "${src}" at ${i}: got ${out[i]}, want ${want}`); break; }
      }
      e.free(out0);
    } else {
      // several reductions fused into one pass
      const items = Array.from({ length: 1 + Math.floor(rnd() * 4) }, () => `${pick(['sum', 'min', 'max', 'count', 'mean'])}(${randExpr(3)})`);
      if (!items.some((src) => ['a', 'b', 'c'].some((v) => used(src, v)))) { trial--; for (const x of Object.values(arrays)) e.free(x); continue; }
      const vars = { k };
      for (const v of ['a', 'b', 'c']) if (items.some((src) => used(src, v))) vars[v] = arrays[v];
      const got = e.eval(items, vars);
      items.forEach((src, j) => {
        checks++;
        const ast = parse(src);
        const { want, scale } = refReduce(ast.fn, ast.args[0], { arrays, scalars: { k } }, n, R);
        if (!same(got[j], want, scale)) fail(`${Type.name} n=${n} "${src}" (in ${JSON.stringify(items)}): got ${got[j]}, want ${want}`);
      });
    }
    for (const x of Object.values(arrays)) e.free(x);
  }
}

// ---- operator precedence matches JavaScript ---------------------------------------
{
  const e = engines[0];
  let a = e.alloc(Float64Array, 7), b = e.alloc(Float64Array, 7), out = e.alloc(Float64Array, 7);
  [a, b, out] = [a, b, out].map((x) => e.live(x));
  a.set([1, -2, 3.5, 0, 7, -0.5, 2]); b.set([2, 2, -1, 4, 0.5, 3, 2]);
  // JavaScript itself computes the expected values (same text, JS precedence)
  const CASES = [
    ['a + b * a - b / a', (a, b) => a + b * a - b / a],
    ['a - b - a', (a, b) => a - b - a],
    ['a / b / 2', (a, b) => a / b / 2],
    ['-a * b', (a, b) => -a * b],
    ['a > b ? a - b : b - a * 2', (a, b) => (a > b ? a - b : b - a * 2)],
    ['a < b == b > a', (a, b) => +(a < b == b > a)],
    ['a > 0 && b > 0 || a < -1', (a, b) => +((a > 0 && b > 0) || a < -1)],
    ['!(a > b) * 3', (a, b) => !(a > b) * 3],
  ];
  for (const [src, js] of CASES) {
    e.eval(src, { a, b }, out);
    for (let i = 0; i < 7; i++) {
      checks++;
      const want = +js(a[i], b[i]);
      if (!same(out[i], want, 0)) { fail(`precedence "${src}" at ${i}: got ${out[i]}, want ${want}`); break; }
    }
  }
}

// ---- errors are reported, not miscompiled ---------------------------------------------
{
  const e = engines[0];
  const a = e.alloc(Float64Array, 10), b = e.alloc(Float64Array, 5);
  const outside = new Float64Array(10); // a plain array, not in engine memory
  const expectThrow = (label, fn) => { checks++; try { fn(); fail(`no error for ${label}`); } catch {} };
  expectThrow('syntax', () => e.eval('sum(a +)', { a }));
  expectThrow('unknown function', () => e.eval('sum(foo(a))', { a }));
  expectThrow('nested reduction', () => e.eval('sum(a - mean(a))', { a }));
  expectThrow('missing variable', () => e.eval('sum(a * z)', { a }));
  expectThrow('length mismatch', () => e.eval('sum(a + b)', { a, b }));
  expectThrow('map without output', () => e.eval('a * 2', { a }));
  expectThrow('array outside engine memory', () => e.eval('sum(a)', { a: outside }));
}

// ---- parallel results are identical run to run ------------------------------------------
{
  const e = engines[1];
  const a = e.alloc(Float64Array, 1_000_003);
  for (let i = 0; i < a.length; i++) a[i] = Math.sin(i) * 1e3;
  const first = e.eval('sum(a * 1.1 - 0.3)', { a });
  for (let r = 0; r < 20; r++) { checks++; if (e.eval('sum(a * 1.1 - 0.3)', { a }) !== first) { fail('parallel sum not deterministic'); break; } }
}

for (const e of engines) await e.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `[${MODE}] all ${checks} fusion checks passed`);
process.exit(failures ? 1 : 0);
