// Kernel fusion: compile an expression like `sum(a * 2 + 1)` or
// `[sum(a), min(a), count(a > 500 && b < 3)]` into ONE WebAssembly SIMD
// function that reads each array once, instead of one pass per operation.

import { I32, F64, V128, TY, Fn, loops, buildModule, get, set, simd, vload, vstore, memarg, emitModScalar, emitModVector } from './wasm.mjs';

// ---- parsing -----------------------------------------------------------------

const TOKEN = /\s*(?:(\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)|([A-Za-z_]\w*)|(&&|\|\||==|!=|<=|>=|[-+*/()<>,?:!])|'((?:[^']|'')*)'|"([^"]*)")/y; // (inside '...', '' is one ')

function tokenize(src) {
  const out = [];
  TOKEN.lastIndex = 0;
  while (TOKEN.lastIndex < src.length && /\S/.test(src.slice(TOKEN.lastIndex))) {
    const pos = TOKEN.lastIndex;
    const m = TOKEN.exec(src);
    if (!m) throw new SyntaxError(`vectro: unexpected "${src.slice(pos).trim()[0]}" in "${src}"`);
    if (m[1] !== undefined) out.push({ t: 'num', v: Number(m[1]) });
    else if (m[2] !== undefined) out.push({ t: 'id', v: m[2] });
    else if (m[3] !== undefined) out.push({ t: 'op', v: m[3] });
    else out.push({ t: 'str', v: m[4] !== undefined ? m[4].replaceAll("''", "'") : m[5] });
  }
  return out;
}

// Same precedence as JavaScript: relational binds tighter than equality.
const BIN_PREC = { '||': 2, '&&': 3, '==': 4, '!=': 4, '<': 5, '>': 5, '<=': 5, '>=': 5, '+': 6, '-': 6, '*': 7, '/': 7 };
const UNARY_PREC = 8;

// Expressions nested deeper than this are refused: compiling them would
// overflow the call stack (they are generated, never written by hand).
export const MAX_DEPTH = 1000;
const tooDeep = () => new SyntaxError(`vectro: expression too deep (more than ${MAX_DEPTH} levels of nesting or terms in a chain); split it up`);

export function parse(src) {
  try { return parseText(src); } catch (err) { // (a smaller stack than ours runs out before the limits below)
    if (err instanceof RangeError && /call stack/i.test(err.message)) throw tooDeep();
    throw err;
  }
}

function parseText(src) {
  if (typeof src !== 'string') throw new TypeError(`vectro: an expression must be text, like "price > 10", got ${src === null ? 'null' : typeof src}`);
  const toks = tokenize(src);
  let i = 0, level = 0;
  const err = (msg) => new SyntaxError(`vectro: ${msg} in "${src}"`);
  const peek = () => toks[i];
  const expect = (v) => { const t = toks[i++]; if (!t || t.v !== v) throw err(`expected "${v}"`); };

  function primary() {
    const t = toks[i++];
    if (!t) throw err('unexpected end');
    if (t.t === 'num') return { t: 'num', v: t.v };
    if (t.t === 'str') return { t: 'str', v: t.v };
    if (t.t === 'id') {
      if (peek()?.v !== '(') {
        if (t.v === 'Infinity') return { t: 'num', v: Infinity };
        if (t.v === 'NaN') return { t: 'num', v: NaN };
        return { t: 'var', name: t.v };
      }
      i++;
      const args = [];
      if (peek()?.v !== ')') {
        args.push(expr(0));
        while (peek()?.v === ',') { i++; args.push(expr(0)); }
      }
      expect(')');
      return { t: 'call', fn: t.v, args };
    }
    if (t.v === '(') { const e = expr(0); expect(')'); return e; }
    if (t.v === '-' || t.v === '!') return { t: 'un', op: t.v, x: expr(UNARY_PREC) };
    throw err(`unexpected "${t.v}"`);
  }

  function expr(minPrec) {
    if (++level > 3 * MAX_DEPTH) throw tooDeep(); // (brackets count here, not below: the stack has room for them)
    let left = primary();
    for (;;) {
      const t = peek();
      if (!t || t.t !== 'op') break;
      if (t.v === '?' && minPrec <= 1) {
        i++;
        const x = expr(1);
        expect(':');
        left = { t: 'cond', c: left, x, y: expr(1) };
        continue;
      }
      const p = BIN_PREC[t.v];
      if (!p || p <= minPrec) break;
      i++;
      left = { t: 'bin', op: t.v, l: left, r: expr(p) };
    }
    level--;
    return left;
  }

  const ast = expr(0);
  if (i < toks.length) throw err(`unexpected "${toks[i].v}"`);
  // (a + b + c + ...: parsed in a loop, but deep for everything after)
  for (const stack = [[ast, 1]]; stack.length;) {
    const [n, d] = stack.pop();
    if (d > MAX_DEPTH) throw tooDeep();
    for (const c of [n.l, n.r, n.x, n.c, n.y, ...(n.args ?? [])]) if (c) stack.push([c, d + 1]);
  }
  return ast;
}

// agg()'s spec: { name: 'sum(x)', ... }
export function checkSpec(spec) {
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) throw new TypeError(`vectro: agg() needs an object of aggregations, like { total: 'sum(price)', n: 'count()' }, got ${spec === null ? 'null' : Array.isArray(spec) ? 'an array' : typeof spec}`);
}

// ---- analysis ----------------------------------------------------------------

const REDUCTIONS = new Set(['sum', 'count', 'mean', 'min', 'max']);
const UNARY_FN = new Set(['abs', 'sqrt', 'floor', 'ceil']);
const BINARY_FN = new Set(['min', 'max', 'mod']); // (mod(a, b): a % b exactly, as JavaScript's %)
const CMP = { '==': 'eq', '!=': 'ne', '<': 'lt', '>': 'gt', '<=': 'le', '>=': 'ge' };
const ARITH = { '+': 'add', '-': 'sub', '*': 'mul', '/': 'div' };

const isReduction = (n) => n.t === 'call' && REDUCTIONS.has(n.fn) && n.args.length === 1;
// Masks are true/false values (comparisons and logic); everything else is a number.
export const isMask = (n) => (n.t === 'bin' && (CMP[n.op] || n.op === '&&' || n.op === '||')) || (n.t === 'un' && n.op === '!');

// Text as the tokenizer reads it back exactly ('' inside '...' is one ').
export function quoteText(v) {
  if (!v.includes("'")) return `'${v}'`;
  if (!v.includes('"')) return `"${v}"`;
  return `'${v.replaceAll("'", "''")}'`; // (both kinds: ' doubled, as SQL writes it)
}

// Turn an AST back into source (fully parenthesized), e.g. after rewriting it.
export function toSource(n) {
  switch (n.t) {
    case 'num': return Number.isNaN(n.v) ? 'NaN' : n.v === Infinity ? 'Infinity' : n.v === -Infinity ? '(-Infinity)' : Object.is(n.v, -0) ? '(-0)' : n.v < 0 ? `(${n.v})` : String(n.v);
    case 'str': return quoteText(n.v);
    case 'var': return n.name;
    case 'un': return `${n.op}(${toSource(n.x)})`;
    case 'bin': return `(${toSource(n.l)} ${n.op} ${toSource(n.r)})`;
    case 'call': return `${n.fn}(${n.args.map(toSource).join(', ')})`;
    case 'cond': return `(${toSource(n.c)} ? ${toSource(n.x)} : ${toSource(n.y)})`;
  }
  throw new Error(`bad node ${n.t}`);
}

export function walk(n, f) {
  f(n);
  if (n.t === 'bin') { walk(n.l, f); walk(n.r, f); }
  else if (n.t === 'un') walk(n.x, f);
  else if (n.t === 'call') n.args.forEach((a) => walk(a, f));
  else if (n.t === 'cond') { walk(n.c, f); walk(n.x, f); walk(n.y, f); }
}

// text left in an expression a kernel computes (a text column was compared with < or >, or
// isn't one): a clear error, not a loop between the value and condition emitters
function noText(n) {
  if (n.t === 'str') throw new SyntaxError(`vectro: text like ${JSON.stringify(n.v)} can only be compared with == or != to a text column (to compare dates, load the column as dates: { dates: ['name'] })`);
}

function checkElementwise(n, src) {
  walk(n, (m) => {
    if (m.t === 'str') throw new SyntaxError(`vectro: text like ${JSON.stringify(m.v)} can only be compared (== / !=) with a text column, in "${src}"`);
    if (m.t !== 'call') return;
    if (isReduction(m)) throw new SyntaxError(`vectro: ${m.fn}() must be the outermost call in "${src}"`);
    const ok = (UNARY_FN.has(m.fn) && m.args.length === 1) || (BINARY_FN.has(m.fn) && m.args.length === 2);
    if (!ok) throw new SyntaxError(`vectro: unknown function ${m.fn}() with ${m.args.length} argument(s) in "${src}"`);
  });
}

// Parse a spec into either one elementwise map or a list of reductions.
// spec: string | string[] | { name: string }
export function analyze(spec) {
  const entries = typeof spec === 'string' ? [[null, spec]] : Array.isArray(spec) ? spec.map((s, i) => [i, s]) : Object.entries(spec);
  const items = entries.map(([key, src]) => {
    const ast = parse(src);
    if (isReduction(ast)) {
      let arg = ast.args[0];
      checkElementwise(arg, src);
      arg = fold(arg);
      if (ast.fn === 'count' && !isMask(arg)) arg = { t: 'bin', op: '!=', l: arg, r: { t: 'num', v: 0 } };
      return { key, src, reduce: ast.fn, ast: arg, raw: ast.args[0] };
    }
    checkElementwise(ast, src);
    return { key, src, reduce: null, ast: fold(ast), raw: ast };
  });
  const maps = items.filter((it) => !it.reduce);
  if (maps.length && items.length > 1) throw new SyntaxError('vectro: an elementwise expression must be compiled on its own (one output array)');
  if (items.filter((it) => it.reduce).length > 8) throw new RangeError('vectro: at most 8 reductions per kernel');

  const vars = [];
  for (const it of items) walk(it.raw, (n) => { if (n.t === 'var' && !vars.includes(n.name)) vars.push(n.name); }); // (before folding: the inputs stay the same)
  return { shape: typeof spec === 'string' ? 'single' : Array.isArray(spec) ? 'array' : 'object', items, vars, isMap: maps.length === 1 };
}

// Constant parts computed now: 2 * 3 -> 6, floor(2.5) -> 2, (1 ? x : y) -> x.
// Same results as the kernels (IEEE doubles; f32 kernels round literals).
const FOLD_BIN = { '+': (a, b) => a + b, '-': (a, b) => a - b, '*': (a, b) => a * b, '/': (a, b) => a / b,
  // as the kernels compute them: 1 / 0, a comparison with NaN false (but != true), NaN counting as true in && and ||
  '==': (a, b) => +(a === b), '!=': (a, b) => +(a !== b), '<': (a, b) => +(a < b), '<=': (a, b) => +(a <= b), '>': (a, b) => +(a > b), '>=': (a, b) => +(a >= b),
  '&&': (a, b) => +(a !== 0 && b !== 0), '||': (a, b) => +(a !== 0 || b !== 0) };
const FOLD_FN = { abs: Math.abs, sqrt: Math.sqrt, floor: Math.floor, ceil: Math.ceil, min: Math.min, max: Math.max, mod: (a, b) => a % b };
export function fold(n) {
  const isNum = (x) => x.t === 'num';
  switch (n.t) {
    case 'un': {
      const x = fold(n.x);
      if (isNum(x)) return { t: 'num', v: n.op === '-' ? -x.v : +(x.v === 0) };
      return { ...n, x };
    }
    case 'bin': {
      const l = fold(n.l), r = fold(n.r);
      if (isNum(l) && isNum(r) && FOLD_BIN[n.op]) return { t: 'num', v: FOLD_BIN[n.op](l.v, r.v) };
      return { ...n, l, r };
    }
    case 'call': {
      const args = n.args.map(fold);
      if (FOLD_FN[n.fn] && args.every(isNum)) return { t: 'num', v: FOLD_FN[n.fn](...args.map((a) => a.v)) };
      return { ...n, args };
    }
    case 'cond': {
      const c = fold(n.c), x = fold(n.x), y = fold(n.y);
      if (isNum(c)) return c.v !== 0 ? x : y; // (as in the kernels: NaN != 0, so NaN counts as true)
      return { ...n, c, x, y };
    }
  }
  return n;
}

// ---- code generation ---------------------------------------------------------

export const VEC = {
  f64: { add: 0xf0, sub: 0xf1, mul: 0xf2, div: 0xf3, min: 0xf4, max: 0xf5, abs: 0xec, neg: 0xed, sqrt: 0xef, ceil: 0x74, floor: 0x75, eq: 0x47, ne: 0x48, lt: 0x49, gt: 0x4a, le: 0x4b, ge: 0x4c },
  f32: { add: 0xe4, sub: 0xe5, mul: 0xe6, div: 0xe7, min: 0xe8, max: 0xe9, abs: 0xe0, neg: 0xe1, sqrt: 0xe3, ceil: 0x67, floor: 0x68, eq: 0x41, ne: 0x42, lt: 0x43, gt: 0x44, le: 0x45, ge: 0x46 },
};
export const SCA = {
  f64: { add: 0xa0, sub: 0xa1, mul: 0xa2, div: 0xa3, min: 0xa4, max: 0xa5, abs: 0x99, neg: 0x9a, sqrt: 0x9f, ceil: 0x9b, floor: 0x9c, eq: 0x61, ne: 0x62, lt: 0x63, gt: 0x64, le: 0x65, ge: 0x66, fromBool: 0xb8, fromF64: [] },
  f32: { add: 0x92, sub: 0x93, mul: 0x94, div: 0x95, min: 0x96, max: 0x97, abs: 0x8b, neg: 0x8c, sqrt: 0x91, ceil: 0x8d, floor: 0x8e, eq: 0x5b, ne: 0x5c, lt: 0x5d, gt: 0x5e, le: 0x5f, ge: 0x60, fromBool: 0xb3, fromF64: [0xb6] },
};
const V128_NOT = 0x4d, V128_AND = 0x4e, V128_OR = 0x50, BITSELECT = 0x52;
const F64X2_PROMOTE_LOW = 0x5f, F64X2_ADD = 0xf0, F64X2_MIN = 0xf4, F64X2_MAX = 0xf5;
const I32_AND = 0x71, I32_OR = 0x72, I32_EQZ = 0x45, SELECT = 0x1b, F64_PROMOTE_F32 = 0xbb;
const F64_OPS = { add: 0xa0, min: 0xa4, max: 0xa5 };
const HIGH_HALF = [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]; // i8x16.shuffle lanes

// Emits code for one AST in vector mode (v128, k = unroll slot) or scalar mode.
// Repeated subexpressions: after prepare(asts), a value subtree that occurs
// more than once in the kernel is computed once per row (per vector) into a
// local and read back after that; call reset() at the start of each loop body.
export class Emitter {
  constructor(f, t, env) {
    this.f = f;
    this.t = t;
    this.ty = TY[t];
    this.env = env;
    this.repeated = new Set();
    this.memo = new Map();
  }

  prepare(asts) {
    const seen = new Map();
    const visit = (n) => {
      if (n.t === 'num' || n.t === 'var' || n.t === 'str') return;
      const key = toSource(n);
      seen.set(key, (seen.get(key) ?? 0) + 1);
      if (seen.get(key) > 1) return; // (its parts are counted once)
      if (n.t === 'bin') { visit(n.l); visit(n.r); } else if (n.t === 'un') visit(n.x);
      else if (n.t === 'call') n.args.forEach(visit); else if (n.t === 'cond') { visit(n.c); visit(n.x); visit(n.y); }
    };
    for (const a of asts) if (a) visit(a);
    for (const [key, c] of seen) if (c > 1) this.repeated.add(key);
    this.keys = new Map();
  }

  reset() { this.memo.clear(); }

  // emit(node) once, then from a local (key: subtree + unroll slot or 'sca')
  #reuse(n, slot, type, emit) {
    if (!this.repeated.size || n.t === 'num' || n.t === 'var') return emit();
    let key = this.keys.get(n);
    if (key === undefined) { key = toSource(n); this.keys.set(n, key); }
    if (!this.repeated.has(key)) return emit();
    const m = `${key}|${slot}`;
    const local = this.memo.get(m);
    if (local !== undefined) return this.f.emit(get(local));
    emit();
    this.locals ??= new Map();
    let l = this.locals.get(m);
    if (l === undefined) { l = this.f.local(type); this.locals.set(m, l); }
    this.f.emit([0x22, ...get(l).slice(1)]); // local.tee
    this.memo.set(m, l);
  }

  vec(n, k) { noText(n); return this.#reuse(n, k, V128, () => this.#vec(n, k)); }
  sca(n) { noText(n); return this.#reuse(n, 'sca', this.ty.val, () => this.#sca(n)); }

  #vec(n, k) {
    const { f, env } = this, V = VEC[this.t];
    switch (n.t) {
      case 'num': return f.emit(get(env.constVec(n.v)));
      case 'var': return f.emit(get(env.arrays.has(n.name) ? env.arrays.get(n.name).vec[k] : env.scalars.get(n.name).vec));
      case 'un':
        if (n.op === '-') { this.vec(n.x, k); return f.emit(simd(V.neg)); }
        break;
      case 'bin':
        if (ARITH[n.op]) { this.vec(n.l, k); this.vec(n.r, k); return f.emit(simd(V[ARITH[n.op]])); }
        break;
      case 'call':
        if (n.fn === 'mod') { // (no vector remainder: lane by lane)
          const va = f.local(V128), vb = f.local(V128);
          this.vec(n.args[0], k); f.emit(set(va)); this.vec(n.args[1], k); f.emit(set(vb));
          return emitModVector(f, this.t, va, vb);
        }
        n.args.forEach((a) => this.vec(a, k));
        return f.emit(simd(V[n.fn]));
      case 'cond':
        this.vec(n.x, k); this.vec(n.y, k); this.vecMask(n.c, k);
        return f.emit(simd(BITSELECT));
    }
    // a mask used as a number: all-ones lanes AND 1.0 = 1.0, zero lanes stay 0.0
    this.vecMask(n, k);
    f.emit(get(env.constVec(1)), simd(V128_AND));
  }

  vecMask(n, k) {
    const { f, env } = this, V = VEC[this.t];
    if (n.t === 'bin' && CMP[n.op]) { this.vec(n.l, k); this.vec(n.r, k); return f.emit(simd(V[CMP[n.op]])); }
    if (n.t === 'bin' && (n.op === '&&' || n.op === '||')) { this.vecMask(n.l, k); this.vecMask(n.r, k); return f.emit(simd(n.op === '&&' ? V128_AND : V128_OR)); }
    if (n.t === 'un' && n.op === '!') { this.vecMask(n.x, k); return f.emit(simd(V128_NOT)); }
    this.vec(n, k); // a number used as a condition: true when != 0
    f.emit(get(env.constVec(0)), simd(V.ne));
  }

  #sca(n) {
    const { f, env, ty } = this, S = SCA[this.t];
    switch (n.t) {
      case 'num': return f.emit(ty.cnst(n.v));
      case 'var': return f.emit(get(env.arrays.has(n.name) ? env.arrays.get(n.name).sca : env.scalars.get(n.name).sca));
      case 'un':
        if (n.op === '-') { this.sca(n.x); return f.emit([S.neg]); }
        break;
      case 'bin':
        if (ARITH[n.op]) { this.sca(n.l); this.sca(n.r); return f.emit([S[ARITH[n.op]]]); }
        break;
      case 'call':
        n.args.forEach((a) => this.sca(a));
        if (n.fn === 'mod') return emitModScalar(f, this.t);
        return f.emit([S[n.fn]]);
      case 'cond':
        this.sca(n.x); this.sca(n.y); this.scaMask(n.c);
        return f.emit([SELECT]);
    }
    this.scaMask(n);
    f.emit([S.fromBool]);
  }

  scaMask(n) {
    const { f, ty } = this, S = SCA[this.t];
    if (n.t === 'bin' && CMP[n.op]) { this.sca(n.l); this.sca(n.r); return f.emit([S[CMP[n.op]]]); }
    if (n.t === 'bin' && (n.op === '&&' || n.op === '||')) { this.scaMask(n.l); this.scaMask(n.r); return f.emit([n.op === '&&' ? I32_AND : I32_OR]); }
    if (n.t === 'un' && n.op === '!') { this.scaMask(n.x); return f.emit([I32_EQZ]); }
    this.sca(n);
    f.emit(ty.cnst(0), [S.ne]);
  }
}

// Packed columns (see pack.mjs): load the codes of one vector's rows, widen
// them to 32-bit lanes, convert to floats, scale, and turn the missing code
// into NaN. off = byte offset of the vector's first row.
const PACKED_VEC = {
  f64: { u8: (off) => [...simd(0x02, ...memarg(0, off)), ...simd(0xa9), ...simd(0xff)], // load8x8_u, extend_low_u, convert_low_u
    u16: (off) => [...simd(0x5c, ...memarg(0, off)), ...simd(0xa9), ...simd(0xff)],     // load32_zero
    i32: (off) => [...simd(0x5d, ...memarg(0, off)), ...simd(0xfe)] },                   // load64_zero, convert_low_s
  f32: { u8: (off) => [...simd(0x5c, ...memarg(0, off)), ...simd(0x89), ...simd(0xa9), ...simd(0xfb)],
    u16: (off) => [...simd(0x04, ...memarg(0, off)), ...simd(0xfb)],                     // load16x4_u
    i32: (off) => [...simd(0x00, ...memarg(0, off)), ...simd(0xfa)] },
};
const PACKED_SCA = {
  f64: { u8: [0x2d, 0, 0, 0xb8], u16: [0x2f, 1, 0, 0xb8], i32: [0x28, 2, 0, 0xb7] },
  f32: { u8: [0x2d, 0, 0, 0xb3], u16: [0x2f, 1, 0, 0xb3], i32: [0x28, 2, 0, 0xb2] },
};
const PACKED_NAN = { u8: 255, u16: 65535, i32: -2147483648 };
export const PACKED_BYTES = { u8: 1, u16: 2, i32: 4 };

// One row of a packed column, decoded, on the stack (scalar kernels). raw and
// tmp: locals of the element type, used when the column has missing values.
export function packedScalar(f, t, enc, ptr, raw, tmp) {
  const ty = TY[t], S = SCA[t];
  const { kind, div, mul, base, nan } = enc;
  f.emit(get(ptr), PACKED_SCA[t][kind]);
  if (nan) f.emit([0x22, ...get(raw).slice(1)]); // local.tee
  if (div) f.emit(ty.cnst(div), [S.div]);
  else {
    if (mul !== 1) f.emit(ty.cnst(mul), [S.mul]);
    if (base !== 0) f.emit(ty.cnst(base), [S.add]);
  }
  if (nan) f.emit(set(tmp), ty.cnst(NaN), get(tmp), get(raw), ty.cnst(PACKED_NAN[kind]), [S.eq], [SELECT]);
}

// Build the kernel for one element type ('f64' | 'f32') and one binding of
// names to arrays/scalars. Signature: (array ptrs..., [out ptr], n, scalars...).
// encs[i]: the packing of array i (null: plain floats).
export function generate(analysis, t, arrayNames, scalarNames, shared = true, encs = []) {
  const ty = TY[t];
  const { items, isMap } = analysis;
  const nPtr = arrayNames.length + (isMap ? 1 : 0);
  const params = [...Array(nPtr).fill(I32), I32, ...Array(scalarNames.length).fill(F64)];
  const reductions = isMap ? [] : items;
  // Vectors per loop iteration. More gives the CPU independent additions to
  // overlap, but every reduction needs U accumulators and ARM has only 32
  // vector registers, so kernels with many reductions use fewer.
  const U = isMap || reductions.length > 2 ? 2 : 4;
  const f = new Fn('k', params, reductions.map(() => F64));
  const ptrs = Array.from({ length: nPtr }, (_, i) => i);
  const n = nPtr;

  // Values reused across the loop live in locals: constants, scalars, loaded vectors.
  const consts = new Map(); // (keyed so -0 and 0 get their own local: a Map takes them as one key)
  const env = {
    arrays: new Map(arrayNames.map((name, i) => [name, { ptr: i, vec: [], sca: 0, enc: encs[i] ?? null }])),
    scalars: new Map(),
    constVec: (v) => {
      const key = Object.is(v, -0) ? '-0' : v;
      if (!consts.has(key)) {
        const l = f.local(V128);
        f.emit(ty.cnst(v), ty.vsplat, set(l));
        consts.set(key, l);
      }
      return consts.get(key);
    },
  };
  for (const a of env.arrays.values()) {
    for (let k = 0; k < U; k++) a.vec.push(f.local(V128));
    a.sca = f.local(ty.val);
  }
  scalarNames.forEach((name, i) => {
    const param = nPtr + 1 + i;
    const vec = f.local(V128), sca = f.local(ty.val);
    f.emit(get(param), SCA[t].fromF64, set(sca), get(sca), ty.vsplat, set(vec));
    env.scalars.set(name, { vec, sca });
  });
  // Constants are created on first use, which must happen before the loop.
  const literals = [0, 1];
  for (const it of items) walk(it.ast, (m) => { if (m.t === 'num') literals.push(m.v); });
  for (const a of env.arrays.values()) {
    if (!a.enc) continue;
    literals.push(a.enc.div || a.enc.mul, a.enc.base);
    if (a.enc.nan) literals.push(NaN, PACKED_NAN[a.enc.kind]);
  }
  literals.forEach((v) => env.constVec(v));
  const V = VEC[t], S = SCA[t];
  const conv = f.local(V128), convS = f.local(ty.val);
  // one vector of a packed column -> decoded values on the stack
  const loadPacked = (a, k) => {
    const { kind, div, mul, base, nan } = a.enc;
    f.emit(get(a.ptr), PACKED_VEC[t][kind](k * ty.lanes * PACKED_BYTES[kind]));
    if (nan) f.emit([0x22, ...get(conv).slice(1)]); // local.tee: keep the raw value for the missing test
    if (div) f.emit(get(env.constVec(div)), simd(V.div));
    else {
      if (mul !== 1) f.emit(get(env.constVec(mul)), simd(V.mul));
      if (base !== 0) f.emit(get(env.constVec(base)), simd(V.add));
    }
    if (nan) { // bitselect(NaN, value, raw == missing code)
      f.emit(set(tmpDec), get(env.constVec(NaN)), get(tmpDec), get(conv), get(env.constVec(PACKED_NAN[kind])), simd(V.eq), simd(BITSELECT));
    }
  };
  const loadPackedSca = (a) => {
    const { kind, div, mul, base, nan } = a.enc;
    f.emit(get(a.ptr), PACKED_SCA[t][kind]);
    if (nan) f.emit([0x22, ...get(convS).slice(1)]);
    if (div) f.emit(ty.cnst(div), [S.div]);
    else {
      if (mul !== 1) f.emit(ty.cnst(mul), [S.mul]);
      if (base !== 0) f.emit(ty.cnst(base), [S.add]);
    }
    if (nan) f.emit(set(tmpDecS), ty.cnst(NaN), get(tmpDecS), get(convS), ty.cnst(PACKED_NAN[kind]), [S.eq], [SELECT]);
  };
  const tmpDec = f.local(V128), tmpDecS = f.local(ty.val);

  const em = new Emitter(f, t, env);
  em.prepare(items.map((it) => it.ast));
  const tmp = f.local(V128);

  // Accumulators. float64: f64x2 per slot. float32: sums widen to float64
  // (two f64x2 per slot, no precision loss); min/max stay f32x4.
  const accs = reductions.map((r) => {
    const op = r.reduce === 'min' ? 'min' : r.reduce === 'max' ? 'max' : 'add';
    const identity = op === 'min' ? Infinity : op === 'max' ? -Infinity : 0;
    const wide = t === 'f32' && op === 'add';
    const slots = Array.from({ length: wide ? 2 * U : U }, () => f.local(V128));
    for (const s of slots) {
      if (wide || t === 'f64') f.emit(TY.f64.cnst(identity), TY.f64.vsplat, set(s));
      else f.emit(ty.cnst(identity), ty.vsplat, set(s));
    }
    return { r, op, wide, slots, s: f.local(F64) };
  });

  const out = ptrs[nPtr - 1];
  loops(f, ty, {
    ptrs, n, U,
    strides: ptrs.map((i) => (encs[i] ? PACKED_BYTES[encs[i].kind] : ty.bytes)),
    body: () => {
      em.reset();
      for (let k = 0; k < U; k++) {
        for (const a of env.arrays.values()) {
          if (a.enc) { loadPacked(a, k); f.emit(set(a.vec[k])); } else f.emit(get(a.ptr), vload(16 * k), set(a.vec[k]));
        }
        if (isMap) { f.emit(get(out)); em.vec(items[0].ast, k); f.emit(vstore(16 * k)); continue; }
        for (const acc of accs) {
          if (acc.wide) {
            const [lo, hi] = [acc.slots[2 * k], acc.slots[2 * k + 1]];
            em.vec(acc.r.ast, k);
            f.emit(set(tmp));
            f.emit(get(lo), get(tmp), simd(F64X2_PROMOTE_LOW), simd(F64X2_ADD), set(lo));
            f.emit(get(hi), get(tmp), get(tmp), simd(0x0d, ...HIGH_HALF), simd(F64X2_PROMOTE_LOW), simd(F64X2_ADD), set(hi));
          } else {
            f.emit(get(acc.slots[k]));
            em.vec(acc.r.ast, k);
            f.emit(simd(VEC[t][acc.op]));
            f.emit(set(acc.slots[k]));
          }
        }
      }
    },
    between: () => {
      for (const acc of accs) {
        const f64Lanes = acc.wide || t === 'f64';
        const vop = f64Lanes ? { add: F64X2_ADD, min: F64X2_MIN, max: F64X2_MAX }[acc.op] : VEC[t][acc.op];
        f.emit(get(acc.slots[0]));
        for (let j = 1; j < acc.slots.length; j++) f.emit(get(acc.slots[j]), simd(vop));
        f.emit(set(acc.slots[0]));
        const lanes = f64Lanes ? 2 : 4;
        const lane = (l) => (f64Lanes ? simd(0x21, l) : [...simd(0x1f, l), F64_PROMOTE_F32]);
        f.emit(get(acc.slots[0]), lane(0));
        for (let l = 1; l < lanes; l++) f.emit(get(acc.slots[0]), lane(l), [F64_OPS[acc.op]]);
        f.emit(set(acc.s));
      }
    },
    tail: () => {
      em.reset();
      for (const a of env.arrays.values()) {
        if (a.enc) { loadPackedSca(a); f.emit(set(a.sca)); } else f.emit(get(a.ptr), ty.load, set(a.sca));
      }
      if (isMap) { f.emit(get(out)); em.sca(items[0].ast); f.emit(ty.store); return; }
      for (const acc of accs) {
        f.emit(get(acc.s));
        em.sca(acc.r.ast);
        if (t === 'f32') f.emit([F64_PROMOTE_F32]);
        f.emit([F64_OPS[acc.op]], set(acc.s));
      }
    },
  });
  for (const acc of accs) f.emit(get(acc.s));
  return buildModule([f], shared);
}
