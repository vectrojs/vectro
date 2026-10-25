// Kernel fusion: compile an expression like `sum(a * 2 + 1)` or
// `[sum(a), min(a), count(a > 500 && b < 3)]` into ONE WebAssembly SIMD
// function that reads each array once, instead of one pass per operation.

import { I32, F64, V128, TY, Fn, loops, buildModule, get, set, simd, vload, vstore } from './wasm.mjs';

// ---- parsing -----------------------------------------------------------------

const TOKEN = /\s*(?:(\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)|([A-Za-z_]\w*)|(&&|\|\||==|!=|<=|>=|[-+*/()<>,?:!])|'([^']*)'|"([^"]*)")/y;

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
    else out.push({ t: 'str', v: m[4] ?? m[5] });
  }
  return out;
}

// Same precedence as JavaScript: relational binds tighter than equality.
const BIN_PREC = { '||': 2, '&&': 3, '==': 4, '!=': 4, '<': 5, '>': 5, '<=': 5, '>=': 5, '+': 6, '-': 6, '*': 7, '/': 7 };
const UNARY_PREC = 8;

export function parse(src) {
  const toks = tokenize(src);
  let i = 0;
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
    return left;
  }

  const ast = expr(0);
  if (i < toks.length) throw err(`unexpected "${toks[i].v}"`);
  return ast;
}

// ---- analysis ----------------------------------------------------------------

const REDUCTIONS = new Set(['sum', 'count', 'mean', 'min', 'max']);
const UNARY_FN = new Set(['abs', 'sqrt', 'floor', 'ceil']);
const BINARY_FN = new Set(['min', 'max']);
const CMP = { '==': 'eq', '!=': 'ne', '<': 'lt', '>': 'gt', '<=': 'le', '>=': 'ge' };
const ARITH = { '+': 'add', '-': 'sub', '*': 'mul', '/': 'div' };

const isReduction = (n) => n.t === 'call' && REDUCTIONS.has(n.fn) && n.args.length === 1;
// Masks are true/false values (comparisons and logic); everything else is a number.
export const isMask = (n) => (n.t === 'bin' && (CMP[n.op] || n.op === '&&' || n.op === '||')) || (n.t === 'un' && n.op === '!');

// Turn an AST back into source (fully parenthesized), e.g. after rewriting it.
export function toSource(n) {
  switch (n.t) {
    case 'num': return Number.isNaN(n.v) ? 'NaN' : n.v === Infinity ? 'Infinity' : n.v === -Infinity ? '(-Infinity)' : n.v < 0 ? `(${n.v})` : String(n.v);
    case 'str': return JSON.stringify(n.v);
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
      if (ast.fn === 'count' && !isMask(arg)) arg = { t: 'bin', op: '!=', l: arg, r: { t: 'num', v: 0 } };
      return { key, src, reduce: ast.fn, ast: arg };
    }
    checkElementwise(ast, src);
    return { key, src, reduce: null, ast };
  });
  const maps = items.filter((it) => !it.reduce);
  if (maps.length && items.length > 1) throw new SyntaxError('vectro: an elementwise expression must be compiled on its own (one output array)');
  if (items.filter((it) => it.reduce).length > 8) throw new RangeError('vectro: at most 8 reductions per kernel');

  const vars = [];
  for (const it of items) walk(it.ast, (n) => { if (n.t === 'var' && !vars.includes(n.name)) vars.push(n.name); });
  return { shape: typeof spec === 'string' ? 'single' : Array.isArray(spec) ? 'array' : 'object', items, vars, isMap: maps.length === 1 };
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
export class Emitter {
  constructor(f, t, env) {
    this.f = f;
    this.t = t;
    this.ty = TY[t];
    this.env = env;
  }

  vec(n, k) {
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

  sca(n) {
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

// Build the kernel for one element type ('f64' | 'f32') and one binding of
// names to arrays/scalars. Signature: (array ptrs..., [out ptr], n, scalars...).
export function generate(analysis, t, arrayNames, scalarNames, shared = true) {
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
  const consts = new Map();
  const env = {
    arrays: new Map(arrayNames.map((name, i) => [name, { ptr: i, vec: [], sca: 0 }])),
    scalars: new Map(),
    constVec: (v) => {
      if (!consts.has(v)) {
        const l = f.local(V128);
        f.emit(ty.cnst(v), ty.vsplat, set(l));
        consts.set(v, l);
      }
      return consts.get(v);
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
  literals.forEach((v) => env.constVec(v));

  const em = new Emitter(f, t, env);
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
    body: () => {
      for (let k = 0; k < U; k++) {
        for (const a of env.arrays.values()) f.emit(get(a.ptr), vload(16 * k), set(a.vec[k]));
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
      for (const a of env.arrays.values()) f.emit(get(a.ptr), ty.load, set(a.sca));
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
