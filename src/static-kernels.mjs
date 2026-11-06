// The engine's fixed WebAssembly module: the built-in kernels plus the
// building blocks of the vectorized interpreter (elementwise math,
// comparisons, select, widening sums, group-by scatter, compaction).
//
// It never changes, so it can be built ahead of time (scripts/build-wasm.mjs)
// for places that forbid compiling WebAssembly at runtime, like Cloudflare
// Workers. With it, nothing else needs code generation.

import { I32, F64, V128, TY, Fn, loops, buildModule, kernelFns, get, set, i32c, simd, vload, vstore, block, loop, end, br, brIf, i32add, i32geu, emitModScalar, emitModVector } from './wasm.mjs';
import { VEC, SCA, F32X4_DEMOTE_ZERO, F32_DEMOTE, F64_PROMOTE } from './fuse.mjs';

const V128_AND = 0x4e, BITSELECT = 0x52, F64X2_PROMOTE_LOW = 0x5f, F64X2_ADD = 0xf0, SHUFFLE = 0x0d;
const SELECT = [0x1b], I32_SUB = [0x6b], I32_SHR_U = [0x76], I32_SHL = [0x74], I32_MUL = [0x6c];
const F64_LOAD = [0x2b, 3, 0], F64_STORE = [0x39, 3, 0], F64_PROMOTE_F32 = [0xbb];
const F64_OP = { add: [0xa0], min: [0xa4], max: [0xa5] };
const TRUNC_SAT_U = { f64: [0xfc, 0x03], f32: [0xfc, 0x01] };
const HIGH_HALF = [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7];
const U = 2;

export const BINARY_OPS = ['add', 'sub', 'mul', 'div', 'min', 'max'];
export const COMPARE_OPS = ['eq', 'ne', 'lt', 'gt', 'le', 'ge'];
export const UNARY_OPS = ['neg', 'abs', 'sqrt', 'floor', 'ceil', 'fround'];

// dst = a OP b; comparisons give 1.0 or 0.0. (dst, a, b, n)
function vbinary(t, op) {
  const ty = TY[t], cmp = COMPARE_OPS.includes(op);
  const f = new Fn(`v${op}_${t}`, [I32, I32, I32, I32], []);
  const [dst, a, b, n] = [0, 1, 2, 3];
  const one = f.local(V128);
  f.emit(ty.cnst(1), ty.vsplat, set(one));
  loops(f, ty, {
    ptrs: [a, b, dst], n, U,
    body: () => {
      for (let k = 0; k < U; k++) {
        f.emit(get(dst), get(a), vload(16 * k), get(b), vload(16 * k), simd(VEC[t][op]));
        if (cmp) f.emit(get(one), simd(V128_AND));
        f.emit(vstore(16 * k));
      }
    },
    tail: () => {
      f.emit(get(dst), get(a), ty.load, get(b), ty.load, [SCA[t][op]]);
      if (cmp) f.emit([SCA[t].fromBool]);
      f.emit(ty.store);
    },
  });
  return f;
}

// dst = a % b exactly (vmod_f64 / vmod_f32; the remainder is function 0 of the module). (dst, a, b, n)
function vmod(t) {
  const ty = TY[t];
  const f = new Fn(`vmod_${t}`, [I32, I32, I32, I32], []);
  const [dst, a, b, n] = [0, 1, 2, 3];
  const va = f.local(V128), vb = f.local(V128);
  loops(f, ty, {
    ptrs: [a, b, dst], n, U,
    body: () => {
      for (let k = 0; k < U; k++) {
        f.emit(get(dst), get(a), vload(16 * k), set(va), get(b), vload(16 * k), set(vb));
        emitModVector(f, t, va, vb);
        f.emit(vstore(16 * k));
      }
    },
    tail: () => { f.emit(get(dst), get(a), ty.load, get(b), ty.load); emitModScalar(f, t); f.emit(ty.store); },
  });
  return f;
}

// dst = OP a. (dst, a, n)
function vunary(t, op) {
  const ty = TY[t];
  const f = new Fn(`v${op}_${t}`, [I32, I32, I32], []);
  const [dst, a, n] = [0, 1, 2];
  // (fround: to a 32-bit float and back; f32 values already are)
  const vop = op !== 'fround' ? simd(VEC[t][op]) : t === 'f64' ? [...simd(F32X4_DEMOTE_ZERO), ...simd(F64X2_PROMOTE_LOW)] : [];
  const sop = op !== 'fround' ? [SCA[t][op]] : t === 'f64' ? [F32_DEMOTE, F64_PROMOTE] : [];
  loops(f, ty, {
    ptrs: [a, dst], n, U,
    body: () => { for (let k = 0; k < U; k++) f.emit(get(dst), get(a), vload(16 * k), vop, vstore(16 * k)); },
    tail: () => f.emit(get(dst), get(a), ty.load, sop, ty.store),
  });
  return f;
}

// dst = m != 0 ? a : b. (dst, m, a, b, n)
function vselect(t) {
  const ty = TY[t];
  const f = new Fn(`vsel_${t}`, [I32, I32, I32, I32, I32], []);
  const [dst, m, a, b, n] = [0, 1, 2, 3, 4];
  const zero = f.local(V128);
  f.emit(ty.cnst(0), ty.vsplat, set(zero));
  loops(f, ty, {
    ptrs: [m, a, b, dst], n, U,
    body: () => {
      for (let k = 0; k < U; k++) {
        f.emit(get(dst), get(a), vload(16 * k), get(b), vload(16 * k));
        f.emit(get(m), vload(16 * k), get(zero), simd(VEC[t].ne), simd(BITSELECT), vstore(16 * k));
      }
    },
    tail: () => f.emit(get(dst), get(a), ty.load, get(b), ty.load, get(m), ty.load, ty.cnst(0), [SCA[t].ne], SELECT, ty.store),
  });
  return f;
}

// Sum of float32 values, added up in float64 so millions of values keep their precision.
function sumWide() {
  const ty = TY.f32;
  const f = new Fn('vsumw_f32', [I32, I32], [F64]);
  const [p, n] = [0, 1];
  const accs = Array.from({ length: 2 * U }, () => f.local(V128));
  const tmp = f.local(V128), s = f.local(F64);
  for (const a of accs) f.emit(TY.f64.cnst(0), TY.f64.vsplat, set(a));
  loops(f, ty, {
    ptrs: [p], n, U,
    body: () => {
      for (let k = 0; k < U; k++) {
        const [lo, hi] = [accs[2 * k], accs[2 * k + 1]];
        f.emit(get(p), vload(16 * k), set(tmp));
        f.emit(get(lo), get(tmp), simd(F64X2_PROMOTE_LOW), simd(F64X2_ADD), set(lo));
        f.emit(get(hi), get(tmp), get(tmp), simd(SHUFFLE, ...HIGH_HALF), simd(F64X2_PROMOTE_LOW), simd(F64X2_ADD), set(hi));
      }
    },
    between: () => {
      f.emit(get(accs[0]));
      for (let j = 1; j < accs.length; j++) f.emit(get(accs[j]), simd(F64X2_ADD));
      f.emit(set(accs[0]), get(accs[0]), simd(0x21, 0), get(accs[0]), simd(0x21, 1), F64_OP.add, set(s));
    },
    tail: () => f.emit(get(s), get(p), ty.load, F64_PROMOTE_F32, F64_OP.add, set(s)),
  });
  f.emit(get(s));
  return f;
}

// acc[key] = acc[key] OP val for each row (rows whose mask is 0 are skipped).
// (key, val, [mask,] acc, n); acc is float64.
function groupScatter(t, op, masked) {
  const ty = TY[t];
  const params = masked ? [I32, I32, I32, I32, I32] : [I32, I32, I32, I32];
  const f = new Fn(`g${op}${masked ? 'm' : ''}_${t}`, params, []);
  const [key, val] = [0, 1];
  const mask = masked ? 2 : -1, acc = masked ? 3 : 2, n = masked ? 4 : 3;
  const stop = f.local(I32), addr = f.local(I32);
  f.emit(get(n), i32c(ty.bytes), I32_MUL, get(key), i32add, set(stop));
  f.emit(block, loop, get(key), get(stop), i32geu, brIf(1));
  f.emit(block);
  if (masked) f.emit(get(mask), ty.load, ty.cnst(0), [SCA[t].eq], brIf(0));
  f.emit(get(key), ty.load, TRUNC_SAT_U[t], i32c(3), I32_SHL, get(acc), i32add, set(addr));
  f.emit(get(addr), get(addr), F64_LOAD, get(val), ty.load, t === 'f32' ? F64_PROMOTE_F32 : [], F64_OP[op], F64_STORE);
  f.emit(end);
  for (const p of masked ? [key, val, mask] : [key, val]) f.emit(get(p), i32c(ty.bytes), i32add, set(p));
  f.emit(br(0), end, end);
  return f;
}

// Copy src rows whose mask is non-zero to dst; returns how many were copied.
// (mask, src, dst, n)
function compact(t) {
  const ty = TY[t];
  const f = new Fn(`compact_${t}`, [I32, I32, I32, I32], [I32]);
  const [mask, src, dst, n] = [0, 1, 2, 3];
  const stop = f.local(I32), start = f.local(I32);
  f.emit(get(dst), set(start));
  f.emit(get(n), i32c(ty.bytes), I32_MUL, get(src), i32add, set(stop));
  f.emit(block, loop, get(src), get(stop), i32geu, brIf(1));
  f.emit(block, get(mask), ty.load, ty.cnst(0), [SCA[t].eq], brIf(0));
  f.emit(get(dst), get(src), ty.load, ty.store, get(dst), i32c(ty.bytes), i32add, set(dst));
  f.emit(end);
  for (const p of [mask, src]) f.emit(get(p), i32c(ty.bytes), i32add, set(p));
  f.emit(br(0), end, end);
  f.emit(get(dst), get(start), I32_SUB, i32c(Math.log2(ty.bytes)), I32_SHR_U);
  return f;
}

export function interpreterFns() {
  const fns = [sumWide()];
  for (const t of ['f64', 'f32']) {
    for (const op of [...BINARY_OPS, ...COMPARE_OPS]) fns.push(vbinary(t, op));
    for (const op of UNARY_OPS) fns.push(vunary(t, op));
    fns.push(vmod(t));
    fns.push(vselect(t), compact(t));
    for (const op of ['add', 'min', 'max']) fns.push(groupScatter(t, op, false), groupScatter(t, op, true));
  }
  return fns;
}

export function buildStaticModule(shared) {
  return buildModule([...kernelFns(), ...interpreterFns()], shared); // (vmod calls the remainder: buildModule puts it first)
}
