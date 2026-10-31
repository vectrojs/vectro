// Kernels for the rows that can rank in the first k of their partition (see
// Table #firstRowsG), run as ranged group kernels (OP.GROUP_RANGE): every
// thread reads all rows and handles only the partitions in its range
// [lo, hi), so each partition's slot is written by one thread.
//
// Keys and partition numbers are float64 arrays. Slot p, k + 1 float64s at
// top + p * (k + 1) * 8, holds how many keys it has, then its k smallest so
// far, ascending, NaN last. neg != 0: keys are negated as read (descending).

import { I32, F64, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32mul } from './wasm.mjs';

const F64_LOAD = [0x2b, 3, 0], F64_STORE = [0x39, 3, 0], I32_STORE8 = [0x3a, 0, 0];
const TRUNC_U = [0xfc, 0x03], F64_FROM_U = [0xb8], F64_NEG = [0x9a], SELECT = [0x1b];
const F64_EQ = [0x61], F64_NE = [0x62], F64_LT = [0x63], F64_LE = [0x65];
const I32_SUB = [0x6b], I32_LT_U = [0x49], I32_AND = [0x71], I32_OR = [0x72], I32_SHL = [0x74], IF = [0x04, 0x40], ELSE = [0x05];
const f64c = (v) => [0x44, ...new Uint8Array(Float64Array.of(v).buffer)];

// Shared start of both loops: p = partition of row i; when it is in range,
// v = its key and s = its slot's address; else on to the next row.
function rowStart(f, { key, g, lo, hi, top, neg }, { i, p, v, s, w8 }, skip) {
  f.emit(get(i), i32c(3), I32_SHL, get(g), i32add, F64_LOAD, TRUNC_U, set(p));
  f.emit(get(p), get(lo), I32_SUB, get(hi), get(lo), I32_SUB, I32_LT_U, [0x45], brIf(skip)); // (p - lo) < (hi - lo), unsigned
  f.emit(get(i), i32c(3), I32_SHL, get(key), i32add, F64_LOAD, set(v));
  f.emit(get(v), F64_NEG, get(v), get(neg), f64c(0), F64_NE, SELECT, set(v));
  f.emit(get(p), get(w8), i32mul, get(top), i32add, set(s));
}

// v sorts before a (NaN last): (a is NaN and v isn't) or v < a
const before = (v, a) => [...get(a), ...get(a), ...F64_NE, ...get(v), ...get(v), ...F64_EQ, ...I32_AND, ...get(v), ...get(a), ...F64_LT, ...I32_OR];

// (key, g, n, top, K, lo, hi, k, neg): each partition's k smallest keys.
export function topModule(shared = true) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, I32, F64, F64], []);
  const P = { key: 0, g: 1, n: 2, top: 3, lo: 5, hi: 6, k: 7, neg: 8 };
  const L = { i: f.local(I32), p: f.local(I32), s: f.local(I32), c: f.local(I32), j: f.local(I32), kk: f.local(I32), w8: f.local(I32), v: f.local(F64), a: f.local(F64) };
  f.emit(get(P.k), TRUNC_U, set(L.kk), get(L.kk), i32c(1), i32add, i32c(8), i32mul, set(L.w8));
  f.emit(i32c(0), set(L.i));
  f.emit(block, loop, get(L.i), get(P.n), [0x4f], brIf(1)); // i >= n: done
  f.emit(block); // (next row)
  rowStart(f, P, L, 0);
  f.emit(get(L.s), F64_LOAD, TRUNC_U, set(L.c));
  f.emit(get(L.c), get(L.kk), I32_LT_U, IF); // fewer than k so far: one more
  f.emit(get(L.c), i32c(1), i32add, set(L.j), get(L.s), get(L.j), F64_FROM_U, F64_STORE);
  f.emit(ELSE); // else only if it sorts before the k-th
  f.emit(get(L.s), get(L.kk), i32c(3), I32_SHL, i32add, F64_LOAD, set(L.a));
  f.emit(before(L.v, L.a), [0x45], brIf(1), get(L.kk), set(L.j));
  f.emit(end);
  // insert at j, moving larger keys up one
  f.emit(block, loop, get(L.j), i32c(2), I32_LT_U, brIf(1));
  f.emit(get(L.s), get(L.j), i32c(1), I32_SUB, i32c(3), I32_SHL, i32add, F64_LOAD, set(L.a));
  f.emit(before(L.v, L.a), [0x45], brIf(1));
  f.emit(get(L.s), get(L.j), i32c(3), I32_SHL, i32add, get(L.a), F64_STORE);
  f.emit(get(L.j), i32c(1), I32_SUB, set(L.j), br(0), end, end);
  f.emit(get(L.s), get(L.j), i32c(3), I32_SHL, i32add, get(L.v), F64_STORE);
  f.emit(end); // (next row)
  f.emit(get(L.i), i32c(1), i32add, set(L.i), br(0), end, end);
  return buildModule([f], shared);
}

// (key, g, mask, n, top, K, lo, hi, k, neg): mask[i] (a byte) = 1 when row i
// can rank in the first k: its partition has fewer than k keys, or its key
// is at most the k-th smallest (every key, when that is NaN).
export function markModule(shared = true) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, I32, I32, F64, F64], []);
  const P = { key: 0, g: 1, mask: 2, n: 3, top: 4, lo: 6, hi: 7, k: 8, neg: 9 };
  const L = { i: f.local(I32), p: f.local(I32), s: f.local(I32), kk: f.local(I32), w8: f.local(I32), v: f.local(F64), a: f.local(F64) };
  f.emit(get(P.k), TRUNC_U, set(L.kk), get(L.kk), i32c(1), i32add, i32c(8), i32mul, set(L.w8));
  f.emit(i32c(0), set(L.i));
  f.emit(block, loop, get(L.i), get(P.n), [0x4f], brIf(1));
  f.emit(block);
  rowStart(f, P, L, 0);
  f.emit(get(L.s), get(L.kk), i32c(3), I32_SHL, i32add, F64_LOAD, set(L.a)); // the k-th smallest
  f.emit(get(P.mask), get(L.i), i32add);
  f.emit(get(L.s), F64_LOAD, TRUNC_U, get(L.kk), I32_LT_U);
  f.emit(get(L.a), get(L.a), F64_NE, I32_OR);
  f.emit(get(L.v), get(L.a), F64_LE, I32_OR);
  f.emit(I32_STORE8);
  f.emit(end);
  f.emit(get(L.i), i32c(1), i32add, set(L.i), br(0), end, end);
  return buildModule([f], shared);
}
