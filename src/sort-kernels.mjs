// WebAssembly kernels for the parallel radix sort (see parallel-sort.mjs).
// Each runs over one unit of rows [start, end) and gets its unit number, so
// it can keep its own digit counts and write to its own output positions
// (OP.UNITS). Pointers are byte offsets into the engine's memory.
//
// Keys become two uint32 words (lo, hi) that sort as unsigned numbers:
// whole-number keys as their offset from the minimum, others as float bits
// with the sign flipped (negatives: every bit flipped; NaN last). Digits are
// 11 bits: lo 0-10, 11-21, 22-31, then hi the same.

import { I32, F64, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32mul, i32and, i32geu } from './wasm.mjs';

const I64 = 0x7e;
const I32_LOAD = [0x28, 2, 0], I32_STORE = [0x36, 2, 0];
const F64_LOAD = [0x2b, 3, 0], F32_LOAD = [0x2a, 2, 0], F64_STORE = [0x39, 3, 0], F32_STORE = [0x38, 2, 0];
const I32_SHR_U = [0x76], I32_SHL = [0x74], SELECT = [0x1b], IF = [0x04, 0x40], IF_I32 = [0x04, 0x7f], ELSE = [0x05];
const f64c = (v) => [0x44, ...new Uint8Array(Float64Array.of(v).buffer)];
const i64c = (v) => { // signed LEB128 of a BigInt
  const out = [0x42];
  for (;;) {
    const b = Number(v & 0x7fn);
    v >>= 7n;
    if ((v === 0n && !(b & 0x40)) || (v === -1n && b & 0x40)) { out.push(b); return out; }
    out.push(b | 0x80);
  }
};
const addr4 = (base, i) => [...get(i), ...i32c(2), ...I32_SHL, ...get(base), ...i32add]; // base + i * 4
export const DIGIT_BITS = 11, BUCKETS = 2048;
export const DIGITS = [[0, 0], [0, 11], [0, 22], [1, 0], [1, 11], [1, 22]]; // [word (0 = lo), shift]

// prep(start, end, unit, keyPtr, idxPtr, hasIdx, loPtr, hiPtr, whole, histPtr, min)
// lo[i], hi[i] = words of key[idx[i]] (key[i] without idx); counts all six
// digits into hist[unit][digit][bucket].
export function prepModule(t, shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, I32, I32, I32, I32, F64], []);
  const [start, stop, unit, keyPtr, idxPtr, hasIdx, loPtr, hiPtr, whole, histPtr, min] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const i = f.local(I32), r = f.local(I32), h = f.local(I32), l = f.local(I32), hb = f.local(I32), a = f.local(I32);
  const v = f.local(F64), o = f.local(F64), hf = f.local(F64), b = f.local(I64);
  f.emit(get(histPtr), get(unit), i32c(6 * BUCKETS * 4), i32mul, i32add, set(hb));
  f.emit(get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(get(hasIdx), IF_I32, addr4(idxPtr, i), I32_LOAD, ELSE, get(i), end, set(r));
  f.emit(get(r), i32c(t === 'f32' ? 2 : 3), I32_SHL, get(keyPtr), i32add, t === 'f32' ? [...F32_LOAD, 0xbb] : F64_LOAD, set(v));
  f.emit(get(whole), IF);
  f.emit(get(v), get(min), [0xa1], set(o));                                     // o = v - min
  f.emit(get(o), f64c(4294967296), [0xa3], [0x9c], set(hf));                    // hf = floor(o / 2^32)
  f.emit(get(hf), [0xfc, 0x03], set(h), get(o), get(hf), f64c(4294967296), [0xa2], [0xa1], [0xfc, 0x03], set(l));
  f.emit(ELSE);
  f.emit(get(v), [0xbd], set(b));                                               // b = bits(v)
  f.emit(get(b), i64c(-1n), [0x85], get(b), i64c(-0x8000000000000000n), [0x84], get(b), i64c(0n), [0x53], SELECT, set(b)); // b < 0 ? ~b : b | sign
  f.emit(get(v), get(v), [0x62], IF, i64c(-1n), set(b), end);                   // NaN: all ones (last)
  f.emit(get(b), i64c(32n), [0x88], [0xa7], set(h), get(b), [0xa7], set(l));
  f.emit(end);
  f.emit(addr4(loPtr, i), get(l), I32_STORE, addr4(hiPtr, i), get(h), I32_STORE);
  DIGITS.forEach(([word, shift], d) => {
    f.emit(get(word ? h : l), i32c(shift), I32_SHR_U, i32c(BUCKETS - 1), i32and, i32c(d * BUCKETS), i32add);
    f.emit(i32c(2), I32_SHL, get(hb), i32add, set(a), get(a), get(a), I32_LOAD, i32c(1), i32add, I32_STORE);
  });
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}

// count(start, end, unit, wordPtr, shift, histPtr): hist[unit][bucket] of one digit.
export function countModule(shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32], []);
  const [start, stop, unit, wordPtr, shift, histPtr] = [0, 1, 2, 3, 4, 5];
  const i = f.local(I32), hb = f.local(I32), a = f.local(I32);
  f.emit(get(histPtr), get(unit), i32c(BUCKETS * 4), i32mul, i32add, set(hb), get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(addr4(wordPtr, i), I32_LOAD, get(shift), I32_SHR_U, i32c(BUCKETS - 1), i32and, i32c(2), I32_SHL, get(hb), i32add, set(a));
  f.emit(get(a), get(a), I32_LOAD, i32c(1), i32add, I32_STORE);
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}

// move(start, end, unit, word, shift, idxS, loS, hiS, idxD, loD, hiD, offPtr, carryLo):
// each row goes to off[unit][its bucket]++ (offsets worked out on the main
// thread, so the sort stays stable and no two units write the same place).
export function moveModule(shared) {
  const f = new Fn('k', Array(13).fill(I32), []);
  const [start, stop, unit, word, shift, idxS, loS, hiS, idxD, loD, hiD, offPtr, carryLo] = [...Array(13).keys()];
  const i = f.local(I32), ob = f.local(I32), a = f.local(I32), p = f.local(I32), l = f.local(I32), h = f.local(I32);
  f.emit(get(offPtr), get(unit), i32c(BUCKETS * 4), i32mul, i32add, set(ob), get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(addr4(loS, i), I32_LOAD, set(l), addr4(hiS, i), I32_LOAD, set(h));
  f.emit(get(h), get(l), get(word), SELECT, get(shift), I32_SHR_U, i32c(BUCKETS - 1), i32and, i32c(2), I32_SHL, get(ob), i32add, set(a));
  f.emit(get(a), I32_LOAD, set(p), get(a), get(p), i32c(1), i32add, I32_STORE);
  f.emit(addr4(idxD, p), addr4(idxS, i), I32_LOAD, I32_STORE);
  f.emit(get(carryLo), IF, addr4(loD, p), get(l), I32_STORE, end);
  f.emit(addr4(hiD, p), get(h), I32_STORE);
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}

// gather(start, end, unit, src, perm, dst): dst[i] = src[perm[i]] (float64 or float32).
export function gatherModule(t, shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32], []);
  const [start, stop, , src, perm, dst] = [0, 1, 2, 3, 4, 5];
  const i = f.local(I32), sh = t === 'f32' ? 2 : 3;
  f.emit(get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(get(i), i32c(sh), I32_SHL, get(dst), i32add);
  f.emit(addr4(perm, i), I32_LOAD, i32c(sh), I32_SHL, get(src), i32add, t === 'f32' ? F32_LOAD : F64_LOAD);
  f.emit(t === 'f32' ? F32_STORE : F64_STORE);
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}
