// Bit-packed codes, for datasets on disk. Packed columns (pack.mjs) keep
// their codes in 1, 2 or 4 bytes so the kernels can read them directly; on
// disk each code takes only the bits it needs (a quantity 1..9: 4 bits, a
// price in cents up to 999.99: 17), so a query reads fewer bytes. Reading a
// chunk unpacks them back to bytes on every thread.
//
// A column's codes are stored as (code - base) in `bits` bits, little-endian
// in 32-bit words; with missing values, the all-ones value means missing.

import { I32, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32geu, i32wrapI64 } from './wasm.mjs';

const I64 = 0x7e;
const I64_EXTEND_U = [0xad], I64_MUL = [0x7e], I64_SHR_U = [0x88], I64_AND = [0x83], I64_SHL = [0x86], I64_SUB = [0x7d], I64_EQ = [0x51];
const I64_LOAD = [0x29, 0, 0], SELECT = [0x1b];
const STORE = { 1: [0x3a, 0, 0], 2: [0x3b, 1, 0], 4: [0x36, 2, 0] }; // i32.store8 / store16 / store
const SHIFT = { 1: 0, 2: 1, 4: 2 };
const i64one = [0x42, 1];

// The fewest bits that hold 0..max (1..32).
export function bitsFor(max) {
  let b = 1;
  while (b < 32 && max >= 2 ** b) b++;
  return b;
}

// How a column's codes would be stored: { bits, base, missing } where
// `missing` is the codes' NaN code if any row has it; or null when packing to
// the bit saves nothing over `byteBits`.
export function bitPlan(codes, n, nanCode, byteBits) {
  let lo = Infinity, hi = -Infinity, missing = false;
  for (let i = 0; i < n; i++) {
    const c = codes[i];
    if (c === nanCode) { missing = true; continue; }
    if (c < lo) lo = c;
    if (c > hi) hi = c;
  }
  if (lo === Infinity) lo = hi = 0;
  const bits = bitsFor(hi - lo + (missing ? 1 : 0));
  return bits < byteBits ? { bits, base: lo, missing: missing ? nanCode : null } : null;
}

// codes[0..n) as bytes (whole 32-bit words).
export function packBits(codes, n, { bits, base, missing }) {
  const words = new Uint32Array(Math.ceil((n * bits) / 32));
  const ones = bits === 32 ? 0xffffffff : 2 ** bits - 1;
  for (let i = 0, pos = 0; i < n; i++, pos += bits) {
    const c = codes[i];
    const v = missing !== null && c === missing ? ones : c - base;
    const w = pos >>> 5, sh = pos & 31;
    words[w] |= v << sh;
    if (sh + bits > 32) words[w + 1] |= v >>> (32 - sh);
  }
  return new Uint8Array(words.buffer);
}

// The same in reverse, in plain JS (engines without compiled kernels).
export function unpackBits(bytes, n, { bits, base, missing }, out) {
  const words = new Uint32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength >>> 2);
  const ones = bits === 32 ? 0xffffffff : 2 ** bits - 1;
  for (let i = 0, pos = 0; i < n; i++, pos += bits) {
    const w = pos >>> 5, sh = pos & 31;
    let v = words[w] >>> sh;
    if (sh + bits > 32) v |= words[w + 1] << (32 - sh);
    v = bits === 32 ? v >>> 0 : (v & ones) >>> 0;
    out[i] = missing !== null && v === ones ? missing : v + base;
  }
}

// unpack(start, end, unit, src, dst, bits, base, hasMissing, missingCode):
// dst[i] = the code of row i, for i in [start, end), as `width`-byte integers.
// Each row: a 64-bit load at its first byte, shifted and masked (bits <= 32).
export function unpackModule(width, shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, I32, I32, I32], []);
  const [start, stop, , src, dst, bits, base, hasMissing, missingCode] = [0, 1, 2, 3, 4, 5, 6, 7, 8];
  const i = f.local(I32), pos = f.local(I64), mask = f.local(I64), v = f.local(I64);
  f.emit(i64one, get(bits), I64_EXTEND_U, I64_SHL, i64one, I64_SUB, set(mask));
  f.emit(get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(get(i), I64_EXTEND_U, get(bits), I64_EXTEND_U, I64_MUL, set(pos));
  // v = (load64(src + pos / 8) >> (pos % 8)) & mask
  f.emit(get(src), get(pos), [0x42, 3], I64_SHR_U, i32wrapI64, i32add, I64_LOAD);
  f.emit(get(pos), [0x42, 7], I64_AND, I64_SHR_U, get(mask), I64_AND, set(v));
  // address of dst[i]
  f.emit(get(i), i32c(SHIFT[width]), [0x74], get(dst), i32add);
  // missing ? missingCode : v + base
  f.emit(get(missingCode));
  f.emit(get(v), i32wrapI64, get(base), i32add);
  f.emit(get(v), get(mask), I64_EQ, get(hasMissing), [0x71], SELECT);
  f.emit(STORE[width]);
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}
