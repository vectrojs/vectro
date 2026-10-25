// Compressed ("packed") number columns. A column whose values are all
//   base + code * mul      (whole numbers: counts, codes, IDs, ms on whole seconds)
//   code / div             (decimals: prices in cents, rates in basis points)
// with small whole-number codes is stored as 1, 2 or 4 bytes per row instead
// of 8 (or 4). Missing values (NaN) take the largest code of the type. Every
// value is checked to decode back exactly, with the same arithmetic the
// kernels use (float32 tables: float32), so packing never changes an answer.
//
// The fused kernels read packed columns directly (SIMD widen + convert, then
// scale), so scans read 2-8x fewer bytes. Other operations decode the
// columns they need first (see Table).

export const KINDS = {
  u8: { Type: Uint8Array, bytes: 1, nan: 255, max: 254, min: 0 },
  u16: { Type: Uint16Array, bytes: 2, nan: 65535, max: 65534, min: 0 },
  i32: { Type: Int32Array, bytes: 4, nan: -2147483648, max: 2147483647, min: -2147483647 },
};
const SLACK = 16; // bytes after the codes: SIMD loads read a little past the last row

const MULS = [1, 1000, 60_000, 3_600_000, 86_400_000];
const DIVS = [10, 100, 1000, 10_000];

// Candidate encodings for a column with these statistics, smallest first.
// (Decimals: the fewest decimals first; each candidate must still be checked
// value by value.)
export function encodingCandidates({ min, max, whole, nan, inf, negZero }, f32 = false) {
  if (min === Infinity || inf || negZero) return [];
  const fullBytes = f32 ? 4 : 8;
  const kindFor = (lo, hi) => {
    for (const [name, k] of Object.entries(KINDS)) if (lo >= k.min && hi <= k.max && k.bytes < fullBytes && !(f32 && name === 'i32' && (Math.abs(lo) > 2 ** 24 || Math.abs(hi) > 2 ** 24))) return name;
    return null;
  };
  const out = [];
  if (whole) {
    for (const mul of MULS) { const kind = kindFor(0, Math.round((max - min) / mul)); if (kind) out.push({ kind, mul, div: 0, base: min, nan }); }
    return out.sort((a, b) => KINDS[a.kind].bytes - KINDS[b.kind].bytes);
  }
  for (const div of DIVS) { const kind = kindFor(Math.round(min * div), Math.round(max * div)); if (kind) out.push({ kind, mul: 0, div, base: 0, nan }); }
  return out;
}

// The encoding to use for values[0..n), or null if none saves memory.
// enc: { kind, mul, div, base }
export function chooseEncoding(values, n, f32 = false) {
  if (!n) return null;
  const R = f32 ? Math.fround : (x) => x;
  let min = Infinity, max = -Infinity, nan = false, whole = true;
  for (let i = 0; i < n; i++) {
    const v = values[i];
    if (v !== v) { nan = true; continue; }
    if (v === Infinity || v === -Infinity) return null;
    if (v < min) min = v;
    if (v > max) max = v;
    if (whole && v !== Math.floor(v)) whole = false;
  }
  if (min === Infinity) return null; // only missing values: leave it
  const fullBytes = f32 ? 4 : 8;
  const kindFor = (lo, hi) => {
    for (const [name, k] of Object.entries(KINDS)) if (lo >= k.min && hi <= k.max && k.bytes < fullBytes && !(f32 && name === 'i32' && (Math.abs(lo) > 2 ** 24 || Math.abs(hi) > 2 ** 24))) return name;
    return null;
  };
  const decode = (enc, code) => (enc.div ? R(R(code) / enc.div) : R(R(R(code) * enc.mul) + enc.base));
  const encode = (enc, v) => (enc.div ? Math.round(v * enc.div) : Math.round((v - enc.base) / enc.mul));
  const fits = (enc, sample) => { // every value (or a sample first) decodes back exactly
    const step = sample ? Math.max(1, Math.floor(n / 512)) : 1;
    for (let i = 0; i < n; i += step) {
      const v = values[i];
      if (v !== v) continue;
      const code = encode(enc, v);
      if (!(code >= KINDS[enc.kind].min && code <= KINDS[enc.kind].max) || decode(enc, code) !== v || Object.is(v, -0) !== Object.is(decode(enc, code), -0)) return false;
    }
    return true;
  };
  const candidates = [];
  if (whole) {
    for (const mul of MULS) {
      const lo = 0, hi = Math.round((max - min) / mul);
      const kind = kindFor(lo, hi);
      if (kind) candidates.push({ kind, mul, div: 0, base: min });
    }
  } else {
    for (const div of DIVS) { // the fewest decimals that give every value back
      const kind = kindFor(Math.round(min * div), Math.round(max * div));
      const enc = kind && { kind, mul: 0, div, base: 0 };
      if (enc && fits(enc, true) && fits(enc, false)) return { ...enc, nan };
    }
    return null;
  }
  candidates.sort((a, b) => KINDS[a.kind].bytes - KINDS[b.kind].bytes);
  for (const enc of candidates) if (fits(enc, true) && fits(enc, false)) return { ...enc, nan }; // nan: some codes mean missing
  return null;
}

// Codes for values[0..n) in a new engine array (with room after the end).
export function packInto(engine, values, n, enc) {
  const k = KINDS[enc.kind];
  const data = engine.alloc(k.Type, n + SLACK / k.bytes);
  const v = engine.live(values);
  for (let i = 0; i < n; i++) {
    const x = v[i];
    data[i] = x !== x ? k.nan : enc.div ? Math.round(x * enc.div) : Math.round((x - enc.base) / enc.mul);
  }
  return data;
}

// Plain JS decode of one code (kernels do this in SIMD).
export function decodeOne(enc, code, f32 = false) {
  if (code === KINDS[enc.kind].nan) return NaN;
  const R = f32 ? Math.fround : (x) => x;
  return enc.div ? R(R(code) / enc.div) : R(R(R(code) * enc.mul) + enc.base);
}

// Bytes per row stored, and what the column would take unpacked.
export const packedBytes = (enc, n) => KINDS[enc.kind].bytes * n;
