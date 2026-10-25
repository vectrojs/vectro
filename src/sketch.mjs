// Distinct-count sketches (HyperLogLog). A sketch is 4,096 one-byte registers
// (4 KB); adding a value sets one register to the largest "first 1 bit"
// position seen in its hash. Sketches merge by taking each register's max, so
// partial sketches from workers, time slices or other machines combine into
// the sketch of everything they saw. The estimate is within about 1.6% (one
// standard error) of the true number of distinct values.
//
// Values hash by what they are, not how they are stored: numbers by their
// bits (-0 as 0; NaN is skipped), text by its characters, so the same label
// hashes alike in every worker's dictionary.

export const SKETCH_BITS = 12;
export const SKETCH_SIZE = 1 << SKETCH_BITS; // registers (bytes) per sketch

const f64 = new Float64Array(1), u32 = new Uint32Array(f64.buffer);
const fmix = (h) => { // murmur3 finalizer
  h ^= h >>> 16; h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13; h = Math.imul(h, 0xc2b2ae35);
  return (h ^ (h >>> 16)) >>> 0;
};

// 32-bit hash of a number (NaN must be skipped by the caller).
export function hashNumber(v) {
  f64[0] = v === 0 ? 0 : v;
  return fmix(u32[0] ^ fmix(u32[1] ^ 0x9e3779b9));
}

// 32-bit hash of a string.
export function hashText(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 0x01000193);
  return fmix(h ^ 0x5bd1e995);
}

// Add a hash to the sketch at regs[at .. at + SKETCH_SIZE).
export function sketchAdd(regs, at, h) {
  const j = h >>> (32 - SKETCH_BITS);
  const rest = (h << SKETCH_BITS) >>> 0;
  const rank = rest === 0 ? 32 - SKETCH_BITS + 1 : Math.clz32(rest) + 1;
  if (rank > regs[at + j]) regs[at + j] = rank;
}

export function sketchMerge(dst, dAt, src, sAt) {
  for (let j = 0; j < SKETCH_SIZE; j++) if (src[sAt + j] > dst[dAt + j]) dst[dAt + j] = src[sAt + j];
}

const ALPHA = 0.7213 / (1 + 1.079 / SKETCH_SIZE);
export function sketchEstimate(regs, at = 0) {
  const m = SKETCH_SIZE;
  let sum = 0, zeros = 0;
  for (let j = 0; j < m; j++) { const r = regs[at + j]; sum += 2 ** -r; if (r === 0) zeros++; }
  let e = (ALPHA * m * m) / sum;
  if (e <= 2.5 * m && zeros) e = m * Math.log(m / zeros); // few values: linear counting
  else if (e > 2 ** 32 / 30) e = -(2 ** 32) * Math.log(1 - e / 2 ** 32); // 32-bit hash: large-range correction
  return Math.round(e);
}

// A sketch as an object: add values, merge, estimate, ship as bytes.
//   const s = new DistinctSketch(); s.add('EU'); s.add(42); s.estimate();
//   DistinctSketch.fromBytes(bytes).merge(s)
export class DistinctSketch {
  constructor(registers = new Uint8Array(SKETCH_SIZE)) {
    if (registers.length !== SKETCH_SIZE) throw new RangeError(`vectro: a sketch has ${SKETCH_SIZE} registers, got ${registers.length}`);
    this.registers = registers;
  }
  add(value) {
    if (typeof value === 'number') { if (value === value) sketchAdd(this.registers, 0, hashNumber(value)); }
    else if (value != null) sketchAdd(this.registers, 0, hashText(String(value)));
    return this;
  }
  merge(other) { sketchMerge(this.registers, 0, other.registers, 0); return this; }
  estimate() { return sketchEstimate(this.registers, 0); }
  toBytes() { return this.registers.slice(); }
  static fromBytes(bytes) { return new DistinctSketch(Uint8Array.from(bytes)); }
}
