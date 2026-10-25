// Kernels run identically on the main thread and on every worker. Work is
// split into units that threads claim one at a time (see runUnits).

import { csvScan, csvParse } from './csv-kernel.mjs';
import { countPattern } from './bytes.mjs';

export const OP = {
  EXIT: 0, SUM: 1, MIN: 2, MAX: 3, ADD: 4, SCALE: 5, DOT: 6, HEAVY: 7,
  COUNT_GT: 8, SORT_CHUNK: 9, MERGE: 10, COUNT_BYTES: 11, COPY: 12, FUSED: 13, GROUP: 14, COMPACT: 15, CSV_SCAN: 16, CSV_PARSE: 17,
};

// Layout of the shared control block (Int32 slots).
export const C = { SEQ: 0, DONE: 1, OP: 2, N: 3, THREADS: 4, NEXT: 5, WASM: 6, KERNEL: 7, NOUT: 8, NSCAL: 9, ERROR: 10, ARGS: 11 };
export const MAX_ARGS = 256; // each array arg = [bufferId, byteOffset, length, typeCode]
export const WASM_MEMORY = 0; // bufferId of the engine's WebAssembly memory
// SEQ holds this while the main thread rewrites the task (a seqlock): a
// worker that read the task during the rewrite sees SEQ change and skips it.
export const SEQ_WRITING = -1;

export const TYPES = [
  Float64Array, Float32Array, Int32Array, Uint32Array,
  Int16Array, Uint16Array, Int8Array, Uint8Array,
];

export const chunkStart = (i, T, n) => Math.floor((i * n) / T);

// Work is cut into many small blocks that threads claim one at a time
// (dynamic scheduling). Fast cores end up doing more blocks than slow ones,
// and a thread that gets paused by the OS doesn't hold everyone up.
const BLOCK = 16_384;
const blockCount = (n, T) => Math.max(1, Math.min(Math.ceil(n / BLOCK), T * 16));

// Offsets are read as unsigned so arrays can live anywhere in 4GB of memory.
// bufferFor(id, bytesNeeded) returns the buffer an array lives in.
export function decodeArgs(ctrl, bufferFor) {
  const u = new Uint32Array(ctrl.buffer);
  const A = [];
  for (let k = 0; k < MAX_ARGS; k++) {
    const base = C.ARGS + k * 4;
    const id = ctrl[base];
    if (id < 0) { A.push(null); continue; }
    const Type = TYPES[u[base + 3]];
    A.push(new Type(bufferFor(id, u[base + 1] + u[base + 2] * Type.BYTES_PER_ELEMENT), u[base + 1], u[base + 2]));
  }
  return A;
}

// Work is handed out as numbered units (blocks, sort chunks or merge pairs).
// NEXT holds [12-bit task tag | 20-bit next unit], so a thread that wakes up
// late and still holds an old task can never claim a unit of a newer one.
export const TAG_SHIFT = 20;
export const tagOf = (seq) => seq & 0xfff;

export function unitCount(op, T, n, sc) {
  if (op === OP.CSV_SCAN || op === OP.CSV_PARSE) return n; // n = number of chunks
  if (op === OP.SORT_CHUNK) return sc[1];
  if (op === OP.MERGE) return Math.ceil(sc[1] / (2 * sc[0]));
  return blockCount(n, T);
}
export const maxUnits = (T) => T * 16;
export const MAX_OUTPUTS = 8; // reductions per fused kernel

function claim(ctrl, tag, units) {
  for (;;) {
    const v = Atomics.load(ctrl, C.NEXT);
    if (v >>> TAG_SHIFT !== tag) return -1; // a newer task has started
    const u = v & 0xfffff;
    if (u >= units) return -1;              // nothing left
    if (Atomics.compareExchange(ctrl, C.NEXT, v, v + 1) === v) return u;
  }
}

// Claim and run units until none are left. Each unit's result goes in its own
// slot of res, so reductions combine in the same order on every run.
// Callers never wait for threads, only for units: a sleeping thread that wakes
// up after the work is done simply finds nothing to claim.
// F is this thread's instance of a generated kernel (FUSED, GROUP, COMPACT);
// self is this thread's index (0 = calling thread). If a unit throws, the
// error goes to report() and the ERROR flag is set; the unit still counts as
// done, so the caller never waits forever.
export function runUnits(op, T, n, A, sc, ctrl, W, res, tag, F, self = 0, report = null) {
  const units = unitCount(op, T, n, sc);
  for (let u; (u = claim(ctrl, tag, units)) >= 0; ) {
    try {
      runUnit(op, u, units, T, n, A, sc, ctrl, W, res, F, self);
    } catch (err) {
      Atomics.store(ctrl, C.ERROR, 1);
      if (report) report(err); else throw err;
    } finally {
      if (Atomics.add(ctrl, C.DONE, 1) + 1 === units) Atomics.notify(ctrl, C.DONE);
    }
  }
}

function runUnit(op, u, units, T, n, A, sc, ctrl, W, res, F, self) {
    if (op === OP.FUSED) {
      runFused(F, u, units, n, A, sc, ctrl, res);
    } else if (op === OP.GROUP) {
      runGroup(F, u, units, n, A, sc, ctrl, self);
    } else if (op === OP.COMPACT) {
      runCompact(F, u, units, n, A, sc, ctrl);
    } else if (op === OP.CSV_SCAN) {
      res[u] = csvScan(u, A, sc);
    } else if (op === OP.CSV_PARSE) {
      csvParse(u, A, sc, F); // F reports each chunk's text dictionary
    } else if (op === OP.SORT_CHUNK) {
      A[0].subarray(chunkStart(u, units, n), chunkStart(u + 1, units, n)).sort();
    } else if (op === OP.MERGE) {
      mergePair(A[0], A[1], u, sc[1], n, sc[0]);
    } else {
      res[u] = rangeKernel(op, chunkStart(u, units, n), chunkStart(u + 1, units, n), A, sc, W);
    }
}

// A fused kernel takes (ptr per array..., length, scalars...) and returns its
// reductions (none for an elementwise map).
function runFused(F, u, units, n, A, sc, ctrl, res) {
  const start = chunkStart(u, units, n);
  const args = [];
  for (const ta of A) if (ta) args.push(ta.byteOffset + start * ta.BYTES_PER_ELEMENT);
  args.push(chunkStart(u + 1, units, n) - start);
  for (let j = 0, ns = ctrl[C.NSCAL]; j < ns; j++) args.push(sc[j]);
  const r = F(...args);
  const R = ctrl[C.NOUT];
  if (R === 1) res[u] = r;
  else for (let j = 0; j < R; j++) res[u * R + j] = r[j];
}

// Group-by kernel: (column ptrs..., key ptr, length, accBase, K, scalars...).
// Each thread adds into its own accumulators (sc holds base, stride and K
// after the scalars), so no two threads ever write the same slot.
function runGroup(F, u, units, n, A, sc, ctrl, self) {
  const start = chunkStart(u, units, n);
  const ns = ctrl[C.NSCAL];
  const args = [];
  for (const ta of A) if (ta) args.push(ta.byteOffset + start * ta.BYTES_PER_ELEMENT);
  args.push(chunkStart(u + 1, units, n) - start, sc[ns] + self * sc[ns + 1], sc[ns + 2]);
  for (let j = 0; j < ns; j++) args.push(sc[j]);
  F(...args);
}

// Compact kernel: (src ptrs..., length, dst ptrs..., scalars...). The last
// array holds each unit's output offset (from a counting pass), so every unit
// writes straight to its final position.
function runCompact(F, u, units, n, A, sc, ctrl) {
  const arrays = A.filter(Boolean);
  const offsets = arrays[arrays.length - 1];
  const S = ctrl[C.NOUT];
  const start = chunkStart(u, units, n);
  const args = [];
  for (let k = 0; k < S; k++) args.push(arrays[k].byteOffset + start * arrays[k].BYTES_PER_ELEMENT);
  args.push(chunkStart(u + 1, units, n) - start);
  for (let k = S; k < arrays.length - 1; k++) args.push(arrays[k].byteOffset + offsets[u] * arrays[k].BYTES_PER_ELEMENT);
  for (let j = 0, ns = ctrl[C.NSCAL]; j < ns; j++) args.push(sc[j]);
  F(...args);
}

// Which ops have a WASM SIMD kernel, by element type.
const WASM_NAMES = {
  [OP.SUM]: { f64: 'sum_f64' },
  [OP.MIN]: { f64: 'min_f64', f32: 'min_f32' },
  [OP.MAX]: { f64: 'max_f64', f32: 'max_f32' },
  [OP.DOT]: { f64: 'dot_f64' },
  [OP.COUNT_GT]: { f64: 'countgt_f64', f32: 'countgt_f32' },
  [OP.ADD]: { f64: 'add_f64', f32: 'add_f32' },
  [OP.SCALE]: { f64: 'scale_f64', f32: 'scale_f32' },
};
const elemType = (ta) => (ta instanceof Float64Array ? 'f64' : ta instanceof Float32Array ? 'f32' : null);

export function hasWasmKernel(op, A) {
  const t = elemType(A[0]);
  return Boolean(t && WASM_NAMES[op]?.[t] && A.every((x) => !x || elemType(x) === t));
}

// The actual work on [start, end). Also used directly for small inputs.
// With W (WASM exports) it runs the SIMD kernel on raw memory addresses.
export function rangeKernel(op, start, end, A, sc, W) {
  if (W) {
    const f = W[WASM_NAMES[op][elemType(A[0])]];
    const len = end - start;
    const at = (ta) => ta.byteOffset + start * ta.BYTES_PER_ELEMENT;
    switch (op) {
      case OP.SUM: case OP.MIN: case OP.MAX: return f(at(A[0]), len);
      case OP.DOT: return f(at(A[0]), at(A[1]), len);
      case OP.COUNT_GT: return f(at(A[0]), len, sc[0]);
      case OP.ADD: f(at(A[0]), at(A[1]), at(A[2]), len); return 0;
      case OP.SCALE: f(at(A[0]), at(A[1]), len, sc[0], sc[1]); return 0;
    }
  }
  switch (op) {
    case OP.SUM: return sum(A[0], start, end);
    case OP.MIN: return min(A[0], start, end);
    case OP.MAX: return max(A[0], start, end);
    case OP.ADD: return add(A[0], A[1], A[2], start, end);
    case OP.SCALE: return scale(A[0], A[1], sc[0], sc[1], start, end);
    case OP.DOT: return dot(A[0], A[1], start, end);
    case OP.HEAVY: return heavy(A[0], start, end);
    case OP.COUNT_GT: return countGreater(A[0], sc[0], start, end);
    case OP.COUNT_BYTES: return countBytes(A[0], A[1], start, end);
    case OP.COPY: A[1].set(A[0].subarray(start, end), start); return 0;
  }
  throw new Error(`unknown op ${op}`);
}

export function sum(a, s, e) {
  let r = 0;
  for (let i = s; i < e; i++) r += a[i];
  return r;
}

export function min(a, s, e) {
  let r = Infinity;
  for (let i = s; i < e; i++) if (a[i] < r) r = a[i];
  return r;
}

export function max(a, s, e) {
  let r = -Infinity;
  for (let i = s; i < e; i++) if (a[i] > r) r = a[i];
  return r;
}

export function add(a, b, out, s, e) {
  for (let i = s; i < e; i++) out[i] = a[i] + b[i];
  return 0;
}

// out = a * k + c
export function scale(a, out, k, c, s, e) {
  for (let i = s; i < e; i++) out[i] = a[i] * k + c;
  return 0;
}

export function dot(a, b, s, e) {
  let r = 0;
  for (let i = s; i < e; i++) r += a[i] * b[i];
  return r;
}

export function heavy(a, s, e) {
  let r = 0;
  for (let i = s; i < e; i++) {
    const x = a[i];
    r += Math.sqrt(x) * Math.sin(x) + Math.cos(x * 0.5);
  }
  return r;
}

export function countGreater(a, x, s, e) {
  let r = 0;
  for (let i = s; i < e; i++) if (a[i] > x) r++;
  return r;
}

// One merge in a round of merge sort: pair p joins two neighbouring runs,
// each `width` sorted chunks long, from src into dst.
function mergePair(src, dst, p, chunks, n, width) {
  const first = p * 2 * width;
  const lo = chunkStart(first, chunks, n);
  const mid = chunkStart(Math.min(first + width, chunks), chunks, n);
  const hi = chunkStart(Math.min(first + 2 * width, chunks), chunks, n);
  let l = lo, r = mid, o = lo;
  while (l < mid && r < hi) dst[o++] = src[l] <= src[r] ? src[l++] : src[r++];
  while (l < mid) dst[o++] = src[l++];
  while (r < hi) dst[o++] = src[r++];
}

// Count occurrences of `pat` that start inside [s, e) (native indexOf finds
// candidate first bytes, so each thread searches its slice at memchr speed).
export function countBytes(text, pat, s, e) {
  return countPattern(text, pat, s, e);
}
