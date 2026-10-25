// Parallel stable sort on every thread: LSD radix sort, 11-bit digits, the
// last key first (see sort-kernels.mjs for the kernels).
//
// For each key: one pass turns the key (in the current order) into two
// sortable words per row and counts every digit in each unit. Digits that
// are the same in every row are skipped. For each remaining digit, each unit
// counts its rows per bucket (the first digit reuses the counts above), this
// thread turns the counts into each unit's starting positions (bucket by
// bucket, unit by unit: that keeps the sort stable), and every unit moves its
// rows (index and words) to those positions at once.

import { OP, unitCount } from './kernels.mjs';
import { prepModule, countModule, moveModule, gatherModule, DIGITS, BUCKETS } from './sort-kernels.mjs';

function kernel(engine, name, make) {
  engine._sortKernels ??= new Map();
  let v = engine._sortKernels.get(name);
  if (!v) { v = engine._register(make()); engine._sortKernels.set(name, v); }
  return v;
}

// keys: engine arrays of the table's type ('f64' | 'f32'), first key most
// important; ascending, NaN last, ties in row order. Returns the order as an
// engine-allocated Uint32Array of n row numbers (free it when done).
export function* sortPermG(engine, keys, n, t) {
  const shared = engine.isShared;
  const prep = kernel(engine, `prep|${t}`, () => prepModule(t, shared));
  const count = kernel(engine, 'count', () => countModule(shared));
  const move = kernel(engine, 'move', () => moveModule(shared));
  const units = unitCount(OP.UNITS, engine.T, n, null);
  let idx = engine.alloc(Uint32Array, n), idx2 = engine.alloc(Uint32Array, n);
  let lo = engine.alloc(Uint32Array, n), lo2 = engine.alloc(Uint32Array, n);
  let hi = engine.alloc(Uint32Array, n), hi2 = engine.alloc(Uint32Array, n);
  const hist6 = engine.alloc(Uint32Array, units * 6 * BUCKETS), off = engine.alloc(Uint32Array, units * BUCKETS);
  const p = (a) => engine.live(a).byteOffset;
  try {
    const id = engine.live(idx);
    for (let i = 0; i < n; i++) id[i] = i;
    for (let k = keys.length - 1; k >= 0; k--) {
      // whole numbers within 2^53 of their minimum sort as that offset
      const [mn, mx, frac] = yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k: keys[k] });
      const whole = frac === 0 && mx - mn < 2 ** 53;
      engine.live(hist6).fill(0);
      const ordered = k < keys.length - 1; // later keys: read in the current order
      yield* engine._unitsG(prep, n, [p(keys[k]), p(idx), ordered ? 1 : 0, p(lo), p(hi), whole ? 1 : 0, p(hist6), whole ? mn : 0]);
      // rows per bucket of every digit, over all units
      const h6 = engine.live(hist6);
      const skip = DIGITS.map((_, d) => {
        const tot = new Float64Array(BUCKETS);
        for (let u = 0; u < units; u++) {
          const base = (u * 6 + d) * BUCKETS;
          for (let b = 0; b < BUCKETS; b++) tot[b] += h6[base + b];
        }
        return tot.some((c) => c === n); // one bucket holds every row
      });
      let fresh = true; // hist6 still describes the current order
      for (let d = 0; d < DIGITS.length; d++) {
        if (skip[d]) continue;
        const [word, shift] = DIGITS[d];
        if (fresh) {
          const o = engine.live(off), h = engine.live(hist6);
          for (let u = 0; u < units; u++) o.set(h.subarray((u * 6 + d) * BUCKETS, (u * 6 + d + 1) * BUCKETS), u * BUCKETS);
        } else {
          engine.live(off).fill(0);
          yield* engine._unitsG(count, n, [p(word ? hi : lo), shift, p(off)]);
        }
        fresh = false;
        // each unit's first position per bucket: bucket by bucket, unit by unit
        const o = engine.live(off);
        let at = 0;
        for (let b = 0; b < BUCKETS; b++) {
          for (let u = 0; u < units; u++) { const c = o[u * BUCKETS + b]; o[u * BUCKETS + b] = at; at += c; }
        }
        const carryLo = DIGITS.slice(d + 1).some(([w], j) => w === 0 && !skip[d + 1 + j]) ? 1 : 0;
        yield* engine._unitsG(move, n, [word, shift, p(idx), p(lo), p(hi), p(idx2), p(lo2), p(hi2), p(off), carryLo]);
        [idx, idx2] = [idx2, idx];
        [lo, lo2] = [lo2, lo];
        [hi, hi2] = [hi2, hi];
      }
    }
    const out = idx;
    idx = null;
    return out;
  } finally {
    for (const a of [idx, idx2, lo, lo2, hi, hi2, hist6, off]) if (a) engine.free(a);
  }
}

// dst[i] = src[perm[i]] for i < n, on every thread; src and the result are
// engine arrays of type t.
export function* gatherG(engine, src, perm, n, t) {
  const g = kernel(engine, `gather|${t}`, () => gatherModule(t, engine.isShared));
  const dst = engine.alloc(t === 'f32' ? Float32Array : Float64Array, n);
  yield* engine._unitsG(g, n, [engine.live(src).byteOffset, engine.live(perm).byteOffset, engine.live(dst).byteOffset]);
  return engine.live(dst);
}
