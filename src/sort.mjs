// Sorting helpers for tables (see Table.sort / Table.top).
//
// Sort keys are given as text: a column name or any expression, sorted from
// small to large; a leading "-" reverses it ('-price', '-(price * qty)').
// A category column sorts by its labels (A to Z, or Z to A with "-").
// Several keys: ['region', '-revenue']. Ties keep table order (stable).
// Missing values (NaN) always come last.

const IDENT = /^\s*(-?)\s*([A-Za-z_][A-Za-z0-9_]*)\s*$/;

// [{ text: expression to compute | null, column: category column | null, desc }]
export function parseKeys(by, cols) {
  const list = Array.isArray(by) ? by : [by];
  if (!list.length) throw new Error('vectro: sort needs at least one key, e.g. "-price"');
  return list.map((k) => {
    if (typeof k !== 'string' || !k.trim()) throw new Error(`vectro: a sort key must be a column or expression, got ${JSON.stringify(k)}`);
    const m = IDENT.exec(k);
    if (m && cols.get(m[2])?.dict) return { column: m[2], desc: m[1] === '-', text: null };
    if (m && cols.get(m[2])?.strings) throw new Error(`vectro: can't sort by free-text column "${m[2]}" (load it as a category)`);
    return { column: null, desc: false, text: k };
  });
}

// Ranks for a category column's codes: code -> position of its label A..Z.
// Missing text ('') ranks NaN, so it comes last either way, as a missing number does.
export function labelRanks(dict, desc) {
  const order = dict.map((_, i) => i).sort((a, b) => byCodePoint(dict[a], dict[b]));
  const rank = new Float64Array(dict.length);
  order.forEach((code, r) => { rank[code] = dict[code] === '' || dict[code] == null ? NaN : desc ? -r : r; });
  return rank;
}

// Text in code point order, as DuckDB sorts it (JavaScript's < compares UTF-16 units, which
// puts 'ﬁ' and full-width letters after emoji): a surrogate unit is part of a code point
// above every unit that isn't one, so it sorts after them.
function byCodePoint(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.charCodeAt(i), y = b.charCodeAt(i);
    if (x === y) continue;
    const sx = x >= 0xd800 && x <= 0xdfff, sy = y >= 0xd800 && y <= 0xdfff;
    return sx !== sy ? (sx ? 1 : -1) : x < y ? -1 : 1;
  }
  return a.length - b.length;
}

// Stable sort of row indices 0..n-1 by several Float64Array keys (first key
// most important): LSD radix sort, last key first. Each key becomes two
// uint32 words that sort as unsigned numbers; the words travel with the
// indices (so every pass reads in order instead of gathering at random), in
// 11-bit digits (2,048 buckets fit in cache). All digit counts are taken in
// one pass per key, and digits that are the same in every row are skipped,
// so small integer keys take one or two passes instead of six. Keys that are
// whole numbers within 2^53 of their minimum (codes, IDs, counts) sort as
// that offset, so only the digits that vary cost a pass.
const DIGITS = [[0, 0], [0, 11], [0, 22], [1, 0], [1, 11], [1, 22]]; // [word (0 = low), shift]
export function sortIndex(keys, n) {
  let idx = new Uint32Array(n);
  for (let i = 0; i < n; i++) idx[i] = i;
  if (n <= 4096) return smallSort(keys, idx); // a comparison sort is quicker than bucket passes
  let lo = new Uint32Array(n), hi = new Uint32Array(n);
  let idx2 = new Uint32Array(n), lo2 = new Uint32Array(n), hi2 = new Uint32Array(n);
  const hist = new Uint32Array(6 * 2048);
  for (let k = keys.length - 1; k >= 0; k--) {
    // float64 -> two uint32 halves that sort as unsigned numbers, in the current order
    const f = keys[k];
    let min = Infinity, max = -Infinity, whole = true;
    for (let i = 0; i < n; i++) { const v = f[i]; if (v < min) min = v; if (v > max) max = v; if (v !== Math.floor(v)) { whole = false; break; } }
    whole = whole && max - min < 2 ** 53; // (NaN and infinities fail `whole`)
    const bits = new Uint32Array(f.buffer, f.byteOffset, n * 2);
    hist.fill(0);
    for (let i = 0; i < n; i++) {
      const r = idx[i];
      let h, l;
      if (whole) { const o = f[r] - min; h = Math.floor(o / 4294967296); l = o - h * 4294967296; }
      else {
        h = bits[2 * r + 1]; l = bits[2 * r];
        if (f[r] !== f[r]) { h = 0xffffffff; l = 0xffffffff; } // NaN: after everything
        else if (f[r] === 0) { h = 0x80000000; l = 0; }          // -0 as 0: equal keys, the next key decides
        else if (h & 0x80000000) { h = ~h >>> 0; l = ~l >>> 0; } // negative: flip all
        else h = (h | 0x80000000) >>> 0;                         // positive: flip the sign
      }
      hi[i] = h; lo[i] = l;
      hist[l & 2047]++; hist[2048 + ((l >>> 11) & 2047)]++; hist[4096 + (l >>> 22)]++;
      hist[6144 + (h & 2047)]++; hist[8192 + ((h >>> 11) & 2047)]++; hist[10240 + (h >>> 22)]++;
    }
    for (let d = 0; d < 6; d++) {
      const [word, shift] = DIGITS[d], base = d * 2048;
      const src = word ? hi : lo;
      if (hist[base + ((src[0] >>> shift) & 2047)] === n) continue; // every row has this digit
      let sum = 0;
      for (let b = 0; b < 2048; b++) { const c = hist[base + b]; hist[base + b] = sum; sum += c; }
      for (let i = 0; i < n; i++) {
        const p = hist[base + ((src[i] >>> shift) & 2047)]++;
        idx2[p] = idx[i]; lo2[p] = lo[i]; hi2[p] = hi[i];
      }
      [idx, idx2] = [idx2, idx]; [lo, lo2] = [lo2, lo]; [hi, hi2] = [hi2, hi];
    }
  }
  return idx;
}

// Stable comparison sort of row indices for small inputs (same order as the
// radix sort: by each key in turn, NaN last, ties in table order).
function smallSort(keys, idx) {
  const order = Array.from(idx);
  order.sort((a, b) => {
    for (const f of keys) {
      const x = f[a], y = f[b];
      if (x === y) continue;
      if (x !== x) { if (y !== y) continue; return 1; }
      if (y !== y) return -1;
      return x < y ? -1 : 1;
    }
    return a - b;
  });
  return Uint32Array.from(order);
}
