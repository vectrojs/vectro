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
export function labelRanks(dict, desc) {
  const order = dict.map((_, i) => i).sort((a, b) => (dict[a] < dict[b] ? -1 : dict[a] > dict[b] ? 1 : 0));
  const rank = new Float64Array(dict.length);
  order.forEach((code, r) => { rank[code] = desc ? -r : r; });
  return rank;
}

// Stable sort of row indices 0..n-1 by several Float64Array keys (first key
// most important): LSD radix sort, 16 bits per pass, last key first. Passes
// where every row has the same 16 bits are skipped, so small integer keys
// take one or two passes instead of four.
export function sortIndex(keys, n) {
  let idx = new Uint32Array(n), tmp = new Uint32Array(n);
  for (let i = 0; i < n; i++) idx[i] = i;
  if (n <= 4096) return smallSort(keys, idx); // a comparison sort is quicker than 65,536-bucket passes
  const count = new Uint32Array(65537);
  for (let k = keys.length - 1; k >= 0; k--) {
    // float64 -> two uint32 halves that sort as unsigned numbers
    const f = keys[k];
    const hi = new Uint32Array(n), lo = new Uint32Array(n);
    const bits = new Uint32Array(f.buffer, f.byteOffset, n * 2);
    for (let i = 0; i < n; i++) {
      let h = bits[2 * i + 1], l = bits[2 * i];
      if (f[i] !== f[i]) { h = 0xffffffff; l = 0xffffffff; } // NaN: after everything
      else if (h & 0x80000000) { h = ~h >>> 0; l = ~l >>> 0; } // negative: flip all
      else h = (h | 0x80000000) >>> 0;                         // positive: flip the sign
      hi[i] = h; lo[i] = l;
    }
    for (const [half, shift] of [[lo, 0], [lo, 16], [hi, 0], [hi, 16]]) {
      count.fill(0);
      for (let i = 0; i < n; i++) count[((half[idx[i]] >>> shift) & 0xffff) + 1]++;
      if (count[((half[idx[0]] >>> shift) & 0xffff) + 1] === n) continue; // every row has these bits
      for (let d = 0; d < 65536; d++) count[d + 1] += count[d];
      for (let i = 0; i < n; i++) { const r = idx[i]; tmp[count[(half[r] >>> shift) & 0xffff]++] = r; }
      [idx, tmp] = [tmp, idx];
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
