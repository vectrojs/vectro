// Functions of whole values, text ones above all (SQL's UPPER, SUBSTR, ||,
// CAST(x AS VARCHAR), text CASE...): a JavaScript function of the inputs'
// values, run once per distinct combination of them rather than per row (a
// category column's labels are few), its answers spread back to the rows.
// Text answers become a category column whose labels live in `state`
// ({ dict, codes }), which only grows: chunk after chunk of a dataset, the
// same text keeps the same code.
import { decodeOne } from './pack.mjs';
import { decodeString } from './csv-kernel.mjs';

// Each row's value of a column as a small whole number (its id), and the
// value of each id: text as strings ('' for missing), numbers and dates as
// numbers (NaN for missing).
function idsOf(engine, c, n, f32) {
  const d = engine.live(c.data), ids = new Int32Array(n);
  if (c.dict) {
    const K = c.dict.length;
    for (let i = 0; i < n; i++) { const v = c.enc ? decodeOne(c.enc, d[i], f32) : d[i]; ids[i] = v === v ? v : K; }
    return { ids, size: K + 1, value: (id) => (id < K ? c.dict[id] : '') };
  }
  if (c.strings) {
    const bytes = engine.live(c.strings), seen = new Map(), vals = [];
    for (let i = 0; i < n; i++) {
      const v = d[i], s = v === v ? decodeString(bytes, v) ?? '' : '';
      let id = seen.get(s);
      if (id === undefined) { id = vals.length; vals.push(s); seen.set(s, id); }
      ids[i] = id;
    }
    return { ids, size: Math.max(1, vals.length), value: (id) => vals[id] };
  }
  const x = c.enc ? Float64Array.from({ length: n }, (_, i) => decodeOne(c.enc, d[i], f32)) : d;
  let lo = Infinity, hi = -Infinity, whole = true;
  for (let i = 0; i < n; i++) { const v = x[i]; if (v !== v) continue; if (v < lo) lo = v; if (v > hi) hi = v; if (whole && v !== Math.floor(v)) whole = false; }
  if (lo === Infinity) { ids.fill(0); return { ids, size: 1, value: () => NaN }; }
  if (whole && hi - lo < 2 ** 22) { // small whole numbers: the id is the value (from the least); missing last
    const miss = hi - lo + 1;
    for (let i = 0; i < n; i++) { const v = x[i]; ids[i] = v === v ? v - lo : miss; }
    return { ids, size: miss + 1, value: (id) => (id === miss ? NaN : (id + lo === 0 ? 0 : id + lo)) };
  }
  const seen = new Map(), vals = [];
  for (let i = 0; i < n; i++) {
    const v = x[i], k = v === v ? (v === 0 ? 0 : v) : NaN; // (-0 and 0 are one value; NaN keys a Map fine)
    let id = seen.get(k);
    if (id === undefined) { id = vals.length; vals.push(k); seen.set(k, id); }
    ids[i] = id;
  }
  return { ids, size: Math.max(1, vals.length), value: (id) => vals[id] };
}

// A column read as ids without a pass of its own, when it can be: id = value -
// lo (category codes, or whole numbers in a small range), missing -> miss.
// range: [min, max, values not whole] of a number column (from the engine), or none
function directOf(engine, c, n, f32, range) {
  if (c.strings || c.enc) return null;
  const d = engine.live(c.data).subarray(0, n);
  if (c.dict) { const K = c.dict.length; return { d, lo: 0, size: K + 1, miss: K, value: (id) => (id < K ? c.dict[id] : '') }; }
  let [lo, hi, frac] = range ?? [Infinity, -Infinity, 0];
  if (!range) for (let i = 0; i < n; i++) { const v = d[i]; if (v !== v) continue; if (v < lo) lo = v; if (v > hi) hi = v; if (v !== Math.floor(v)) frac++; }
  if (!(lo <= hi) || frac || hi - lo >= 2 ** 22) return null;
  const miss = hi - lo + 1;
  if (c.numbered) return { d, lo, size: miss + 1, miss, value: (id) => (id === miss ? NaN : c.numbered(id + lo)) }; // (group numbers of the values)
  return { d, lo, size: miss + 1, miss, value: (id) => (id === miss ? NaN : id + lo === 0 ? 0 : id + lo) };
}

// dst[i] = fn(values of row i): codes (text, labels added to state) or numbers.
export function textmapRows(engine, cols, n, f32, fn, text, state, dst, ranges = []) {
  const codeOf = (r) => {
    const s = r === null || r === undefined || (typeof r === 'number' && r !== r) ? '' : String(r);
    let code = state.codes.get(s);
    if (code === undefined) { code = state.dict.length; state.dict.push(s); state.codes.set(s, code); }
    return code;
  };
  const answer = text ? codeOf : (r) => (typeof r === 'number' ? r : r === null || r === undefined || r === '' ? NaN : Number(r));
  if (!cols.length) { dst.fill(answer(fn()), 0, n); return; }
  const direct = cols.map((c, k) => directOf(engine, c, n, f32, ranges[k]));
  if (cols.length === 1) { // one input: each value once, then a lookup per row
    let x = direct[0];
    if (!x) { const r = idsOf(engine, cols[0], n, f32); x = { d: r.ids, lo: 0, size: r.size, miss: -1, value: r.value }; } // (ids: never missing)
    const seen = new Uint8Array(x.size + 1), res = new Float64Array(x.size + 1);
    markIds(x.d, x.lo, x.miss, seen, n);
    for (let id = 0; id < seen.length; id++) if (seen[id]) res[id] = answer(fn(x.value(id)));
    spreadIds(x.d, x.lo, x.miss, res, dst, n);
    return;
  }
  const product = direct.every(Boolean) ? direct.reduce((a, x) => a * x.size, 1) : Infinity;
  if (product <= 2 ** 22) { // every input direct and few combinations: the combination as one number, read as it goes
    const slot = new Int32Array(product).fill(-1), res = [];
    const strides = direct.map((_, k) => direct.slice(k + 1).reduce((a, x) => a * x.size, 1));
    const [a, b] = direct, sa = strides[0];
    if (direct.length === 2) {
      markPairs(a.d, a.lo, a.miss, b.d, b.lo, b.miss, sa, slot, n); // (slot: each pair's first row)
      const byKey = new Float64Array(product), id = (x, v) => (v === v ? v - x.lo : x.miss);
      for (let key = 0; key < product; key++) { const i = slot[key]; if (i >= 0) byKey[key] = answer(fn(a.value(id(a, a.d[i])), b.value(id(b, b.d[i])))); }
      spreadPairs(a.d, a.lo, a.miss, b.d, b.lo, b.miss, sa, byKey, dst, n);
      return;
    }
    for (let i = 0; i < n; i++) {
      let key;
      if (direct.length === 2) { const u = a.d[i], w = b.d[i]; key = ((u === u ? u - a.lo : a.miss) * sa + (w === w ? w - b.lo : b.miss)) | 0; }
      else { key = 0; for (let k = 0; k < direct.length; k++) { const v = direct[k].d[i]; key += (v === v ? v - direct[k].lo : direct[k].miss) * strides[k]; } key |= 0; }
      let j = slot[key];
      if (j < 0) {
        j = slot[key] = res.length;
        res.push(answer(fn(...direct.map((x) => { const v = x.d[i]; return x.value(v === v ? v - x.lo : x.miss); }))));
      }
      dst[i] = res[j];
    }
    return;
  }
  dst.set(rowsByCombination(engine, cols, n, f32, fn, answer));
}

// (hot loops as small functions with no calls inside: a callback there,
// a new one each time, would make V8 throw the compiled loop away)
function markIds(d, lo, miss, seen, n) { for (let i = 0; i < n; i++) { const v = d[i]; seen[v === v ? (v - lo) | 0 : miss] = 1; } }
function spreadIds(d, lo, miss, res, dst, n) { for (let i = 0; i < n; i++) { const v = d[i]; dst[i] = res[v === v ? (v - lo) | 0 : miss]; } }
function markPairs(ad, alo, amiss, bd, blo, bmiss, sa, first, n) {
  for (let i = 0; i < n; i++) {
    const u = ad[i], w = bd[i], x = u === u ? (u - alo) | 0 : amiss, y = w === w ? (w - blo) | 0 : bmiss, key = x * sa + y;
    if (first[key] < 0) first[key] = i;
  }
}
function spreadPairs(ad, alo, amiss, bd, blo, bmiss, sa, byKey, dst, n) {
  for (let i = 0; i < n; i++) { const u = ad[i], w = bd[i], x = u === u ? (u - alo) | 0 : amiss, y = w === w ? (w - blo) | 0 : bmiss; dst[i] = byKey[x * sa + y]; }
}

// several inputs, any kind: one number per row for its combination (mixed
// radix, renumbered when it would grow too big), each distinct one once
function rowsByCombination(engine, cols, n, f32, fn, answer) {
  const out = new Float64Array(n);
  const ins = cols.map((c) => idsOf(engine, c, n, f32));
  // several: one number per row for its combination (mixed radix, renumbered
  // when it would grow too big), then each distinct combination once
  let comb = Float64Array.from(ins[0].ids), size = ins[0].size;
  const renumber = () => {
    const m = new Map();
    for (let i = 0; i < n; i++) { let id = m.get(comb[i]); if (id === undefined) { id = m.size; m.set(comb[i], id); } comb[i] = id; }
    size = Math.max(1, m.size);
  };
  for (let k = 1; k < ins.length; k++) {
    if (size * ins[k].size > 2 ** 52) renumber();
    const { ids, size: s } = ins[k];
    for (let i = 0; i < n; i++) comb[i] = comb[i] * s + ids[i];
    size *= s;
  }
  const dense = size <= 2 ** 24 ? new Int32Array(size).fill(-1) : null, map = dense ? null : new Map();
  const res = [];
  for (let i = 0; i < n; i++) {
    const key = comb[i];
    let j = dense ? dense[key] : map.get(key);
    if (j === undefined || j < 0) {
      j = res.length;
      res.push(answer(fn(...ins.map((x) => x.value(x.ids[i])))));
      if (dense) dense[key] = j; else map.set(key, j);
    }
    out[i] = res[j];
  }
  return out;
}
