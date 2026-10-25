// Hash join: left.join(right, { on, how }). The right table is the build
// side (put the smaller table there), the left one is probed.
//
//   1. Number the right table's distinct keys in one shared hash table, on
//      every thread (the group-by kernel), and keep the table.
//   2. Look up every left row's key in it, on every thread (no inserts).
//   3. List the right rows of each key side by side (a counting sort), and
//      make the pairs (left row, right row): left rows in order, each with
//      its matches in right-table order.
//   4. Gather every column in pair order, on every thread.
//
// Text keys match by label (the two tables have their own dictionaries).
// A key with a missing part (NaN) never matches, as NULL in SQL. With
// how: 'left', left rows without a match are kept once, with NaN (numbers)
// or '' (text) in the right table's columns.

import { generateHashGroups, generateHashProbe } from './table-kernels.mjs';
import { gatherG } from './parallel-sort.mjs';
import { maxUnits } from './kernels.mjs';

function kernel(engine, name, make) {
  engine._tableKernels ??= new Map();
  let v = engine._tableKernels.get(name);
  if (!v) { v = engine._register(make()); engine._tableKernels.set(name, v); }
  return v;
}

function parseOn(on) {
  if (typeof on === 'string' || Array.isArray(on)) { const k = [on].flat(); return [k, k]; }
  if (on && on.left != null && on.right != null) return [[on.left].flat(), [on.right].flat()];
  throw new Error("vectro: join needs on: 'key', ['key1', 'key2'] or { left: 'id', right: 'customer_id' }");
}

// L, R: tables without filters. makeTable(cols, n) builds the result.
export function* joinTablesG(L, R, { on, how = 'inner', suffix = '_right' } = {}, makeTable) {
  const engine = L.engine;
  if (R.engine !== engine) throw new Error('vectro: join needs two tables of the same engine');
  if (how !== 'inner' && how !== 'left') throw new Error(`vectro: join how must be 'inner' or 'left', got "${how}"`);
  if (L.type !== R.type) throw new Error('vectro: join needs two tables of the same number type (both float64 or both float32)');
  const [lk, rk] = parseOn(on);
  if (!lk.length || lk.length !== rk.length) throw new Error('vectro: join needs as many left keys as right keys');
  const t = L.type === Float32Array ? 'f32' : 'f64';
  const nk = lk.length, nL = L.n, nR = R.n;
  const made = []; // engine arrays to free at the end
  const alloc = (Type, n) => { const a = engine.alloc(Type, n); made.push(a); return a; };
  try {
    // keys as numbers: left as they are; right text keys as left codes
    const leftKeys = [], rightKeys = [];
    for (let k = 0; k < nk; k++) {
      const a = L.cols.get(lk[k]), b = R.cols.get(rk[k]);
      if (!a) throw new Error(`vectro: join: the left table has no column "${lk[k]}"`);
      if (!b) throw new Error(`vectro: join: the right table has no column "${rk[k]}"`);
      if (a.strings || b.strings) throw new Error(`vectro: join: can't join on free-text column "${a.strings ? lk[k] : rk[k]}" (load it as a category)`);
      if (!a.dict !== !b.dict && nL && nR) throw new Error(`vectro: join: "${lk[k]}" and "${rk[k]}" must both be text or both numbers`);
      leftKeys.push(a.data);
      if (!b.dict) { rightKeys.push(b.data); continue; }
      const lut = b.dict.map((label) => a.codes?.get(label) ?? NaN); // a label the left table lacks matches nothing
      const mapped = alloc(L.type, nR);
      const src = engine.live(b.data), dst = engine.live(mapped);
      for (let i = 0; i < nR; i++) dst[i] = lut[src[i]];
      rightKeys.push(mapped);
    }

    // 1-2. group numbers of the right keys; each left row's number or -1
    const rid = alloc(L.type, nR), lid = alloc(Int32Array, nL);
    const K = engine.jit ? yield* hashKeysG(engine, t, rightKeys, leftKeys, rid, lid, alloc) : hashKeysJS(engine, rightKeys, leftKeys, rid, lid);

    // 3. right rows per key, side by side; then the pairs
    const ids = engine.live(rid);
    const start = new Int32Array(K + 1);
    for (let r = 0; r < nR; r++) start[ids[r] + 1]++;
    for (let g = 0; g < K; g++) start[g + 1] += start[g];
    const rows = new Int32Array(nR), fill = start.slice(0, K);
    for (let r = 0; r < nR; r++) rows[fill[ids[r]]++] = r;
    const found = engine.live(lid);
    let total = 0;
    for (let i = 0; i < nL; i++) {
      const g = found[i];
      const m = g >= 0 ? start[g + 1] - start[g] : 0;
      total += m || (how === 'left' ? 1 : 0);
    }
    const li = alloc(Uint32Array, total), ri = alloc(Uint32Array, total);
    {
      const a = engine.live(li), b = engine.live(ri), f = engine.live(lid);
      let o = 0;
      for (let i = 0; i < nL; i++) {
        const g = f[i];
        if (g >= 0) for (let j = start[g]; j < start[g + 1]; j++) { a[o] = i; b[o++] = rows[j]; }
        else if (how === 'left') { a[o] = i; b[o++] = nR; } // row nR: the "no match" row
      }
    }

    // 4. gather; right columns that share the left keys' names are left out
    const gather = function* (src, idx) {
      if (engine.jit) return yield* gatherG(engine, src, idx, total, t);
      const dst = engine.alloc(L.type, total), a = engine.live(src), ix = engine.live(idx);
      for (let i = 0; i < total; i++) dst[i] = a[ix[i]];
      return dst;
    };
    const out = new Map();
    for (const [name, c] of L.cols) out.set(name, { ...c, data: yield* gather(c.data, li), zones: undefined });
    const sameKey = new Set(rk.filter((name, k) => lk[k] === name));
    for (const [name, c] of R.cols) {
      if (sameKey.has(name)) continue;
      let src = c.data, dict = c.dict, codes = c.codes;
      if (how === 'left') { // one more row at the end: the value for "no match"
        src = alloc(R.type, nR + 1);
        const s = engine.live(src);
        s.set(engine.live(c.data).subarray(0, nR));
        if (dict) {
          let e = codes.get('');
          if (e === undefined) { dict = [...dict, '']; codes = new Map(codes).set('', dict.length - 1); e = dict.length - 1; }
          s[nR] = e;
        } else s[nR] = NaN;
      }
      let outName = name;
      while (out.has(outName)) outName += suffix;
      out.set(outName, { data: yield* gather(src, ri), dict, codes, strings: c.strings, date: c.date, zones: undefined });
    }
    for (const c of out.values()) c.data = engine.live(c.data);
    return makeTable(out, total);
  } finally {
    for (const a of made) engine.free(a);
  }
}

// Steps 1 and 2 on every thread: number the right keys in a shared hash
// table, then look up the left keys. Returns K, the group numbers used.
function* hashKeysG(engine, t, rightKeys, leftKeys, rid, lid, alloc) {
  const nk = rightKeys.length, nR = engine.live(rid).length, nL = engine.live(lid).length;
  const room = nR + 64 * maxUnits(Math.max(1, engine.T)); // every group number < room
  if (t === 'f32' && room > 2 ** 24) throw new RangeError('vectro: join on a float32 table is limited to 16 million right-side rows');
  let cap = 1024;
  while (cap < room * 2) cap *= 2;
  const slots = alloc(Int32Array, cap), keyBits = alloc(Float64Array, room * nk), counter = alloc(Int32Array, 4);
  engine.live(slots).fill(-1);
  engine.live(counter)[0] = 0;
  const p = (a) => engine.live(a).byteOffset;
  const build = kernel(engine, `hashgroups|${t}|${nk}`, () => generateHashGroups(t, nk, engine.isShared));
  const { parts } = yield* engine._fusedG(build, nR, [...rightKeys, rid], [p(slots), cap - 1, p(keyBits), room, p(counter)], 1);
  if (parts.some((x) => x !== 0)) throw new Error('vectro: join hash table overflowed'); // can't happen: room > rows
  const probe = kernel(engine, `hashprobe|${t}|${nk}`, () => generateHashProbe(t, nk, engine.isShared));
  yield* engine._fusedG(probe, nL, [...leftKeys, lid], [p(slots), cap - 1, p(keyBits)], 0);
  return engine.live(counter)[0];
}

// The same in plain JavaScript (engines without the WebAssembly compiler).
function hashKeysJS(engine, rightKeys, leftKeys, rid, lid) {
  const keyOf = (cols, i) => {
    let key = '';
    for (const c of cols) { const v = c[i]; if (v !== v) return null; key += (v === 0 ? 0 : v) + ','; }
    return key;
  };
  const R = rightKeys.map((a) => engine.live(a)), L = leftKeys.map((a) => engine.live(a));
  const index = new Map(), r = engine.live(rid), l = engine.live(lid);
  for (let i = 0; i < r.length; i++) {
    const key = keyOf(R, i) ?? `NaN${i}`; // (NaN keys get a number of their own; nothing finds it)
    let g = index.get(key);
    if (g === undefined) { g = index.size; index.set(key, g); }
    r[i] = g;
  }
  for (let i = 0; i < l.length; i++) { const key = keyOf(L, i); l[i] = key === null ? -1 : index.get(key) ?? -1; }
  return index.size;
}
