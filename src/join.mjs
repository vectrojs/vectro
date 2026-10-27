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
import { I32, F64, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32geu } from './wasm.mjs';

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
  // While a dataset runs the same join chunk after chunk, the right table's
  // side (hash table, rows per key) is built once and kept in
  // engine._joinBuilds (freed when the run ends).
  const cache = engine._joinBuilds ?? null;
  const cacheKey = `${how}|${rk.join(',')}|${lk.map((n) => dictId(L.cols.get(n)?.dict)).join(',')}`;
  const target = R.cols.get(rk[0])?.data ?? R; // (views of one table share its arrays: one build for all of them)
  let build = cache?.get(target)?.get(`${cacheKey}|${rk.map((n) => arrayId(R.cols.get(n)?.data)).join(',')}`) ?? null, ownBuild = false;
  try {
    const leftKeys = [];
    for (let k = 0; k < nk; k++) {
      const a = L.cols.get(lk[k]), b = R.cols.get(rk[k]);
      if (!a) throw new Error(`vectro: join: the left table has no column "${lk[k]}"`);
      if (!b) throw new Error(`vectro: join: the right table has no column "${rk[k]}"`);
      if (a.strings || b.strings) throw new Error(`vectro: join: can't join on free-text column "${a.strings ? lk[k] : rk[k]}" (load it as a category)`);
      if (!a.dict !== !b.dict && nL && nR) throw new Error(`vectro: join: "${lk[k]}" and "${rk[k]}" must both be text or both numbers`);
      leftKeys.push(a.data);
    }
    if (!build) {
      build = yield* buildRight(engine, t, L, R, lk, rk, how);
      if (cache) { if (!cache.has(target)) cache.set(target, new Map()); cache.get(target).set(`${cacheKey}|${rk.map((n) => arrayId(R.cols.get(n)?.data)).join(',')}`, build); } else ownBuild = true;
    }
    const { K, start, rows } = build;

    // 2. each left row's group number, or -1
    const lid = alloc(Int32Array, nL);
    if (engine.jit) yield* probeG(engine, t, build, leftKeys, lid);
    else probeJS(engine, build, leftKeys, lid);

    // 3. the pairs (left rows in order, each with its matches in right-table order)
    const left = how === 'left';
    const total = countPairs(engine.live(lid), nL, start, build.rowOf, left);
    const li = alloc(Uint32Array, total), ri = alloc(Uint32Array, total);
    makePairs(engine.live(lid), nL, nR, start, rows, build.rowOf, left, engine.live(li), engine.live(ri));

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
        let x = build.extended.get(`${name}|${arrayId(c.data)}`);
        if (!x) {
          const ext = engine.alloc(R.type, nR + 1);
          build.arrays.push(ext);
          const s = engine.live(ext);
          s.set(engine.live(c.data).subarray(0, nR));
          if (dict) {
            let e = codes.get('');
            if (e === undefined) { dict = [...dict, '']; codes = new Map(codes).set('', dict.length - 1); e = dict.length - 1; }
            s[nR] = e;
          } else s[nR] = NaN;
          build.extended.set(`${name}|${arrayId(c.data)}`, (x = { data: ext, dict, codes }));
        }
        ({ data: src, dict, codes } = x);
      }
      let outName = name;
      while (out.has(outName)) outName += suffix;
      out.set(outName, { data: yield* gather(src, ri), dict, codes, strings: c.strings, date: c.date, zones: undefined });
    }
    for (const c of out.values()) c.data = engine.live(c.data);
    return makeTable(out, total);
  } finally {
    for (const a of made) engine.free(a);
    if (ownBuild) freeBuild(engine, build);
  }
}

// Unique right keys (each key one right row, e.g. an ID column), as a view:
// an inner join keeps the left rows that have a match and a left join keeps
// them all, so instead of pairing rows, the right columns are looked up for
// every left row (in group-number order, "no match" last), and the left
// columns stay as they are (packed ones too). An inner join's "has a match"
// becomes the result's filter (column __vectro_found). Used chunk after
// chunk by datasets (opts.view). Returns null when the right keys repeat.
//   collectLeft(): the left table's matching rows (a generator)
//   makeView(cols, n, pred, left): the result, which takes over `left`'s columns
export const FOUND = '__vectro_found';
export function* joinViewG(L, R, { on, how = 'inner', suffix = '_right' } = {}, collectLeft, makeView) {
  const engine = L.engine;
  if (R.engine !== engine || L.type !== R.type || (how !== 'inner' && how !== 'left')) return null;
  const [lk, rk] = parseOn(on);
  if (!lk.length || lk.length !== rk.length) return null;
  for (let k = 0; k < lk.length; k++) {
    const a = L.cols.get(lk[k]), b = R.cols.get(rk[k]);
    if (!a || !b || a.strings || b.strings || !a.dict !== !b.dict) return null; // (the normal path reports it)
  }
  const t = L.type === Float32Array ? 'f32' : 'f64';
  const cache = engine._joinBuilds ?? null;
  const cacheKey = `${how}|${rk.join(',')}|${lk.map((n) => dictId(L.cols.get(n)?.dict)).join(',')}`;
  const target = R.cols.get(rk[0])?.data ?? R; // (views of one table share its arrays: one build for all of them)
  let build = cache?.get(target)?.get(`${cacheKey}|${rk.map((n) => arrayId(R.cols.get(n)?.data)).join(',')}`) ?? null, ownBuild = false;
  const made = [];
  const alloc = (Type, n) => { const a = engine.alloc(Type, n); made.push(a); return a; };
  try {
    if (!build) {
      build = yield* buildRight(engine, t, L, R, lk, rk, how);
      if (cache) { if (!cache.has(target)) cache.set(target, new Map()); cache.get(target).set(`${cacheKey}|${rk.map((n) => arrayId(R.cols.get(n)?.data)).join(',')}`, build); } else ownBuild = true;
    }
    if (!build.rowOf || !engine.jit) return null;
    const left = yield* collectLeft();
    const n = left.n, K = build.K;
    // left keys as plain numbers for the probe (packed keys decoded)
    const keys = [];
    for (const name of lk) {
      const c = left.cols.get(name);
      if (!c.enc) { keys.push(c.data); continue; }
      const plain = alloc(L.type, n);
      if (n) yield* engine.evalG('x', { x: { data: c.data, enc: c.enc, n, type: t } }, plain);
      keys.push(plain);
    }
    const idx = alloc(Uint32Array, n);
    if (build.lut) { if (n) yield* probeG(engine, t, build, keys, idx, K); } // (misses go straight to slot K)
    else {
      const lid = alloc(Int32Array, n);
      if (n) yield* probeG(engine, t, build, keys, lid);
      noMatchAt(engine.live(lid), engine.live(idx), n, K);
    }
    // right columns in group-number order (made once per build), then looked up
    const byGroup = (name, c) => {
      let x = build.byGroup?.get(name);
      if (x) return x;
      build.byGroup ??= new Map();
      const data = engine.alloc(R.type, K + 1);
      build.arrays.push(data);
      let dict = c?.dict ?? null, codes = c?.codes ?? null, missing = NaN;
      if (dict && how === 'left') { // "no match" reads as ''
        missing = codes.get('');
        if (missing === undefined) { dict = [...dict, '']; codes = new Map(codes).set('', dict.length - 1); missing = dict.length - 1; }
      }
      const d = engine.live(data), src = c ? engine.live(c.data) : null, rowOf = build.rowOf;
      for (let g = 0; g < K; g++) { const r = rowOf[g]; d[g] = r < 0 ? missing : src ? src[r] : 1; }
      d[K] = missing;
      build.byGroup.set(name, (x = { data, dict, codes, date: c?.date }));
      return x;
    };
    const cols = new Map(left.cols);
    const sameKey = new Set(rk.filter((name, k) => lk[k] === name));
    const looked = [];
    for (const [name, c] of R.cols) {
      if (sameKey.has(name)) continue;
      let outName = name;
      while (cols.has(outName)) outName += suffix;
      const x = byGroup(`c:${name}|${arrayId(c.data)}`, c);
      cols.set(outName, { data: null, dict: x.dict, codes: x.codes, date: x.date });
      looked.push([outName, x]);
    }
    if (how === 'inner') looked.push([FOUND, byGroup('found', null)]), cols.set(FOUND, { data: null, dict: null, codes: null });
    for (const [outName, x] of looked) cols.get(outName).data = yield* gatherG(engine, x.data, idx, n, t);
    for (const c of cols.values()) c.data = engine.live(c.data);
    return makeView(cols, n, how === 'inner' ? `${FOUND} == 1` : null, left, looked.map(([nm]) => nm));
  } finally {
    for (const a of made) engine.free(a);
    if (ownBuild) freeBuild(engine, build);
  }
}

function noMatchAt(lid, idx, n, K) { for (let i = 0; i < n; i++) { const g = lid[i]; idx[i] = g < 0 ? K : g; } }

// (plain functions: hot loops run faster outside generators)
function countPairs(found, nL, start, rowOf, left) {
  let total = 0;
  if (rowOf) { // unique right keys (an ID column): one right row per key
    if (left) return nL;
    for (let i = 0; i < nL; i++) if (found[i] >= 0) total++;
    return total;
  }
  for (let i = 0; i < nL; i++) {
    const g = found[i];
    const m = g >= 0 ? start[g + 1] - start[g] : 0;
    total += m || (left ? 1 : 0);
  }
  return total;
}

function makePairs(f, nL, nR, start, rows, rowOf, left, a, b) {
  let o = 0;
  if (rowOf) {
    for (let i = 0; i < nL; i++) {
      const g = f[i];
      if (g >= 0) { a[o] = i; b[o++] = rowOf[g]; } else if (left) { a[o] = i; b[o++] = nR; }
    }
    return;
  }
  for (let i = 0; i < nL; i++) {
    const g = f[i];
    if (g >= 0) for (let j = start[g]; j < start[g + 1]; j++) { a[o] = i; b[o++] = rows[j]; }
    else if (left) { a[o] = i; b[o++] = nR; } // row nR: the "no match" row
  }
}

const ids = new WeakMap();
let nextId = 1;
const idOf = (x) => { if (!x) return 0; let id = ids.get(x); if (!id) ids.set(x, (id = nextId++)); return id; };
const dictId = idOf, arrayId = idOf;

export function freeBuild(engine, build) { for (const a of build?.arrays ?? []) engine.free(a); }

// Step 1: the right table's side. Keys as numbers (text keys as the left
// table's codes), numbered in one hash table; then the right rows of each
// group number side by side: rows[start[g] .. start[g + 1]).
function* buildRight(engine, t, L, R, lk, rk, how) {
  const nk = rk.length, nR = R.n;
  const arrays = [];
  const alloc = (Type, n) => { const a = engine.alloc(Type, n); arrays.push(a); return a; };
  const build = { arrays, extended: new Map(), K: 0, start: null, rows: null, rightKeys: [] };
  engine._joinBuildCount = (engine._joinBuildCount ?? 0) + 1; // (tests: a dataset hashes the right side once)
  try {
    for (let k = 0; k < nk; k++) {
      const a = L.cols.get(lk[k]), b = R.cols.get(rk[k]);
      if (!b.dict) { build.rightKeys.push(b.data); continue; }
      const lut = b.dict.map((label) => a.codes?.get(label) ?? NaN); // a label the left table lacks matches nothing
      const mapped = alloc(L.type, nR);
      const src = engine.live(b.data), dst = engine.live(mapped);
      for (let i = 0; i < nR; i++) dst[i] = lut[src[i]];
      build.rightKeys.push(mapped);
    }
    const rid = alloc(L.type, nR);
    if (engine.jit) {
      const room = nR + 64 * maxUnits(Math.max(1, engine.T)); // every group number < room
      if (t === 'f32' && room > 2 ** 24) throw new RangeError('vectro: join on a float32 table is limited to 16 million right-side rows');
      let cap = 1024;
      while (cap < room * 2) cap *= 2;
      const slots = alloc(Int32Array, cap), keyBits = alloc(Float64Array, room * nk), counter = alloc(Int32Array, 4);
      engine.live(slots).fill(-1);
      engine.live(counter)[0] = 0;
      const p = (a) => engine.live(a).byteOffset;
      const k = kernel(engine, `hashgroups|${t}|${nk}`, () => generateHashGroups(t, nk, engine.isShared));
      const { parts } = yield* engine._fusedG(k, nR, [...build.rightKeys, rid], [p(slots), cap - 1, p(keyBits), room, p(counter)], 1);
      if (parts.some((x) => x !== 0)) throw new Error('vectro: join hash table overflowed'); // can't happen: room > rows
      Object.assign(build, { slots, keyBits, cap, K: engine.live(counter)[0] });
    } else {
      const R = build.rightKeys.map((a) => engine.live(a));
      const index = new Map(), r = engine.live(rid);
      for (let i = 0; i < r.length; i++) {
        const key = keyOf(R, i) ?? `NaN${i}`; // (NaN keys get a number of their own; nothing finds it)
        let g = index.get(key);
        if (g === undefined) { g = index.size; index.set(key, g); }
        r[i] = g;
      }
      Object.assign(build, { index, K: index.size });
    }
    const ids = engine.live(rid), K = build.K;
    if (nk === 1 && engine.jit) { // whole-number keys in a small range: a direct lookup array too
      const keys = engine.live(build.rightKeys[0]);
      let lo = Infinity, hi = -Infinity, whole = true;
      for (let r = 0; r < nR && whole; r++) { const v = keys[r]; if (v !== v) continue; if (v !== Math.floor(v)) whole = false; if (v < lo) lo = v; if (v > hi) hi = v; }
      const size = hi - lo + 1;
      if (whole && lo !== Infinity && size <= Math.max(4 * nR, 1 << 16) && size <= 2 ** 26) {
        const lut = alloc(Int32Array, size);
        const L = engine.live(lut), k2 = engine.live(build.rightKeys[0]), g2 = engine.live(rid);
        L.fill(-1);
        for (let r = 0; r < nR; r++) { const v = k2[r]; if (v === v) L[v - lo] = g2[r]; }
        Object.assign(build, { lut, lutMin: lo, lutSize: size });
      }
    }
    const start = new Int32Array(K + 1);
    for (let r = 0; r < nR; r++) start[ids[r] + 1]++;
    for (let g = 0; g < K; g++) start[g + 1] += start[g];
    const rows = new Int32Array(nR), fill = start.slice(0, K);
    for (let r = 0; r < nR; r++) rows[fill[ids[r]]++] = r;
    // unique right keys (an ID column): each group number's one row, if any
    // (the parallel build reserves numbers in batches, so some go unused)
    let rowOf = new Int32Array(K);
    for (let g = 0; g < K; g++) {
      const m = start[g + 1] - start[g];
      if (m > 1) { rowOf = null; break; }
      rowOf[g] = m ? rows[start[g]] : -1;
    }
    Object.assign(build, { start, rows, rowOf });
    return build;
  } catch (err) {
    freeBuild(engine, build);
    throw err;
  }
}

// Step 2 on every thread: look up the left keys (no inserts).
function* probeG(engine, t, build, leftKeys, lid, miss = -1) {
  const p = (a) => engine.live(a).byteOffset;
  if (build.lut) { // whole-number keys in a small range: a direct lookup (lut[key - min])
    const k = kernel(engine, `denseprobe|${t}`, () => denseProbeModule(t, engine.isShared));
    yield* engine._unitsG(k, engine.live(lid).length, [p(leftKeys[0]), p(build.lut), p(lid), build.lutMin, build.lutSize, miss]);
    return true;
  }
  const probe = kernel(engine, `hashprobe|${t}|${leftKeys.length}`, () => generateHashProbe(t, leftKeys.length, engine.isShared));
  yield* engine._fusedG(probe, engine.live(lid).length, [...leftKeys, lid], [p(build.slots), build.cap - 1, p(build.keyBits)], 0);
  return false;
}

// probe(start, end, unit, keys, lut, out, min, size, miss): out[i] = lut[key - min]
// for a whole-number key in [min, min + size) that the right table has
// (lut holds -1 for keys it lacks), else miss.
function denseProbeModule(t, shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, F64, I32, I32], []);
  const [start, stop, , keys, lut, out, min, size, miss] = [0, 1, 2, 3, 4, 5, 6, 7, 8];
  const i = f.local(I32), v = f.local(F64), g = f.local(I32), ok = f.local(I32), x = f.local(I32);
  const bytes = t === 'f32' ? 4 : 8;
  f.emit(get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(get(i), i32c(Math.log2(bytes)), [0x74], get(keys), i32add, t === 'f32' ? [0x2a, 2, 0, 0xbb] : [0x2b, 3, 0], get(min), [0xa1], set(v)); // v = key - min
  f.emit(get(v), [0xfc, 0x02], set(g)); // g = trunc_sat(v)
  f.emit(get(g), get(size), [0x49], get(g), [0xb7], get(v), [0x61], [0x71], set(ok)); // ok = g < size (unsigned) && f64(g) == v
  f.emit(get(g), i32c(0), get(ok), [0x1b], i32c(2), [0x74], get(lut), i32add, [0x28, 2, 0], set(x)); // x = lut[ok ? g : 0]
  f.emit(get(i), i32c(2), [0x74], get(out), i32add); // address of out[i]
  f.emit(get(x), get(miss), get(ok), get(x), i32c(0), [0x4e], [0x71], [0x1b]); // ok && x >= 0 ? x : miss
  f.emit([0x36, 2, 0]);
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}

// The same in plain JavaScript (engines without the WebAssembly compiler).
const keyOf = (cols, i) => {
  let key = '';
  for (const c of cols) { const v = c[i]; if (v !== v) return null; key += (v === 0 ? 0 : v) + ','; }
  return key;
};
function probeJS(engine, build, leftKeys, lid) {
  const L = leftKeys.map((a) => engine.live(a)), l = engine.live(lid);
  for (let i = 0; i < l.length; i++) { const key = keyOf(L, i); l[i] = key === null ? -1 : build.index.get(key) ?? -1; }
}
