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
import { parse } from './fuse.mjs';

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
export function* joinTablesG(L, R, opts = {}, makeTable) {
  const { on, how = 'inner', suffix = '_right' } = opts;
  const engine = L.engine;
  if (R.engine !== engine) throw new Error('vectro: join needs two tables of the same engine');
  if (how !== 'inner' && how !== 'left') throw new Error(`vectro: join how must be 'inner', 'left', 'right' or 'full', got "${how}"`);
  if (L.type !== R.type) throw new Error('vectro: join needs two tables of the same number type (both float64 or both float32)');
  const [lk, rk] = parseOn(on);
  if (!lk.length) throw new Error("vectro: join needs keys in on ('key', ['k1', 'k2'] or { left, right }); for a join without keys pass range or cross");
  if (lk.length !== rk.length) throw new Error(`vectro: join needs as many left keys as right keys (${lk.length} and ${rk.length})`);
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
      if (!a.dict !== !b.dict && nL && nR) throw new Error(`vectro: join: "${opts.labels?.left?.[k] ?? lk[k]}" and "${opts.labels?.right?.[k] ?? rk[k]}" must both be text or both numbers`); // (labels: the names SQL wrote, not its hidden ones)
      leftKeys.push(a.data);
    }
    if (!build) {
      build = yield* buildRight(engine, t, L, R, lk, rk, how);
      if (cache) { if (!cache.has(target)) cache.set(target, new Map()); cache.get(target).set(`${cacheKey}|${rk.map((n) => arrayId(R.cols.get(n)?.data)).join(',')}`, build); } else ownBuild = true;
    }
    if (opts.single && repeatedKey(engine, build)) throw new Error(opts.single); // (a key with more than one right row where one was promised)
    const { K, start, rows } = build;

    // 2. each left row's group number, or -1
    const lid = alloc(Int32Array, nL);
    const hit = opts.unmatched ? newHits(engine, build, alloc) : null;
    let marked = false;
    if (engine.jit) marked = yield* probeG(engine, t, build, leftKeys, lid, -1, hit);
    else probeJS(engine, build, leftKeys, lid);
    if (hit) opts.unmatched.result = yield* unmatchedRightG(engine, R, build, hit, marked ? null : lid, nL, t);

    // 3. the pairs (left rows in order, each with its matches in right-table order)
    const left = how === 'left';
    if (opts.pairsOnly && opts.range) return keyRangePairs(engine, L, R, engine.live(lid), nL, start, rows, build.rowOf, opts.range, opts.range2); // (only the pairs the range keeps: R6-1.9)
    const total = countPairs(engine.live(lid), nL, start, build.rowOf, left);
    if (total > MAX_PAIRS) throw tooMany(total);
    const li = alloc(Uint32Array, total), ri = alloc(Uint32Array, total);
    makePairs(engine.live(lid), nL, nR, start, rows, build.rowOf, left, engine.live(li), engine.live(ri));
    if (opts.pairsOnly) return { li: Uint32Array.from(engine.live(li)), ri: Uint32Array.from(engine.live(ri)), n: total }; // (see anyJoinG)

    // 4. gather; right columns that share the left keys' names are left out
    const gather = function* (src, idx) {
      if (engine.jit) return yield* gatherG(engine, src, idx, total, t);
      const dst = engine.alloc(L.type, total), a = engine.live(src), ix = engine.live(idx);
      for (let i = 0; i < total; i++) dst[i] = a[ix[i]];
      return dst;
    };
    const out = new Map();
    for (const [name, c] of L.cols) if (!c.hidden) out.set(name, { ...c, data: yield* gather(c.data, li), zones: undefined }); // (hidden: a text filter's 0 / 1, not shown)
    const sameKey = new Set(rk.filter((name, k) => lk[k] === name));
    for (const [name, c] of R.cols) {
      if (sameKey.has(name) || c.hidden) continue;
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
export function* joinViewG(L, R, opts = {}, collectLeft, makeView) {
  const { on, how = 'inner', suffix = '_right' } = opts;
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
    if (opts.single && repeatedKey(engine, build)) throw new Error(opts.single);
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
    const hit = opts.unmatched ? newHits(engine, build, alloc) : null;
    let marked = false;
    if (build.lut) { if (n) marked = yield* probeG(engine, t, build, keys, idx, K, hit); } // (misses go straight to slot K)
    else {
      const lid = alloc(Int32Array, n);
      if (n) yield* probeG(engine, t, build, keys, lid);
      noMatchAt(engine.live(lid), engine.live(idx), n, K);
    }
    if (hit) opts.unmatched.result = yield* unmatchedRightG(engine, R, build, hit, marked ? null : idx, n, t);
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
      build.byGroup.set(name, (x = { data, dict, codes, date: c?.date, strings: c?.strings ?? null })); // (free text: its places in the right table's text, as the other join path keeps them)
      return x;
    };
    const cols = new Map(left.cols);
    const sameKey = new Set(rk.filter((name, k) => lk[k] === name));
    const looked = [];
    for (const [name, c] of R.cols) {
      if (sameKey.has(name) || c.hidden) continue;
      let outName = name;
      while (cols.has(outName)) outName += suffix;
      const x = byGroup(`c:${name}|${arrayId(c.data)}`, c);
      cols.set(outName, { data: null, dict: x.dict, codes: x.codes, date: x.date, strings: x.strings });
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

// (RIGHT / FULL JOIN) which right rows some left row matched: hit[g + 1]
// is 1 once a probe found group g (hit[0] and hit[K + 1] take the misses,
// -1 or K). The dense probe marks them as it goes; otherwise one pass over
// the probe's group numbers does.
function newHits(engine, build, alloc) { const h = alloc(Int32Array, build.K + 2); engine.live(h).fill(0); return h; }

// The right rows no left row matched, in right-table order, with all of R's
// columns: { cols, n }. ids: the probe's group numbers (n of them) when
// the probe didn't mark hit itself.
function* unmatchedRightG(engine, R, build, hit, ids, n, t) {
  const nR = R.n;
  let rows = null;
  try {
    if (ids && n) {
      if (engine.jit) {
        const k = kernel(engine, 'markhits', () => markHitsModule(engine.isShared));
        yield* engine._unitsG(k, n, [engine.live(ids).byteOffset, engine.live(hit).byteOffset + 4]);
      } else { const g = engine.live(ids), h = engine.live(hit); for (let i = 0; i < n; i++) h[g[i] + 1] = 1; }
    }
    let h = engine.live(hit), rid = engine.live(build.rid);
    let u = 0;
    for (let r = 0; r < nR; r++) u += 1 - h[rid[r] + 1];
    rows = engine.alloc(Uint32Array, u);
    const rv = engine.live(rows);
    h = engine.live(hit); rid = engine.live(build.rid); // (the allocation may have moved memory)
    if (u) for (let r = 0, o = 0; r < nR; r++) if (!h[rid[r] + 1]) rv[o++] = r;
    const cols = new Map();
    for (const [name, c] of R.cols) {
      if (c.hidden) continue;
      let data;
      if (engine.jit && u) data = yield* gatherG(engine, c.data, rows, u, t);
      else { data = engine.alloc(R.type, u); const a = engine.live(c.data), d = engine.live(data); for (let i = 0; i < u; i++) d[i] = a[rv[i]]; }
      cols.set(name, { data: engine.live(data), dict: c.dict, codes: c.codes, strings: c.strings, date: c.date });
    }
    return { cols, n: u };
  } finally {
    if (rows) engine.free(rows);
  }
}

// mark(start, end, unit, ids, hit): hit[ids[i]] = 1 (int32s; threads
// racing on one slot all store 1)
function markHitsModule(shared) {
  const f = new Fn('k', [I32, I32, I32, I32, I32], []);
  const [start, stop, , ids, hit] = [0, 1, 2, 3, 4];
  const i = f.local(I32), at = f.local(I32);
  f.emit(get(start), set(i));
  f.emit(block, loop, get(i), get(stop), i32geu, brIf(1));
  f.emit(get(i), i32c(2), [0x74], get(ids), i32add, [0x28, 2, 0], i32c(2), [0x74], get(hit), i32add, set(at)); // &hit[ids[i]]
  f.emit(get(at), [0x28, 2, 0], [0x45], [0x04, 0x40], get(at), i32c(1), [0x36, 2, 0], end); // (stored only while still 0: the slot's cache line stays shared)
  f.emit(get(i), i32c(1), i32add, set(i), br(0), end, end);
  return buildModule([f], shared);
}

// GROUP BY right columns over an inner join on unique right keys (see
// Table._lookupGroupAgg): instead of joined rows, one int32 lookup array
// from the left key straight to a group number, so the group kernel adds
// each matching left row into its group as it reads the key. Groups are the
// distinct tuples of the right columns `rightKeys` (numbers ascending, NaN
// last; text in dictionary order), numbered in that order. Made once per
// right table and kept with its hash table while a dataset runs (freed
// with it). Null when the right keys repeat or aren't whole numbers in a
// small range (no direct lookup array): the usual join path then runs.
//   -> { lut, min, size, G, values: [Float64Array(G)] per key, meta: [{ dict, codes, date }] }
export function* lookupGroupsG(L, R, on, rightKeys) {
  const engine = L.engine;
  if (!engine.jit || R.engine !== engine || L.type !== R.type) return null;
  const a = L.cols.get(on.left), b = R.cols.get(on.right);
  if (!a || !b || a.strings || b.strings || a.dict || b.dict) return null; // (number keys; text keys: the usual path)
  const cols = rightKeys.map((n) => R.cols.get(n));
  if (cols.some((c) => !c || c.strings)) return null;
  const cache = engine._joinBuilds ?? null;
  if (!cache) return null; // (one-off joins: the usual path)
  const sig = `lookup|${rightKeys.map((n) => arrayId(R.cols.get(n).data)).join(',')}`;
  let x = cache.get(b.data)?.get(sig);
  if (x !== undefined) return x;
  engine._joinBuildCount = (engine._joinBuildCount ?? 0) + 1; // (tests: once per run, as the hash build)
  x = directLookup(engine, R, b, cols);
  if (!cache.has(b.data)) cache.set(b.data, new Map());
  cache.get(b.data).set(sig, x ?? null); // (null: tried, doesn't apply; freed with the builds)
  return x;
}

// One pass over the right rows: keys must be whole numbers in a small range
// and unique (a repeat: null), group tuples numbered in sorted order.
function directLookup(engine, R, b, cols) {
  const nR = R.n, keys = engine.live(b.data);
  const kr = keyRange(keys, nR);
  if (!kr) return null;
  const [lo, hi] = kr, size = hi - lo + 1;
  if (!(size <= Math.max(4 * nR, 1 << 16) && size <= 2 ** 26)) return null;
  const nk = cols.length, src = cols.map((c) => engine.live(c.data));
  let lut;
  try { lut = engine.alloc(Int32Array, size); } catch (err) { if (err instanceof RangeError) return null; throw err; } // (no room: the usual join path)
  const T = engine.live(lut);
  T.fill(-1);
  let G, values, ok;
  const one = nk === 1 ? wholeRange(src[0], nR) : null;
  if (one && one[1] - one[0] + 1 <= Math.max(nR, 1 << 16)) { // one whole-number column in a small range: its own group number
    G = one[1] - one[0] + 1;
    values = [Float64Array.from({ length: G }, (_, g) => one[0] + g)];
    ok = fillDirect(T, keys, src[0], nR, lo, one[0]);
  } else {
    const index = new Map(), firstRow = [], at = new Int32Array(nR);
    for (let r = 0; r < nR; r++) {
      let id = '';
      for (let k = 0; k < nk; k++) { const v = src[k][r]; id += (v === v ? (v === 0 ? 0 : v) : 'NaN') + ','; } // (-0 as 0, NaN alike)
      let c = index.get(id);
      if (c === undefined) { c = firstRow.length; index.set(id, c); firstRow.push(r); }
      at[r] = c;
    }
    G = firstRow.length;
    const cmp = (x, y) => (x === y || (x !== x && y !== y) ? 0 : x !== x ? 1 : y !== y ? -1 : x < y ? -1 : 1); // NaN last
    const order = Array.from({ length: G }, (_, i) => i).sort((i, j) => { for (let k = 0; k < nk; k++) { const d = cmp(src[k][firstRow[i]], src[k][firstRow[j]]); if (d) return d; } return 0; });
    const rank = new Int32Array(G);
    order.forEach((i, r) => { rank[i] = r; });
    for (let r = 0; r < nR; r++) at[r] = rank[at[r]];
    values = src.map((s) => Float64Array.from({ length: G }, (_, g) => { const v = s[firstRow[order[g]]]; return v === 0 ? 0 : v; }));
    ok = fillDirect(T, keys, at, nR, lo, 0);
  }
  if (!ok) { engine.free(lut); return null; } // the right keys repeat
  return { arrays: [lut], lut, min: lo, size, G, values, meta: cols.map((c) => ({ dict: c.dict ?? null, codes: c.dict ? c.codes : null, date: c.date })) };
}

// (plain functions: hot loops)
function keyRange(keys, n) { // whole-number keys (NaN skipped): [min, max], else null
  let lo = Infinity, hi = -Infinity;
  for (let r = 0; r < n; r++) { const v = keys[r]; if (v !== v) continue; if (v !== Math.floor(v)) return null; if (v < lo) lo = v; if (v > hi) hi = v; }
  return lo === Infinity ? null : [lo, hi];
}
function wholeRange(v, n) { // every value whole (no NaN): [min, max], else null
  let lo = Infinity, hi = -Infinity;
  for (let r = 0; r < n; r++) { const x = v[r]; if (x !== Math.floor(x)) return null; if (x < lo) lo = x; if (x > hi) hi = x; }
  return lo === Infinity ? null : [lo, hi];
}
function fillDirect(T, keys, code, n, lo, base) { // T[key - lo] = code[r] - base; false on a repeated key
  for (let r = 0; r < n; r++) {
    const v = keys[r];
    if (v !== v) continue; // (a missing key matches nothing)
    const j = v - lo;
    if (T[j] !== -1) return false;
    T[j] = code[r] - base;
  }
  return true;
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
// (a dictionary and its length: a text column made on a dataset adds labels to one dictionary chunk by
// chunk, and a build from before, which maps the right labels to the left codes, lacks the new ones)
const dictId = (d) => (d ? `${idOf(d)}:${d.length}` : 0), arrayId = idOf;

export function freeBuild(engine, build) { for (const a of build?.arrays ?? []) engine.free(a); }

// Step 1: the right table's side. Keys as numbers (text keys as the left
// table's codes), numbered in one hash table; then the right rows of each
// group number side by side: rows[start[g] .. start[g + 1]).
// Does a right key that can match (no missing part) have more than one row? (A
// missing key, or a label the left table lacks, matches nothing: rows with one
// may share a group.)
function repeatedKey(engine, build) {
  if (build.rowOf) return false;
  const { K, start, rows } = build, keys = build.rightKeys.map((a) => engine.live(a));
  for (let g = 0; g < K; g++) {
    if (start[g + 1] - start[g] < 2) continue;
    const r = rows[start[g]];
    if (keys.every((k) => k[r] === k[r])) return true;
  }
  return false;
}

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
      const lut = b.dict.map((label) => (label === '' ? NaN : a.codes?.get(label) ?? NaN)); // missing text, or a label the left table lacks, matches nothing
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
    Object.assign(build, { start, rows, rowOf, rid });
    return build;
  } catch (err) {
    freeBuild(engine, build);
    throw err;
  }
}

// Step 2 on every thread: look up the left keys (no inserts).
// hit (RIGHT / FULL JOIN): the dense probe also marks the groups it finds
// (see newHits); returns whether it did.
function* probeG(engine, t, build, leftKeys, lid, miss = -1, hit = null) {
  const p = (a) => engine.live(a).byteOffset;
  if (build.lut) { // whole-number keys in a small range: a direct lookup (lut[key - min])
    const k = kernel(engine, `denseprobe|${t}|${hit ? 'mark' : ''}`, () => denseProbeModule(t, engine.isShared, !!hit));
    yield* engine._unitsG(k, engine.live(lid).length, [p(leftKeys[0]), p(build.lut), p(lid), build.lutMin, build.lutSize, miss, ...(hit ? [p(hit) + 4] : [])]);
    return !!hit;
  }
  const probe = kernel(engine, `hashprobe|${t}|${leftKeys.length}`, () => generateHashProbe(t, leftKeys.length, engine.isShared));
  yield* engine._fusedG(probe, engine.live(lid).length, [...leftKeys, lid], [p(build.slots), build.cap - 1, p(build.keyBits)], 0);
  return false;
}

// probe(start, end, unit, keys, lut, out, min, size, miss): out[i] = lut[key - min]
// for a whole-number key in [min, min + size) that the right table has
// (lut holds -1 for keys it lacks), else miss.
function denseProbeModule(t, shared, mark = false) {
  const f = new Fn('k', [I32, I32, I32, I32, I32, I32, F64, I32, I32, ...(mark ? [I32] : [])], []);
  const [start, stop, , keys, lut, out, min, size, miss, hit] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
  const i = f.local(I32), v = f.local(F64), g = f.local(I32), ok = f.local(I32), x = f.local(I32), at = f.local(I32);
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
  if (mark) { // a match: hit[x] = 1 (stored only while still 0, so the slot's cache line stays shared)
    f.emit(get(ok), get(x), i32c(0), [0x4e], [0x71], [0x04, 0x40]);
    f.emit(get(x), i32c(2), [0x74], get(hit), i32add, set(at));
    f.emit(get(at), [0x28, 2, 0], [0x45], [0x04, 0x40], get(at), i32c(1), [0x36, 2, 0], end, end);
  }
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

// ---- joins on any condition (SQL: ON a.t < b.t, BETWEEN, OR ..., and outer joins with conditions
// that aren't keys) -----------------------------------------------------------------------------
// The pairs of rows (li[i], ri[i]) that meet the condition, then one gather. MISSING as a row number:
// that side has no row (an outer join's row that matched nothing), its columns missing.
const MISSING = 0xffffffff, MAX_PAIRS = 2 ** 31;
const tooMany = (n) => new RangeError(`vectro: join: ${n} pairs of rows is too many (at most 2^31); add an equality, or one comparison such as a.t < b.t, to the ON condition`);

// L, R: tables without filters. opts: { on (keys, may be none), how: 'inner' | 'left' | 'right' | 'full',
// range: { left, right, op } (op '<', '<=', '>' or '>=': left op right), range2 (a second one, with range:
// checked as the pairs are made, so BETWEEN's other bound doesn't make every pair first), where (an
// expression on the output names, true for the pairs to keep), suffix }. keepG(table, expr) gives a 0 / 1 per row of a table.
export function* anyJoinG(L, R, opts, makeTable, keepG) {
  const { how = 'inner', range = null, range2 = null, where = null, suffix = '_right' } = opts;
  const engine = L.engine;
  if (R.engine !== engine) throw new Error('vectro: join needs two tables of the same engine');
  if (!['inner', 'left', 'right', 'full'].includes(how)) throw new Error(`vectro: join how must be 'inner', 'left', 'right' or 'full', got "${how}"`);
  if (L.type !== R.type) throw new Error('vectro: join needs two tables of the same number type (both float64 or both float32)');
  for (const r of [range, range2]) if (r && !COMPARE[r.op]) throw new Error(`vectro: join range op must be '<', '<=', '>' or '>=', got "${r.op}"`); // ('==' became '>=')
  const [lk, rk] = opts.on ? parseOn(opts.on) : [[], []];
  const nL = L.n, nR = R.n;
  // 1. the candidate pairs
  // (keys and a range: each key group's right rows sorted by the range key and searched per left row, so only
  // the pairs the range keeps are made; every key pair first took 1.9 GB for 200K x 1K rows, R6-1.9)
  let { li, ri, n } = lk.length ? yield* joinTablesG(L, R, { on: opts.on, how: 'inner', pairsOnly: true, labels: opts.labels, range, range2 }, makeTable)
    : range ? rangePairs(engine, L, R, range, range2) : crossPairs(nL, nR);
  const cols = outputColumns(L, R, lk, rk, suffix);
  // 2. the other conditions, on a table of the columns they read
  if (where) {
    const used = new Set(whereNames(where)), read = cols.filter((c) => used.has(c.out));
    for (const n of used) if (!cols.some((c) => c.out === n)) throw new Error(`vectro: join where: no column "${n}" (the joined columns: ${cols.map((c) => c.out).join(', ')})`);
    if (!read.length) read.push(cols[0]); // (ON TRUE, ON 1 = 0: a constant reads no column, but a table needs one)
    const C = gatherPairs(engine, L, R, li, ri, n, read, makeTable);
    let keep;
    try { keep = yield* keepG(C, where); } finally { C.dispose(); }
    let m = 0;
    for (let i = 0; i < n; i++) if (keep[i] === 1) { li[m] = li[i]; ri[m] = ri[i]; m++; }
    n = m;
  }
  // 3. outer joins: the rows of one side that no pair kept, the other side missing
  const extraL = [], extraR = [];
  if (how === 'left' || how === 'full') { const seen = new Uint8Array(nL); for (let i = 0; i < n; i++) seen[li[i]] = 1; for (let r = 0; r < nL; r++) if (!seen[r]) extraL.push(r); }
  if (how === 'right' || how === 'full') { const seen = new Uint8Array(nR); for (let i = 0; i < n; i++) seen[ri[i]] = 1; for (let r = 0; r < nR; r++) if (!seen[r]) extraR.push(r); }
  const total = n + extraL.length + extraR.length;
  if (total > MAX_PAIRS) throw tooMany(total);
  const LI = new Uint32Array(total), RI = new Uint32Array(total);
  LI.set(li.subarray(0, n)); RI.set(ri.subarray(0, n));
  extraL.forEach((r, k) => { LI[n + k] = r; RI[n + k] = MISSING; });
  extraR.forEach((r, k) => { LI[n + extraL.length + k] = MISSING; RI[n + extraL.length + k] = r; });
  return gatherPairs(engine, L, R, LI, RI, total, cols, makeTable);
}

const COMPARE = { '<': (a, b) => a < b, '<=': (a, b) => a <= b, '>': (a, b) => a > b, '>=': (a, b) => a >= b };
// a column's values as numbers (a range key: numbers or dates)
function numbers(engine, T, name) {
  const c = T.cols.get(name);
  if (!c) throw new Error(`vectro: join: no column "${name}"`);
  if (c.dict || c.strings) throw new Error(`vectro: join: a range condition (<, <=, >, >=) needs number or date columns, not the text column "${name}"`);
  return engine.live(c.data);
}
// keys and a range: the pairs of a hash join (left row i, its key group g) that also meet left op right (and range2)
function keyRangePairs(engine, L, R, found, nL, start, rows, rowOf, { left, right, op }, range2) {
  const lv = numbers(engine, L, left), rv = numbers(engine, R, right);
  const lv2 = range2 && numbers(engine, L, range2.left), rv2 = range2 && numbers(engine, R, range2.right), cmp2 = range2 && COMPARE[range2.op];
  let lo, hi, order; // each group's right rows in [lo[g], hi[g]) of order, sorted by key, missing keys left out
  if (rowOf) { // (unique right keys: one row per group)
    const G = rowOf.length;
    order = Int32Array.from(rowOf); lo = new Int32Array(G); hi = new Int32Array(G);
    for (let g = 0; g < G; g++) { lo[g] = g; hi[g] = rv[rowOf[g]] === rv[rowOf[g]] ? g + 1 : g; }
  } else {
    const G = start.length - 1;
    order = Int32Array.from(rows.subarray(0, start[G])); lo = new Int32Array(G); hi = new Int32Array(G);
    for (let g = 0; g < G; g++) {
      const a = start[g], b = start[g + 1];
      let w = a;
      for (let j = a; j < b; j++) if (rv[order[j]] === rv[order[j]]) order[w++] = order[j];
      order.subarray(a, w).sort((x, y) => rv[x] - rv[y] || x - y);
      lo[g] = a; hi[g] = w;
    }
  }
  const firstAbove = (a, b, x, strict) => { while (a < b) { const h = (a + b) >> 1; if (strict ? rv[order[h]] > x : rv[order[h]] >= x) b = h; else a = h + 1; } return a; };
  const li = [], ri = [];
  for (let i = 0; i < nL; i++) {
    const g = found[i], x = lv[i];
    if (g < 0 || x !== x) continue;
    const a = op === '<' ? firstAbove(lo[g], hi[g], x, true) : op === '<=' ? firstAbove(lo[g], hi[g], x, false) : lo[g];
    const b = op === '>' ? firstAbove(lo[g], hi[g], x, false) : op === '>=' ? firstAbove(lo[g], hi[g], x, true) : hi[g];
    for (let j = a; j < b; j++) {
      const r = order[j];
      if (cmp2 && !cmp2(lv2[i], rv2[r])) continue;
      li.push(i); ri.push(r);
      if (li.length > MAX_PAIRS) throw tooMany(li.length);
    }
  }
  return { li: Uint32Array.from(li), ri: Uint32Array.from(ri), n: li.length };
}
// left op right: the right rows sorted by key, then each left key's run of them found by binary search
function rangePairs(engine, L, R, { left, right, op }, range2) {
  const lv = numbers(engine, L, left), rv = numbers(engine, R, right), nL = L.n, nR = R.n;
  const lv2 = range2 && numbers(engine, L, range2.left), rv2 = range2 && numbers(engine, R, range2.right), cmp2 = range2 && COMPARE[range2.op];
  const order = [];
  for (let r = 0; r < nR; r++) if (rv[r] === rv[r]) order.push(r); // (missing keys never pair)
  order.sort((a, b) => rv[a] - rv[b] || a - b);
  const keys = Float64Array.from(order, (r) => rv[r]), m = keys.length;
  const firstAbove = (x, strict) => { let lo = 0, hi = m; while (lo < hi) { const h = (lo + hi) >> 1; if (strict ? keys[h] > x : keys[h] >= x) hi = h; else lo = h + 1; } return lo; };
  // each left row's [from, to) in `order`
  const from = new Uint32Array(nL), to = new Uint32Array(nL);
  let total = 0;
  for (let i = 0; i < nL; i++) {
    const x = lv[i];
    if (x !== x) continue;
    let a, b;
    if (op === '<') { a = firstAbove(x, true); b = m; } // (right > x)
    else if (op === '<=') { a = firstAbove(x, false); b = m; }
    else if (op === '>') { a = 0; b = firstAbove(x, false); } // (right < x)
    else { a = 0; b = firstAbove(x, true); }
    from[i] = a; to[i] = b;
    if (!range2) total += b - a;
    else { const y = lv2[i]; for (let j = a; j < b; j++) if (cmp2(y, rv2[order[j]])) total++; }
  }
  if (total > MAX_PAIRS) throw tooMany(total);
  const li = new Uint32Array(total), ri = new Uint32Array(total);
  let k = 0;
  if (!range2) for (let i = 0; i < nL; i++) for (let j = from[i]; j < to[i]; j++) { li[k] = i; ri[k] = order[j]; k++; }
  else for (let i = 0; i < nL; i++) { const y = lv2[i]; for (let j = from[i]; j < to[i]; j++) { const r = order[j]; if (cmp2(y, rv2[r])) { li[k] = i; ri[k] = r; k++; } } }
  return { li, ri, n: total };
}
function crossPairs(nL, nR) {
  const total = nL * nR;
  if (total > MAX_PAIRS) throw tooMany(total);
  const li = new Uint32Array(total), ri = new Uint32Array(total);
  for (let i = 0, k = 0; i < nL; i++) for (let j = 0; j < nR; j++, k++) { li[k] = i; ri[k] = j; }
  return { li, ri, n: total };
}
// the names a where expression reads
function whereNames(src) {
  const out = [];
  const visit = (x) => { if (!x || typeof x !== 'object') return; if (x.t === 'var') out.push(x.name); for (const v of Object.values(x)) if (Array.isArray(v)) v.forEach(visit); else if (v && typeof v === 'object') visit(v); };
  visit(parse(src));
  return out;
}
// output columns, as joinTablesG names them: { out, side, name, key } (key: a right key named like its
// left key, which goes away: an unmatched right row's value comes from it)
function outputColumns(L, R, lk, rk, suffix) {
  const cols = [], names = new Set(), sameKey = new Map(rk.map((name, k) => [lk[k] === name ? name : null, name]).filter(([x]) => x));
  for (const [name, c] of L.cols) if (!c.hidden) { cols.push({ out: name, side: 'L', name, key: sameKey.get(name) ?? null }); names.add(name); }
  for (const [name, c] of R.cols) {
    if (sameKey.has(name) || c.hidden) continue;
    let out = name;
    while (names.has(out)) out += suffix;
    names.add(out);
    cols.push({ out, side: 'R', name });
  }
  return cols;
}
// the output table of pairs (li[i], ri[i]); MISSING: that side's columns missing ('' in text)
function gatherPairs(engine, L, R, li, ri, n, cols, makeTable) {
  const out = new Map();
  for (const { out: name, side, name: src, key } of cols) {
    const T = side === 'L' ? L : R, c = T.cols.get(src), ix = side === 'L' ? li : ri, alt = key ? R.cols.get(key) : null, ax = ri;
    let { dict, codes } = c;
    let none = NaN;
    if (dict) {
      none = codes.get('');
      if (none === undefined) { dict = [...dict, '']; codes = new Map(codes).set('', dict.length - 1); none = dict.length - 1; }
    }
    let recode = null; // (a text key: the right table's codes are in its own dictionary, so they become this column's)
    if (dict && alt?.dict) {
      recode = new Float64Array(alt.dict.length);
      alt.dict.forEach((label, j) => {
        let k = codes.get(label);
        if (k === undefined) { if (dict === c.dict) { dict = [...dict]; codes = new Map(codes); } dict.push(label); k = dict.length - 1; codes.set(label, k); }
        recode[j] = k;
      });
    }
    const data = engine.alloc(L.type, n), d = engine.live(data), a = engine.live(c.data), b = alt ? engine.live(alt.data) : null;
    for (let i = 0; i < n; i++) { const r = ix[i]; d[i] = r !== MISSING ? a[r] : b && ax[i] !== MISSING ? (recode ? recode[b[ax[i]]] ?? none : b[ax[i]]) : none; }
    out.set(name, { data, dict, codes, strings: c.strings, date: c.date, zones: undefined });
  }
  for (const c of out.values()) c.data = engine.live(c.data);
  return makeTable(out, n);
}
