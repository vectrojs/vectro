// WebAssembly kernels for group-by with many groups (see partition.mjs).
// Pointers are byte offsets into the engine's memory. Every kernel takes one
// of them: a block of parameters (layouts below), so keys, columns and
// aggregations aren't limited by the dispatcher's scalars.
//
// A row's key becomes r: 1-4 uint64 words that compare (word by word, as
// unsigned numbers) like the keys do: the first key first, text in dictionary
// order, numbers ascending, NaN last. Each key is a bit field: whole numbers
// (and text codes) as their offset from the minimum, NaN as the code after
// the largest; other numbers as their float bits with the sign flipped
// (negatives: every bit; -0 as 0; NaN: all ones). Fields never straddle
// words. A field's lo, NaN code, width and place come in the parameters, so
// a kernel serves any data of the same shape.

import { I32, F64, TY, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32mul, i32and, i32geu } from './wasm.mjs';
import { Emitter, packedScalar, PACKED_BYTES } from './fuse.mjs';

const I64 = 0x7e;
const uleb = (n) => { const out = []; do { let b = n & 0x7f; n >>>= 7; if (n) b |= 0x80; out.push(b); } while (n); return out; };
const i64c = (v) => { // signed LEB128
  v = BigInt.asIntN(64, BigInt(v));
  const out = [0x42];
  for (;;) {
    const b = Number(v & 0x7fn);
    v >>= 7n;
    if ((v === 0n && !(b & 0x40)) || (v === -1n && b & 0x40)) { out.push(b); return out; }
    out.push(b | 0x80);
  }
};
const f64c = (v) => [0x44, ...new Uint8Array(Float64Array.of(v).buffer)];
const tee = (i) => [0x22, ...uleb(i)];
const L32 = (o = 0) => [0x28, 2, ...uleb(o)], S32 = (o = 0) => [0x36, 2, ...uleb(o)];
const L64 = (o = 0) => [0x29, 3, ...uleb(o)], S64 = (o = 0) => [0x37, 3, ...uleb(o)];
const LF64 = (o = 0) => [0x2b, 3, ...uleb(o)], SF64 = (o = 0) => [0x39, 3, ...uleb(o)];
const L16U = (o = 0) => [0x2f, 1, ...uleb(o)];
const X = {
  i32sub: [0x6b], i32shl: [0x74], i32shru: [0x76], i32gtu: [0x4b], i32eq: [0x46], i32ne: [0x47], i32eqz: [0x45], i32or: [0x72], i32clz: [0x67], i32ctz: [0x68], i32divu: [0x6e],
  i64sub: [0x7d], i64mul: [0x7e], i64and: [0x83], i64or: [0x84], i64xor: [0x85], i64shl: [0x86], i64shru: [0x88], i64eq: [0x51], i64lts: [0x53], i64ltu: [0x54], i64gtu: [0x56], i64clz: [0x79],
  wrap: [0xa7], extu: [0xad], truncSatU64: [0xfc, 0x07], f64FromU64: [0xba], i64bits: [0xbd], f64bits: [0xbf],
  f64add: [0xa0], f64sub: [0xa1], f64ne: [0x62], f64min: [0xa4], f64max: [0xa5], promote: [0xbb], select: [0x1b],
  fill: [0xfc, 0x0b, 0x00], copy: [0xfc, 0x0a, 0x00, 0x00],
};
const IF = [0x04, 0x40], RETURN = [0x0f];
const MIX = 0x9e3779b97f4a7c15n, MIX2 = 0xc2b2ae3d27d4eb4fn, SIGN = 0x8000000000000000n;
const align8 = (x) => (x + 7) & ~7;
const addr4 = (base, i) => [...get(i), ...i32c(2), ...X.i32shl, ...get(base), ...i32add]; // base + i * 4

export const NB = 65_536; // lookup buckets: (word 0 - base) >> shift, the last one open-ended
export const FIELD = 40;  // parameters per key: lo (f64), NaN code, width, place in its word, mask (u64)
export const DIGIT_BITS = 11; // radix sort of a partition's groups
const OP_OF = { add: X.f64add, min: X.f64min, max: X.f64max };
const IDENT = { add: 0, min: Infinity, max: -Infinity };

// field i's parameters, loaded once into locals
function fieldLocals(f, prm, at, fields) {
  return fields.map((k, i) => {
    const o = at + FIELD * i, x = { lo: f.local(F64), nan: f.local(I64), bits: f.local(I64), shift: f.local(I64), mask: f.local(I64) };
    f.emit(get(prm), LF64(o), set(x.lo), get(prm), L64(o + 8), set(x.nan), get(prm), L64(o + 16), set(x.bits), get(prm), L64(o + 24), set(x.shift), get(prm), L64(o + 32), set(x.mask));
    return x;
  });
}

// ---- rows: count, then scatter (OP.UNITS) ------------------------------------
// rows(start, end, unit, prm) over the table's rows [start, end). prm: +0 the
// lookup (u16[NB]: bucket -> partition), +4 counters (u32 [units][P]; for
// the scatter, each unit's next record in each partition), +8 records, +12
// P, +16 base (u64), +24 shift, +32 each array's address, then (8-aligned)
// a FIELD block per key, then the scalars (f64).
// count: counters[unit][partition]++. scatter: the row's record [r words |
// the values to add up (f64), at their accumulators' places] at
// records + RB * counters[unit][partition]++ (rows in order, so every
// partition holds its rows in row order).
export function rowsLayout(nArrays, nKeys, nScalars) {
  const FIELDS = align8(32 + 4 * nArrays), SC = FIELDS + FIELD * nKeys;
  return { FIELDS, SC, bytes: SC + 8 * nScalars };
}

export function rowsModule({ t, arrays, encs, fields, W, vals, scalarNames, RB }, mode, shared) {
  const ty = TY[t], lay = rowsLayout(arrays.length, fields.length, scalarNames.length);
  const f = new Fn('k', [I32, I32, I32, I32], []);
  const [start, stop, unit, prm] = [0, 1, 2, 3];
  const env = { arrays: new Map(), scalars: new Map(), constVec: () => { throw new Error('vector constants are not used here'); } };
  arrays.forEach((name, i) => env.arrays.set(name, { ptr: f.local(I32), sca: f.local(ty.val), enc: encs[i] ?? null }));
  env.t = t; env.raw = f.local(ty.val); env.tmp = f.local(ty.val);
  scalarNames.forEach((name, i) => {
    const sca = f.local(ty.val);
    f.emit(get(prm), LF64(lay.SC + 8 * i), t === 'f32' ? [0xb6] : [], set(sca)); // (f32 kernels: demoted)
    env.scalars.set(name, { sca });
  });
  const em = new Emitter(f, t, env);
  if (mode === 'scatter') em.prepare(vals.map((v) => v.ast));
  const endP = f.local(I32), lut = f.local(I32), cnt = f.local(I32), recs = f.local(I32), a = f.local(I32), pos = f.local(I32), rec = f.local(I32);
  const base = f.local(I64), sh = f.local(I64), d = f.local(I64), b = f.local(I64);
  const v = f.local(F64);
  const r = Array.from({ length: W }, () => f.local(I64));
  const fl = fieldLocals(f, prm, lay.FIELDS, fields);
  const bytesOf = (i) => (encs[i] ? PACKED_BYTES[encs[i].kind] : ty.bytes);

  arrays.forEach((name, i) => f.emit(get(prm), L32(32 + 4 * i), get(start), i32c(bytesOf(i)), i32mul, i32add, set(env.arrays.get(name).ptr)));
  const p0 = env.arrays.get(arrays[0]).ptr;
  f.emit(get(stop), get(start), X.i32sub, i32c(bytesOf(0)), i32mul, get(p0), i32add, set(endP));
  f.emit(get(prm), L32(0), set(lut), get(prm), L32(8), set(recs));
  f.emit(get(prm), L32(4), get(unit), get(prm), L32(12), i32mul, i32c(2), X.i32shl, i32add, set(cnt));
  f.emit(get(prm), L64(16), set(base), get(prm), L32(24), X.extu, set(sh));

  const read = arrays.filter((name) => mode === 'scatter' || fields.some((k) => k.arr === name)); // (counting reads only the keys)
  f.emit(block, loop, get(p0), get(endP), i32geu, brIf(1));
  for (const name of read) {
    const x = env.arrays.get(name);
    if (x.enc) { packedScalar(f, t, x.enc, x.ptr, env.raw, env.tmp); f.emit(set(x.sca)); } else f.emit(get(x.ptr), ty.load, set(x.sca));
  }
  for (let w = 0; w < W; w++) f.emit(i64c(0), set(r[w]));
  fields.forEach((k, i) => {
    f.emit(get(env.arrays.get(k.arr).sca), t === 'f32' ? X.promote : [], set(v));
    if (k.kind === 'int') { // r = (r << width) | (v - lo), NaN as its code
      f.emit(get(r[k.word]), get(fl[i].bits), X.i64shl);
      f.emit(get(fl[i].nan), get(v), get(fl[i].lo), X.f64sub, X.truncSatU64, get(v), get(v), X.f64ne, X.select);
      f.emit(X.i64or, set(r[k.word]));
    } else { // the whole word: float bits that sort as unsigned
      f.emit(get(v), f64c(0), X.f64add, X.i64bits, set(b)); // (-0 as 0)
      f.emit(get(b), i64c(-1), X.i64xor, get(b), i64c(SIGN), X.i64or, get(b), i64c(0), X.i64lts, X.select, set(b));
      f.emit(i64c(-1), get(b), get(v), get(v), X.f64ne, X.select, set(r[k.word]));
    }
  });
  // partition: lookup[min((r0 - base) >> shift, NB - 1)] (below base: bucket 0)
  f.emit(i64c(0), get(r[0]), get(base), X.i64sub, get(r[0]), get(base), X.i64ltu, X.select, get(sh), X.i64shru, set(d));
  f.emit(i64c(NB - 1), get(d), get(d), i64c(NB - 1), X.i64gtu, X.select, X.wrap, i32c(1), X.i32shl, get(lut), i32add, L16U(), i32c(2), X.i32shl, get(cnt), i32add, set(a));
  if (mode === 'count') f.emit(get(a), get(a), L32(), i32c(1), i32add, S32());
  else {
    f.emit(get(a), get(a), L32(), tee(pos), i32c(1), i32add, S32());
    f.emit(get(pos), i32c(RB), i32mul, get(recs), i32add, set(rec));
    for (let w = 0; w < W; w++) f.emit(get(rec), get(r[w]), S64(8 * w));
    for (const x of vals) {
      f.emit(get(rec));
      em.sca(x.ast);
      if (t === 'f32') f.emit(X.promote);
      f.emit(SF64(8 * W + 8 * x.slot));
    }
  }
  for (const name of read) { const p = env.arrays.get(name).ptr; f.emit(get(p), i32c(bytesOf(arrays.indexOf(name))), i32add, set(p)); }
  f.emit(br(0), end, end);
  return buildModule([f], shared);
}

// ---- aggregate: one partition per task (OP.TASKS) ----------------------------
// aggregate(task, thread, prm): the partition order[task] (biggest first).
// prm: +0 records, +4 partition starts (u32[P + 1]), +8 groups out (u32[P];
// 0xffffffff: more than gMax groups, records left as they were), +12 scratch
// (64-aligned), +16 scratch bytes per thread, +20 capMax (a power of 2), +24
// gMax, +28 order (u32[P]).
// The partition's rows are added up in a hash table in this thread's
// scratch (rows in order: the same sums on any number of threads), with
// twice as many slots as rows, or capMax slots (then it holds up to gMax
// groups; more: the partition is split again). Its groups are then sorted by r (LSD
// radix, 11-bit digits, only the bits that differ) and copied, in order,
// over the partition's first records: a group is a record (its accumulators
// in the value places), and a partition has at least as many rows as groups.
// Scratch: slots i32[capMax] | groups [gMax * RB] | K u64[gMax] | K2 u64[gMax]
// | I u32[gMax] | I2 u32[gMax] | counts u32[2^11].
// spec: { W, RB, accs: [{ op: 'add' | 'min' | 'max', stored: bool (else adds 1) }] }
// branchFree: every row stores its group (found or new) without a branch on
// which it was, best when many rows start a new group (a branch would guess wrong).
export const scratchBytes = (capMax, gMax, RB) => Math.ceil((capMax * 4 + gMax * RB + gMax * 24 + (1 << DIGIT_BITS) * 4) / 64) * 64;

export function aggregateModule({ W, RB, accs }, branchFree, shared) {
  const f = new Fn('k', [I32, I32, I32], []);
  const [task, self, prm] = [0, 1, 2];
  const L = (type) => f.local(type);
  const part = L(I32), recs = L(I32), s = L(I32), e = L(I32), rows = L(I32), slots = L(I32), dense = L(I32), K = L(I32), K2 = L(I32), I = L(I32), I2 = L(I32), hist = L(I32);
  const cap = L(I32), lg = L(I32), mask = L(I32), G = L(I32), gMax = L(I32), capMax = L(I32), rec = L(I32), stopRec = L(I32), h = L(I32), idx = L(I32), sa = L(I32), d = L(I32), tmp = L(I32), k = L(I32), dg = L(I32), at = L(I32), nb = L(I32), q = L(I32), lim = L(I32), empty = L(I32), cnt = L(I32);
  const r = Array.from({ length: W }, () => L(I64)), rr = Array.from({ length: W }, () => L(I64));
  const x = L(I64), y = L(I64), sh64 = L(I64), digitMask = L(I64), dbits = L(I32), passBits = L(I32), shiftW = L(I32);
  const groupsOut = (val) => [...get(prm), ...L32(8), ...get(part), ...i32c(2), ...X.i32shl, ...i32add, ...val, ...S32()]; // groups[part] = val
  const hashOf = (words) => { // h = the top lg bits of a mix of every word
    f.emit(get(words[0]));
    for (let w = 1; w < W; w++) f.emit(i64c(MIX2), X.i64mul, get(words[w]), X.i64xor);
    f.emit(i64c(MIX), X.i64mul, i64c(64), get(lg), X.extu, X.i64sub, X.i64shru, X.wrap, set(h));
  };
  const setCap = () => { // cap = 1 << lg, slots cleared; lim = cap / 2, or gMax once the table can't grow
    f.emit(i32c(1), get(lg), X.i32shl, set(cap), get(cap), i32c(1), X.i32sub, set(mask));
    f.emit(get(slots), i32c(0xff), get(cap), i32c(2), X.i32shl, X.fill);
    f.emit(get(cap), i32c(1), X.i32shru, set(lim), get(cap), get(capMax), i32geu, IF, get(gMax), set(lim), end);
  };
  const grow = () => { // twice the slots, every group rehashed (rare); full: give up on this partition
    f.emit(get(cap), get(capMax), i32geu, IF, groupsOut(i32c(-1)), RETURN, end);
    f.emit(get(lg), i32c(1), i32add, set(lg));
    setCap();
    f.emit(i32c(0), set(k), block, loop, get(k), get(G), i32geu, brIf(1));
    for (let w = 0; w < W; w++) f.emit(get(k), i32c(RB), i32mul, get(dense), i32add, L64(8 * w), set(rr[w]));
    hashOf(rr);
    f.emit(block, loop, get(h), i32c(2), X.i32shl, get(slots), i32add, tee(sa), L32(), i32c(-1), X.i32ne, IF, get(h), i32c(1), i32add, get(mask), i32and, set(h), br(1), end, end, end);
    f.emit(get(sa), get(k), S32());
    f.emit(get(k), i32c(1), i32add, set(k), br(0), end, end);
  };
  const sameKey = () => { // the group at d has key r
    f.emit(get(d), L64(0), get(r[0]), X.i64eq);
    for (let w = 1; w < W; w++) f.emit(get(d), L64(8 * w), get(r[w]), X.i64eq, i32and);
  };
  const value = (ac, j) => (ac.stored ? [...get(rec), ...LF64(8 * W + 8 * j)] : f64c(1));

  f.emit(get(prm), L32(28), get(task), i32c(2), X.i32shl, i32add, L32(), set(part));
  f.emit(get(prm), L32(0), set(recs));
  f.emit(get(prm), L32(4), get(part), i32c(2), X.i32shl, i32add, set(tmp), get(tmp), L32(0), set(s), get(tmp), L32(4), set(e));
  f.emit(get(e), get(s), X.i32sub, set(rows));
  f.emit(get(rows), X.i32eqz, IF, groupsOut(i32c(0)), RETURN, end);
  f.emit(get(prm), L32(12), get(self), get(prm), L32(16), i32mul, i32add, set(slots));
  f.emit(get(prm), L32(20), set(capMax), get(prm), L32(24), set(gMax));
  f.emit(get(capMax), i32c(2), X.i32shl, get(slots), i32add, set(dense));
  f.emit(get(gMax), i32c(RB), i32mul, get(dense), i32add, set(K), get(gMax), i32c(3), X.i32shl, get(K), i32add, set(K2));
  f.emit(get(gMax), i32c(3), X.i32shl, get(K2), i32add, set(I), get(gMax), i32c(2), X.i32shl, get(I), i32add, set(I2), get(gMax), i32c(2), X.i32shl, get(I2), i32add, set(hist));
  // room for every row to be a group (it never grows then; a partition's
  // rows fit in cache anyway): lg = log2 of twice the rows, within [4, log2 capMax]
  f.emit(i32c(32), get(rows), i32c(1), X.i32shl, i32c(1), X.i32sub, X.i32clz, X.i32sub, set(lg));
  f.emit(get(lg), i32c(4), get(lg), i32c(4), X.i32gtu, X.select, set(lg));
  f.emit(get(capMax), X.i32ctz, set(tmp), get(tmp), get(lg), get(lg), get(tmp), X.i32gtu, X.select, set(lg));
  setCap();
  f.emit(i32c(0), set(G));
  f.emit(get(s), i32c(RB), i32mul, get(recs), i32add, set(rec), get(e), i32c(RB), i32mul, get(recs), i32add, set(stopRec));

  // rows -> groups
  f.emit(block, loop, get(rec), get(stopRec), i32geu, brIf(1));
  if (branchFree) {
    for (let w = 0; w < W; w++) f.emit(get(rec), L64(8 * w), set(r[w]));
    hashOf(r);
    // probe until an empty slot (the next group number) or the same key;
    // a new group with the table half full: grow first, then probe again
    // (dense has room for group G < lim: its key is compared, not trusted)
    f.emit(block, loop);
    f.emit(block, loop);
    f.emit(get(h), i32c(2), X.i32shl, get(slots), i32add, tee(sa), L32(), tee(idx), i32c(-1), X.i32eq, set(empty));
    f.emit(get(G), get(idx), get(empty), X.select, tee(idx), i32c(RB), i32mul, get(dense), i32add, set(d));
    sameKey();
    f.emit(get(empty), X.i32or, brIf(1));
    f.emit(get(h), i32c(1), i32add, get(mask), i32and, set(h), br(0));
    f.emit(end, end);
    f.emit(get(empty), get(G), get(lim), i32geu, i32and, IF); grow(); hashOf(r); f.emit(br(1), end);
    f.emit(br(1), end, end);
    f.emit(get(sa), get(idx), S32());
    for (let w = 0; w < W; w++) f.emit(get(d), get(r[w]), S64(8 * w));
    f.emit(get(G), get(empty), i32add, set(G));
    accs.forEach((ac, j) => { // a new group starts from the identity
      const o = 8 * W + 8 * j;
      f.emit(get(d), f64c(IDENT[ac.op]), get(d), LF64(o), get(empty), X.select, value(ac, j), OP_OF[ac.op], SF64(o));
    });
  } else {
    for (let w = 0; w < W; w++) f.emit(get(rec), L64(8 * w), set(r[w]));
    hashOf(r);
    f.emit(block, loop); // probe
    f.emit(get(h), i32c(2), X.i32shl, get(slots), i32add, tee(sa), L32(), tee(idx), i32c(-1), X.i32eq, IF);
    f.emit(get(G), get(lim), i32geu, IF); grow(); hashOf(r); f.emit(br(2), end); // (then probe the bigger table)
    f.emit(get(sa), get(G), S32(), get(G), i32c(RB), i32mul, get(dense), i32add, set(d), get(G), i32c(1), i32add, set(G));
    for (let w = 0; w < W; w++) f.emit(get(d), get(r[w]), S64(8 * w));
    accs.forEach((ac, j) => f.emit(get(d), f64c(IDENT[ac.op]), SF64(8 * W + 8 * j)));
    f.emit(br(2), end); // a new group
    f.emit(get(idx), i32c(RB), i32mul, get(dense), i32add, set(d));
    sameKey();
    f.emit(brIf(1));
    f.emit(get(h), i32c(1), i32add, get(mask), i32and, set(h), br(0));
    f.emit(end, end);
    accs.forEach((ac, j) => {
      const o = 8 * W + 8 * j;
      f.emit(get(d), get(d), LF64(o), value(ac, j), OP_OF[ac.op], SF64(o));
    });
  }
  f.emit(get(rec), i32c(RB), i32add, set(rec), br(0), end, end);

  // sort: I = 0..G-1; per key word, last first: K = that word of group I[k],
  // then LSD radix passes over the bits that differ, moving (K, I) pairs
  f.emit(i32c(0), set(k), block, loop, get(k), get(G), i32geu, brIf(1), addr4(I, k), get(k), S32(), get(k), i32c(1), i32add, set(k), br(0), end, end);
  for (let w = W - 1; w >= 0; w--) {
    f.emit(i64c(0), set(x), get(dense), L64(8 * w), set(y));
    f.emit(i32c(0), set(k), block, loop, get(k), get(G), i32geu, brIf(1));
    f.emit(get(k), i32c(3), X.i32shl, get(K), i32add, addr4(I, k), L32(), i32c(RB), i32mul, get(dense), i32add, L64(8 * w), tee(sh64), S64());
    f.emit(get(x), get(sh64), get(y), X.i64xor, X.i64or, set(x));
    f.emit(get(k), i32c(1), i32add, set(k), br(0), end, end);
    // bits = 64 - clz(x); passes = ceil(bits / 11); bits per pass = ceil(bits / passes)
    f.emit(i32c(64), get(x), X.i64clz, X.wrap, X.i32sub, set(dbits));
    f.emit(get(dbits), i32c(DIGIT_BITS - 1), i32add, i32c(DIGIT_BITS), X.i32divu, set(tmp));
    f.emit(get(tmp), IF, get(dbits), get(tmp), i32add, i32c(1), X.i32sub, get(tmp), X.i32divu, set(passBits), end);
    f.emit(i32c(0), set(shiftW));
    f.emit(block, loop, get(shiftW), get(dbits), i32geu, brIf(1));
    f.emit(i32c(1), get(passBits), X.i32shl, set(nb), i64c(1), get(passBits), X.extu, X.i64shl, i64c(1), X.i64sub, set(digitMask), get(shiftW), X.extu, set(sh64));
    f.emit(get(hist), i32c(0), get(nb), i32c(2), X.i32shl, X.fill);
    const digitOf = () => f.emit(get(k), i32c(3), X.i32shl, get(K), i32add, L64(), set(y), get(y), get(sh64), X.i64shru, get(digitMask), X.i64and, X.wrap, set(dg));
    f.emit(i32c(0), set(k), block, loop, get(k), get(G), i32geu, brIf(1));
    digitOf();
    f.emit(addr4(hist, dg), tee(tmp), get(tmp), L32(), i32c(1), i32add, S32());
    f.emit(get(k), i32c(1), i32add, set(k), br(0), end, end);
    f.emit(i32c(0), set(at), i32c(0), set(q), block, loop, get(q), get(nb), i32geu, brIf(1)); // counts -> first places
    f.emit(addr4(hist, q), tee(tmp), L32(), set(cnt), get(tmp), get(at), S32(), get(at), get(cnt), i32add, set(at));
    f.emit(get(q), i32c(1), i32add, set(q), br(0), end, end);
    f.emit(i32c(0), set(k), block, loop, get(k), get(G), i32geu, brIf(1));
    digitOf();
    f.emit(addr4(hist, dg), tee(tmp), L32(), set(q), get(tmp), get(q), i32c(1), i32add, S32());
    f.emit(get(q), i32c(3), X.i32shl, get(K2), i32add, get(y), S64());
    f.emit(addr4(I2, q), addr4(I, k), L32(), S32());
    f.emit(get(k), i32c(1), i32add, set(k), br(0), end, end);
    f.emit(get(K), set(tmp), get(K2), set(K), get(tmp), set(K2), get(I), set(tmp), get(I2), set(I), get(tmp), set(I2));
    f.emit(get(shiftW), get(passBits), i32add, set(shiftW), br(0), end, end);
  }
  // the groups, sorted, over the partition's first records
  f.emit(i32c(0), set(k), get(s), i32c(RB), i32mul, get(recs), i32add, set(rec));
  f.emit(block, loop, get(k), get(G), i32geu, brIf(1));
  f.emit(get(rec), addr4(I, k), L32(), i32c(RB), i32mul, get(dense), i32add, i32c(RB), X.copy);
  f.emit(get(rec), i32c(RB), i32add, set(rec), get(k), i32c(1), i32add, set(k), br(0), end, end);
  f.emit(groupsOut(get(G)));
  return buildModule([f], shared);
}

// ---- write: each partition's groups into the result columns (OP.TASKS) -------
// write(task, thread, prm): partition `task`. prm: +0 records, +4 partition
// starts, +8 groups per partition, +12 output offsets (u32[P]), +16 each
// key column's address, then each accumulator column's, then (8-aligned) a
// FIELD block per key. Keys are decoded from r; the columns are float64.
export function writeLayout(nKeys, J) {
  const KO = 16, AO = KO + 4 * nKeys, FIELDS = align8(AO + 4 * J);
  return { KO, AO, FIELDS, bytes: FIELDS + FIELD * nKeys };
}

export function writeModule({ W, RB, fields, J }, shared) {
  const lay = writeLayout(fields.length, J);
  const f = new Fn('k', [I32, I32, I32], []);
  const [task, , prm] = [0, 1, 2];
  const L = (type) => f.local(type);
  const rec = L(I32), stopRec = L(I32), o8 = L(I32);
  const r = Array.from({ length: W }, () => L(I64));
  const code = L(I64);
  const kp = fields.map(() => L(I32)), ap = Array.from({ length: J }, () => L(I32));
  const fl = fieldLocals(f, prm, lay.FIELDS, fields);
  f.emit(get(prm), L32(4), get(task), i32c(2), X.i32shl, i32add, L32(), i32c(RB), i32mul, get(prm), L32(0), i32add, set(rec));
  f.emit(get(prm), L32(8), get(task), i32c(2), X.i32shl, i32add, L32(), i32c(RB), i32mul, get(rec), i32add, set(stopRec));
  f.emit(get(prm), L32(12), get(task), i32c(2), X.i32shl, i32add, L32(), i32c(3), X.i32shl, set(o8));
  fields.forEach((_, i) => f.emit(get(prm), L32(lay.KO + 4 * i), get(o8), i32add, set(kp[i])));
  for (let j = 0; j < J; j++) f.emit(get(prm), L32(lay.AO + 4 * j), get(o8), i32add, set(ap[j]));
  f.emit(block, loop, get(rec), get(stopRec), i32geu, brIf(1));
  for (let w = 0; w < W; w++) f.emit(get(rec), L64(8 * w), set(r[w]));
  fields.forEach((k, i) => {
    f.emit(get(kp[i]));
    if (k.kind === 'int') { // lo + code; the NaN code: NaN
      f.emit(get(r[k.word]), get(fl[i].shift), X.i64shru, get(fl[i].mask), X.i64and, set(code));
      f.emit(f64c(NaN), get(code), X.f64FromU64, get(fl[i].lo), X.f64add, get(code), get(fl[i].nan), X.i64eq, X.select);
    } else { // back from sortable bits (top bit set: it was positive); all ones: NaN
      f.emit(f64c(NaN), get(r[k.word]), i64c(SIGN), X.i64xor, get(r[k.word]), i64c(-1), X.i64xor, get(r[k.word]), i64c(0), X.i64lts, X.select, X.f64bits);
      f.emit(get(r[k.word]), i64c(-1), X.i64eq, X.select);
    }
    f.emit(SF64(0), get(kp[i]), i32c(8), i32add, set(kp[i]));
  });
  for (let j = 0; j < J; j++) f.emit(get(ap[j]), get(rec), LF64(8 * W + 8 * j), SF64(0), get(ap[j]), i32c(8), i32add, set(ap[j]));
  f.emit(get(rec), i32c(RB), i32add, set(rec), br(0), end, end);
  return buildModule([f], shared);
}

// ---- split one partition again (OP.UNITS over its records) -------------------
// split(start, end, unit, prm) over records s0 + [start, end) of src, by
// one word of r (only once the words before it are the same in every
// record here): as rows(), from that word. count: counters[unit][part]++ and
// each unit's min and max of the word (unsigned; u64 [units][2]), so a word
// that is the same everywhere shows; copy: each record to dst at
// counters[unit][part]++. prm: +0 lookup, +4 counters, +8 src, +12 P, +16
// base (u64), +24 shift, +32 dst, +36 s0, +40 the word's byte offset in a
// record, +44 min/max (count).
export function splitModule(RB, mode, shared) {
  const f = new Fn('k', [I32, I32, I32, I32], []);
  const [start, stop, unit, prm] = [0, 1, 2, 3];
  const L = (type) => f.local(type);
  const rec = L(I32), endR = L(I32), lut = L(I32), cnt = L(I32), a = L(I32), pos = L(I32), dst = L(I32), wo = L(I32), mm = L(I32);
  const base = L(I64), sh = L(I64), r0 = L(I64), d = L(I64), mn = L(I64), mx = L(I64);
  f.emit(get(prm), L32(36), get(start), i32add, i32c(RB), i32mul, get(prm), L32(8), i32add, set(rec));
  f.emit(get(prm), L32(36), get(stop), i32add, i32c(RB), i32mul, get(prm), L32(8), i32add, set(endR));
  f.emit(get(prm), L32(0), set(lut), get(prm), L32(40), set(wo), get(prm), L32(32), set(dst));
  f.emit(get(prm), L32(4), get(unit), get(prm), L32(12), i32mul, i32c(2), X.i32shl, i32add, set(cnt));
  f.emit(get(prm), L64(16), set(base), get(prm), L32(24), X.extu, set(sh));
  f.emit(i64c(-1), set(mn), i64c(0), set(mx));
  f.emit(block, loop, get(rec), get(endR), i32geu, brIf(1));
  f.emit(get(rec), get(wo), i32add, L64(), set(r0));
  if (mode === 'count') f.emit(get(r0), get(mn), get(r0), get(mn), X.i64ltu, X.select, set(mn), get(r0), get(mx), get(r0), get(mx), X.i64gtu, X.select, set(mx));
  f.emit(i64c(0), get(r0), get(base), X.i64sub, get(r0), get(base), X.i64ltu, X.select, get(sh), X.i64shru, set(d));
  f.emit(i64c(NB - 1), get(d), get(d), i64c(NB - 1), X.i64gtu, X.select, X.wrap, i32c(1), X.i32shl, get(lut), i32add, L16U(), i32c(2), X.i32shl, get(cnt), i32add, set(a));
  if (mode === 'count') f.emit(get(a), get(a), L32(), i32c(1), i32add, S32());
  else {
    f.emit(get(a), get(a), L32(), tee(pos), i32c(1), i32add, S32());
    f.emit(get(pos), i32c(RB), i32mul, get(dst), i32add, get(rec), i32c(RB), X.copy);
  }
  f.emit(get(rec), i32c(RB), i32add, set(rec), br(0), end, end);
  if (mode === 'count') f.emit(get(prm), L32(44), get(unit), i32c(4), X.i32shl, i32add, set(mm), get(mm), get(mn), S64(0), get(mm), get(mx), S64(8));
  return buildModule([f], shared);
}
