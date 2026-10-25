// Code generators for table operations. Both are scalar loops (each row goes
// to a different place, which SIMD can't do), but they still fuse the filter
// and every aggregation into one pass over memory.

import { I32, F64, TY, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32mul, i32and, i32geu } from './wasm.mjs';
import { Emitter } from './fuse.mjs';

const F64_LOAD = [0x2b, 3, 0], F64_STORE = [0x39, 3, 0];
const F64_OP = { add: [0xa0], min: [0xa4], max: [0xa5] };
const F64_PROMOTE_F32 = [0xbb], F32_DEMOTE_F64 = [0xb6], I32_EQZ = [0x45], I32_SHL = [0x74];
const TRUNC_SAT_U = { f64: [0xfc, 0x03], f32: [0xfc, 0x01] }; // float -> unsigned int, no traps

// Locals for each array (current row value) and scalar, shared by both kernels.
function bindValues(f, t, arrayNames, firstScalarParam, scalarNames) {
  const ty = TY[t];
  const env = { arrays: new Map(), scalars: new Map(), constVec: () => { throw new Error('vector constants are not used here'); } };
  arrayNames.forEach((name, i) => env.arrays.set(name, { ptr: i, sca: f.local(ty.val) }));
  scalarNames.forEach((name, i) => {
    const sca = f.local(ty.val);
    f.emit(get(firstScalarParam + i), t === 'f32' ? F32_DEMOTE_F64 : [], set(sca));
    env.scalars.set(name, { sca });
  });
  return env;
}

function loadRow(f, ty, env) {
  for (const a of env.arrays.values()) f.emit(get(a.ptr), ty.load, set(a.sca));
}

function advance(f, ty, ptrs) {
  for (const p of ptrs) f.emit(get(p), i32c(ty.bytes), i32add, set(p));
}

// Group-by: for every row (that passes `pred`), add each aggregation's value
// into its group's accumulator. acc is float64, laid out as J blocks of K*C
// slots: each group has C copies that consecutive rows rotate through, so a
// run of rows in the same group doesn't wait on its own previous update.
// Signature: (array ptrs..., key ptr, n, accBase, K, scalars...)
// ranged: two more params (lo, hi) after K; only rows whose group is in
// [lo, hi) are added (see runGroupRange).
export function generateGroupBy(items, pred, t, arrayNames, scalarNames, C = 1, shared = true, ranged = false) {
  const log2C = Math.log2(C);
  if (!Number.isInteger(log2C)) throw new Error('copies must be a power of two');
  const ty = TY[t];
  const A = arrayNames.length;
  const [key, n, accBase, K, lo, hi] = [A, A + 1, A + 2, A + 3, A + 4, A + 5];
  const extra = ranged ? 2 : 0;
  const f = new Fn('k', [...Array(A).fill(I32), I32, I32, I32, I32, ...Array(extra).fill(I32), ...Array(scalarNames.length).fill(F64)], []);
  const env = bindValues(f, t, arrayNames, A + 4 + extra, scalarNames);
  const em = new Emitter(f, t, env);
  const stop = f.local(I32), idx = f.local(I32), addr = f.local(I32), copy = f.local(I32);
  const bases = items.map(() => f.local(I32));

  // base_j = accBase + j * K * C * 8
  items.forEach((_, j) => f.emit(get(K), i32c(j * C * 8), [0x6c], get(accBase), i32add, set(bases[j])));
  f.emit(get(n), i32c(ty.bytes), [0x6c], get(key), i32add, set(stop));

  f.emit(block, loop, get(key), get(stop), i32geu, brIf(1));
  loadRow(f, ty, env);
  // slot = key * C + copy
  f.emit(get(key), ty.load, TRUNC_SAT_U[t], i32c(log2C), I32_SHL, get(copy), i32add, set(idx));
  f.emit(block);
  if (ranged) f.emit(get(idx), get(lo), [0x6b], get(hi), get(lo), [0x6b], i32geu, brIf(0)); // (idx - lo) >= (hi - lo), unsigned: not ours
  if (pred) { em.scaMask(pred); f.emit(I32_EQZ, brIf(0)); }
  items.forEach((it, j) => {
    const op = it.reduce === 'min' ? 'min' : it.reduce === 'max' ? 'max' : 'add';
    f.emit(get(bases[j]), get(idx), i32c(3), I32_SHL, i32add, set(addr));
    f.emit(get(addr), get(addr), F64_LOAD);
    em.sca(it.ast);
    if (t === 'f32') f.emit(F64_PROMOTE_F32);
    f.emit(F64_OP[op], F64_STORE);
  });
  f.emit(end);
  if (C > 1) f.emit(get(copy), i32c(1), i32add, i32c(C - 1), [0x71], set(copy));
  advance(f, ty, [...env.arrays.values()].map((a) => a.ptr));
  advance(f, ty, [key]);
  f.emit(br(0), end, end);
  return buildModule([f], shared);
}

// Compact: copy the rows that pass `pred` from source columns to destination
// columns. copyFrom[d] is the index (into arrayNames) that destination d copies.
// Signature: (src ptrs..., n, dst ptrs..., scalars...)
export function generateCompact(pred, t, arrayNames, copyFrom, scalarNames, shared = true) {
  const ty = TY[t];
  const A = arrayNames.length, D = copyFrom.length;
  const n = A;
  const dst = Array.from({ length: D }, (_, d) => A + 1 + d);
  const f = new Fn('k', [...Array(A).fill(I32), I32, ...Array(D).fill(I32), ...Array(scalarNames.length).fill(F64)], []);
  const env = bindValues(f, t, arrayNames, A + 1 + D, scalarNames);
  const em = new Emitter(f, t, env);
  const stop = f.local(I32);
  const srcLocals = arrayNames.map((name) => env.arrays.get(name));

  f.emit(get(n), i32c(ty.bytes), [0x6c], get(0), i32add, set(stop));
  f.emit(block, loop, get(0), get(stop), i32geu, brIf(1));
  loadRow(f, ty, env);
  f.emit(block);
  em.scaMask(pred);
  f.emit(I32_EQZ, brIf(0));
  copyFrom.forEach((src, d) => f.emit(get(dst[d]), get(srcLocals[src].sca), ty.store));
  advance(f, ty, dst);
  f.emit(end);
  advance(f, ty, srcLocals.map((a) => a.ptr));
  f.emit(br(0), end, end);
  return buildModule([f], shared);
}

// Hash numbering for group-by on any keys: every thread runs this over its
// own rows, all inserting into ONE table in shared memory. A slot holds -1
// (empty), -2 (being filled), -3 (the table ran out of room) or a group
// number. A new key claims an empty slot with compare-and-swap, takes the next
// group number from a shared counter, stores its key bits and then publishes
// the number; a thread that meets a slot being filled waits for it. Group
// numbers are taken from the counter in growing batches (1, 2, 4... 64 per
// unit: one shared counter hit per group would be a queue of every thread);
// numbers a unit doesn't use stay empty groups, which have no rows and are
// dropped. Keys are
// compared as bits after -0 becomes 0 and every NaN the same NaN.
// Signature: (key ptrs..., ids ptr, n, slotsBase, mask, keysBase, limit,
// counterAddr) -> 0, or 1 if more than `limit` groups showed up.
const I64 = 0x7e;
const op = (...b) => b;
const ATOMIC = { load: [0xfe, 0x10, 2, 0], store: [0xfe, 0x17, 2, 0], add: [0xfe, 0x1e, 2, 0], cmpxchg: [0xfe, 0x48, 2, 0] };
const i64c = (v) => { // signed LEB128 of a BigInt
  const out = [0x42];
  for (;;) {
    const b = Number(v & 0x7fn);
    v >>= 7n;
    if ((v === 0n && !(b & 0x40)) || (v === -1n && b & 0x40)) { out.push(b); return out; }
    out.push(b | 0x80);
  }
};
const f64c = (v) => [0x44, ...new Uint8Array(Float64Array.of(v).buffer)];
const CANON_NAN = 0x7ff8000000000000n, MIX = -0x61c8864680b583ebn; // 0x9E3779B97F4A7C15 as signed

// Key bits of the current row (params 0..nk-1 point at it) and its first
// slot. Shared by the numbering and lookup kernels, so both hash alike.
function keyHash(f, t, nk, bits, v, h, mk, slot) {
  const ty = TY[t];
  f.emit(i64c(MIX), set(h));
  for (let k = 0; k < nk; k++) {
    f.emit(get(k), ty.load, t === 'f32' ? [0xbb] : [], f64c(0), op(0xa0), set(v)); // v = key + 0 (-0 -> 0)
    f.emit(get(v), op(0xbd), i64c(CANON_NAN), get(v), get(v), op(0x61), op(0x1b), set(bits[k])); // NaN -> one NaN
    f.emit(get(h), get(bits[k]), op(0x85), i64c(MIX), op(0x7e), set(h));
  }
  f.emit(get(h), get(h), i64c(32n), op(0x88), op(0x85), i64c(MIX), op(0x7e), set(h));
  f.emit(get(h), get(h), i64c(29n), op(0x88), op(0x85), op(0xa7), get(mk), i32and, set(slot));
}

export function generateHashGroups(t, nk, shared = true) {
  const ty = TY[t];
  const ids = nk, n = nk + 1, S = nk + 2; // S..S+4: slotsBase, mask, keysBase, limit, counter (f64)
  const f = new Fn('k', [...Array(nk).fill(I32), I32, I32, F64, F64, F64, F64, F64], [F64]);
  const [sb, mk, kb, lim, ctr] = [0, 1, 2, 3, 4].map(() => f.local(I32));
  const stop = f.local(I32), slot = f.local(I32), s = f.local(I32), g = f.local(I32), addr = f.local(I32), kaddr = f.local(I32);
  const v = f.local(F64), h = f.local(I64), next = f.local(I32), last = f.local(I32), batch = f.local(I32);
  const bits = Array.from({ length: nk }, () => f.local(I64));
  const IF = [0x04, 0x40], RETURN = [0x0f];

  [sb, mk, kb, lim, ctr].forEach((l, i) => f.emit(get(S + i), [0xfc, 0x03], set(l))); // i32.trunc_sat_f64_u
  f.emit(get(n), i32c(ty.bytes), i32mul, get(ids), i32add, set(stop));
  f.emit(i32c(0), set(next), i32c(0), set(last), i32c(1), set(batch)); // no numbers reserved yet
  f.emit(block, loop, get(ids), get(stop), i32geu, brIf(1));
  keyHash(f, t, nk, bits, v, h, mk, slot);
  // probe
  f.emit(block, loop);
  f.emit(get(slot), i32c(2), op(0x74), get(sb), i32add, set(addr));
  f.emit(get(addr), ATOMIC.load, set(s));
  f.emit(get(s), i32c(-1), op(0x46), IF); // empty: try to claim it
  f.emit(get(addr), i32c(-1), i32c(-2), ATOMIC.cmpxchg, i32c(-1), op(0x46), IF);
  f.emit(get(next), get(last), op(0x46), IF, get(ctr), get(batch), ATOMIC.add, set(next), get(next), get(batch), i32add, set(last)); // reserve more
  f.emit(get(batch), i32c(1), op(0x74), set(batch), i32c(64), get(batch), get(batch), i32c(64), op(0x4b), op(0x1b), set(batch), end); // batch = min(2 * batch, 64): select(64, batch, batch > 64)
  f.emit(get(next), set(g), get(next), i32c(1), i32add, set(next));
  f.emit(get(g), get(lim), i32geu, IF, get(addr), i32c(-3), ATOMIC.store, f64c(1), RETURN, end);
  f.emit(get(g), i32c(nk * 8), i32mul, get(kb), i32add, set(kaddr));
  for (let k = 0; k < nk; k++) f.emit(get(kaddr), get(bits[k]), [0x37, 3, ...uleb(k * 8)]); // i64.store
  f.emit(get(addr), get(g), ATOMIC.store, br(3)); // publish; found
  f.emit(end, br(1), end); // lost the race: look at the slot again
  f.emit(get(s), i32c(-2), op(0x46), brIf(0)); // being filled: wait
  f.emit(get(s), i32c(-3), op(0x46), IF, f64c(1), RETURN, end);
  f.emit(get(s), i32c(nk * 8), i32mul, get(kb), i32add, set(kaddr));
  f.emit(block);
  for (let k = 0; k < nk; k++) f.emit(get(kaddr), [0x29, 3, ...uleb(k * 8)], get(bits[k]), op(0x52), brIf(0)); // i64.load, i64.ne
  f.emit(get(s), set(g), br(2)); // same key
  f.emit(end);
  f.emit(get(slot), i32c(1), i32add, get(mk), i32and, set(slot), br(0));
  f.emit(end, end);
  // group number out, next row
  f.emit(get(ids), get(g), t === 'f32' ? op(0xb3) : op(0xb8), ty.store);
  advance(f, ty, [...Array(nk).keys(), ids]);
  f.emit(br(0), end, end, f64c(0));
  return buildModule([f], shared);
}

function uleb(n) {
  const out = [];
  do { let b = n & 0x7f; n >>>= 7; if (n) b |= 0x80; out.push(b); } while (n);
  return out;
}

// Lookup in a table filled by generateHashGroups (no inserts): out[i] = the
// group number of row i's key, or -1 if no row of the build side has it. A
// key with a NaN part never matches (as NULL in SQL).
// Signature: (key ptrs..., out ptr (int32), n, slotsBase, mask, keysBase).
export function generateHashProbe(t, nk, shared = true) {
  const ty = TY[t];
  const out = nk, n = nk + 1, [sb, mk, kb] = [nk + 2, nk + 3, nk + 4];
  const f = new Fn('k', [...Array(nk).fill(I32), I32, I32, I32, I32, I32], []);
  const stop = f.local(I32), slot = f.local(I32), s = f.local(I32), g = f.local(I32), kaddr = f.local(I32);
  const v = f.local(F64), h = f.local(I64);
  const bits = Array.from({ length: nk }, () => f.local(I64));
  f.emit(get(n), i32c(4), i32mul, get(out), i32add, set(stop));
  f.emit(block, loop, get(out), get(stop), i32geu, brIf(1));
  keyHash(f, t, nk, bits, v, h, mk, slot);
  f.emit(i32c(-1), set(g));
  f.emit(block); // -> found or not
  for (let k = 0; k < nk; k++) f.emit(get(bits[k]), i64c(CANON_NAN), op(0x51), brIf(0)); // NaN: no match
  f.emit(loop);
  f.emit(get(slot), i32c(2), op(0x74), get(sb), i32add, [0x28, 2, 0], set(s)); // i32.load
  f.emit(get(s), i32c(-1), op(0x46), brIf(1)); // empty slot: not there
  f.emit(get(s), i32c(nk * 8), i32mul, get(kb), i32add, set(kaddr));
  f.emit(block);
  for (let k = 0; k < nk; k++) f.emit(get(kaddr), [0x29, 3, ...uleb(k * 8)], get(bits[k]), op(0x52), brIf(0));
  f.emit(get(s), set(g), br(2)); // same key
  f.emit(end);
  f.emit(get(slot), i32c(1), i32add, get(mk), i32and, set(slot), br(0));
  f.emit(end, end);
  f.emit(get(out), get(g), [0x36, 2, 0]); // i32.store
  for (let k = 0; k < nk; k++) f.emit(get(k), i32c(ty.bytes), i32add, set(k));
  f.emit(get(out), i32c(4), i32add, set(out), br(0), end, end);
  return buildModule([f], shared);
}
