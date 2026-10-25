// Code generators for table operations. Both are scalar loops (each row goes
// to a different place, which SIMD can't do), but they still fuse the filter
// and every aggregation into one pass over memory.

import { I32, F64, TY, Fn, buildModule, get, set, i32c, block, loop, end, br, brIf, i32add, i32geu } from './wasm.mjs';
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
export function generateGroupBy(items, pred, t, arrayNames, scalarNames, C = 1, shared = true) {
  const log2C = Math.log2(C);
  if (!Number.isInteger(log2C)) throw new Error('copies must be a power of two');
  const ty = TY[t];
  const A = arrayNames.length;
  const [key, n, accBase, K] = [A, A + 1, A + 2, A + 3];
  const f = new Fn('k', [...Array(A).fill(I32), I32, I32, I32, I32, ...Array(scalarNames.length).fill(F64)], []);
  const env = bindValues(f, t, arrayNames, A + 4, scalarNames);
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
