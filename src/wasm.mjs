// Generates the SIMD kernels as a WebAssembly binary at startup, so the
// package needs no compiler toolchain. Only the handful of instructions the
// kernels use are encoded here.

export const I32 = 0x7f, F32 = 0x7d, F64 = 0x7c, V128 = 0x7b;

function uleb(n) {
  const out = [];
  do {
    let b = n & 0x7f;
    n >>>= 7;
    if (n) b |= 0x80;
    out.push(b);
  } while (n);
  return out;
}

function sleb(n) {
  const out = [];
  for (;;) {
    const b = n & 0x7f;
    n >>= 7;
    if ((n === 0 && !(b & 0x40)) || (n === -1 && b & 0x40)) { out.push(b); return out; }
    out.push(b | 0x80);
  }
}

const bytesOf = (Type, v) => [...new Uint8Array(new Type([v]).buffer)];
const name = (s) => { const b = [...new TextEncoder().encode(s)]; return [...uleb(b.length), ...b]; };
const vec = (items) => [...uleb(items.length), ...items.flat()];
const section = (id, content) => [id, ...uleb(content.length), ...content];

// ---- instructions -----------------------------------------------------------
export const get = (i) => [0x20, ...uleb(i)];
export const set = (i) => [0x21, ...uleb(i)];
export const i32c = (v) => [0x41, ...sleb(v)];
export const block = [0x02, 0x40], loop = [0x03, 0x40], end = [0x0b];
export const br = (l) => [0x0c, ...uleb(l)];
export const brIf = (l) => [0x0d, ...uleb(l)];
export const i32add = [0x6a], i32mul = [0x6c], i32and = [0x71], i32geu = [0x4f], i32wrapI64 = [0xa7];
export const simd = (code, ...imm) => [0xfd, ...uleb(code), ...imm];
export const memarg = (align, offset) => [...uleb(align), ...uleb(offset)];
export const vload = (off) => simd(0x00, ...memarg(0, off));
export const vstore = (off) => simd(0x0b, ...memarg(0, off));

// Per element type: scalar opcodes and their SIMD (128-bit) equivalents.
export const TY = {
  f64: {
    val: F64, bytes: 8, lanes: 2, cnst: (v) => [0x44, ...bytesOf(Float64Array, v)],
    load: [0x2b, ...memarg(3, 0)], store: [0x39, ...memarg(3, 0)],
    add: [0xa0], mul: [0xa2], min: [0xa4], max: [0xa5], gt: [0x64], toF64: [],
    vsplat: simd(0x14), vlane: (l) => simd(0x21, l),
    vadd: simd(0xf0), vmul: simd(0xf2), vmin: simd(0xf4), vmax: simd(0xf5), vgt: simd(0x4a),
    // comparison masks are 64-bit integer lanes
    mlanes: 2, msub: simd(0xd1), madd: simd(0xce), mlane: (l) => [...simd(0x1d, l), ...i32wrapI64],
  },
  f32: {
    val: F32, bytes: 4, lanes: 4, cnst: (v) => [0x43, ...bytesOf(Float32Array, v)],
    load: [0x2a, ...memarg(2, 0)], store: [0x38, ...memarg(2, 0)],
    add: [0x92], mul: [0x94], min: [0x96], max: [0x97], gt: [0x5e], toF64: [0xbb],
    vsplat: simd(0x13), vlane: (l) => simd(0x1f, l),
    vadd: simd(0xe4), vmul: simd(0xe6), vmin: simd(0xe8), vmax: simd(0xe9), vgt: simd(0x44),
    mlanes: 4, msub: simd(0xb1), madd: simd(0xae), mlane: (l) => simd(0x1b, l),
  },
};

export class Fn {
  constructor(exportName, params, results) {
    this.exportName = exportName;
    this.params = params;
    this.results = results;
    this.locals = [];
    this.code = [];
  }
  local(type) { this.locals.push(type); return this.params.length + this.locals.length - 1; }
  emit(...parts) { for (const p of parts) this.code.push(...p); }
}

// Loop skeleton shared by every kernel. `ptrs` advance together; the main loop
// handles U vectors (U*16 bytes) per iteration, then a scalar loop does the
// leftover elements.
export function loops(f, ty, { ptrs, n, U, body, between = () => {}, tail }) {
  const vend = f.local(I32), stop = f.local(I32);
  const perIter = ty.lanes * U;
  f.emit(get(n), i32c(~(perIter - 1)), i32and, i32c(ty.bytes), i32mul, get(ptrs[0]), i32add, set(vend));
  f.emit(get(n), i32c(ty.bytes), i32mul, get(ptrs[0]), i32add, set(stop));
  f.emit(block, loop, get(ptrs[0]), get(vend), i32geu, brIf(1));
  body();
  for (const p of ptrs) f.emit(get(p), i32c(16 * U), i32add, set(p));
  f.emit(br(0), end, end);
  between();
  f.emit(block, loop, get(ptrs[0]), get(stop), i32geu, brIf(1));
  tail();
  for (const p of ptrs) f.emit(get(p), i32c(ty.bytes), i32add, set(p));
  f.emit(br(0), end, end);
}

// sum / min / max with 4 independent vector accumulators, so the CPU can run
// several additions at once instead of waiting on each one.
function reduction(t, kind) {
  const ty = TY[t];
  const f = new Fn(`${kind}_${t}`, [I32, I32], [F64]);
  const [p, n] = [0, 1];
  const U = 4;
  const acc = Array.from({ length: U }, () => f.local(V128));
  const s = f.local(ty.val);
  const identity = kind === 'sum' ? 0 : kind === 'min' ? Infinity : -Infinity;
  const vop = ty[{ sum: 'vadd', min: 'vmin', max: 'vmax' }[kind]];
  const sop = ty[{ sum: 'add', min: 'min', max: 'max' }[kind]];
  for (const a of acc) f.emit(ty.cnst(identity), ty.vsplat, set(a));
  loops(f, ty, {
    ptrs: [p], n, U,
    body: () => acc.forEach((a, k) => f.emit(get(a), get(p), vload(16 * k), vop, set(a))),
    between: () => {
      f.emit(get(acc[0]), get(acc[1]), vop, get(acc[2]), get(acc[3]), vop, vop, set(acc[0]));
      f.emit(get(acc[0]), ty.vlane(0));
      for (let l = 1; l < ty.lanes; l++) f.emit(get(acc[0]), ty.vlane(l), sop);
      f.emit(set(s));
    },
    tail: () => f.emit(get(s), get(p), ty.load, sop, set(s)),
  });
  f.emit(get(s), ty.toF64);
  return f;
}

function dot(t) {
  const ty = TY[t];
  const f = new Fn(`dot_${t}`, [I32, I32, I32], [F64]);
  const [pa, pb, n] = [0, 1, 2];
  const U = 4;
  const acc = Array.from({ length: U }, () => f.local(V128));
  const s = f.local(ty.val);
  for (const a of acc) f.emit(ty.cnst(0), ty.vsplat, set(a));
  loops(f, ty, {
    ptrs: [pa, pb], n, U,
    body: () => acc.forEach((a, k) => f.emit(get(a), get(pa), vload(16 * k), get(pb), vload(16 * k), ty.vmul, ty.vadd, set(a))),
    between: () => {
      f.emit(get(acc[0]), get(acc[1]), ty.vadd, get(acc[2]), get(acc[3]), ty.vadd, ty.vadd, set(acc[0]));
      f.emit(get(acc[0]), ty.vlane(0));
      for (let l = 1; l < ty.lanes; l++) f.emit(get(acc[0]), ty.vlane(l), ty.add);
      f.emit(set(s));
    },
    tail: () => f.emit(get(s), get(pa), ty.load, get(pb), ty.load, ty.mul, ty.add, set(s)),
  });
  f.emit(get(s), ty.toF64);
  return f;
}

// Count elements > x. A SIMD compare gives all-ones (-1) per matching lane,
// so subtracting the mask adds 1 to each lane's counter.
function countGreater(t) {
  const ty = TY[t];
  const f = new Fn(`countgt_${t}`, [I32, I32, ty.val], [I32]);
  const [p, n, x] = [0, 1, 2];
  const U = 4;
  const vx = f.local(V128), c = f.local(I32);
  const acc = Array.from({ length: U }, () => f.local(V128));
  f.emit(get(x), ty.vsplat, set(vx));
  for (const a of acc) f.emit(i32c(0), simd(0x11), set(a)); // i32x4.splat(0) = all-zero vector
  loops(f, ty, {
    ptrs: [p], n, U,
    body: () => acc.forEach((a, k) => f.emit(get(a), get(p), vload(16 * k), get(vx), ty.vgt, ty.msub, set(a))),
    between: () => {
      f.emit(get(acc[0]), get(acc[1]), ty.madd, get(acc[2]), get(acc[3]), ty.madd, ty.madd, set(acc[0]));
      f.emit(get(acc[0]), ty.mlane(0));
      for (let l = 1; l < ty.mlanes; l++) f.emit(get(acc[0]), ty.mlane(l), i32add);
      f.emit(set(c));
    },
    tail: () => f.emit(get(c), get(p), ty.load, get(x), ty.gt, i32add, set(c)),
  });
  f.emit(get(c));
  return f;
}

// out = a + b
function add(t) {
  const ty = TY[t];
  const f = new Fn(`add_${t}`, [I32, I32, I32, I32], []);
  const [pa, pb, po, n] = [0, 1, 2, 3];
  const U = 2;
  loops(f, ty, {
    ptrs: [pa, pb, po], n, U,
    body: () => { for (let k = 0; k < U; k++) f.emit(get(po), get(pa), vload(16 * k), get(pb), vload(16 * k), ty.vadd, vstore(16 * k)); },
    tail: () => f.emit(get(po), get(pa), ty.load, get(pb), ty.load, ty.add, ty.store),
  });
  return f;
}

// out = a * k + c
function scale(t) {
  const ty = TY[t];
  const f = new Fn(`scale_${t}`, [I32, I32, I32, ty.val, ty.val], []);
  const [pa, po, n, k, c] = [0, 1, 2, 3, 4];
  const U = 2;
  const vk = f.local(V128), vc = f.local(V128);
  f.emit(get(k), ty.vsplat, set(vk), get(c), ty.vsplat, set(vc));
  loops(f, ty, {
    ptrs: [pa, po], n, U,
    body: () => { for (let j = 0; j < U; j++) f.emit(get(po), get(pa), vload(16 * j), get(vk), ty.vmul, get(vc), ty.vadd, vstore(16 * j)); },
    tail: () => f.emit(get(po), get(pa), ty.load, get(k), ty.mul, get(c), ty.add, ty.store),
  });
  return f;
}

// float32 sums are left to JS: a 4-lane float32 accumulator loses too much
// precision over millions of values (it would need pairwise summation).
export function kernelFns() {
  return [
    reduction('f64', 'sum'), reduction('f64', 'min'), reduction('f64', 'max'),
    reduction('f32', 'min'), reduction('f32', 'max'),
    dot('f64'), countGreater('f64'), countGreater('f32'),
    add('f64'), add('f32'), scale('f64'), scale('f32'),
  ];
}

export function buildKernels(shared = true) {
  return buildModule(kernelFns(), shared);
}

// Assemble functions into a module that imports env.memory: shared memory
// when threads use it, plain memory for single-thread and edge builds.
export function buildModule(fns, shared = true) {
  const types = [];
  const typeIndex = fns.map((f) => {
    const sig = [0x60, ...vec(f.params.map((x) => [x])), ...vec(f.results.map((x) => [x]))];
    let i = types.findIndex((t) => t.join() === sig.join());
    if (i < 0) { types.push(sig); i = types.length - 1; }
    return i;
  });

  const importMemory = [...name('env'), ...name('memory'), 0x02, shared ? 0x03 : 0x01, ...uleb(1), ...uleb(65536)]; // max 4GB
  const bodies = fns.map((f) => {
    const groups = [];
    for (const t of f.locals) {
      const last = groups[groups.length - 1];
      if (last && last[1] === t) last[0]++;
      else groups.push([1, t]);
    }
    const body = [...vec(groups.map(([count, t]) => [...uleb(count), t])), ...f.code, ...end];
    return [...uleb(body.length), ...body];
  });

  return new Uint8Array([
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
    ...section(1, vec(types)),
    ...section(2, vec([importMemory])),
    ...section(3, vec(typeIndex.map((i) => uleb(i)))),
    ...section(7, vec(fns.map((f, i) => [...name(f.exportName), 0x00, ...uleb(i)]))),
    ...section(10, vec(bodies)),
  ]);
}
