// Vectorized interpreter: runs expressions, group-by and filtered copies with
// only the precompiled kernels in static-kernels.mjs, for places that forbid
// generating code at runtime (Cloudflare Workers, Vercel Edge).
//
// An expression becomes a list of vector operations over registers. Rows are
// processed in blocks of CHUNK: every operation runs over the whole block with
// SIMD before the next one starts, so the cost of interpreting is paid once
// per block instead of once per row. Registers for intermediate results live
// in a scratch area small enough to stay in the CPU cache.

import { walk, isMask } from './fuse.mjs';

export const CHUNK = 4096;
const ARITH = { '+': 'add', '-': 'sub', '*': 'mul', '/': 'div' };
// Operations on constants are worked out once, while compiling (constant
// folding), with the same results the vector kernels would give.
const FOLD = {
  add: (a, b) => a + b, sub: (a, b) => a - b, mul: (a, b) => a * b, div: (a, b) => a / b,
  min: Math.min, max: Math.max, neg: (a) => -a, abs: Math.abs, sqrt: Math.sqrt, floor: Math.floor, ceil: Math.ceil,
  eq: (a, b) => +(a === b), ne: (a, b) => +(a !== b), lt: (a, b) => +(a < b), gt: (a, b) => +(a > b),
  le: (a, b) => +(a <= b), ge: (a, b) => +(a >= b), sel: (m, a, b) => (m !== 0 ? a : b),
};
const CMP = { '==': 'eq', '!=': 'ne', '<': 'lt', '>': 'gt', '<=': 'le', '>=': 'ge' };

// Registers: { k: 'col', name } a slice of a column (no copy) · { k: 'const', i }
// a block filled with one value · { k: 'tmp', i } an intermediate result.
class Program {
  constructor(t) {
    this.t = t;
    this.ops = [];       // { fn, dst, args }
    this.nTmp = 0;
    this.consts = [];    // { value } or { param }
    this.constIndex = new Map();
    this.memo = new Map(); // value numbering: the same op on the same inputs is computed once
  }

  #const(key, entry) {
    if (!this.constIndex.has(key)) { this.constIndex.set(key, this.consts.length); this.consts.push(entry); }
    return { k: 'const', i: this.constIndex.get(key) };
  }
  num(v) { return this.#const(`n:${Object.is(v, -0) ? '-0' : v}`, { value: v }); }
  #op(name, args) {
    if (args.every((r) => r.k === 'const' && 'value' in this.consts[r.i])) {
      const R = this.t === 'f32' ? Math.fround : (x) => x; // constants are stored at the column type
      return this.num(R(FOLD[name](...args.map((r) => R(this.consts[r.i].value)))));
    }
    const key = `${name}(${args.map((r) => (r.k === 'col' ? `c:${r.name}` : `${r.k[0]}${r.i}`)).join(',')})`;
    const seen = this.memo.get(key);
    if (seen) return seen;
    const dst = { k: 'tmp', i: this.nTmp++ };
    this.ops.push({ fn: `v${name}_${this.t}`, dst, args });
    this.memo.set(key, dst);
    return dst;
  }

  // true/false as 1/0: masks already are; numbers count as true when != 0
  truth(n, isColumn) {
    const r = this.emit(n, isColumn);
    return isMask(n) ? r : this.#op('ne', [r, this.num(0)]);
  }

  emit(n, isColumn) {
    switch (n.t) {
      case 'num': return this.num(n.v);
      case 'var': return isColumn(n.name) ? { k: 'col', name: n.name } : this.#const(`p:${n.name}`, { param: n.name });
      case 'un':
        return n.op === '-' ? this.#op('neg', [this.emit(n.x, isColumn)]) : this.#op('eq', [this.emit(n.x, isColumn), this.num(0)]);
      case 'bin':
        if (ARITH[n.op]) return this.#op(ARITH[n.op], [this.emit(n.l, isColumn), this.emit(n.r, isColumn)]);
        if (CMP[n.op]) return this.#op(CMP[n.op], [this.emit(n.l, isColumn), this.emit(n.r, isColumn)]);
        return this.#op(n.op === '&&' ? 'mul' : 'max', [this.truth(n.l, isColumn), this.truth(n.r, isColumn)]);
      case 'call':
        return this.#op(n.fn, n.args.map((a) => this.emit(a, isColumn)));
      case 'cond':
        return this.#op('sel', [this.emit(n.c, isColumn), this.emit(n.x, isColumn), this.emit(n.y, isColumn)]);
    }
    throw new Error(`vectro: can't interpret node ${n.t}`);
  }
}

// Runs a compiled program block by block. `cols` maps names to arrays in
// engine memory; `params` holds scalar values.
class Runner {
  static bytes(program) {
    const bpe = program.t === 'f32' ? 4 : 8;
    return CHUNK * Math.max(1, program.nTmp + program.consts.length) * bpe;
  }

  // `base`: where this program's registers go (default: the scratch area).
  constructor(engine, program, cols, params, n, base = null) {
    this.engine = engine;
    this.p = program;
    this.n = n;
    this.Type = program.t === 'f32' ? Float32Array : Float64Array;
    this.bpe = this.Type.BYTES_PER_ELEMENT;
    // Get scratch first: if it ever has to grow it moves array views.
    this.base = base ?? engine.interpreterScratch(Runner.bytes(program));
    this.tmpBase = this.base + program.consts.length * CHUNK * this.bpe;
    const mem = engine.memory.buffer;
    program.consts.forEach((c, i) => {
      const v = 'param' in c ? params[c.param] : c.value;
      new this.Type(mem, this.base + i * CHUNK * this.bpe, CHUNK).fill(v);
    });
    this.colPtr = new Map();
    for (const [name, arr] of Object.entries(cols)) {
      if (ArrayBuffer.isView(arr)) this.colPtr.set(name, engine.live(arr).byteOffset);
    }
    this.W = engine.kernels;
  }

  ptr(r, start) {
    if (r.k === 'col') return this.colPtr.get(r.name) + start * this.bpe;
    if (r.k === 'const') return this.base + r.i * CHUNK * this.bpe;
    if (r.k === 'out') return r.ptr + start * this.bpe;
    return this.tmpBase + r.i * CHUNK * this.bpe;
  }

  // Run every op for the block [start, start + len). `redirect` sends one
  // tmp register straight to an output array instead of scratch.
  block(start, len, redirect) {
    const W = this.W;
    for (const op of this.p.ops) {
      const dst = redirect && op.dst === redirect.reg ? redirect.out : op.dst;
      const d = this.ptr(dst, start);
      const a = op.args;
      if (a.length === 1) W[op.fn](d, this.ptr(a[0], start), len);
      else if (a.length === 2) W[op.fn](d, this.ptr(a[0], start), this.ptr(a[1], start), len);
      else W[op.fn](d, this.ptr(a[0], start), this.ptr(a[1], start), this.ptr(a[2], start), len);
    }
  }

  done() {}
}

const isColumnOf = (cols) => (name) => ArrayBuffer.isView(cols[name]);

// Reductions: items = [{ reduce: 'sum'|'count'|'mean'|'min'|'max', ast }].
// Returns one value per item (mean = sum / n).
export function interpretReduce(engine, t, items, cols, params, n) {
  const p = new Program(t);
  const isColumn = isColumnOf(cols);
  const regs = items.map((it) => p.emit(it.ast, isColumn));
  const run = new Runner(engine, p, cols, params, n);
  const W = engine.kernels;
  const sum = t === 'f32' ? W.vsumw_f32 : W.sum_f64;
  const acc = items.map((it) => (it.reduce === 'min' ? Infinity : it.reduce === 'max' ? -Infinity : 0));
  try {
    for (let start = 0; start < n; start += CHUNK) {
      const len = Math.min(CHUNK, n - start);
      run.block(start, len);
      items.forEach((it, j) => {
        const ptr = run.ptr(regs[j], start);
        if (it.reduce === 'min') acc[j] = Math.min(acc[j], W[`min_${t}`](ptr, len));
        else if (it.reduce === 'max') acc[j] = Math.max(acc[j], W[`max_${t}`](ptr, len));
        else acc[j] += sum(ptr, len);
      });
    }
  } finally {
    run.done();
  }
  return items.map((it, j) => (it.reduce === 'mean' ? acc[j] / n : acc[j]));
}

// Reductions under a filter. Per block the filter is evaluated first; then
//  - few rows match (< 25%): the matching rows of the columns the reductions
//    need are packed together (compact kernel) and reduced on their own
//    ("filter pushdown"), so the math only touches matching rows;
//  - many rows match: packing would cost more than it saves, so every row is
//    computed and non-matching ones are replaced by the reduction's identity
//    (sum(p ? e : 0), min(p ? e : Infinity), ...).
// items: [{ reduce, ast }]; ast null = count of matching rows.
export function interpretFilteredReduce(engine, t, pred, items, cols, params, n) {
  const isColumn = isColumnOf(cols);
  const A = new Program(t); // the filter
  const mask = A.truth(pred, isColumn);
  const B = new Program(t); // each value, over packed rows
  const packedRegs = items.map((it) => (it.ast ? B.emit(it.ast, isColumn) : null));
  // each value with the filter applied, over all rows; it reads the mask A
  // already computed through a pseudo-column (MASK) instead of redoing it
  const D = new Program(t);
  const MASK = '\0mask';
  const identity = { min: Infinity, max: -Infinity };
  const denseRegs = items.map((it) => (it.ast
    ? D.emit({ t: 'cond', c: { t: 'var', name: MASK }, x: it.ast, y: { t: 'num', v: identity[it.reduce] ?? 0 } }, (nm) => nm === MASK || isColumn(nm))
    : null));
  const used = [...new Set(items.flatMap((it) => (it.ast ? varsOf(it.ast) : [])))].filter(isColumn);

  const bpe = t === 'f32' ? 4 : 8;
  const [aBytes, bBytes, dBytes] = [Runner.bytes(A), Runner.bytes(B), Runner.bytes(D)];
  const base = engine.interpreterScratch(aBytes + bBytes + dBytes + Math.max(1, used.length) * CHUNK * bpe);
  const runA = new Runner(engine, A, cols, params, n, base);
  const runB = new Runner(engine, B, {}, params, n, base + aBytes);
  const runD = new Runner(engine, D, cols, params, n, base + aBytes + bBytes);
  const packed = base + aBytes + bBytes + dBytes;
  runB.colPtr = new Map(used.map((nm, k) => [nm, packed + k * CHUNK * bpe])); // packed rows start at 0
  const src = used.map((nm) => runA.colPtr.get(nm));

  const W = engine.kernels;
  const sum = t === 'f32' ? W.vsumw_f32 : W.sum_f64;
  const acc = items.map((it) => identity[it.reduce] ?? 0);
  const reduceInto = (j, it, ptr, len) => {
    if (it.reduce === 'min') acc[j] = Math.min(acc[j], W[`min_${t}`](ptr, len));
    else if (it.reduce === 'max') acc[j] = Math.max(acc[j], W[`max_${t}`](ptr, len));
    else acc[j] += sum(ptr, len);
  };
  let matched = 0;
  for (let start = 0; start < n; start += CHUNK) {
    const len = Math.min(CHUNK, n - start);
    runA.block(start, len);
    const m0 = runA.ptr(mask, start);
    const m = sum(m0, len); // the mask is 1 or 0 per row
    matched += m;
    if (m === 0) continue;
    if (m < len / 4) {
      used.forEach((_, k) => W[`compact_${t}`](m0, src[k] + start * bpe, packed + k * CHUNK * bpe, len));
      runB.block(0, m);
      items.forEach((it, j) => { if (it.ast) reduceInto(j, it, runB.ptr(packedRegs[j], 0), m); });
    } else {
      runD.colPtr.set(MASK, m0 - start * bpe); // the mask block, addressed like a column
      runD.block(start, len);
      items.forEach((it, j) => { if (it.ast) reduceInto(j, it, runD.ptr(denseRegs[j], start), len); });
    }
  }
  return items.map((it, j) => (!it.ast ? matched : it.reduce === 'mean' ? acc[j] / matched : acc[j]));
}

// Elementwise: out[i] = ast(row i).
export function interpretMap(engine, t, ast, cols, params, out, n) {
  const p = new Program(t);
  let reg = p.emit(ast, isColumnOf(cols));
  if (reg.k !== 'tmp') reg = p.emit({ t: 'bin', op: '*', l: ast, r: { t: 'num', v: 1 } }, isColumnOf(cols)); // a copy: x * 1 keeps -0 and NaN
  const run = new Runner(engine, p, cols, params, n);
  const outRef = { k: 'out', ptr: engine.live(out).byteOffset };
  try {
    for (let start = 0; start < n; start += CHUNK) run.block(start, Math.min(CHUNK, n - start), { reg, out: outRef });
  } finally {
    run.done();
  }
  return out;
}

// Group-by: for each row passing `pred`, acc[j*K + key] OP= item j's value.
export function interpretGroup(engine, t, items, pred, cols, params, key, acc, K, n) {
  const p = new Program(t);
  const isColumn = isColumnOf(cols);
  const regs = items.map((it) => p.emit(it.ast, isColumn));
  const mask = pred ? p.truth(pred, isColumn) : null;
  const run = new Runner(engine, p, cols, params, n);
  const W = engine.kernels;
  const keyPtr = engine.live(key).byteOffset, accPtr = engine.live(acc).byteOffset;
  const bpe = run.bpe;
  try {
    for (let start = 0; start < n; start += CHUNK) {
      const len = Math.min(CHUNK, n - start);
      run.block(start, len);
      items.forEach((it, j) => {
        const op = it.reduce === 'min' ? 'min' : it.reduce === 'max' ? 'max' : 'add';
        const accJ = accPtr + j * K * 8;
        if (mask) W[`g${op}m_${t}`](keyPtr + start * bpe, run.ptr(regs[j], start), run.ptr(mask, start), accJ, len);
        else W[`g${op}_${t}`](keyPtr + start * bpe, run.ptr(regs[j], start), accJ, len);
      });
    }
  } finally {
    run.done();
  }
}

// Filtered copy: returns the matching rows of each column in `names`.
export function interpretCompact(engine, t, pred, cols, params, names, n) {
  const Type = t === 'f32' ? Float32Array : Float64Array;
  const isColumn = isColumnOf(cols);
  const count = interpretReduce(engine, t, [{ reduce: 'count', ast: isMask(pred) ? pred : { t: 'bin', op: '!=', l: pred, r: { t: 'num', v: 0 } } }], cols, params, n)[0];
  const dst = names.map(() => engine.alloc(Type, count));
  const p = new Program(t);
  const mask = p.truth(pred, isColumn);
  const run = new Runner(engine, p, cols, params, n);
  const W = engine.kernels, bpe = run.bpe;
  const dstPtr = dst.map((d) => engine.live(d).byteOffset);
  const srcPtr = names.map((nm) => engine.live(cols[nm]).byteOffset);
  let pos = 0;
  try {
    for (let start = 0; start < n; start += CHUNK) {
      const len = Math.min(CHUNK, n - start);
      run.block(start, len);
      let wrote = 0;
      names.forEach((_, k) => { wrote = W[`compact_${t}`](run.ptr(mask, start), srcPtr[k] + start * bpe, dstPtr[k] + pos * bpe, len); });
      pos += wrote;
    }
  } finally {
    run.done();
  }
  return dst.map((d) => engine.live(d)); // scratch allocation may have moved memory
}

// Every column an expression refers to.
export function varsOf(ast) {
  const names = [];
  walk(ast, (m) => { if (m.t === 'var' && !names.includes(m.name)) names.push(m.name); });
  return names;
}
