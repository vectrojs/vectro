// The engine core. It imports nothing platform-specific: threads are a
// plug-in (`platform`, provided by node.mjs), and without one the engine runs
// single-threaded on plain (non-shared) memory, which works in browsers
// without cross-origin isolation and on edge runtimes.
import { C, OP, MAX_ARGS, MAX_OUTPUTS, TYPES, WASM_MEMORY, SEQ_WRITING, TAG_SHIFT, runUnits, rangeKernel, hasWasmKernel, unitCount, maxUnits, tagOf } from './kernels.mjs';
import { buildStaticModule } from './static-kernels.mjs';
import { analyze, generate } from './fuse.mjs';
import { interpretReduce, interpretMap } from './interp.mjs';
import { Table } from './table.mjs';
import { decodeOne } from './pack.mjs';

const COMPRESS_ALL = !!globalThis.process?.env?.VECTRO_COMPRESS; // tests: pack every table
const POISON = !!globalThis.process?.env?.VECTRO_POISON; // tests: freed memory overwritten (a read after free gives wrong answers)
import { parseCSV, parseCSVAsync } from './csv.mjs';
import { readArrow } from './arrow.mjs';
import { encode } from './bytes.mjs';

// Below these sizes the work is done on the calling thread: waking other
// threads would cost more than it saves. Tuned on an M1 Pro; see bench.mjs.
const DEFAULT_MIN_PARALLEL = {
  [OP.SUM]: 100_000, [OP.MIN]: 100_000, [OP.MAX]: 100_000, [OP.DOT]: 100_000,
  [OP.ADD]: 50_000, [OP.SCALE]: 50_000, [OP.COUNT_GT]: 100_000,
  [OP.HEAVY]: 5_000, [OP.SORT_CHUNK]: 50_000, [OP.COUNT_BYTES]: 500_000,
};
// WASM SIMD kernels are ~5-20x faster per thread, so it takes more data
// before splitting it up is worth it.
const DEFAULT_MIN_PARALLEL_WASM = {
  [OP.SUM]: 200_000, [OP.MIN]: 200_000, [OP.MAX]: 200_000, [OP.DOT]: 200_000,
  [OP.COUNT_GT]: 200_000, [OP.ADD]: 100_000, [OP.SCALE]: 100_000, [OP.FUSED]: 100_000,
};

const PAGE = 65536;
const MAX_PAGES = 65536; // 4GB, the most a 32-bit WASM address can reach
const HEAP_START = 64;   // keep address 0 unused
const align16 = (n) => Math.ceil(n / 16) * 16; // (not bitwise: sizes may pass 2^31 bytes, and must not wrap)

// Can this runtime compile WebAssembly from bytes? (Cloudflare Workers and
// Vercel Edge can't: they only run modules bundled ahead of time.)
let compileAllowed;
export function canCompileWasm() {
  if (compileAllowed === undefined) {
    try {
      new WebAssembly.Module(new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]));
      compileAllowed = true;
    } catch {
      compileAllowed = false;
    }
  }
  return compileAllowed;
}

export class Engine {
  // options:
  //   threads     how many threads (needs a platform with workers; default: all cores)
  //   platform    thread support, provided by the Node entry (node.mjs)
  //   jit         true: compile a kernel per expression (fastest);
  //               false: run expressions with the vectorized interpreter;
  //               'auto' (default): jit wherever the runtime allows it
  //   wasmModule  the precompiled dist/vectro.wasm, for runtimes that can't
  //               compile WebAssembly (Cloudflare Workers)
  //   initialMemoryMB  memory to start with (single-thread default 16): plain
  //               memory moves when it grows, so starting bigger avoids that
  constructor({ threads, spin = 20_000, minParallel, wasm = true, jit = 'auto', wasmModule = null, platform = null, initialMemoryMB } = {}) {
    const canThread = Boolean(platform) && typeof SharedArrayBuffer === 'function';
    this.T = canThread ? Math.max(1, threads ?? platform.cpus()) : 1;
    this.W = this.T - 1; // the calling thread does part of the work itself
    this.isShared = this.W > 0;
    this.platform = platform;
    // Browsers don't let the page's main thread block (Atomics.wait throws
    // there). With such a platform, sync calls run on the calling thread
    // alone and only the async API uses the workers.
    this.canBlock = platform?.canBlock !== false;
    this.spin = spin;
    this.minParallel = minParallel ?? DEFAULT_MIN_PARALLEL;
    this.minParallelWasm = minParallel ?? DEFAULT_MIN_PARALLEL_WASM;
    this.useWasm = wasm;

    const compile = canCompileWasm();
    this.jit = jit === 'auto' ? compile : jit;
    if (this.jit && !compile) throw new Error('vectro: this runtime cannot compile WebAssembly, so jit must be false');
    if (!wasmModule && !compile) {
      throw new Error("vectro: this runtime cannot compile WebAssembly; pass the precompiled module: new Engine({ wasmModule }) with dist/vectro.wasm");
    }
    if (wasmModule && this.isShared) throw new Error('vectro: a precompiled wasmModule is single-threaded; use threads: 1');

    // One WebAssembly memory; arrays are allocated in it. Shared between
    // threads when there are workers, plain memory otherwise.
    const Buf = this.isShared ? SharedArrayBuffer : ArrayBuffer;
    const initialPages = Math.max(1, Math.ceil(((initialMemoryMB ?? (this.isShared ? 0 : 16)) * 2 ** 20) / PAGE));
    this.memory = new WebAssembly.Memory({ initial: initialPages, maximum: MAX_PAGES, shared: this.isShared });
    this.module = wasmModule ?? new WebAssembly.Module(buildStaticModule(this.isShared));
    this.kernels = new WebAssembly.Instance(this.module, { env: { memory: this.memory } }).exports;
    this.memBuffers = new WeakSet(); // every buffer object handed out for this memory
    this.allocInfo = new WeakMap(); // array -> { ptr, n, Type }, to rebuild views (see live())
    this.heapTop = HEAP_START;
    this.freeList = []; // [{ ptr, size }] sorted by ptr
    this.sizes = new Map(); // ptr -> allocated size

    this.ctrl = new Int32Array(new Buf(4 * (C.ARGS + MAX_ARGS * 4)));
    this.ctrlU = new Uint32Array(this.ctrl.buffer);
    this.sc = new Float64Array(new Buf(8 * 32));
    this.res = new Float64Array(new Buf(8 * maxUnits(this.T) * MAX_OUTPUTS)); // result slots per unit of work
    this.seq = 0;
    this.ids = new WeakMap();
    this.nextId = WASM_MEMORY + 1;
    this.ports = [];
    this.workers = [];
    this.scratch = null;
    this.patBuf = null;
    this.compiled = new Map(); // spec -> Kernel
    this.nextKernelId = 0;
    this.interpScratch = null; // the interpreter's registers (see interpreterScratch)
    if (!this.jit) this.interpreterScratch(64 * 4096 * 8);

    for (let i = 1; i <= this.W; i++) {
      const { worker, port } = platform.spawn({
        ctrlBuf: this.ctrl.buffer, scalarBuf: this.sc.buffer, resultBuf: this.res.buffer,
        spin, index: i, module: this.module, memory: this.memory,
      });
      this.ports.push(port);
      this.workers.push(worker);
    }
  }

  #received(port) {
    const out = [];
    let m;
    while ((m = this.platform.receive(port))) out.push(m.message);
    return out;
  }

  // ---- memory -------------------------------------------------------------

  #memoryBuffer() {
    const b = this.memory.buffer;
    this.memBuffers.add(b);
    return b;
  }

  // Allocate a typed array in the engine's memory. These arrays get the
  // WASM SIMD kernels; other shared arrays fall back to JS kernels.
  alloc(Type, n) {
    const size = align16(Math.max(16, n * Type.BYTES_PER_ELEMENT));
    // too big for the 4GB memory (a huge join output, say): fail before touching the free list
    if (!(size <= MAX_PAGES * PAGE - 64)) throw new RangeError('vectro: out of memory (4GB limit)');
    let ptr = -1;
    for (let i = 0; i < this.freeList.length; i++) {
      const blk = this.freeList[i];
      if (blk.size < size) continue;
      ptr = blk.ptr;
      if (blk.size === size) this.freeList.splice(i, 1);
      else { blk.ptr += size; blk.size -= size; }
      break;
    }
    if (ptr < 0) {
      ptr = this.heapTop;
      const top = ptr + size;
      const have = this.memory.buffer.byteLength;
      if (top > have) {
        const havePages = have / PAGE;
        let pages = Math.ceil((top - have) / PAGE);
        // Plain memory moves when it grows (old views go stale), so grow in
        // big steps to make that rare.
        if (!this.isShared) pages = Math.max(pages, Math.min(MAX_PAGES - havePages, Math.max(256, havePages >> 1)));
        // (nothing may end at the 4GB address itself: kernels' end pointers would wrap to 0)
        if (havePages + pages > MAX_PAGES || top > MAX_PAGES * PAGE - 64) throw new RangeError('vectro: out of memory (4GB limit)');
        this.memory.grow(pages);
      }
      this.heapTop = top;
    }
    this.sizes.set(ptr, size);
    const view = new Type(this.#memoryBuffer(), ptr, n);
    this.allocInfo.set(view, { ptr, n, Type });
    return view;
  }

  // Byte offset of a scratch area of at least `bytes` for the interpreter's
  // registers. Reserved once (and only regrown for unusually big
  // expressions), so running queries doesn't grow memory.
  interpreterScratch(bytes) {
    if (!this.interpScratch || this.interpScratch.length < bytes) {
      if (this.interpScratch) this.free(this.interpScratch);
      this.interpScratch = this.alloc(Uint8Array, bytes);
    }
    return this.live(this.interpScratch).byteOffset;
  }

  // Plain (non-shared) WebAssembly memory gets a new buffer whenever it
  // grows, and views of the old one become empty. live() returns a working
  // view of the same array; every engine method calls it on its inputs, so
  // stale arrays can still be passed in. Shared memory never goes stale.
  live(ta) {
    const info = this.allocInfo.get(ta);
    const stale = ta.buffer.detached ?? (ta.byteLength === 0 && (info?.n ?? 0) > 0);
    if (!stale) return ta;
    if (!info) throw new Error('vectro: this array is stale (engine memory grew); use engine.live(array) on the array engine.alloc() returned');
    const fresh = new info.Type(this.#memoryBuffer(), info.ptr, info.n);
    this.allocInfo.set(fresh, info);
    return fresh;
  }

  free(ta) {
    const ptr = this.allocInfo.get(ta)?.ptr ?? ta.byteOffset;
    const size = this.sizes.get(ptr);
    if (size === undefined || !this.memBuffers.has(ta.buffer)) throw new Error('vectro: free() of an array not allocated by this engine');
    this.sizes.delete(ptr);
    if (POISON) new Uint8Array(this.memory.buffer, ptr, size).fill(0xff);
    let i = 0;
    while (i < this.freeList.length && this.freeList[i].ptr < ptr) i++;
    this.freeList.splice(i, 0, { ptr, size });
    // merge with neighbours
    for (const j of [i, i - 1]) {
      const a = this.freeList[j], b = this.freeList[j + 1];
      if (a && b && a.ptr + a.size === b.ptr) { a.size += b.size; this.freeList.splice(j + 1, 1); }
    }
    const last = this.freeList[this.freeList.length - 1];
    if (last && last.ptr + last.size === this.heapTop) { this.heapTop = last.ptr; this.freeList.pop(); }
  }

  shared(Type, n) { return this.alloc(Type, n); }

  // Copy a normal typed array into engine memory (costs a full copy).
  toShared(ta) {
    const out = this.alloc(ta.constructor, ta.length);
    out.set(ta);
    return out;
  }

  #inMemory(ta) { return this.memBuffers.has(ta.buffer); }
  _inMemory(ta) { return this.#inMemory(ta); }

  #bufferId(ta) {
    const buf = ta.buffer;
    if (this.memBuffers.has(buf)) return WASM_MEMORY;
    if (typeof SharedArrayBuffer !== 'function' || !(buf instanceof SharedArrayBuffer)) {
      throw new TypeError('vectro: array must be shared memory (use engine.alloc() or engine.toShared())');
    }
    let id = this.ids.get(buf);
    if (id === undefined) {
      id = this.nextId++;
      this.ids.set(buf, id);
      for (const p of this.ports) p.postMessage({ id, buffer: buf });
    }
    return id;
  }

  // ---- dispatch -----------------------------------------------------------

  #wasmFor(op, arrays) {
    return this.useWasm && hasWasmKernel(op, arrays) && arrays.every((a) => !a || this.#inMemory(a)) ? this.kernels : null;
  }

  // Write a task into the control block and wake the workers.
  #publish(op, n, arrays, W) {
    const ctrl = this.ctrl, u = this.ctrlU;
    // Seqlock: mark the task as being rewritten first. A worker still reading
    // the previous task sees SEQ change and drops what it read.
    Atomics.store(ctrl, C.SEQ, SEQ_WRITING);
    for (let k = 0; k < (this.W ? MAX_ARGS : 0); k++) { // workers read the arrays from here
      const base = C.ARGS + k * 4;
      const ta = arrays[k];
      if (!ta) { ctrl[base] = -1; continue; }
      ctrl[base] = this.#bufferId(ta);
      u[base + 1] = ta.byteOffset;
      u[base + 2] = ta.length;
      u[base + 3] = TYPES.findIndex((T) => ta instanceof T); // Buffer counts as Uint8Array
    }
    ctrl[C.OP] = op;
    ctrl[C.N] = n;
    ctrl[C.THREADS] = this.T;
    ctrl[C.WASM] = W ? 1 : 0;
    const tag = tagOf(++this.seq);
    const units = unitCount(op, this.T, n, this.sc);
    Atomics.store(ctrl, C.DONE, 0);
    Atomics.store(ctrl, C.ERROR, 0);
    Atomics.store(ctrl, C.NEXT, tag << TAG_SHIFT);
    Atomics.store(ctrl, C.SEQ, this.seq); // publishes everything written above
    Atomics.notify(ctrl, C.SEQ);
    return { tag, units };
  }

  #results(op, units, mainError) {
    if (Atomics.load(this.ctrl, C.ERROR)) this.#throwWorkerError(mainError);
    return this.res.subarray(0, units * Math.max(1, op === OP.FUSED ? this.ctrl[C.NOUT] : 1));
  }

  // Run `op` on all threads and block until done; returns the per-unit results.
  #run(op, n, arrays, W = null, F = null) {
    this.#assertIdle();
    arrays = arrays.map((a) => a && this.live(a));
    if (this.W && !this.canBlock) return this.#runHere(op, n, arrays, W, F);
    const { tag, units } = this.#publish(op, n, arrays, W);

    // The calling thread starts working right away; whichever workers wake up
    // in time help. We wait only for unfinished units, never for idle threads.
    let mainError = null;
    runUnits(op, this.T, n, arrays, this.sc, this.ctrl, W, this.res, tag, F, 0, (err) => { mainError ??= err; });

    const ctrl = this.ctrl;
    let d = Atomics.load(ctrl, C.DONE);
    for (let k = 0; d < units && k < this.spin * 10; k++) d = Atomics.load(ctrl, C.DONE);
    while (d < units) {
      Atomics.wait(ctrl, C.DONE, d);
      d = Atomics.load(ctrl, C.DONE);
    }
    return this.#results(op, units, mainError);
  }

  // Run every unit of `op` on the calling thread, without waking the workers
  // (SEQ is not changed, so they don't see a task). For hosts that can't block.
  #runHere(op, n, arrays, W, F) {
    const ctrl = this.ctrl;
    ctrl[C.OP] = op;
    ctrl[C.N] = n;
    ctrl[C.THREADS] = this.T;
    const tag = tagOf(++this.seq); // a fresh tag: late workers can't claim these units
    Atomics.store(ctrl, C.DONE, 0);
    Atomics.store(ctrl, C.ERROR, 0);
    Atomics.store(ctrl, C.NEXT, tag << TAG_SHIFT);
    // (workers don't run this task: put back the last published SEQ so idle
    // ones keep sleeping; a late one sees the fresh tag and skips the fields)
    if (this.W && Atomics.load(ctrl, C.SEQ) === SEQ_WRITING) Atomics.store(ctrl, C.SEQ, this.#prevSeq);
    let mainError = null;
    runUnits(op, this.T, n, arrays, this.sc, ctrl, W, this.res, tag, F, 0, (err) => { mainError ??= err; });
    if (mainError) throw mainError;
    return this.#results(op, unitCount(op, this.T, n, this.sc), null);
  }

  // Run `op` on the workers without blocking: the calling thread only waits
  // (Atomics.waitAsync), so its event loop keeps serving other work. Without
  // workers there is nothing to hand off, so this runs like #run.
  async #runAsync(op, n, arrays, W = null, F = null) {
    if (this.W === 0) return this.#run(op, n, arrays, W, F);
    arrays = arrays.map((a) => a && this.live(a));
    const { units } = this.#publish(op, n, arrays, W);
    const ctrl = this.ctrl;
    this.#inFlight = true;
    // Atomics.waitAsync doesn't keep the event loop alive (and the workers
    // are unref'd so scripts can exit), so hold a timer open while we wait.
    const keepAlive = setTimeout(() => {}, 2 ** 31 - 1);
    try {
      let d = Atomics.load(ctrl, C.DONE);
      while (d < units) {
        if (typeof Atomics.waitAsync === 'function') {
          const w = Atomics.waitAsync(ctrl, C.DONE, d);
          if (w.async) await w.value;
        } else {
          await new Promise((r) => setTimeout(r, 0)); // browsers without waitAsync: poll
        }
        d = Atomics.load(ctrl, C.DONE);
      }
    } finally {
      this.#inFlight = false;
      clearTimeout(keepAlive);
    }
    // A worker failed: in browsers its message may still be on its way.
    if (Atomics.load(ctrl, C.ERROR) && this.platform?.nextMessage) await this.#awaitWorkerError();
    return this.#results(op, units, null);
  }

  async #awaitWorkerError() {
    const deadline = Date.now() + 2000;
    for (;;) {
      for (const p of this.ports) {
        for (const m of this.#received(p)) {
          if (m.kind === 'error') { Atomics.store(this.ctrl, C.ERROR, 0); throw new Error(`vectro: a worker thread failed:\n${m.message}`); }
        }
      }
      if (Date.now() > deadline) return; // #results reports a generic failure
      await Promise.race([this.platform.nextMessage(this.ports), new Promise((r) => setTimeout(r, 50))]);
    }
  }

  // ---- sync and async drivers ------------------------------------------------
  //
  // Operations are written once, as generators that yield "run this on the
  // threads" requests: { op, n, arrays, W?, F?, setup?(), after?(parts) }.
  // _drive runs them blocking; _driveAsync awaits each run, one query at a
  // time (a queue), so several awaited queries can be in flight safely.

  #inFlight = false; // an async run is on the workers right now
  #queue = Promise.resolve();

  // True while an async query is running on the worker threads.
  get busy() { return this.#inFlight; }

  // The control block and scalar slots belong to the running task; touching
  // them while an async run is on the workers would corrupt it.
  #assertIdle() {
    if (this.#inFlight) {
      throw new Error('vectro: an async query is running on this engine; await it, or use the async API (e.g. aggAsync) for this call too');
    }
  }

  #exec1(req, parts) {
    return req.after ? req.after(parts) : Float64Array.from(parts); // results are copied out before the next run
  }

  // Before a request's setup() writes the control block (kernel, scalars...),
  // mark it as being rewritten: a worker still reading the previous task
  // must not take the new fields for it (the seqlock in #publish).
  #prevSeq = 0;
  #beginWrite() {
    if (!this.W) return;
    const s = Atomics.load(this.ctrl, C.SEQ);
    if (s !== SEQ_WRITING) this.#prevSeq = s;
    Atomics.store(this.ctrl, C.SEQ, SEQ_WRITING);
  }

  _execSync(req) {
    this.#assertIdle(); // before setup(), which writes the shared control block
    this.#beginWrite();
    req.setup?.();
    return this.#exec1(req, this.#run(req.op, req.n, req.arrays, req.W ?? null, req.F ?? null));
  }

  async _execAsync(req) {
    const before = this.#queue;
    let release;
    this.#queue = new Promise((r) => { release = r; });
    await before;
    try {
      this.#beginWrite();
      req.setup?.();
      return await this.#exec1(req, await this.#runAsync(req.op, req.n, req.arrays, req.W ?? null, req.F ?? null));
    } finally {
      release();
    }
  }

  _drive(gen) {
    let step = gen.next();
    while (!step.done) {
      let out;
      try {
        out = this._execSync(step.value);
      } catch (err) {
        step = gen.throw(err);
        continue;
      }
      step = gen.next(out);
    }
    return step.value;
  }

  async _driveAsync(gen) {
    // One thread: nothing runs in the background, so run the whole operation
    // at once (other operations can't slip in between its steps).
    if (this.W === 0) return this._drive(gen);
    let step = gen.next();
    while (!step.done) {
      let out;
      try {
        out = await this._execAsync(step.value);
      } catch (err) {
        step = gen.throw(err);
        continue;
      }
      step = gen.next(out);
    }
    return step.value;
  }

  // A unit failed somewhere. Every unit has finished (so nothing is still
  // running); report the first error. Other messages are dropped.
  #throwWorkerError(mainError) {
    Atomics.store(this.ctrl, C.ERROR, 0);
    if (mainError) throw mainError;
    for (const p of this.ports) {
      for (const m of this.#received(p)) {
        if (m.kind === 'error') throw new Error(`vectro: a worker thread failed:\n${m.message}`);
      }
    }
    throw new Error('vectro: a worker thread failed');
  }

  #serial(op, n, W = null) {
    return this.W === 0 || n < ((W ? this.minParallelWasm : this.minParallel)[op] ?? 0);
  }

  #exec(op, n, arrays) {
    arrays = arrays.map((a) => a && this.live(a));
    if (n === undefined) n = arrays[0].length;
    const W = this.#wasmFor(op, arrays);
    if (this.#serial(op, n, W)) return [rangeKernel(op, 0, n, arrays, this.sc, W)];
    return this.#run(op, n, arrays, W);
  }

  // ---- operations ---------------------------------------------------------

  sum(a) { return reduce(this.#exec(OP.SUM, undefined, [a]), (x, y) => x + y, 0); }
  min(a) { return reduce(this.#exec(OP.MIN, undefined, [a]), Math.min, Infinity); }
  max(a) { return reduce(this.#exec(OP.MAX, undefined, [a]), Math.max, -Infinity); }
  dot(a, b) { return reduce(this.#exec(OP.DOT, undefined, [a, b]), (x, y) => x + y, 0); }
  heavy(a) { return reduce(this.#exec(OP.HEAVY, undefined, [a]), (x, y) => x + y, 0); }

  countGreater(a, x) {
    this.#assertIdle();
    this.sc[0] = x;
    return reduce(this.#exec(OP.COUNT_GT, undefined, [a]), (p, q) => p + q, 0);
  }

  add(a, b, out) { this.#exec(OP.ADD, undefined, [a, b, out]); return this.live(out); }

  // out = a * k + c
  scale(a, k, c, out) {
    this.#assertIdle();
    this.sc[0] = k;
    this.sc[1] = c;
    this.#exec(OP.SCALE, undefined, [a, out]);
    return this.live(out);
  }

  // Count occurrences of `pattern` (string or bytes) in shared bytes.
  countBytes(text, pattern) {
    this.#assertIdle();
    const p = typeof pattern === 'string' ? encode(pattern) : pattern;
    if (!this.patBuf || this.patBuf.length < p.length) {
      this.patBuf = new Uint8Array(new (this.isShared ? SharedArrayBuffer : ArrayBuffer)(Math.max(256, p.length)));
    }
    const pat = this.patBuf.subarray(0, p.length);
    pat.set(p);
    return reduce(this.#exec(OP.COUNT_BYTES, undefined, [text, pat]), (x, y) => x + y, 0);
  }

  // In-place numeric sort: threads sort small chunks, then merge them in rounds.
  sort(a) {
    this.#assertIdle();
    a = this.live(a);
    const n = a.length;
    if (this.#serial(OP.SORT_CHUNK, n)) return a.sort();
    const chunks = this.T * 4;
    this.sc[1] = chunks;
    this.#run(OP.SORT_CHUNK, n, [a]);
    let src = a;
    let dst = this.#scratchFor(a);
    for (let width = 1; width < chunks; width *= 2) {
      this.sc[0] = width;
      this.#run(OP.MERGE, n, [src, dst]);
      [src, dst] = [dst, src];
    }
    if (src !== a) this.#run(OP.COPY, n, [src, a]);
    return a;
  }

  #scratchFor(a) {
    const bytes = a.length * a.BYTES_PER_ELEMENT;
    if (!this.scratch || this.scratch.byteLength < bytes) this.scratch = new SharedArrayBuffer(bytes);
    return new a.constructor(this.scratch, 0, a.length);
  }

  // ---- kernel fusion ------------------------------------------------------

  // Compile an expression (or a list/object of reductions) into one SIMD
  // kernel. Compiling is cached; the WASM is generated per element type.
  //   engine.compile('sum(a * 2 + 1)').run({ a })
  //   engine.compile(['sum(a)', 'sum(a*a)', 'max(a)']).run({ a })
  //   engine.compile('a * k + b').run({ a, b, k: 2 }, out)
  compile(spec) {
    const key = typeof spec === 'string' ? spec : JSON.stringify(spec);
    let k = this.compiled.get(key);
    if (!k) {
      k = new Kernel(this, analyze(spec));
      this.compiled.set(key, k);
    }
    return k;
  }

  eval(spec, vars, out) { return this.compile(spec).run(vars, out); }
  evalAsync(spec, vars, out) { return this.compile(spec).runAsync(vars, out); }
  *evalG(spec, vars, out) { return yield* this.compile(spec).runG(vars, out); }

  // Generate, compile and hand a kernel variant to every thread.
  _variant(analysis, t, arrayNames, scalarNames, encs = []) {
    return this._register(generate(analysis, t, arrayNames, scalarNames, this.isShared, encs));
  }

  // Compile generated WASM and send it to every thread; returns { id, fn }.
  _register(bytes) {
    const module = new WebAssembly.Module(bytes);
    const id = this.nextKernelId++;
    for (const p of this.ports) p.postMessage({ kind: 'kernel', id, module });
    const fn = new WebAssembly.Instance(module, { env: { memory: this.memory } }).exports.k;
    return { id, fn };
  }

  // Each of these yields at most one run request (see _drive / _driveAsync).
  // Small inputs run right here on the calling thread instead.

  // A fused kernel over n elements; returns per-unit results (R per unit).
  *_fusedG(v, n, arrays, scalars, R) {
    arrays = arrays.map((a) => this.live(a));
    if (arrays.some((a) => !this.#inMemory(a))) {
      throw new TypeError('vectro: fused expressions need arrays from engine.alloc() (or engine.toShared())');
    }
    if (this.#serial(OP.FUSED, n, this.kernels)) {
      const r = v.fn(...arrays.map((a) => a.byteOffset), n, ...scalars);
      return { parts: R === 1 ? [r] : r ?? [], units: 1 };
    }
    const parts = yield {
      op: OP.FUSED, n, arrays, F: v.fn,
      setup: () => {
        for (let j = 0; j < scalars.length; j++) this.sc[j] = scalars[j];
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NOUT] = R;
        this.ctrl[C.NSCAL] = scalars.length;
      },
    };
    return { parts, units: parts.length / Math.max(1, R) };
  }

  // Group-by kernel over n rows; arrays = [columns..., key]. Each thread
  // accumulates into its own block: accBase + threadIndex * stride.
  *_groupG(v, n, arrays, scalars, accBase, stride, K) {
    arrays = arrays.map((a) => this.live(a));
    if (this.#serial(OP.FUSED, n, this.kernels)) {
      v.fn(...arrays.map((a) => a.byteOffset), n, accBase, K, ...scalars);
      return;
    }
    yield {
      op: OP.GROUP, n, arrays, F: v.fn,
      setup: () => {
        const ns = scalars.length;
        for (let j = 0; j < ns; j++) this.sc[j] = scalars[j];
        this.sc[ns] = accBase;
        this.sc[ns + 1] = stride;
        this.sc[ns + 2] = K;
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NSCAL] = ns;
      },
    };
  }

  // A kernel run over n rows in units on every thread (see runIndexed);
  // returns the number of units (they decide each unit's own slots).
  *_unitsG(v, n, scalars) {
    yield {
      op: OP.UNITS, n, arrays: [], F: v.fn,
      setup: () => {
        for (let j = 0; j < scalars.length; j++) this.sc[j] = scalars[j];
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NSCAL] = scalars.length;
      },
    };
  }

  // A kernel run once per task, n tasks claimed one at a time on every
  // thread (see runTask): (task, thread index, scalars...).
  *_tasksG(v, n, scalars) {
    yield {
      op: OP.TASKS, n, arrays: [], F: v.fn,
      setup: () => {
        for (let j = 0; j < scalars.length; j++) this.sc[j] = scalars[j];
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NSCAL] = scalars.length;
      },
    };
  }

  // Group-by kernel split by group ranges (see runGroupRange): one set of
  // accumulators at accBase, J blocks of K slots.
  *_groupRangeG(v, n, arrays, scalars, accBase, K) {
    arrays = arrays.map((a) => this.live(a));
    if (this.#serial(OP.FUSED, n, this.kernels)) {
      v.fn(...arrays.map((a) => a.byteOffset), n, accBase, K, 0, K, ...scalars);
      return;
    }
    yield {
      op: OP.GROUP_RANGE, n, arrays, F: v.fn,
      setup: () => {
        const ns = scalars.length;
        for (let j = 0; j < ns; j++) this.sc[j] = scalars[j];
        this.sc[ns] = accBase;
        this.sc[ns + 1] = K;
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NSCAL] = ns;
      },
    };
  }

  // Compact kernel; offsets[u] is where unit u starts writing. units must
  // match the counting pass (1 = it ran on the calling thread alone).
  *_compactG(v, n, src, dst, offsets, scalars, units) {
    src = src.map((a) => this.live(a));
    dst = dst.map((a) => this.live(a));
    if (units === 1) {
      v.fn(...src.map((a) => a.byteOffset), n, ...dst.map((a) => a.byteOffset), ...scalars);
      return;
    }
    yield {
      op: OP.COMPACT, n, arrays: [...src, ...dst, offsets], F: v.fn,
      setup: () => {
        for (let j = 0; j < scalars.length; j++) this.sc[j] = scalars[j];
        this.ctrl[C.KERNEL] = v.id;
        this.ctrl[C.NOUT] = src.length;
        this.ctrl[C.NSCAL] = scalars.length;
      },
    };
  }

  // A per-chunk op (CSV_SCAN / CSV_PARSE) over `chunks` units. Returns the
  // per-unit results and the messages threads sent back; `expect` is how many
  // messages there will be in total (browsers deliver them asynchronously).
  *_chunksG(op, chunks, arrays, scalars, expect = 0) {
    const messages = [];
    return yield {
      op, n: chunks, arrays, F: (msg) => messages.push(msg),
      setup: () => { for (let j = 0; j < scalars.length; j++) this.sc[j] = scalars[j]; },
      after: (parts) => {
        parts = Float64Array.from(parts.subarray(0, chunks));
        const pull = () => { for (const p of this.ports) messages.push(...this.#received(p)); };
        pull();
        if (messages.length >= expect || !this.platform?.nextMessage) return { parts, messages };
        return (async () => {
          while (messages.length < expect) {
            await this.platform.nextMessage(this.ports);
            pull();
          }
          return { parts, messages };
        })();
      },
    };
  }

  // Blocking versions, kept for callers that want them.
  _runFused(...args) { return this._drive(this._fusedG(...args)); }
  _runGroup(...args) { return this._drive(this._groupG(...args)); }
  _runCompact(...args) { return this._drive(this._compactG(...args)); }
  _runChunks(...args) { return this._drive(this._chunksG(...args)); }

  // ---- tables ---------------------------------------------------------------

  // Build a table from columns: { price: Float64Array | number[], region: string[] }
  // opts.compress: pack the columns (see Table.compress).
  table(columns, opts) { return Table.fromColumns(this, columns, { ...opts, compress: opts?.compress ?? COMPRESS_ALL }); }

  // Load Arrow IPC bytes (stream or file format, e.g. a .arrow / .feather
  // file or what pyarrow, Polars or DuckDB send). columns: names to read.
  fromArrow(bytes, { columns, type } = {}) {
    const r = readArrow(bytes, { columns });
    return Table.fromColumns(this, r.columns, { type, dates: r.dates, compress: COMPRESS_ALL });
  }

  // Build a table from an array of objects (one-time conversion cost).
  tableFromRows(rows, opts) { return Table.fromRows(this, rows, opts); }

  // Load CSV text or bytes into a table, parsing on all threads.
  // (Reading files is in node.mjs: engine.readCSV(path).)
  // opts.compress: pack the columns afterwards (see Table.compress).
  parseCSV(text, opts) { const t = parseCSV(this, text, opts); return opts?.compress ?? COMPRESS_ALL ? t.compress() : t; }
  async parseCSVAsync(text, opts) { const t = await parseCSVAsync(this, text, opts); return opts?.compress ?? COMPRESS_ALL ? t.compress() : t; }

  async close() {
    this.ctrl[C.OP] = OP.EXIT;
    Atomics.store(this.ctrl, C.SEQ, ++this.seq);
    Atomics.notify(this.ctrl, C.SEQ);
    await Promise.all(this.workers.map((w) => w.terminate()));
    for (const p of this.ports) p.close?.();
  }
}

const COMBINE = { sum: (x, y) => x + y, count: (x, y) => x + y, mean: (x, y) => x + y, min: Math.min, max: Math.max };
const IDENTITY = { sum: 0, count: 0, mean: 0, min: Infinity, max: -Infinity };

class Kernel {
  constructor(engine, analysis) {
    this.engine = engine;
    this.analysis = analysis;
    this.variants = new Map(); // "f64|a,b|k" -> { id, fn }
  }

  // vars: { name: typedArray | number }. Arrays must share one type and length.
  run(vars, out) { return this.engine._drive(this.runG(vars, out)); }
  runAsync(vars, out) { return this.engine._driveAsync(this.runG(vars, out)); }

  *runG(vars, out) {
    const { analysis } = this;
    if (!this.engine.jit) return this.#interpret(vars, out);
    const { v, n, arrays, scalars, R } = this.#prepare(vars, out);
    const { parts, units } = yield* this.engine._fusedG(v, n, arrays, scalars, R);
    if (analysis.isMap) return this.engine.live(out);

    const values = analysis.items.map((it, j) => {
      let r = IDENTITY[it.reduce];
      for (let u = 0; u < units; u++) r = COMBINE[it.reduce](r, parts[u * R + j]);
      return it.reduce === 'mean' ? r / n : r;
    });
    if (analysis.shape === 'single') return values[0];
    if (analysis.shape === 'array') return values;
    return Object.fromEntries(analysis.items.map((it, j) => [it.key, values[j]]));
  }

  // Like run() for one reduction, but returns each unit's partial result
  // (used to find where each unit's rows go when compacting a filter).
  runParts(vars) { return this.engine._drive(this.runPartsG(vars)); }

  *runPartsG(vars) {
    const { v, n, arrays, scalars, R } = this.#prepare(vars);
    const { parts, units } = yield* this.engine._fusedG(v, n, arrays, scalars, R);
    return { parts: Float64Array.from(parts.slice(0, units)), units };
  }

  // Same results as the compiled kernels, with only precompiled code.
  #interpret(vars, out) {
    const { analysis } = this;
    const { type, n, arrayNames, arrays: raw, scalarNames, scalars, encs } = this.#bind(vars, out);
    // packed columns: decode first (the interpreter's kernels read plain floats)
    const temps = [];
    const arrays = raw.map((a, i) => {
      if (!encs[i]) return a;
      const plain = this.engine.alloc(type === 'f32' ? Float32Array : Float64Array, n);
      const codes = this.engine.live(a);
      for (let r = 0; r < n; r++) plain[r] = decodeOne(encs[i], codes[r], type === 'f32');
      temps.push(plain);
      return plain;
    });
    try { return this.#interpretPlain(analysis, type, n, arrayNames, arrays.map((x) => this.engine.live(x)), scalarNames, scalars, out); } finally { for (const x of temps) this.engine.free(x); }
  }

  #interpretPlain(analysis, type, n, arrayNames, arrays, scalarNames, scalars, out) {
    if (arrays.some((a) => !this.engine._inMemory(a))) {
      throw new TypeError('vectro: fused expressions need arrays from engine.alloc() (or engine.toShared())');
    }
    const cols = Object.fromEntries(arrayNames.map((nm, i) => [nm, arrays[i]]));
    const params = Object.fromEntries(scalarNames.map((nm, i) => [nm, scalars[i]]));
    if (analysis.isMap) return interpretMap(this.engine, type, analysis.items[0].ast, cols, params, arrays[arrays.length - 1], n);
    const values = interpretReduce(this.engine, type, analysis.items, cols, params, n);
    if (analysis.shape === 'single') return values[0];
    if (analysis.shape === 'array') return values;
    return Object.fromEntries(analysis.items.map((it, j) => [it.key, values[j]]));
  }

  #prepare(vars, out) {
    const { analysis } = this;
    const { type, n, arrayNames, arrays, scalarNames, scalars, encs } = this.#bind(vars, out);
    const key = `${type}|${arrayNames}|${scalarNames}|${encs.map((e) => (e ? `${e.kind}:${e.mul}:${e.div}:${e.base}:${e.nan}` : '')).join(',')}`;
    let v = this.variants.get(key);
    if (!v) {
      v = this.engine._variant(analysis, type, arrayNames, scalarNames, encs);
      this.variants.set(key, v);
    }
    const R = analysis.isMap ? 0 : analysis.items.length;
    return { v, n, arrays, scalars, R };
  }

  // vars[name]: a number, a Float64Array / Float32Array, or a packed column
  // { data, enc, n, type } (see pack.mjs).
  #bind(vars, out) {
    const { analysis } = this;
    const arrayNames = [], scalarNames = [], arrays = [], scalars = [], encs = [], lengths = [];
    let packedType = null;
    for (const name of analysis.vars) {
      const v = vars[name];
      if (typeof v === 'number') { scalarNames.push(name); scalars.push(v); }
      else if (ArrayBuffer.isView(v)) { const a = this.engine.live(v); arrayNames.push(name); arrays.push(a); encs.push(null); lengths.push(a.length); } // (live: a stale view has length 0)
      else if (v?.enc && ArrayBuffer.isView(v.data)) { arrayNames.push(name); arrays.push(this.engine.live(v.data)); encs.push(v.enc); lengths.push(v.n); packedType = v.type; }
      else throw new TypeError(`vectro: missing value for "${name}"`);
    }
    if (analysis.isMap) {
      if (!out) throw new TypeError('vectro: an elementwise expression needs an output array: run(vars, out)');
      const o = this.engine.live(out);
      arrays.push(o); encs.push(null); lengths.push(o.length);
    } else if (!arrays.length) {
      throw new TypeError('vectro: a reduction needs at least one array');
    }
    const plain = arrays.filter((_, i) => !encs[i]);
    const ctor = plain[0]?.constructor ?? (packedType === 'f32' ? Float32Array : Float64Array);
    const type = ctor === Float64Array ? 'f64' : ctor === Float32Array ? 'f32' : null;
    if (!type) throw new TypeError('vectro: fused expressions support Float64Array and Float32Array');
    const n = lengths[0];
    for (let i = 0; i < arrays.length; i++) {
      if (!encs[i] && arrays[i].constructor !== ctor) throw new TypeError('vectro: all arrays in an expression must have the same type');
      if (lengths[i] !== n) throw new RangeError('vectro: all arrays in an expression must have the same length');
    }
    return { type, n, arrayNames, arrays, scalarNames, scalars, encs };
  }
}

function reduce(parts, f, init) {
  let r = init;
  for (let i = 0; i < parts.length; i++) r = f(r, parts[i]);
  return r;
}
