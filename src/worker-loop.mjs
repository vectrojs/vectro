// The worker side of the engine, shared by Node (worker.mjs) and browsers
// (worker-browser.mjs). A worker blocks on the control block between tasks
// (Atomics.wait is allowed in workers everywhere) and only needs messages for
// two things: newly compiled kernels and arrays outside engine memory.
//
// host = {
//   post(msg)          send a message to the main thread
//   pullSync(handle)   hand every message that has already arrived to handle()
//   pullAsync()        browsers: a promise for the next message (a browser
//                      worker only receives messages between event-loop turns);
//                      Node: undefined (messages can be read synchronously)
// }
import { C, OP, WASM_MEMORY, SEQ_WRITING, MAX_ARGS, decodeArgs, runUnits, tagOf } from './kernels.mjs';

export async function workerLoop({ ctrlBuf, scalarBuf, resultBuf, spin, module, memory, index }, host) {
  const ctrl = new Int32Array(ctrlBuf);
  const sc = new Float64Array(scalarBuf);
  const res = new Float64Array(resultBuf);
  const buffers = new Map(); // bufferId -> SharedArrayBuffer (arrays outside WASM memory)
  const fusedModules = new Map(); // kernel id -> compiled WebAssembly.Module
  const fusedKernels = new Map(); // kernel id -> this thread's instance of it

  // Same compiled module, same shared memory as every other thread.
  const W = new WebAssembly.Instance(module, { env: { memory } }).exports;

  // memory.buffer is re-read each time so views see memory that grew since.
  // The engine tells other threads about a grow asynchronously; grow(0) catches up.
  function bufferFor(id, bytesNeeded) {
    if (id !== WASM_MEMORY) return buffers.get(id);
    if (memory.buffer.byteLength < bytesNeeded) memory.grow(0);
    return memory.buffer;
  }

  const handle = (msg) => {
    if (msg.kind === 'kernel') fusedModules.set(msg.id, msg.module);
    else buffers.set(msg.id, msg.buffer);
  };

  const usesKernel = (op) => op === OP.FUSED || op === OP.GROUP || op === OP.GROUP_RANGE || op === OP.UNITS || op === OP.COMPACT;

  // Does the current task need a kernel or array we haven't received yet?
  function missing(op) {
    if (usesKernel(op) && !fusedModules.has(ctrl[C.KERNEL])) return true;
    for (let k = 0; k < MAX_ARGS; k++) {
      const id = ctrl[C.ARGS + k * 4];
      if (id > WASM_MEMORY && !buffers.has(id)) return true;
    }
    return false;
  }

  function fused(id) {
    let k = fusedKernels.get(id);
    if (!k) {
      k = new WebAssembly.Instance(fusedModules.get(id), { env: { memory } }).exports.k;
      fusedKernels.set(id, k);
    }
    return k;
  }

  let seen = 0;
  for (;;) {
    // Spin briefly (cheap wake-up for back-to-back calls), then sleep.
    let s = Atomics.load(ctrl, C.SEQ);
    for (let k = 0; (s === seen || s === SEQ_WRITING) && k < spin; k++) s = Atomics.load(ctrl, C.SEQ);
    while (s === seen || s === SEQ_WRITING) {
      if (s === seen) Atomics.wait(ctrl, C.SEQ, seen);
      s = Atomics.load(ctrl, C.SEQ);
    }
    seen = s;

    const op = ctrl[C.OP];
    if (op === OP.EXIT) break;

    // If we woke up late, the main thread may already be on a newer task and
    // rewriting these fields. Reading them can then fail or mix two tasks, so
    // re-check SEQ: a replaced task is skipped (claiming would fail anyway).
    let A, F;
    try {
      host.pullSync(handle);
      // A browser worker gets messages only between event-loop turns: if the
      // task needs something that was sent but not yet received, step out of
      // the loop until it arrives (the main thread sent it before the task).
      while (host.pullAsync && missing(op) && Atomics.load(ctrl, C.SEQ) === s) {
        await host.pullAsync();
        host.pullSync(handle);
      }
      A = decodeArgs(ctrl, bufferFor);
      F = usesKernel(op) ? fused(ctrl[C.KERNEL])
        : op === OP.CSV_PARSE ? (msg) => host.post(msg) // results go back to the main thread
        : op === OP.READ ? (jobs, u) => { // a file read straight into engine memory (Node: host.read)
          const dst = jobs[u * 4 + 3], len = jobs[u * 4 + 2];
          if (memory.buffer.byteLength < dst + len) memory.grow(0);
          host.read(jobs[u * 4], new Uint8Array(memory.buffer, dst, len), jobs[u * 4 + 1]);
        }
        : null;
    } catch (err) {
      if (Atomics.load(ctrl, C.SEQ) !== s) continue;
      throw err;
    }
    if (Atomics.load(ctrl, C.SEQ) !== s) continue;
    runUnits(op, ctrl[C.THREADS], ctrl[C.N], A, sc, ctrl, ctrl[C.WASM] ? W : null, res, tagOf(s), F, index,
      (err) => host.post({ kind: 'error', message: err?.stack ?? String(err) }));
  }
}
