// Which engine the tests run against, from VECTRO_MODE:
//   node    (default) worker threads on all cores, compiled kernels
//   single  one thread, plain memory, compiled kernels (what browsers get)
//   interp  one thread, plain memory, vectorized interpreter
//   edge    like interp, set up the way Cloudflare Workers is: compiling
//           WebAssembly throws, so the precompiled dist/vectro.wasm is passed
//           in, and there is no Buffer or SharedArrayBuffer. Run with
//           --disallow-code-generation-from-strings to block eval as well.
//   browser-sim  the browser threading model, simulated in Node: the main side
//           can't block (sync calls run on the calling thread, the async API
//           uses the workers) and workers get messages only between
//           event-loop turns, like Web Workers.
import { readFileSync } from 'node:fs';

export const MODE = process.env.VECTRO_MODE ?? 'node';
let Engine;

if (MODE === 'node') {
  ({ Engine } = await import('../src/node.mjs'));
} else if (MODE === 'browser-sim') {
  const { Worker, MessageChannel } = await import('node:worker_threads');
  const core = await import('../src/engine.mjs');
  const platform = {
    canBlock: false,
    cpus: () => 8,
    spawn(workerData) {
      const { port1, port2 } = new MessageChannel();
      const worker = new Worker(new URL('./test-browser-sim-worker.mjs', import.meta.url), { workerData: { ...workerData, port: port2 }, transferList: [port2] });
      worker.unref();
      port1.inbox = [];
      port1.waiters = [];
      port1.on('message', (m) => { port1.inbox.push(m); for (const w of port1.waiters.splice(0)) w(); });
      return { worker, port: port1 };
    },
    receive: (port) => (port.inbox.length ? { message: port.inbox.shift() } : undefined),
    nextMessage: (ports) => new Promise((resolve) => { for (const p of ports) p.waiters.push(resolve); }),
  };
  Engine = class extends core.Engine {
    constructor(opts = {}) { super({ platform, ...opts }); }
    readCSV(path, opts) { return this.parseCSV(new Uint8Array(readFileSync(path)), opts); }
    readCSVAsync(path, opts) { return this.parseCSVAsync(new Uint8Array(readFileSync(path)), opts); }
  };
} else {
  let base;
  if (MODE === 'single') base = { jit: true };
  else if (MODE === 'interp') base = { jit: false };
  else if (MODE === 'edge') {
    // Stands in for the bundler, which hands the worker a compiled module.
    const wasmModule = new WebAssembly.Module(readFileSync(new URL('../dist/vectro.wasm', import.meta.url)));
    const refuse = () => { throw new WebAssembly.CompileError('Wasm code generation disallowed by embedder'); };
    const instantiate = WebAssembly.instantiate;
    WebAssembly.Module = new Proxy(WebAssembly.Module, { construct: refuse });
    WebAssembly.compile = async () => refuse();
    WebAssembly.instantiate = (src, imports) => (src instanceof WebAssembly.Module ? instantiate(src, imports) : Promise.reject(new WebAssembly.CompileError('Wasm code generation disallowed by embedder')));
    delete globalThis.Buffer;
    delete globalThis.SharedArrayBuffer;
    base = { wasmModule };
  } else {
    throw new Error(`unknown VECTRO_MODE ${MODE}`);
  }
  // Imported only now, so the engine sees the restricted globals.
  const core = await import('../src/portable.mjs');
  Engine = class extends core.Engine {
    constructor(opts = {}) { super({ ...base, ...opts }); }
    // the portable engine has no file access; tests read files here
    readCSV(path, opts) { return this.parseCSV(new Uint8Array(readFileSync(path)), opts); }
  };
}

export { Engine };
