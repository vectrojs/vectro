// Browser entry. On cross-origin-isolated pages (served with
//   Cross-Origin-Opener-Policy: same-origin
//   Cross-Origin-Embedder-Policy: require-corp)
// the engine runs Web Workers on every core: the async API (aggAsync,
// groupBy().aggAsync, collectAsync, ...) spreads queries across them while the
// page stays responsive. Sync calls still work, on the page's thread only
// (the page's main thread is not allowed to block and wait for workers).
// Other pages get the single-thread engine, which needs no headers.
//
//   import { Engine } from 'vectrojs/browser';
//   const engine = new Engine();
//   const orders = await engine.loadCSV('/orders.csv');
//   await orders.filter("region == 'EU'").sumAsync('price * qty');
import { Engine as CoreEngine, trackAsyncMethods } from './engine.mjs';
import { openDatasetAsync, writeDatasetAsync, csvToDatasetAsync, createDatasetAsync } from './dataset.mjs';
import { writableFile, storedFiles, storedFile, removeStored, storageAvailable, opfsTemp } from './files-browser.mjs';
import { setTempStorage } from './dataset.mjs';
if (storageAvailable()) setTempStorage(opfsTemp); // (huge group-bys, sorts and joins spill to browser storage)

export const browserPlatform = {
  canBlock: false,
  cpus: () => navigator.hardwareConcurrency || 4,
  spawn(workerData) {
    const { port1, port2 } = new MessageChannel();
    const worker = new Worker(new URL('./worker-browser.mjs', import.meta.url), { type: 'module' });
    worker.postMessage({ ...workerData, port: port2 }, [port2]);
    // Messages from the worker arrive asynchronously; keep them until read.
    port1.inbox = [];
    port1.waiters = [];
    port1.onmessage = (ev) => {
      port1.inbox.push(ev.data);
      for (const w of port1.waiters.splice(0)) w();
    };
    return { worker, port: port1 };
  },
  // A message that has already arrived, or undefined.
  receive: (port) => (port.inbox.length ? { message: port.inbox.shift() } : undefined),
  // Resolves when any of the ports receives a message.
  nextMessage: (ports) => new Promise((resolve) => { for (const p of ports) p.waiters.push(resolve); }),
};

export const threadsAvailable = () => Boolean(globalThis.crossOriginIsolated) && typeof SharedArrayBuffer === 'function';

export class Engine extends CoreEngine {
  // spin: 0, workers sleep as soon as a task is done. Spinning only pays off
  // when the caller blocks (Node); a page awaits anyway, and in Safari seven
  // spinning workers starved the page: 4x slower queries, 9-40 ms stalls.
  constructor(opts = {}) {
    super({ platform: threadsAvailable() ? browserPlatform : null, spin: 0, ...opts });
  }

  // Fetch a CSV and parse it (on the workers when there are any).
  async loadCSV(url, opts) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`vectro: ${url}: HTTP ${res.status}`);
    return this.parseCSVAsync(new Uint8Array(await res.arrayBuffer()), opts);
  }

  // A dataset (a .vectro table of any size, see dataset.mjs) from a File the
  // user picked or dropped, a Blob, or a URL (read with HTTP range requests):
  // only the parts a query needs are read. Query it with the async calls.
  //   const orders = await engine.openDataset(input.files[0]);
  //   await sql('SELECT region, COUNT(*) FROM orders GROUP BY region', { orders }).toRowsAsync();
  openDataset(source) { return openDatasetAsync(this, source); }

  // Writing datasets in the browser: into its file storage (OPFS) by name,
  // or to a file the user chose (a FileSystemFileHandle), or memoryFile().
  // Big inputs stream: each chunk is on disk before the next is made.
  //   await engine.csvToDataset(droppedCsvFile, 'orders.vectro');   // a CSV of any size
  //   await engine.writeDataset('eu.vectro', orders.filter("region == 'EU'"));
  //   const orders = await engine.openStoredDataset('orders.vectro');
  async writeDataset(target, source, opts) { return writeDatasetAsync(this, await target_(target, opts), source, opts); }
  async csvToDataset(csv, target, opts) { return csvToDatasetAsync(this, csv, await target_(target, opts), opts); }
  // A writer: appendAsync(table or columns) as often as needed, then closeAsync().
  async createDataset(target, opts) { return createDatasetAsync(this, await target_(target, opts), opts); }
  async storedDatasets() { return (await storedFiles()).filter((f) => f.name.endsWith('.vectro')); }
  async openStoredDataset(name) { return openDatasetAsync(this, await storedFile(name)); }
  storedDatasetFile(name) { return storedFile(name); } // (a File: to download, or to open)
  removeStoredDataset(name) { return removeStored(name); }
  // the Node Engine's names for the same calls, so code shared with Node runs here too
  openDatasetAsync(source) { return this.openDataset(source); }
  writeDatasetAsync(target, source, opts) { return this.writeDataset(target, source, opts); }
  csvToDatasetAsync(csv, target, opts) { return this.csvToDataset(csv, target, opts); }
  createDatasetAsync(target, opts) { return this.createDataset(target, opts); }
}

// a name in browser storage or a FileSystemFileHandle -> a writable file; file objects as they are
const target_ = (t, opts) => (typeof t === 'string' || typeof t?.createWritable === 'function' ? writableFile(t, { append: opts?.append }) : t);

// ---- split tables: multi-core on pages WITHOUT isolation headers ------------
// No shared memory: each Web Worker owns a slice of every table and answers
// for it; the page combines the partial results (see split.mjs). Async API only.
//
//   import { SplitEngine } from 'vectrojs/browser';
//   const engine = new SplitEngine();                 // one worker per core
//   const orders = await engine.loadCSV('/orders.csv');
//   await orders.filter("region == 'EU'").sumAsync('price * qty');
import { SplitEngine as BaseSplitEngine } from './split.mjs';

export function spawnSplitWorker() {
  const w = new Worker(new URL('./split-worker.mjs', import.meta.url), { type: 'module' });
  return {
    post: (msg, transfer) => w.postMessage(msg, transfer ?? []),
    onMessage: (fn) => { w.onmessage = (ev) => fn(ev.data); },
    onError: (fn) => {
      w.onerror = (ev) => { ev.preventDefault?.(); fn(new Error(ev.message || 'worker error')); };
      w.onmessageerror = () => fn(new Error('a message from the worker could not be read'));
    },
    terminate: () => w.terminate(),
  };
}

export class SplitEngine extends BaseSplitEngine {
  constructor(opts = {}) {
    super({ workers: navigator.hardwareConcurrency || 4, spawn: spawnSplitWorker, ...opts });
  }

  async loadCSV(url, opts) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`vectro: ${url}: HTTP ${res.status}`);
    return this.parseCSVAsync(new Uint8Array(await res.arrayBuffer()), opts);
  }
}

export { canCompileWasm } from './engine.mjs';
export { Table } from './table.mjs';

export { VectorIndex } from './vectors.mjs';
export { BatchEncoder, FrameReader, encodeSchema, encodeEnd } from './frames.mjs';
export { readArrow, writeArrow, ArrowReader, ArrowEncoder } from './arrow.mjs';
export { DistinctSketch } from './sketch.mjs';
export { sql } from './sql.mjs';
export { Dataset, openDatasetAsync, blobFile, urlFile, writeDatasetAsync, csvToDatasetAsync, createDatasetAsync } from './dataset.mjs';
export { memoryFile, writableFile, storageAvailable } from './files-browser.mjs';

trackAsyncMethods(Engine, (e) => e);
