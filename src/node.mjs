// Node.js entry: the engine with worker threads on all cores, plus file reading.
//   import { Engine } from 'vectrojs';   ->   new Engine()  (all cores)
import { Worker, MessageChannel, receiveMessageOnPort } from 'node:worker_threads';
import { availableParallelism } from 'node:os';
import { openSync, fstatSync, readSync, closeSync } from 'node:fs';
import { open } from 'node:fs/promises';
import { Engine as CoreEngine, trackAsyncMethods } from './engine.mjs';
import { loadBytes, loadBytesAsync } from './csv.mjs';
import { Dataset, DatasetWriter, writeDataset, csvToDataset, csvToDatasetAsync, openDatasetAsync, writeDatasetAsync, createDatasetAsync, setFiles } from './dataset.mjs';
import { nodeFiles } from './files-node.mjs';
setFiles(nodeFiles);

// Worker threads take the process's Node flags, but Node refuses --input-type
// for workers: a script run with `node --input-type=module -e` or piped into
// node stopped once the first worker started. Then the flags are given without
// it (or none, if Node refuses another of them for workers too).
function newWorker(url, opts = {}) {
  const args = process.execArgv;
  if (!args.some((a) => a === '--input-type' || a.startsWith('--input-type='))) return new Worker(url, opts);
  const execArgv = args.filter((a, i) => !a.startsWith('--input-type') && args[i - 1] !== '--input-type'); // (--input-type module: its value too)
  try { return new Worker(url, { ...opts, execArgv }); } catch (err) {
    if (err.code !== 'ERR_WORKER_INVALID_EXEC_ARGV') throw err;
    return new Worker(url, { ...opts, execArgv: [] });
  }
}

export const nodePlatform = {
  cpus: () => availableParallelism(),
  spawn(workerData) {
    const { port1, port2 } = new MessageChannel();
    port1.unref();
    const worker = newWorker(new URL('./worker.mjs', import.meta.url), {
      workerData: { ...workerData, port: port2 },
      transferList: [port2],
    });
    worker.unref();
    return { worker, port: port1 };
  },
  receive: (port) => receiveMessageOnPort(port),
};

export class Engine extends CoreEngine {
  constructor(opts = {}) {
    super({ platform: nodePlatform, ...opts });
  }

  // Load a CSV file into a table, parsing on all threads.
  readCSV(path, opts = {}) {
    const fd = openSync(path, 'r');
    try {
      const size = fstatSync(fd).size;
      const bytes = this.alloc(Uint8Array, size);
      for (let off = 0; off < size; ) {
        const got = readSync(fd, bytes, off, size - off, off);
        if (got === 0) throw new Error(`vectro: ${path} ended early`);
        off += got;
      }
      const t = loadBytes(this, bytes, opts);
      return opts.compress ? t.compress() : t;
    } finally {
      closeSync(fd);
    }
  }

  // Same, without blocking: the file is read with async I/O straight into
  // engine memory, and parsing runs on the worker threads.
  async readCSVAsync(path, opts = {}) {
    const file = await open(path, 'r');
    try {
      const { size } = await file.stat();
      const bytes = this.alloc(Uint8Array, size);
      for (let off = 0; off < size; ) {
        const { bytesRead } = await file.read(this.live(bytes), off, size - off, off);
        if (bytesRead === 0) throw new Error(`vectro: ${path} ended early`);
        off += bytesRead;
      }
      const t = await loadBytesAsync(this, bytes, opts);
      return opts.compress ? await t.compressAsync() : t;
    } finally {
      await file.close();
    }
  }

  // ---- datasets: tables bigger than memory, on disk (see dataset.mjs) ----------
  // Open one for queries (filter, groupBy, join, sql... read it chunk by chunk).
  openDataset(path) { return new Dataset(this, path); }
  // The same from a path, a URL (HTTP range requests) or a Blob; async.
  openDatasetAsync(source) { return openDatasetAsync(this, source); }
  // A writer: append(table or columns) as often as needed, then close().
  createDataset(path, opts) { return new DatasetWriter(this, path, opts); }
  // A table, a query (over a dataset: written chunk by chunk) or columns.
  writeDataset(path, source, opts) { return writeDataset(this, path, source, opts); }
  // A CSV file of any size, read in pieces (async: also from a URL or Blob).
  csvToDataset(csvPath, path, opts) { return csvToDataset(this, csvPath, path, opts); }
  csvToDatasetAsync(csv, path, opts) { return csvToDatasetAsync(this, csv, path, opts); }
  // writeDataset / createDataset without blocking (a writer with appendAsync / closeAsync).
  writeDatasetAsync(path, source, opts) { return writeDatasetAsync(this, path, source, opts); }
  createDatasetAsync(path, opts) { return createDatasetAsync(this, path, opts); }
}

export { canCompileWasm } from './engine.mjs';
export { Dataset, DatasetWriter, blobFile, urlFile } from './dataset.mjs';
export { memoryFile } from './files-browser.mjs';
export { Table } from './table.mjs';

// ---- split tables and streams: each worker thread owns its rows --------------
//   import { SplitEngine } from 'vectrojs';
//   const engine = new SplitEngine();               // one worker per core
//   const live = await engine.streamAsync({ columns: { ts: 'number', region: 'category' } });
import { SplitEngine as BaseSplitEngine } from './split.mjs';

export function spawnSplitWorker() {
  const w = newWorker(new URL('./split-worker-node.mjs', import.meta.url));
  w.unref();
  return {
    post: (msg, transfer) => w.postMessage(msg, transfer ?? []),
    onMessage: (fn) => { w.on('message', fn); w.unref(); }, // (a listener refs the worker again: a script that never calls close() would never end)
    ref: () => w.ref(), unref: () => w.unref(), // (SplitEngine keeps the process alive while it waits for answers, see #hold)
    onError: (fn) => { w.on('error', fn); w.on('exit', (code) => fn(new Error(`exited with code ${code}`))); },
    terminate: () => w.terminate(),
  };
}

export class SplitEngine extends BaseSplitEngine {
  constructor(opts = {}) {
    super({ workers: availableParallelism(), spawn: spawnSplitWorker, ...opts });
  }
}

export { VectorIndex } from './vectors.mjs';
export { Emitter } from './emitter.mjs';
export { BatchEncoder, FrameReader, encodeSchema, encodeEnd } from './frames.mjs';
export { readArrow, writeArrow, ArrowReader, ArrowEncoder } from './arrow.mjs';
export { DistinctSketch } from './sketch.mjs';
export { sql } from './sql.mjs';

trackAsyncMethods(Engine, (e) => e);
