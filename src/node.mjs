// Node.js entry: the engine with worker threads on all cores, plus file reading.
//   import { Engine } from 'vectro';   ->   new Engine()  (all cores)
import { Worker, MessageChannel, receiveMessageOnPort } from 'node:worker_threads';
import { availableParallelism } from 'node:os';
import { openSync, fstatSync, readSync, closeSync } from 'node:fs';
import { open } from 'node:fs/promises';
import { Engine as CoreEngine } from './engine.mjs';
import { loadBytes, loadBytesAsync } from './csv.mjs';

export const nodePlatform = {
  cpus: () => availableParallelism(),
  spawn(workerData) {
    const { port1, port2 } = new MessageChannel();
    port1.unref();
    const worker = new Worker(new URL('./worker.mjs', import.meta.url), {
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
      return loadBytes(this, bytes, opts);
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
      return await loadBytesAsync(this, bytes, opts);
    } finally {
      await file.close();
    }
  }
}

export { canCompileWasm } from './engine.mjs';
export { Table } from './table.mjs';

// ---- split tables and streams: each worker thread owns its rows --------------
//   import { SplitEngine } from 'vectro';
//   const engine = new SplitEngine();               // one worker per core
//   const live = await engine.streamAsync({ columns: { ts: 'number', region: 'category' } });
import { SplitEngine as BaseSplitEngine } from './split.mjs';

export function spawnSplitWorker() {
  const w = new Worker(new URL('./split-worker-node.mjs', import.meta.url));
  w.unref();
  return {
    post: (msg, transfer) => w.postMessage(msg, transfer ?? []),
    onMessage: (fn) => w.on('message', fn),
    onError: (fn) => { w.on('error', fn); w.on('exit', (code) => fn(new Error(`exited with code ${code}`))); },
    terminate: () => w.terminate(),
  };
}

export class SplitEngine extends BaseSplitEngine {
  constructor(opts = {}) {
    super({ workers: availableParallelism(), spawn: spawnSplitWorker, ...opts });
  }
}
