// Node worker thread for split tables (see split-worker-core.mjs).
import { parentPort } from 'node:worker_threads';
import { serveSplitWorker } from './split-worker-core.mjs';

serveSplitWorker(
  (msg, transfer) => parentPort.postMessage(msg, transfer),
  (handle) => parentPort.on('message', handle),
);
