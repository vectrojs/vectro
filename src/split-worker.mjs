// Browser Web Worker for split tables (see split-worker-core.mjs).
import { serveSplitWorker } from './split-worker-core.mjs';

serveSplitWorker(
  (msg, transfer) => self.postMessage(msg, transfer),
  (handle) => { self.onmessage = (ev) => handle(ev.data); },
);
