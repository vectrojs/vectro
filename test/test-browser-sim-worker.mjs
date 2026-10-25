// A Node worker that behaves like a browser Web Worker: messages are only
// delivered between event-loop turns (port 'message' events), never read
// synchronously. Used by VECTRO_MODE=browser-sim.
import { workerData } from 'node:worker_threads';
import { workerLoop } from '../src/worker-loop.mjs';

const { port } = workerData;
const inbox = [];
let wake = null;
port.on('message', (m) => {
  inbox.push(m);
  if (wake) { const w = wake; wake = null; w(); }
});
workerLoop(workerData, {
  post: (m) => port.postMessage(m),
  pullSync(handle) { while (inbox.length) handle(inbox.shift()); },
  pullAsync: () => new Promise((resolve) => { wake = resolve; }),
});
