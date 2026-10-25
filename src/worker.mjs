// Node worker thread: runs the shared worker loop. Messages are read
// synchronously, so the loop never has to return to the event loop.
import { workerData, receiveMessageOnPort } from 'node:worker_threads';
import { workerLoop } from './worker-loop.mjs';

const { port } = workerData;
workerLoop(workerData, {
  post: (msg) => port.postMessage(msg),
  pullSync(handle) {
    let m;
    while ((m = receiveMessageOnPort(port))) handle(m.message);
  },
  pullAsync: undefined,
});
