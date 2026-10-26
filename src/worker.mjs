// Node worker thread: runs the shared worker loop. Messages are read
// synchronously, so the loop never has to return to the event loop.
import { workerData, receiveMessageOnPort } from 'node:worker_threads';
import { readSync } from 'node:fs';
import { workerLoop } from './worker-loop.mjs';

const { port } = workerData;
workerLoop(workerData, {
  post: (msg) => port.postMessage(msg),
  pullSync(handle) {
    let m;
    while ((m = receiveMessageOnPort(port))) handle(m.message);
  },
  pullAsync: undefined,
  read(fd, view, pos) { // (datasets: chunks are read on every thread)
    for (let done = 0; done < view.length; ) {
      const got = readSync(fd, view, done, view.length - done, pos + done);
      if (got === 0) throw new Error('vectro: dataset file ended early');
      done += got;
    }
  },
});
