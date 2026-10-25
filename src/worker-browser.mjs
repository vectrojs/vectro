// Browser Web Worker: runs the shared worker loop. Messages arrive only
// between event-loop turns, so they are queued here and the loop awaits them
// when a task needs one (pullAsync).
import { workerLoop } from './worker-loop.mjs';

self.onmessage = (first) => {
  self.onmessage = null;
  const data = first.data;
  const { port } = data;
  const inbox = [];
  let wake = null;
  port.onmessage = (ev) => {
    inbox.push(ev.data);
    if (wake) { const w = wake; wake = null; w(); }
  };
  workerLoop(data, {
    post: (msg) => port.postMessage(msg),
    pullSync(handle) { while (inbox.length) handle(inbox.shift()); },
    pullAsync: () => new Promise((resolve) => { wake = resolve; }),
  }).catch((err) => port.postMessage({ kind: 'error', message: err?.stack ?? String(err) }));
};
