// A sender thread of a threaded Emitter (see emitter.mjs): runs its own
// Emitter on its own port and sends the batches the main thread puts in its
// shared slots.
import { parentPort, workerData } from 'node:worker_threads';
import { Emitter } from './emitter.mjs';

const { host, columns, format, slots, stride, maxRows } = workerData;
const names = Object.keys(columns);
const em = await Emitter.listen({ host, port: 0, columns, format });
em.onChange = (n) => parentPort.postMessage({ type: 'conns', n });

parentPort.on('message', async (m) => {
  try {
    if (m.type === 'close') { await em.close(); parentPort.postMessage({ type: 'closed' }); return; }
    const buf = slots[m.slot], batch = {};
    let off = 0;
    names.forEach((name, k) => {
      if (stride[k] === 8) batch[name] = new Float64Array(buf, off, m.rows);
      else if (m.labels[name]) batch[name] = { codes: new Uint32Array(buf, off, m.rows), labels: m.labels[name] };
      else batch[name] = m.strings[name];
      off += stride[k] * maxRows;
    });
    await em.write(batch); // framed (copied) and queued on a socket
    parentPort.postMessage({ type: 'done', slot: m.slot });
  } catch (err) {
    parentPort.postMessage({ type: 'error', message: err.message });
  }
});
parentPort.postMessage({ type: 'ready', port: em.port });
