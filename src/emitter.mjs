// Emitter: the sending side of binary Vectro streams (Node). Collectors
// connect to it, typically one connection per collector worker, and every
// batch you write goes out as column frames, dealt round-robin to the
// connections that aren't backed up.
//
//   const feed = await Emitter.listen({ port: 9000, columns: { ts: 'number', price: 'number', region: 'category' } });
//   await feed.write({ ts, price, region });   // arrays of one batch (any length)
//   await feed.close();
//
// On the collector: engine.streamAsync({ format: 'vectro', columns }) and
// stream.connect(worker, { host, port }) for each worker.
//
// format: 'arrow' sends an Arrow IPC stream per connection instead (numbers
// as float64, categories as dictionary-encoded strings with delta
// dictionaries), which pyarrow, Polars and other Arrow readers understand.
import { createServer } from 'node:net';
import { Worker } from 'node:worker_threads';
import { encodeSchema, encodeEnd, BatchEncoder } from './frames.mjs';
import { ArrowEncoder } from './arrow.mjs';

const ENCODERS = {
  vectro(columns) {
    const e = new BatchEncoder(columns);
    return { schema: () => encodeSchema(columns), encode: (batch) => e.encode(batch), end: encodeEnd };
  },
  arrow: (columns) => new ArrowEncoder(columns),
};

const MAX_FRAME_ROWS = 65_536;        // larger batches are split into frames this size
const SLOTS_PER_THREAD = 3;           // batches in flight per sender thread
export const REDIRECT = 0x44525856;   // 'VXRD': first 4 bytes of "connect to this port instead"
const HIGH_WATER = 8 * 1024 * 1024;   // bytes queued on a connection before it counts as full

export class Emitter {
  constructor(server, columns, format) {
    this.server = server;
    this.columns = columns;
    this.format = format;
    this.conns = [];     // { socket, encoder }
    this.next = 0;       // round-robin position
    this.waiting = [];   // writers waiting for room or a connection
    this.rowsSent = 0;
  }

  // threads > 1: sender threads encode and write (see ThreadedEmitter).
  static listen({ port = 0, host = '127.0.0.1', columns, format = 'vectro', threads = 1 }) {
    if (!columns || !Object.keys(columns).length) throw new Error('vectro: Emitter.listen needs columns, e.g. { ts: "number", region: "category" }');
    if (!ENCODERS[format]) throw new Error(`vectro: Emitter format must be 'vectro' or 'arrow', got "${format}"`);
    ENCODERS[format](columns); // check the column kinds now
    if (threads > 1) return ThreadedEmitter.listen({ port, host, columns, format, threads });
    const server = createServer();
    const em = new Emitter(server, columns, format);
    server.on('connection', (socket) => em.#add(socket));
    return new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => resolve(em));
    });
  }

  get port() { return this.server.address().port; }
  get ports() { return [this.port]; } // where collectors can connect directly
  get connections() { return this.conns.length; }

  #add(socket) {
    socket.setNoDelay(true);
    const conn = { socket, encoder: ENCODERS[this.format](this.columns) };
    socket.write(conn.encoder.schema());
    socket.on('drain', () => this.#wake());
    const drop = () => { this.conns = this.conns.filter((c) => c !== conn); this.onChange?.(this.conns.length); };
    socket.on('close', drop);
    socket.on('error', drop);
    this.conns.push(conn);
    this.onChange?.(this.conns.length);
    this.#wake();
  }

  #wake() { for (const resolve of this.waiting.splice(0)) resolve(); }

  // Resolves once `n` collector connections are open.
  async waitForConnections(n) {
    while (this.conns.length < n) await new Promise((r) => this.waiting.push(r));
  }

  // A connection with room, round-robin; waits when all are full (backpressure).
  async #pick() {
    for (;;) {
      for (let tries = 0; tries < this.conns.length; tries++) {
        const c = this.conns[this.next++ % this.conns.length];
        if (c.socket.writableLength < HIGH_WATER) return c;
      }
      await new Promise((r) => this.waiting.push(r));
    }
  }

  // batch: { column: Float64Array | number[] | string[] }, all the same length.
  async write(batch) {
    const names = Object.keys(this.columns);
    const first = batch[names[0]];
    const rows = (first?.codes ?? first)?.length ?? 0;
    const cut = (v, a, b) => (v.codes ? { codes: v.codes.subarray ? v.codes.subarray(a, b) : v.codes.slice(a, b), labels: v.labels } : v.subarray ? v.subarray(a, b) : v.slice(a, b));
    for (let at = 0; at < rows; at += MAX_FRAME_ROWS) {
      const end = Math.min(rows, at + MAX_FRAME_ROWS);
      const part = at === 0 && end === rows ? batch : Object.fromEntries(names.map((n) => [n, cut(batch[n], at, end)]));
      const conn = await this.#pick();
      conn.socket.write(conn.encoder.encode(part));
      this.rowsSent += end - at;
    }
  }

  // Ends every connection cleanly and stops listening.
  async close() {
    for (const { socket, encoder } of this.conns) socket.end(encoder.end());
    await new Promise((r) => this.server.close(r));
    this.#wake();
  }
}

// An emitter whose encoding and socket writes run on several threads.
// Each sender thread runs its own Emitter on its own port. Collectors still
// connect to one port: it answers with an 8-byte redirect ('VXRD', u32 port)
// to the sender thread with the fewest connections, and Vectro collectors
// follow it (other Arrow readers can connect to `ports` directly).
// write() copies each batch into a shared slot of a thread that has
// collectors and returns; the thread frames and sends it. When every slot is
// in flight, write() waits (backpressure, as with one thread).
class ThreadedEmitter {
  static async listen({ port, host, columns, format, threads }) {
    const em = new ThreadedEmitter(columns);
    em.threads = await Promise.all(Array.from({ length: threads }, () => em.#spawn(host, columns, format)));
    em.server = createServer((socket) => {
      const t = em.threads.reduce((a, b) => (b.conns + b.assigned < a.conns + a.assigned ? b : a));
      t.assigned++;
      setTimeout(() => { t.assigned = Math.max(0, t.assigned - 1); }, 2000); // until its connection shows up
      const msg = Buffer.alloc(8);
      msg.writeUInt32LE(REDIRECT, 0);
      msg.writeUInt32LE(t.port, 4);
      socket.on('error', () => {});
      socket.end(msg);
    });
    await new Promise((resolve, reject) => { em.server.once('error', reject); em.server.listen(port, host, resolve); });
    return em;
  }

  constructor(columns) {
    this.columns = Object.entries(columns);
    this.waiting = [];
    this.rowsSent = 0;
    this.error = null;
  }

  #spawn(host, columns, format) {
    // slot layout: per column (number: 8 bytes a row, category as codes: 4)
    const stride = this.columns.map(([, kind]) => (kind === 'number' ? 8 : 4));
    const bytes = stride.reduce((a, b) => a + b, 0) * MAX_FRAME_ROWS;
    const slots = Array.from({ length: SLOTS_PER_THREAD }, () => new SharedArrayBuffer(bytes));
    const worker = new Worker(new URL('./emitter-thread.mjs', import.meta.url), { workerData: { host, columns, format, slots, stride, maxRows: MAX_FRAME_ROWS } });
    const t = { worker, slots, free: slots.map((_, i) => i), conns: 0, assigned: 0, port: 0 };
    return new Promise((resolve, reject) => {
      worker.on('message', (m) => {
        if (m.type === 'ready') { t.port = m.port; resolve(t); }
        else if (m.type === 'conns') { t.conns = m.n; if (m.n) t.assigned = Math.max(0, t.assigned - 1); this.#wake(); }
        else if (m.type === 'done') { t.free.push(m.slot); this.#wake(); }
        else if (m.type === 'error') { this.error ??= new Error(m.message); this.#wake(); }
        else if (m.type === 'closed') t.onClosed?.();
      });
      worker.once('error', (err) => { this.error ??= err; reject(err); this.#wake(); });
    });
  }

  get port() { return this.server.address().port; }
  get ports() { return this.threads.map((t) => t.port); }
  get connections() { return this.threads.reduce((a, t) => a + t.conns, 0); }

  #wake() { for (const resolve of this.waiting.splice(0)) resolve(); }

  async waitForConnections(n) {
    while (this.connections < n) await new Promise((r) => this.waiting.push(r));
  }

  // A thread with collectors and a free slot (the one with most free slots).
  async #pick() {
    for (;;) {
      if (this.error) throw this.error;
      let best = null;
      for (const t of this.threads) if (t.conns && t.free.length && (!best || t.free.length > best.free.length)) best = t;
      if (best) return best;
      await new Promise((r) => this.waiting.push(r));
    }
  }

  async write(batch) {
    const first = batch[this.columns[0][0]];
    const rows = (first?.codes ?? first)?.length ?? 0;
    for (let at = 0; at < rows; at += MAX_FRAME_ROWS) {
      const end = Math.min(rows, at + MAX_FRAME_ROWS), n = end - at;
      const t = await this.#pick();
      const slot = t.free.pop(), buf = t.slots[slot];
      const msg = { type: 'batch', slot, rows: n, labels: {}, strings: {} };
      let off = 0;
      for (const [name, kind] of this.columns) {
        const v = batch[name];
        if (!v) throw new Error(`vectro: batch is missing column "${name}"`);
        if (kind === 'number') {
          const dst = new Float64Array(buf, off, n);
          dst.set(v.subarray ? v.subarray(at, end) : v.slice(at, end));
          off += 8 * MAX_FRAME_ROWS;
          continue;
        }
        if (v.codes) {
          new Uint32Array(buf, off, n).set(v.codes.subarray ? v.codes.subarray(at, end) : v.codes.slice(at, end));
          msg.labels[name] = v.labels;
        } else msg.strings[name] = v.slice(at, end);
        off += 4 * MAX_FRAME_ROWS;
      }
      t.worker.postMessage(msg);
      this.rowsSent += n;
    }
  }

  async close() {
    for (const t of this.threads) while (t.free.length < t.slots.length && !this.error) await new Promise((r) => this.waiting.push(r));
    await Promise.all(this.threads.map((t) => new Promise((resolve) => { t.onClosed = resolve; t.worker.postMessage({ type: 'close' }); })));
    await Promise.all(this.threads.map((t) => t.worker.terminate()));
    await new Promise((r) => this.server.close(r));
    this.#wake();
  }
}
