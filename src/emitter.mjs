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
import { createServer } from 'node:net';
import { encodeSchema, encodeEnd, BatchEncoder } from './frames.mjs';

const MAX_FRAME_ROWS = 65_536;        // larger batches are split into frames this size
const HIGH_WATER = 8 * 1024 * 1024;   // bytes queued on a connection before it counts as full

export class Emitter {
  constructor(server, columns) {
    this.server = server;
    this.columns = columns;
    this.conns = [];     // { socket, encoder }
    this.next = 0;       // round-robin position
    this.waiting = [];   // writers waiting for room or a connection
    this.rowsSent = 0;
  }

  static listen({ port = 0, host = '127.0.0.1', columns }) {
    if (!columns || !Object.keys(columns).length) throw new Error('vectro: Emitter.listen needs columns, e.g. { ts: "number", region: "category" }');
    new BatchEncoder(columns); // check the column kinds now
    const server = createServer();
    const em = new Emitter(server, columns);
    server.on('connection', (socket) => em.#add(socket));
    return new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => resolve(em));
    });
  }

  get port() { return this.server.address().port; }
  get connections() { return this.conns.length; }

  #add(socket) {
    socket.setNoDelay(true);
    const conn = { socket, encoder: new BatchEncoder(this.columns) };
    socket.write(encodeSchema(this.columns));
    socket.on('drain', () => this.#wake());
    const drop = () => { this.conns = this.conns.filter((c) => c !== conn); };
    socket.on('close', drop);
    socket.on('error', drop);
    this.conns.push(conn);
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
    for (const { socket } of this.conns) socket.end(encodeEnd());
    await new Promise((r) => this.server.close(r));
    this.#wake();
  }
}
