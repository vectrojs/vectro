// Worker side of streams. Rows arrive as CSV lines without a header, either
// from the page (streamPush) or from this worker's own TCP connection
// (streamConnect). They are parsed in batches of about 1 MB with the normal
// CSV parser and appended to growing columns in engine memory. Queries get a
// Table over the rows still kept (see keepRows / keepMs), so every split-table
// query (agg, groupBy, collect, toRows) works on a live stream unchanged.
import { Table } from './table.mjs';

const BATCH_BYTES = 256 * 1024; // parse once this much has arrived...
const FLUSH_MS = 20;         // ...or this long after the first unparsed line
// Parsing happens in its own event-loop turn, one batch per turn, never while
// receiving. A query that arrives meanwhile runs before the next batch, so it
// waits for at most one batch (a few ms), not for everything queued.
const nextTurn = globalThis.setImmediate ?? ((fn) => setTimeout(fn, 0));
// Backpressure: with more than HIGH bytes waiting to be parsed, sockets pause
// and push replies wait, until the backlog is down to LOW. TCP then slows the
// sender, instead of unparsed data piling up in memory.
const HIGH = 4 << 20, LOW = 1 << 20;
const NL = 10;
const KINDS = { number: 'number', category: 'category' };

export function createStreams(getEngine) {
  const streams = new Map(); // id -> stream state

  function open({ id, columns, keepRows = Infinity, keepMs = null, timeColumn = null }) {
    const engine = getEngine();
    const names = Object.keys(columns);
    for (const [name, kind] of Object.entries(columns)) {
      if (!KINDS[kind]) throw new Error(`vectro: stream column "${name}" must be 'number' or 'category', got "${kind}"`);
    }
    if (keepMs != null && !names.includes(timeColumn)) throw new Error('vectro: keepMs needs timeColumn, the name of a number column holding milliseconds');
    const cols = new Map(names.map((name) => [name, {
      data: engine.alloc(Float64Array, 1 << 16),
      dict: columns[name] === 'category' ? [] : null,
      codes: columns[name] === 'category' ? new Map() : null,
    }]));
    streams.set(id, {
      id, names, types: columns, header: new TextEncoder().encode(`${names.join(',')}\n`),
      cols, cap: 1 << 16, n: 0, start: 0, keepRows, keepMs, timeColumn, maxTime: -Infinity,
      pending: [], pendingBytes: 0, partial: null, timer: null, scheduled: false, sockets: [], waiters: [], paused: false,
      rowsIn: 0, bytesIn: 0, batches: 0, error: null,
    });
    return true;
  }

  const get = (id) => {
    const s = streams.get(id);
    if (!s) throw new Error(`vectro: no stream ${id} in this worker`);
    return s;
  };

  // Room for `need` rows: drop the rows no longer kept, then grow by doubling.
  function reserve(s, need) {
    const engine = getEngine();
    if (need <= s.cap) return;
    if (s.start > 0) { // move the kept rows to the front
      for (const c of s.cols.values()) { const d = engine.live(c.data); d.copyWithin(0, s.start, s.n); }
      s.n -= s.start; need -= s.start; s.start = 0;
    }
    if (need <= s.cap) return;
    const cap = Math.max(need, s.cap * 2);
    for (const c of s.cols.values()) {
      const bigger = engine.alloc(Float64Array, cap);
      bigger.set(engine.live(c.data).subarray(0, s.n));
      engine.free(c.data);
      c.data = bigger;
    }
    s.cap = cap;
  }

  // Parse complete lines and append them.
  function ingest(s, bytes) {
    if (!bytes.length) return;
    const engine = getEngine();
    const buf = new Uint8Array(s.header.length + bytes.length);
    buf.set(s.header, 0);
    buf.set(bytes, s.header.length);
    const t = engine.parseCSV(buf, { types: s.types, sampleRows: 1 }); // types are known
    try {
      const m = t.length;
      reserve(s, s.n + m);
      for (const [name, c] of s.cols) {
        const from = t.cols.get(name);
        const src = engine.live(from.data), dst = engine.live(c.data);
        if (!c.dict) { dst.set(src, s.n); continue; }
        // the batch has its own dictionary; renumber into the stream's
        const lut = from.dict.map((str) => {
          let g = c.codes.get(str);
          if (g === undefined) { g = c.dict.length; c.dict.push(str); c.codes.set(str, g); }
          return g;
        });
        for (let i = 0; i < m; i++) dst[s.n + i] = lut[src[i]];
      }
      if (s.timeColumn) {
        const ts = engine.live(s.cols.get(s.timeColumn).data);
        for (let i = s.n; i < s.n + m; i++) if (ts[i] > s.maxTime) s.maxTime = ts[i];
      }
      s.n += m;
      s.rowsIn += m;
      s.bytesIn += bytes.length;
      s.batches++;
    } finally {
      t.dispose();
    }
    // what is kept: the last keepRows rows, and rows newer than maxTime - keepMs
    if (s.n - s.start > s.keepRows) s.start = s.n - s.keepRows;
    if (s.keepMs != null) {
      const ts = engine.live(s.cols.get(s.timeColumn).data), oldest = s.maxTime - s.keepMs;
      while (s.start < s.n && ts[s.start] < oldest) s.start++;
    }
  }

  // Parse up to `limit` bytes of queued lines (everything by default).
  function flush(s, limit = Infinity) {
    clearTimeout(s.timer);
    s.timer = null;
    while (s.pendingBytes && limit > 0) {
      let take = 0, bytes = 0;
      while (take < s.pending.length && (take === 0 || bytes + s.pending[take].length <= Math.min(limit, BATCH_BYTES))) bytes += s.pending[take++].length;
      const part = s.pending.splice(0, take);
      s.pendingBytes -= bytes;
      limit -= bytes;
      try { ingest(s, part.length === 1 ? part[0] : concat(part, bytes)); } catch (err) { s.error ??= err.message; }
    }
    drained(s);
  }

  // One batch per event-loop turn while a full batch is waiting.
  function tick(s) {
    s.scheduled = false;
    if (!streams.has(s.id)) return;
    if (s.pendingBytes >= BATCH_BYTES) flush(s, BATCH_BYTES);
    schedule(s);
    drained(s);
  }

  // The backlog went down: resume sockets and answer waiting pushes.
  function drained(s) {
    if (s.pendingBytes > LOW) return;
    if (s.paused) { s.paused = false; for (const socket of s.sockets) socket.resume(); }
    for (const resolve of s.waiters.splice(0)) resolve(true);
  }
  function schedule(s) {
    if (s.pendingBytes >= BATCH_BYTES) {
      if (!s.scheduled) { s.scheduled = true; nextTurn(() => tick(s)); }
    } else if (s.pendingBytes && !s.timer) {
      s.timer = setTimeout(() => flush(s), FLUSH_MS);
    }
  }

  // Queue complete lines; they are parsed in later turns (see nextTurn).
  function queue(s, lines) {
    if (!lines.length) return;
    s.pending.push(lines);
    s.pendingBytes += lines.length;
    schedule(s);
    if (s.pendingBytes > HIGH && !s.paused && s.sockets.length) { s.paused = true; for (const socket of s.sockets) socket.pause(); }
  }

  // Bytes cut anywhere: keep what follows the last newline for next time.
  function receive(s, chunk) {
    const last = chunk.lastIndexOf(NL);
    if (last === -1) { s.partial = s.partial ? concat([s.partial, chunk]) : Uint8Array.from(chunk); return; }
    let lines = chunk.subarray(0, last + 1);
    if (s.partial) { lines = concat([s.partial, lines]); s.partial = null; }
    else lines = Uint8Array.prototype.slice.call(lines); // own copy (socket buffers get reused)
    if (last + 1 < chunk.length) s.partial = Uint8Array.prototype.slice.call(chunk, last + 1);
    queue(s, lines);
  }

  const handlers = {
    streamOpen: open,
    // Complete lines from the page (it cuts at newlines before routing).
    // The reply waits while the backlog is high, so the page slows down too.
    streamPush({ id, bytes }) {
      const s = get(id);
      queue(s, bytes);
      return s.pendingBytes > HIGH ? new Promise((resolve) => s.waiters.push(resolve)) : true;
    },
    // This worker reads its own TCP connection; nothing passes through the page.
    async streamConnect({ id, host = '127.0.0.1', port }) {
      const s = get(id);
      const net = await import('node:net');
      return new Promise((resolve, reject) => {
        const socket = net.connect({ host, port }, () => resolve(true));
        socket.on('data', (chunk) => receive(s, chunk));
        socket.on('end', () => { if (s.partial) { queue(s, concat([s.partial, Uint8Array.of(NL)])); s.partial = null; } flush(s); });
        socket.on('error', (err) => { s.error ??= err.message; reject(err); });
        s.sockets.push(socket);
      });
    },
    // Parse everything received so far, so the next query sees it.
    streamFlush({ id }) {
      const s = get(id);
      flush(s);
      return stats(s);
    },
    streamStats({ id }) { return stats(get(id)); },
    // A copy of the rows kept right now, as a normal table `newId`.
    streamSnapshot({ id, newId, tables }) {
      const engine = getEngine(), s = get(id);
      flush(s);
      const n = s.n - s.start;
      const cols = new Map();
      for (const [name, c] of s.cols) {
        const data = engine.alloc(Float64Array, n);
        data.set(engine.live(c.data).subarray(s.start, s.n));
        cols.set(name, { data, dict: c.dict && [...c.dict], codes: c.dict && new Map(c.codes), strings: null });
      }
      for (const c of cols.values()) c.data = engine.live(c.data);
      tables.set(newId, new Table(engine, cols, n, { owned: [...cols.keys()] }));
      return n;
    },
  };

  const stats = (s) => ({ rows: s.n - s.start, rowsIn: s.rowsIn, bytesIn: s.bytesIn, batches: s.batches, unparsedBytes: s.pendingBytes + (s.partial?.length ?? 0), error: s.error });

  return {
    handlers,
    has: (id) => streams.has(id),
    // A Table over the rows kept; it owns nothing, so disposing it is harmless.
    view(id) {
      const s = streams.get(id);
      if (!s) return null;
      const engine = getEngine();
      const cols = new Map();
      for (const [name, c] of s.cols) cols.set(name, { data: engine.live(c.data).subarray(s.start, s.n), dict: c.dict, codes: c.codes, strings: null });
      return new Table(engine, cols, s.n - s.start, { owned: [] });
    },
    close(id) {
      const s = streams.get(id);
      if (!s) return false;
      clearTimeout(s.timer);
      for (const resolve of s.waiters.splice(0)) resolve(false);
      for (const socket of s.sockets) socket.destroy();
      for (const c of s.cols.values()) getEngine().free(c.data);
      streams.delete(id);
      return true;
    },
  };
}

function concat(parts, total = parts.reduce((a, p) => a + p.length, 0)) {
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}
