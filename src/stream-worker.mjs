// Worker side of streams. Rows arrive as CSV lines without a header, or as
// JSON lines (format: 'ndjson', one flat object per line), either
// from the page (streamPush) or from this worker's own TCP connection
// (streamConnect). They are parsed in batches of about 1 MB with the normal
// CSV parser and appended to growing columns in engine memory. Queries get a
// Table over the rows still kept (see keepRows / keepMs), so every split-table
// query (agg, groupBy, collect, toRows) works on a live stream unchanged.
import { Table } from './table.mjs';
import { parseNDJSON } from './ndjson.mjs';
import { ZONE_ROWS } from './zones.mjs';
import { FrameReader } from './frames.mjs';
import { ArrowReader } from './arrow.mjs';
import { parse, isMask } from './fuse.mjs';
import { SKETCH_SIZE, sketchAdd, sketchMerge, hashNumber, hashText } from './sketch.mjs';
import { csvTail, CSV_FIELD_START } from './csv-kernel.mjs';

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
const NL = 10, COMMA = 44;
const textEncoder = new TextEncoder();
const KINDS = { number: 'number', category: 'category', date: 'date' }; // date: ms (ISO text in CSV / JSON lines)

export function createStreams(getEngine) {
  const streams = new Map(); // id -> stream state

  // share: this worker's share of keepRows, for rows from connections (pushed
  // rows are kept by their place in push order over the whole stream, see trim).
  function open({ id, columns, format = 'csv', share = Infinity, keepMs = null, timeColumn = null }) {
    if (!['csv', 'ndjson', 'vectro', 'arrow'].includes(format)) throw new Error(`vectro: stream format must be 'csv', 'ndjson', 'vectro' or 'arrow', got "${format}"`);
    const engine = getEngine();
    const names = Object.keys(columns);
    for (const [name, kind] of Object.entries(columns)) {
      if (!KINDS[kind]) throw new Error(`vectro: stream column "${name}" must be 'number', 'category' or 'date', got "${kind}"`);
    }
    if (keepMs != null && !names.includes(timeColumn)) throw new Error('vectro: keepMs needs timeColumn, the name of a number column holding milliseconds');
    const cols = new Map(names.map((name) => [name, {
      data: engine.alloc(Float64Array, 1 << 16),
      dict: columns[name] === 'category' ? [] : null,
      codes: columns[name] === 'category' ? new Map() : null,
      date: columns[name] === 'date',
    }]));
    streams.set(id, {
      id, names, format, badLines: 0, windows: new Map(),
      types: columns, header: new TextEncoder().encode(`${names.join(',')}\n`),
      zones: new Map(names.map((name) => [name, { mins: [], maxs: [] }])), // per block of ZONE_ROWS buffer rows
      cols, cap: 1 << 16, n: 0, start: 0, share, keepMs, timeColumn, maxTime: -Infinity,
      pos: new Float64Array(1 << 16), cut: -Infinity, newest: -Infinity, connected: false, // see trim
      pending: [], marks: [], pendingBytes: 0, partial: null, quote: CSV_FIELD_START, timer: null, scheduled: false, sockets: [], waiters: [], paused: false,
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
      s.pos.copyWithin(0, s.start, s.n);
      s.n -= s.start; need -= s.start; s.start = 0;
      for (const z of s.zones.values()) { z.mins.length = 0; z.maxs.length = 0; }
      updateZones(s, 0, s.n); // rows moved: rebuild the zone maps
    }
    // keep at least as much free room as kept rows, so rows move at most once
    // per row added (compaction copies every kept row)
    if (need <= s.cap && s.cap - need >= Math.min(need, s.n)) return;
    const cap = Math.max(need * 2, s.cap * 2);
    for (const c of s.cols.values()) {
      const bigger = engine.alloc(Float64Array, cap);
      bigger.set(engine.live(c.data).subarray(0, s.n));
      engine.free(c.data);
      c.data = bigger;
    }
    const pos = new Float64Array(cap);
    pos.set(s.pos.subarray(0, s.n));
    s.pos = pos;
    s.cap = cap;
  }

  // Parse complete lines (CSV or JSON lines) and append them. marks: per
  // pushed chunk in `bytes`, [place of its first row in push order, rows].
  function ingest(s, bytes, marks) {
    if (!bytes.length) return;
    if (s.format === 'ndjson') {
      const r = parseNDJSON(bytes, 0, bytes.length, s.names, s.names.map((n) => s.types[n]));
      s.badLines += r.bad;
      append(s, r.rows, (k) => ({ data: r.cols[k], dict: r.dicts[k] }), bytes.length, marks);
      return;
    }
    const engine = getEngine();
    const buf = new Uint8Array(s.header.length + bytes.length);
    buf.set(s.header, 0);
    buf.set(bytes, s.header.length);
    const t = engine.parseCSV(buf, { types: s.types, sampleRows: 1, compress: false }); // types are known; a batch is copied in at once
    try {
      append(s, t.length, (k) => { const f = t.cols.get(s.names[k]); return { data: engine.live(f.data), dict: f.dict }; }, bytes.length, marks);
    } finally {
      t.dispose();
    }
  }

  // Append m parsed rows. part(k) gives column k's values (and, for
  // categories, the batch's own dictionary); it is called after growing,
  // because growing can move engine memory.
  function append(s, m, part, nbytes, marks = null) {
    const engine = getEngine();
    reserve(s, s.n + m);
    s.names.forEach((name, k) => {
      const c = s.cols.get(name), { data: src, dict } = part(k), dst = engine.live(c.data);
      if (!c.dict || !dict) { dst.set(src, s.n); return; } // numbers, or codes already in this stream's numbering
      // the batch has its own dictionary; renumber into the stream's
      const lut = dict.map((str) => {
        let g = c.codes.get(str);
        if (g === undefined) { g = c.dict.length; c.dict.push(str); c.codes.set(str, g); }
        return g;
      });
      for (let i = 0; i < m; i++) dst[s.n + i] = lut[src[i]];
    });
    if (s.timeColumn) {
      const ts = engine.live(s.cols.get(s.timeColumn).data);
      for (let i = s.n; i < s.n + m; i++) if (ts[i] > s.maxTime) s.maxTime = ts[i];
    }
    for (const w of s.windows.values()) updateWindow(s, w, s.n, m);
    updateZones(s, s.n, m);
    place(s, m, marks);
    s.n += m;
    s.rowsIn += m;
    s.bytesIn += nbytes;
    s.batches++;
    trim(s);
  }

  // Each new row's place in push order over the whole stream, from the
  // chunks' marks (Infinity for rows from connections). If the parser made
  // fewer rows than the page counted (blank or bad lines), the last rows get
  // the last places.
  function place(s, m, marks) {
    const pos = s.pos, from = s.n;
    let i = from + m;
    for (let k = (marks?.length ?? 0) - 1; k >= 0 && i > from; k--) {
      if (!marks[k]) continue;
      const [p, rows] = marks[k];
      for (let j = rows - 1; j >= 0 && i > from; j--) pos[--i] = p + j;
    }
    const rest = marks?.find(Boolean)?.[0] ?? Infinity;
    while (i > from) pos[--i] = rest;
  }

  // What is kept: pushed rows at or after place s.cut (the newest keepRows of
  // the whole stream; the page sends the cut with pushes and before queries),
  // this worker's share of keepRows from connections, and rows newer than the
  // stream's newest time minus keepMs (the newest over all workers, also from
  // the page, or this worker's own if that is newer).
  function trim(s) {
    while (s.start < s.n && s.pos[s.start] < s.cut) s.start++;
    if (s.connected && s.n - s.start > s.share) s.start = s.n - s.share;
    if (s.keepMs != null) {
      const ts = getEngine().live(s.cols.get(s.timeColumn).data), oldest = Math.max(s.maxTime, s.newest) - s.keepMs;
      while (s.start < s.n && ts[s.start] < oldest) s.start++;
      dropLate(s, oldest);
    }
  }
  // Rows that arrived late with an old time sit behind newer ones, where the
  // trim above doesn't reach. A block whose smallest time is older than the
  // cutoff holds one: the rows after it are moved up over such rows, in order.
  function dropLate(s, oldest) {
    const engine = getEngine(), z = s.zones.get(s.timeColumn), ts = engine.live(s.cols.get(s.timeColumn).data);
    let first = -1;
    for (let b = Math.floor(s.start / ZONE_ROWS); b * ZONE_ROWS < s.n && first < 0; b++) {
      if (!(z.mins[b] < oldest)) continue;
      for (let i = Math.max(s.start, b * ZONE_ROWS), e = Math.min(s.n, (b + 1) * ZONE_ROWS); i < e; i++) if (ts[i] < oldest) { first = i; break; }
    }
    if (first < 0) return;
    const keep = [];
    for (let i = first; i < s.n; i++) if (!(ts[i] < oldest)) keep.push(i); // (missing times are kept, as before)
    for (const c of s.cols.values()) { const d = engine.live(c.data); keep.forEach((r, j) => { d[first + j] = d[r]; }); }
    keep.forEach((r, j) => { s.pos[first + j] = s.pos[r]; });
    s.n = first + keep.length;
    const b0 = Math.floor(first / ZONE_ROWS); // (zone maps from that block on: rebuilt)
    for (const zz of s.zones.values()) { zz.mins.length = b0; zz.maxs.length = b0; }
    updateZones(s, b0 * ZONE_ROWS, s.n - b0 * ZONE_ROWS);
  }

  // Parse up to `limit` bytes of queued lines (everything by default).
  function flush(s, limit = Infinity) {
    clearTimeout(s.timer);
    s.timer = null;
    while (s.pendingBytes && limit > 0) {
      let take = 0, bytes = 0;
      while (take < s.pending.length && (take === 0 || bytes + s.pending[take].length <= Math.min(limit, BATCH_BYTES))) bytes += s.pending[take++].length;
      const part = s.pending.splice(0, take), marks = s.marks.splice(0, take);
      s.pendingBytes -= bytes;
      limit -= bytes;
      try { ingest(s, part.length === 1 ? part[0] : concat(part, bytes), marks); } catch (err) { s.error ??= err.message; }
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
  // mark: [place in push order, rows] for pushed chunks (see place).
  function queue(s, lines, mark = null) {
    if (!lines.length) return;
    s.pending.push(lines);
    s.marks.push(mark);
    s.pendingBytes += lines.length;
    schedule(s);
    if (s.pendingBytes > HIGH && !s.paused && s.sockets.length) { s.paused = true; for (const socket of s.sockets) socket.pause(); }
  }

  // Bytes cut anywhere: keep what follows the last row end for next time
  // (CSV: the last newline outside quotes, with the quote state carried on).
  function receive(s, chunk) {
    let last = chunk.lastIndexOf(NL);
    if (s.format === 'csv') { const r = csvTail(chunk, 0, chunk.length, COMMA, s.quote); s.quote = r.state; last = r.last === -1 ? -1 : r.last - 1; }
    if (last === -1) { s.partial = s.partial ? concat([s.partial, chunk]) : Uint8Array.from(chunk); return; }
    let lines = chunk.subarray(0, last + 1);
    if (s.partial) { lines = concat([s.partial, lines]); s.partial = null; }
    else lines = Uint8Array.prototype.slice.call(lines); // own copy (socket buffers get reused)
    if (last + 1 < chunk.length) s.partial = Uint8Array.prototype.slice.call(chunk, last + 1);
    queue(s, lines);
  }

  // Zone maps for rows [from, from + m): min and max per block of ZONE_ROWS
  // buffer rows, kept up to date as batches arrive, so filtered queries on
  // live data (usually time-ordered) skip old blocks. See zones.mjs.
  // Min and max skip NaN (x == x is false only for NaN). Long runs use one
  // compiled SIMD pass per block; short ones a plain loop.
  const ZONE_SPEC = ['min(x == x ? x : Infinity)', 'max(x == x ? x : -Infinity)'];
  function updateZones(s, from, m) {
    const engine = getEngine();
    for (const [name, z] of s.zones) {
      const col = s.cols.get(name);
      for (let i = from, end = from + m; i < end; ) {
        const b = Math.floor(i / ZONE_ROWS), stop = Math.min(end, (b + 1) * ZONE_ROWS);
        let lo = z.mins[b] ?? Infinity, hi = z.maxs[b] ?? -Infinity;
        if (stop - i >= 1024 && engine.jit) {
          const [a, c] = engine.eval(ZONE_SPEC, { x: engine.live(col.data).subarray(i, stop) });
          if (a < lo) lo = a;
          if (c > hi) hi = c;
        } else {
          const d = engine.live(col.data);
          for (let j = i; j < stop; j++) { const v = d[j]; if (v < lo) lo = v; if (v > hi) hi = v; }
        }
        z.mins[b] = lo; z.maxs[b] = hi;
        i = stop;
      }
    }
  }

  // ---- running totals over time windows ---------------------------------------
  // A window keeps, per time slice of `every` ms and per group, the partial
  // aggregates of every row that arrived (sum and count for means). Slices
  // older than `last` are dropped. Rows can be dropped (keepRows) while their
  // totals stay. Each batch is grouped by (slice, group) with the compiled
  // group-by: key = (slice - first slice in the batch) * groups + group code.
  const GROUP_KEYS = 2 ** 24;

  function openWindow(s, { wid, by = null, every, last, timeColumn, ask, fns, sketches = [] }) {
    const time = timeColumn ?? s.timeColumn;
    if (!s.cols.has(time)) throw new Error(`vectro: window needs timeColumn, a number column of milliseconds (columns: ${s.names.join(', ')})`);
    if (by != null && !s.cols.get(by)?.dict) throw new Error(`vectro: window "by" must be a category column, got "${by}"`);
    if (!(every > 0) || !(last >= every)) throw new Error('vectro: window needs every > 0 and last >= every (milliseconds)');
    const w = { by, every, slices: Math.ceil(last / every), time, ask, fns, buckets: new Map(), newest: -Infinity, sketches, sk: new Map() };
    s.windows.set(wid, w);
    if (s.n > s.start) updateWindow(s, w, s.start, s.n - s.start); // rows already kept count too
    return true;
  }

  // Add rows [from, from + m) of stream s to window w.
  function updateWindow(s, w, from, m) {
    if (!m) return;
    const engine = getEngine();
    const view = (extra) => {
      const cols = new Map();
      for (const [name, c] of s.cols) cols.set(name, { data: engine.live(c.data).subarray(from, from + m), dict: c.dict, codes: c.codes, strings: null, date: c.date });
      return new Table(engine, cols, m, { owned: [], zones: false });
    };
    const t = view();
    const { lo, hi } = t.filter(`${w.time} == ${w.time}`).agg({ lo: `min(${w.time})`, hi: `max(${w.time})` });
    if (!(lo <= hi)) return; // no row has a time
    const first = Math.floor(lo / w.every), G = w.by ? Math.max(1, s.cols.get(w.by).dict.length) : 1;
    const span = Math.floor(hi / w.every) - first + 1;
    const add = (slice, group, values) => {
      if (slice > w.newest) w.newest = slice;
      let b = w.buckets.get(slice);
      if (!b) w.buckets.set(slice, (b = new Map()));
      const acc = b.get(group);
      if (!acc) { b.set(group, values.slice()); return; }
      for (let j = 0; j < values.length; j++) { // (min / max of a part with no values is NaN: skip it)
        const f = w.fns[j], v = values[j];
        acc[j] = f === 'min' ? (v < acc[j] || acc[j] !== acc[j] ? v : acc[j]) : f === 'max' ? (v > acc[j] || acc[j] !== acc[j] ? v : acc[j]) : acc[j] + v;
      }
    };
    const askNames = Object.keys(w.ask);
    const label = (code) => (w.by ? s.cols.get(w.by).dict[code] : null);
    if (span * G < GROUP_KEYS) {
      const keyExpr = `${w.time} == ${w.time} ? (floor(${w.time} / __np_every) - __np_first) * __np_g + ${w.by ?? 0} : 0`;
      const keyed = t.derive('__np_key', keyExpr, { __np_every: w.every, __np_first: first, __np_g: G });
      try {
        // group-by returns only keys that have rows; a pass takes at most 7 aggregates
        const timed = keyed.filter(`${w.time} == ${w.time}`);
        const byKey = new Map();
        for (let i = 0; i < askNames.length; i += 7) {
          const part = askNames.slice(i, i + 7);
          const g = timed.groupBy('__np_key').agg(Object.fromEntries(part.map((a) => [a, w.ask[a]])));
          try {
            const keys = engine.live(g.cols.get('__np_key').data);
            const cols = part.map((a) => engine.live(g.cols.get(a).data));
            for (let r = 0; r < keys.length; r++) {
              let vals = byKey.get(keys[r]);
              if (!vals) byKey.set(keys[r], (vals = new Array(askNames.length)));
              cols.forEach((c, j) => { vals[i + j] = c[r]; });
            }
          } finally { g.dispose(); }
        }
        for (const [key, vals] of byKey) add(first + Math.floor(key / G), label(key % G), vals);
      } finally { keyed.dispose(); }
    } else {
      // times spread too far for one group-by: compute each argument, then add row by row
      const argCols = askNames.map((a) => {
        const src = w.ask[a], arg = src.slice(src.indexOf('(') + 1, -1).trim();
        return w.fns[askNames.indexOf(a)] === 'count' && !arg ? null : arg;
      });
      const isCondition = argCols.map((arg) => arg && isMask(parse(arg)));
      const derived = argCols.map((arg, j) => (arg ? t.derive(`__np_a${j}`, arg) : null));
      try {
        const ts = engine.live(s.cols.get(w.time).data).subarray(from, from + m);
        const codes = w.by ? engine.live(s.cols.get(w.by).data).subarray(from, from + m) : null;
        const vals = derived.map((d, j) => (d ? engine.live(d.cols.get(`__np_a${j}`).data) : null));
        const values = new Array(askNames.length);
        for (let i = 0; i < m; i++) {
          if (Number.isNaN(ts[i])) continue;
          for (let j = 0; j < values.length; j++) { // the table rules: missing values are skipped
            const v = vals[j] ? vals[j][i] : 1, f = w.fns[j];
            values[j] = f === 'count' ? (isCondition[j] ? (v !== 0 ? 1 : 0) : v === v ? 1 : 0)
              : v === v ? v : f === 'min' || f === 'max' ? NaN : 0;
          }
          add(Math.floor(ts[i] / w.every), label(codes ? codes[i] : 0), values);
        }
      } finally { for (const d of derived) d?.dispose(); }
    }
    if (w.sketches.length) updateSketches(s, w, t, from, m);
    // drop slices that fell out of the window
    for (const slice of w.buckets.keys()) if (slice <= w.newest - w.slices) w.buckets.delete(slice);
    for (const slice of w.sk.keys()) if (slice <= w.newest - w.slices) w.sk.delete(slice);
  }

  // Distinct-count sketches of a window: per (slice, group), one sketch per
  // approxDistinct (SKETCH_SIZE bytes each, side by side). Text hashes by
  // label, so every worker's dictionary gives the same hashes.
  function updateSketches(s, w, t, from, m) {
    const engine = getEngine(), J = w.sketches.length;
    const ts = engine.live(s.cols.get(w.time).data).subarray(from, from + m);
    const codes = w.by ? engine.live(s.cols.get(w.by).data).subarray(from, from + m) : null;
    const dictBy = w.by ? s.cols.get(w.by).dict : null;
    const derived = [];
    const vals = w.sketches.map(({ arg }, j) => {
      const name = arg.trim(), c = /^[A-Za-z_]\w*$/.test(name) ? s.cols.get(name) : null;
      if (c) return { data: engine.live(c.data).subarray(from, from + m), dict: c.dict };
      const d = t.derive(`__np_s${j}`, arg);
      derived.push(d);
      return { data: engine.live(d.cols.get(`__np_s${j}`).data), dict: null };
    });
    try {
      const hashes = vals.map((v) => (v.dict ? v.dict.map(hashText) : null)); // per label, once
      const cache = new Map();
      for (let i = 0; i < m; i++) {
        const tv = ts[i];
        if (tv !== tv) continue;
        const slice = Math.floor(tv / w.every), code = codes ? codes[i] : 0;
        if (slice > w.newest) w.newest = slice;
        let bySlice = cache.get(slice);
        if (!bySlice) cache.set(slice, (bySlice = []));
        let regs = bySlice[code];
        if (!regs) {
          let b = w.sk.get(slice);
          if (!b) w.sk.set(slice, (b = new Map()));
          const group = dictBy ? dictBy[code] : null;
          regs = b.get(group);
          if (!regs) b.set(group, (regs = new Uint8Array(J * SKETCH_SIZE)));
          bySlice[code] = regs;
        }
        for (let j = 0; j < J; j++) {
          const x = vals[j].data[i];
          if (hashes[j]) sketchAdd(regs, j * SKETCH_SIZE, hashes[j][x]);
          else if (x === x) sketchAdd(regs, j * SKETCH_SIZE, hashNumber(x));
        }
      }
    } finally {
      for (const d of derived) d.dispose();
    }
  }

  const handlers = {
    streamOpen: open,
    // A window's sketches from slice `since` on: merged per group, or per
    // (slice, group) with series. -> { keys: [[slice | null, group]], sketches }
    streamWindowSketches({ id, wid, since, series }) {
      const s = get(id), w = s.windows.get(wid);
      if (!w) throw new Error(`vectro: no window ${wid} on this stream`);
      flush(s);
      const J = w.sketches.length, out = new Map();
      for (const [slice, groups] of w.sk) {
        if (slice < since) continue;
        for (const [group, regs] of groups) {
          const key = series ? `${slice}\u0000${group}` : group;
          let e = out.get(key);
          if (!e) out.set(key, (e = { slice: series ? slice : null, group, regs: new Uint8Array(J * SKETCH_SIZE) }));
          sketchMerge(e.regs, 0, regs, 0);
          for (let j = 1; j < J; j++) sketchMerge(e.regs, j * SKETCH_SIZE, regs, j * SKETCH_SIZE);
        }
      }
      const list = [...out.values()];
      const sketches = new Uint8Array(list.length * J * SKETCH_SIZE);
      list.forEach((e, i) => sketches.set(e.regs, i * J * SKETCH_SIZE));
      return { keys: list.map((e) => [e.slice, e.group]), sketches };
    },
    streamWindow({ id, ...opts }) { return openWindow(get(id), opts); },
    // Every slice kept: [slice, group label, ...values in ask order].
    streamWindowRead({ id, wid }) {
      const s = get(id), w = s.windows.get(wid);
      if (!w) throw new Error(`vectro: no window ${wid} on this stream`);
      flush(s);
      const out = [];
      for (const [slice, groups] of w.buckets) for (const [group, acc] of groups) out.push([slice, group, ...acc]);
      return out;
    },
    streamWindowClose({ id, wid }) { return get(id).windows.delete(wid); },
    // Complete lines from the page (it cuts at newlines before routing).
    // The reply waits while the backlog is high, so the page slows down too.
    // pos, rows, cut: see place and trim.
    streamPush({ id, bytes, pos = 0, rows = 0, cut = -Infinity }) {
      const s = get(id);
      if (s.format === 'vectro' || s.format === 'arrow') throw new Error("vectro: binary streams take frames over connections: connect each worker to the sender (push() is for 'csv' and 'ndjson')");
      if (cut > s.cut) s.cut = cut;
      queue(s, bytes, [pos, rows]);
      return s.pendingBytes > HIGH ? new Promise((resolve) => s.waiters.push(resolve)) : true;
    },
    // This worker reads its own connection; nothing passes through the page.
    // url: a WebSocket (browsers and Node 22+); each message is one or more
    // whole lines. host + port: raw TCP (Node only), cut anywhere.
    async streamConnect({ id, url, host = '127.0.0.1', port }) {
      const s = get(id);
      s.connected = true;
      if (url) return connectWebSocket(s, url);
      const net = await import('node:net');
      return new Promise((resolve, reject) => {
        const socket = net.connect({ host, port }, () => resolve(true));
        if (s.format === 'vectro' || s.format === 'arrow') { const reader = frameReceiver(s); socket.on('data', (chunk) => receiveFrames(s, reader, chunk)); } else
        socket.on('data', (chunk) => receive(s, chunk));
        socket.on('end', () => { if (s.partial) { queue(s, concat([s.partial, Uint8Array.of(NL)])); s.partial = null; s.quote = CSV_FIELD_START; } flush(s); });
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
    // What the whole stream keeps, from the page (see trim): the keepRows cut
    // and the newest time over all workers.
    streamKeep({ id, cut, newest }) {
      const s = get(id);
      if (cut > s.cut) s.cut = cut;
      if (newest > s.newest) s.newest = newest;
      trim(s);
      return true;
    },
    // A copy of the rows kept right now, as a normal table `newId`.
    streamSnapshot({ id, newId, tables }) {
      const engine = getEngine(), s = get(id);
      flush(s);
      const n = s.n - s.start;
      const cols = new Map();
      for (const [name, c] of s.cols) {
        const data = engine.alloc(Float64Array, n);
        data.set(engine.live(c.data).subarray(s.start, s.n));
        cols.set(name, { data, dict: c.dict && [...c.dict], codes: c.dict && new Map(c.codes), strings: null, date: c.date });
      }
      for (const c of cols.values()) c.data = engine.live(c.data);
      tables.set(newId, new Table(engine, cols, n, { owned: [...cols.keys()] }));
      return n;
    },
  };

  // Binary Vectro frames (see frames.mjs) from one connection: number columns
  // are copied straight in; category codes go through a small table from
  // this connection's label numbers to the stream's.
  function frameReceiver(s) {
    let map = null; // stream column k -> index in the connection's schema
    const luts = s.names.map(() => []);
    const Reader = s.format === 'arrow' ? ArrowReader : FrameReader;
    return new Reader({
      onSchema(columns, info) {
        const names = Object.keys(columns);
        map = s.names.map((name) => {
          const at = names.indexOf(name);
          if (at < 0) throw new Error(`vectro: the sender doesn't send column "${name}"`);
          if (columns[name] !== (s.types[name] === 'date' ? 'number' : s.types[name])) throw new Error(`vectro: column "${name}" is ${columns[name] ?? info?.[at]?.type} in the sender but ${s.types[name]} in this stream`);
          return at;
        });
        return s.names; // (Arrow) read only these
      },
      onBatch(rows, columns) {
        const bytes = columns.reduce((a, c) => a + (c.values ? c.values.byteLength : c.codes.byteLength), 0);
        append(s, rows, (k) => {
          const col = columns[map[k]];
          if (col.values) return { data: col.values, dict: null };
          const c = s.cols.get(s.names[k]), lut = luts[k];
          for (let i = lut.length; i < col.labels.length; i++) { // labels new on this connection
            const label = col.labels[i];
            let g = c.codes.get(label);
            if (g === undefined) { g = c.dict.length; c.dict.push(label); c.codes.set(label, g); }
            lut.push(g);
          }
          const out = new Float64Array(rows);
          for (let i = 0; i < rows; i++) out[i] = lut[col.codes[i]];
          return { data: out, dict: null };
        }, bytes);
      },
    });
  }
  const receiveFrames = (s, reader, chunk) => {
    try { reader.push(chunk); } catch (err) { s.error ??= err.message; }
  };

  // WebSockets can't be paused (in browsers or Node), so there is no
  // backpressure here: a feed faster than the workers grows the backlog.
  function connectWebSocket(s, url) {
    if (typeof WebSocket !== 'function') throw new Error('vectro: this runtime has no WebSocket (Node 22+ and browsers do)');
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
      let opened = false;
      ws.onopen = () => { opened = true; resolve(true); };
      const reader = s.format === 'vectro' || s.format === 'arrow' ? frameReceiver(s) : null;
      ws.onmessage = (ev) => {
        if (reader) { if (typeof ev.data !== 'string') receiveFrames(s, reader, new Uint8Array(ev.data)); return; }
        const bytes = typeof ev.data === 'string' ? textEncoder.encode(ev.data) : new Uint8Array(ev.data);
        if (!bytes.length) return;
        queue(s, bytes[bytes.length - 1] === NL ? bytes : concat([bytes, Uint8Array.of(NL)]));
      };
      ws.onerror = () => { const err = new Error(`vectro: WebSocket ${url} failed`); s.error ??= err.message; if (!opened) reject(err); };
      ws.onclose = () => flush(s);
      s.sockets.push({ destroy: () => ws.close(), pause() {}, resume() {} });
    });
  }

  const stats = (s) => ({ rows: s.n - s.start, rowsIn: s.rowsIn, badLines: s.badLines, bytesIn: s.bytesIn, batches: s.batches, unparsedBytes: s.pendingBytes + (s.partial?.length ?? 0), error: s.error, maxTime: s.maxTime });

  return {
    handlers,
    has: (id) => streams.has(id),
    // A Table over the rows kept; it owns nothing, so disposing it is harmless.
    view(id) {
      const s = streams.get(id);
      if (!s) return null;
      const engine = getEngine();
      const cols = new Map();
      for (const [name, c] of s.cols) {
        const z = s.zones.get(name);
        cols.set(name, { data: engine.live(c.data).subarray(s.start, s.n), dict: c.dict, codes: c.codes, strings: null, date: c.date, zones: { block: ZONE_ROWS, shift: s.start, mins: z.mins, maxs: z.maxs } });
      }
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
