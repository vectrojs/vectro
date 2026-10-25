// Split tables: multi-core analytics without shared memory, for pages that
// can't be cross-origin isolated. Each worker runs its own engine and owns a
// slice of every table; the page keeps only a small description (columns,
// dictionaries, rows per slice). A query sends its expression text to every
// worker, each one answers for its slice, and the page combines the small
// partial results:
//   sum, count -> added   min, max -> min/max   mean -> total sum / total count
//   groupBy    -> partial groups merged by key
// The API matches the async API of normal tables (aggAsync, groupBy().aggAsync,
// collectAsync, deriveAsync, toRowsAsync), so the same code runs on either.
//
// Rows are dealt out in blocks, round-robin (block 0 to worker 0, block 1 to
// worker 1, ...), not cut into one range per worker. Real data is often
// sorted (by date, by id), and a filter like "the last month" then matches
// in a few blocks on every worker instead of all in the last worker, so the
// filtered table that collect() makes is still spread over every worker.
// The page remembers rows per block ("segments") to put rows back in order.
import { Engine as LocalEngine } from './engine.mjs';
import { inspectCSV } from './csv.mjs';
import { encode, countByte } from './bytes.mjs';
import { parse, toSource } from './fuse.mjs';
import { mergeHits } from './vectors.mjs';
import { isCoded } from './table.mjs';
import { readArrow, writeArrow } from './arrow.mjs';

const NL = 10, QUOTE = 34;
const BLOCK_ROWS = 4096; // rows per block when dealing out arrays
const CSV_PIECE_BYTES = 64 * 1024, MAX_CSV_PIECES_PER_WORKER = 64;
const AGG_FNS = new Set(['sum', 'mean', 'min', 'max', 'count']);

// spawn() must return { post(msg, transfer), onMessage(fn), terminate() } and
// may add onError(fn): fn(error) when the worker crashes or is killed.
export class SplitEngine {
  constructor({ workers, spawn, engineOptions = {} }) {
    if (!spawn) throw new Error('vectro: SplitEngine needs a spawn() function (vectro/browser provides one)');
    this.size = Math.max(1, workers);
    this.workers = [];
    this.failed = []; // failed[i]: why worker i stopped (its tables are gone)
    this.pending = new Map(); // rid -> { resolve, reject, worker, op }
    this.rid = 0;
    this.nextTable = 0;
    this.localEngine = null;
    for (let i = 0; i < this.size; i++) {
      const w = spawn();
      w.onMessage((m) => {
        const p = this.pending.get(m.rid);
        if (!p) return;
        this.pending.delete(m.rid);
        if (m.ok) p.resolve(m.value); else p.reject(new Error(m.error));
      });
      w.onError?.((err) => this.#fail(i, err));
      this.workers.push(w);
    }
    this.ready = Promise.all(this.workers.map((_, i) => this._call(i, 'init', { engineOptions })));
  }

  // A worker died (often: the browser stopped it for using too much memory).
  // Its answers will never come, so everything waiting on it fails now.
  #fail(i, err) {
    if (this.failed[i]) return;
    this.failed[i] = new Error(`vectro: split-table worker ${i} stopped (${err?.message || 'no reason given; the browser may have run out of memory'}). Tables on this engine are incomplete; create a new SplitEngine.`);
    for (const [rid, p] of this.pending) if (p.worker === i) { this.pending.delete(rid); p.reject(this.failed[i]); }
  }

  _call(i, op, args, transfer = []) {
    if (this.failed[i]) return Promise.reject(this.failed[i]);
    const rid = ++this.rid;
    return new Promise((resolve, reject) => {
      this.pending.set(rid, { resolve, reject, worker: i, op });
      try {
        this.workers[i].post({ rid, op, args }, transfer);
      } catch (err) { // e.g. out of memory while copying the message
        this.pending.delete(rid);
        reject(err);
      }
    });
  }

  // A small single-thread engine on the page for results (group-by tables).
  get local() { return (this.localEngine ??= new LocalEngine()); }

  // Blocks of rows, dealt to workers round-robin: blocks[w] = [[start, end], ...].
  #blocks(n) {
    const count = Math.max(1, Math.ceil(n / BLOCK_ROWS));
    const blocks = Array.from({ length: Math.min(this.size, count) }, () => []);
    for (let b = 0; b < count; b++) blocks[b % blocks.length].push([b * BLOCK_ROWS, Math.min(n, (b + 1) * BLOCK_ROWS)]);
    return blocks;
  }

  // Build a table from columns: { price: Float64Array | number[], region: string[] }.
  // type: Float32Array stores every column as float32 (half the memory).
  // Text is encoded here once (codes + one dictionary), so only numbers travel
  // to the workers, moved without copying.
  async tableAsync(columns, { type = Float64Array } = {}) {
    await this.ready;
    const entries = Object.entries(columns);
    const len = (v) => (isCoded(v) ? v.codes.length : v.length);
    const n = entries[0] ? len(entries[0][1]) : 0;
    const text = {};
    for (const [name, v] of entries) {
      if (len(v) !== n) throw new RangeError(`vectro: column "${name}" has ${len(v)} rows, expected ${n}`);
      if (isCoded(v)) { // codes + labels: repeated labels share one code
        const dict = [], index = new Map(), lut = v.labels.map((s) => {
          let c = index.get(s);
          if (c === undefined) { c = dict.length; dict.push(s); index.set(s, c); }
          return c;
        });
        const codes = new Float64Array(n);
        for (let i = 0; i < n; i++) {
          const c = lut[v.codes[i]];
          if (c === undefined) throw new RangeError(`vectro: code ${v.codes[i]} at row ${i} has no label (${v.labels.length} labels)`);
          codes[i] = c;
        }
        text[name] = { codes, dict };
        continue;
      }
      if (ArrayBuffer.isView(v) || !v.length || typeof v[0] !== 'string') continue;
      const codes = new Float64Array(v.length), dict = [], index = new Map();
      for (let i = 0; i < v.length; i++) {
        let c = index.get(v[i]);
        if (c === undefined) { c = dict.length; dict.push(v[i]); index.set(v[i], c); }
        codes[i] = c;
      }
      text[name] = { codes, dict };
    }
    const id = ++this.nextTable;
    const blocks = this.#blocks(n);
    const gather = (src, list, Type) => { // this worker's blocks, back to back
      const out = new Type(list.reduce((a, [s, e]) => a + e - s, 0));
      let at = 0;
      for (const [s, e] of list) { out.set(ArrayBuffer.isView(src) ? src.subarray(s, e) : src.slice(s, e), at); at += e - s; }
      return out;
    };
    await Promise.all(blocks.map((list, w) => {
      const numbers = {}, textPart = {}, transfer = [];
      for (const [name, v] of entries) {
        let part;
        if (text[name]) {
          part = gather(text[name].codes, list, Float64Array);
          textPart[name] = { codes: part, dict: text[name].dict };
        } else {
          part = gather(v, list, ArrayBuffer.isView(v) ? v.constructor : Float64Array);
          numbers[name] = part;
        }
        transfer.push(part.buffer);
      }
      return this._call(w, 'loadEncoded', { id, order: entries.map(([name]) => name), numbers, text: textPart, float32: type === Float32Array }, transfer);
    }));
    const dicts = Object.fromEntries(Object.entries(text).map(([name, t]) => [name, t.dict]));
    const names = entries.map(([name]) => name);
    const segs = blocks.map((list) => Float64Array.from(list, ([s, e]) => e - s));
    return new SplitTable(this, id, segs, { names, dicts, strings: new Set() }, [], {}, true);
  }

  // Every worker encoded its own text; agree on one dictionary per column
  // (first appearance, slice by slice) and renumber each worker's codes.
  async #shareDictionaries(id, loaded) {
    const shared = {};
    const maps = loaded.map(() => ({}));
    for (const name of Object.keys(loaded[0].dicts ?? {})) {
      const dict = [], codes = new Map();
      loaded.forEach((r, w) => {
        const local = r.dicts[name] ?? [];
        const lut = new Float64Array(Math.max(1, local.length));
        local.forEach((str, i) => {
          let g = codes.get(str);
          if (g === undefined) { g = dict.length; dict.push(str); codes.set(str, g); }
          lut[i] = g;
        });
        maps[w][name] = { lut, dict };
      });
      shared[name] = dict;
    }
    if (Object.keys(shared).length) await Promise.all(maps.map((m, w) => this._call(w, 'remap', { id, maps: m })));
    return shared;
  }

  // Parse CSV text or bytes: the page cuts it at row boundaries (quote-aware)
  // and every worker parses its piece in parallel.
  // Load Arrow IPC bytes (stream or file format) as a split table.
  async fromArrowAsync(bytes, { columns, type } = {}) { return this.tableAsync(readArrow(bytes, { columns }).columns, { type }); }

  async parseCSVAsync(text, opts = {}) {
    await this.ready;
    const bytes = typeof text === 'string' ? encode(text) : text;
    const info = inspectCSV(bytes, opts);
    const types = Object.fromEntries(info.names.map((nm, f) => [nm, info.kinds[f]]));
    const head = bytes.subarray(info.headerStart, info.start);
    // Small pieces, dealt round-robin like array blocks. Each worker gets the
    // header plus its pieces back to back (every piece ends at a row end, and
    // the file's last piece, which may not, is the last one of its worker).
    const len = info.end - info.start;
    const pieces = Math.max(1, Math.min(this.size * MAX_CSV_PIECES_PER_WORKER, Math.ceil(len / CSV_PIECE_BYTES)));
    const cuts = cutRows(bytes, info.start, info.end, pieces);
    const workers = Math.min(this.size, cuts.length - 1);
    const mine = Array.from({ length: workers }, () => []);
    for (let p = 0; p + 1 < cuts.length; p++) mine[p % workers].push([cuts[p], cuts[p + 1]]);
    const id = ++this.nextTable;
    const loaded = await Promise.all(mine.map((list, w) => {
      const buf = new Uint8Array(head.length + list.reduce((a, [s, e]) => a + e - s, 0));
      buf.set(head, 0);
      let at = head.length;
      for (const [s, e] of list) { buf.set(bytes.subarray(s, e), at); at += e - s; }
      const sizes = list.map(([s, e]) => e - s);
      return this._call(w, 'loadCSV', { id, bytes: buf, pieces: sizes, opts: { ...opts, types: { ...types, ...(opts.types ?? {}) } } }, [buf.buffer]);
    }));
    const dicts = await this.#shareDictionaries(id, loaded);
    const segs = loaded.map((r) => Float64Array.from(r.segs));
    return new SplitTable(this, id, segs, { names: loaded[0].names, dicts, strings: new Set(loaded[0].strings) }, [], {}, true);
  }

  // A live table fed by a stream, spread over the workers. format: 'csv'
  // (lines, no header; columns in the order of the fields), 'ndjson' (one
  // flat JSON object per line; columns pick the keys) or 'vectro' (binary
  // column frames from an Emitter; connect each worker, no parsing).
  // columns: { name: 'number' | 'category' }. keepRows: rows kept in total (the oldest are
  // dropped); keepMs + timeColumn: keep rows newer than the newest time minus
  // keepMs. Feed it with push() from here, or connect() each worker to its own
  // TCP stream (Node), so the data never passes through this thread.
  async streamAsync({ columns, format = 'csv', keepRows = Infinity, keepMs = null, timeColumn = null } = {}) {
    await this.ready;
    if (!columns || !Object.keys(columns).length) throw new Error('vectro: streamAsync needs columns, e.g. { ts: "number", region: "category" }');
    const id = ++this.nextTable;
    const perWorker = Number.isFinite(keepRows) ? Math.ceil(keepRows / this.size) : Infinity;
    await Promise.all(this.workers.map((_, w) => this._call(w, 'streamOpen', { id, columns, format, keepRows: perWorker, keepMs, timeColumn })));
    const t = new StreamTable(this, id, Object.keys(columns));
    t.format = format;
    return t;
  }

  async close() {
    for (const w of this.workers) w.terminate();
    this.workers = [];
    await this.localEngine?.close();
  }
}

// `parts` + 1 cut points in [start, end), each at the start of a row. With
// quotes in the data, a newline inside a quoted field is not a row end, so
// the quotes before each cut are counted first.
function cutRows(bytes, start, end, parts) {
  const cuts = [start];
  const hasQuotes = bytes.indexOf(QUOTE, start) !== -1;
  let quotes = 0, counted = start;
  for (let k = 1; k < parts; k++) {
    let j = Math.max(cuts[k - 1], start + Math.floor(((end - start) * k) / parts));
    if (hasQuotes) {
      quotes += countByte(bytes, QUOTE, counted, j);
      counted = j;
      let inQuotes = quotes % 2 === 1;
      for (; j < end; j++) {
        const c = bytes[j];
        if (c === QUOTE) inQuotes = !inQuotes;
        else if (c === NL && !inQuotes) break;
      }
    } else {
      j = bytes.indexOf(NL, j);
      if (j === -1 || j >= end) j = end;
    }
    cuts.push(Math.min(end, j + 1));
  }
  // strictly increasing, ending at `end` (so no piece is empty unless the data is)
  const out = [start];
  for (const c of cuts.slice(1)) if (c > out[out.length - 1] && c < end) out.push(c);
  out.push(end);
  return out;
}

// 'mean(price)' -> what to ask each worker for, and how to combine answers.
// A mean becomes a sum and a count (hidden names __np_sum_<out>, __np_n_<out>).
function plan(spec) {
  const ask = {};
  const outs = Object.entries(spec).map(([out, src]) => {
    const ast = parse(src);
    if (ast.t !== 'call' || !AGG_FNS.has(ast.fn) || ast.args.length > 1 || (ast.fn !== 'count' && ast.args.length !== 1)) {
      throw new SyntaxError(`vectro: an aggregation must look like sum(expr), mean(expr), min(expr), max(expr) or count([expr]), got "${src}"`);
    }
    if (ast.fn === 'mean') {
      const arg = toSource(ast.args[0]);
      ask[`__np_sum_${out}`] = `sum(${arg})`;
      ask[`__np_n_${out}`] = 'count()';
      return { out, fn: 'mean' };
    }
    ask[out] = src;
    return { out, fn: ast.fn };
  });
  return { ask, outs };
}

const START = { sum: 0, count: 0, min: Infinity, max: -Infinity };
const COMBINE = { sum: (a, b) => a + b, count: (a, b) => a + b, min: Math.min, max: Math.max };

// Table order is round by round: block 0 of every worker (worker 0 first),
// then block 1 of every worker, and so on. Calls fn(worker, block, rows).
function inOrder(segs, fn) {
  const rounds = Math.max(0, ...segs.map((s) => s.length));
  for (let r = 0; r < rounds; r++) {
    for (let w = 0; w < segs.length; w++) if (r < segs[w].length && fn(w, r, segs[w][r]) === false) return;
  }
}

// Workers' columns, interleaved back into table order by their segments.
// Category labels are merged (stream workers each have their own).
function mergeColumns(parts) {
  const total = parts.reduce((a, p) => a + p.segs.reduce((x, y) => x + y, 0), 0);
  const names = Object.keys(parts[0]?.columns ?? {});
  const out = {}, luts = {};
  for (const name of names) {
    const first = parts[0].columns[name];
    if (ArrayBuffer.isView(first)) out[name] = new first.constructor(total);
    else if (first.codes) {
      const labels = [], index = new Map();
      luts[name] = parts.map((p) => Uint32Array.from(p.columns[name].labels, (s) => {
        let g = index.get(s);
        if (g === undefined) { g = labels.length; labels.push(s); index.set(s, g); }
        return g;
      }));
      out[name] = { codes: new Uint32Array(total), labels };
    } else out[name] = [];
  }
  let at = 0;
  const next = parts.map(() => 0);
  inOrder(parts.map((p) => p.segs), (w, _, k) => {
    const from = next[w];
    for (const name of names) {
      const src = parts[w].columns[name], dst = out[name];
      if (ArrayBuffer.isView(dst)) dst.set(src.subarray(from, from + k), at);
      else if (dst.codes) { const lut = luts[name][w]; for (let i = 0; i < k; i++) dst.codes[at + i] = lut[src.codes[from + i]]; }
      else for (let i = 0; i < k; i++) dst.push(src[from + i]);
    }
    next[w] += k;
    at += k;
  });
  return out;
}

export class SplitTable {
  constructor(engine, id, segs, meta, preds, params, owned) {
    this.engine = engine;
    this.id = id;
    this.segs = segs;     // segs[w][k]: rows in worker w's k-th block (see inOrder)
    this.counts = segs.map((s) => s.reduce((a, b) => a + b, 0)); // rows per worker
    this.meta = meta;     // { names, dicts: { text column: shared dictionary }, strings }
    this.preds = preds;   // filters, as { expr, params }, applied by the workers
    this.params = params;
    this.owned = owned;   // false for filter views of another table
  }

  get length() { return this.counts.reduce((a, b) => a + b, 0); } // rows before filtering
  get columnNames() { return this.meta.names; }

  #all(op, args, withSegs = false) {
    return Promise.all(this.counts.map((_, w) => this.engine._call(w, op, { id: this.id, preds: this.preds, ...args, ...(withSegs ? { segs: this.segs[w] } : {}) })));
  }

  // Lazy filter (applied by the workers on every query).
  filter(expr, params = {}) {
    return new SplitTable(this.engine, this.id, this.segs, this.meta, [...this.preds, { expr, params }], { ...this.params, ...params }, false);
  }

  async aggAsync(spec, params = {}) {
    const { ask, outs } = plan(spec);
    const parts = await this.#all('agg', { spec: ask, params });
    return Object.fromEntries(outs.map(({ out, fn }) => {
      if (fn === 'mean') {
        const s = parts.reduce((a, p) => a + p[`__np_sum_${out}`], 0);
        const n = parts.reduce((a, p) => a + p[`__np_n_${out}`], 0);
        return [out, s / n];
      }
      return [out, parts.reduce((a, p) => COMBINE[fn](a, p[out]), START[fn])];
    }));
  }

  async sumAsync(expr, params) { return (await this.aggAsync({ v: `sum(${expr})` }, params)).v; }
  async meanAsync(expr, params) { return (await this.aggAsync({ v: `mean(${expr})` }, params)).v; }
  async minAsync(expr, params) { return (await this.aggAsync({ v: `min(${expr})` }, params)).v; }
  async maxAsync(expr, params) { return (await this.aggAsync({ v: `max(${expr})` }, params)).v; }
  async countAsync(expr, params) { return (await this.aggAsync({ v: expr ? `count(${expr})` : 'count()' }, params)).v; }

  groupBy(key) {
    const keys = Array.isArray(key) ? key : [key];
    if (!keys.length) throw new Error('vectro: groupBy needs at least one key column');
    for (const k of keys) if (!this.meta.names.includes(k)) throw new Error(`vectro: no column "${k}"`);
    return { aggAsync: (spec, params = {}) => this.#groupAgg(keys, spec, params) };
  }

  // Each worker groups its slice; partial groups are merged here by key and
  // returned as a normal (small) table on the page.
  async #groupAgg(keys, spec, params) {
    const { ask, outs } = plan(spec);
    const askKeys = Object.keys(ask);
    const parts = await this.#all('groupBy', { keys, spec: ask, params });
    const index = new Map();
    const acc = askKeys.map(() => []);
    const fnOf = askKeys.map((k) => outs.find((o) => o.out === k)?.fn ?? (k.startsWith('__np_n_') ? 'count' : 'sum'));
    const groups = []; // key tuples
    const one = keys.length === 1;
    for (const { keyCols, cols } of parts) {
      const rows = keyCols[0].length;
      for (let r = 0; r < rows; r++) {
        const tuple = keyCols.map((c) => c[r]);
        const id = one ? tuple[0] : tuple.map((v) => (typeof v === 'string' ? JSON.stringify(v) : String(v))).join(','); // exact: NaN, -0, Infinity kept apart
        let g = index.get(id);
        if (g === undefined) {
          g = groups.length;
          index.set(id, g);
          groups.push(tuple);
          askKeys.forEach((_, j) => acc[j].push(START[fnOf[j]]));
        }
        askKeys.forEach((k, j) => { acc[j][g] = COMBINE[fnOf[j]](acc[j][g], cols[k][r]); });
      }
    }
    // Same group order as a normal table: dictionary order for text, numbers
    // ascending (NaN last), earlier keys first. Streams have no shared
    // dictionary: their labels sort A to Z.
    const ranks = keys.map((k) => { const d = this.meta.dicts[k]; return d ? new Map(d.map((s, i) => [s, i])) : null; });
    const cmp = (x, y, rank) => {
      if (rank) { x = rank.get(x); y = rank.get(y); }
      if (x === y) return 0;
      if (typeof x === 'number' && x !== x) return y !== y ? 0 : 1;
      if (typeof y === 'number' && y !== y) return -1;
      return x < y ? -1 : 1;
    };
    const order = groups.map((_, g) => g).sort((a, b) => {
      for (let k = 0; k < keys.length; k++) { const c = cmp(groups[a][k], groups[b][k], ranks[k]); if (c) return c; }
      return 0;
    });
    const col = (k) => order.map((g) => acc[askKeys.indexOf(k)][g]);
    const columns = {};
    keys.forEach((key, k) => { columns[key] = order.map((g) => groups[g][k]); });
    for (const { out, fn } of outs) {
      if (fn === 'mean') {
        const s = col(`__np_sum_${out}`), n = col(`__np_n_${out}`);
        columns[out] = s.map((v, i) => v / n[i]);
      } else columns[out] = col(out);
    }
    return this.engine.local.table(columns);
  }

  // The filtered rows as a new split table: each worker keeps its own
  // matches, which stay spread out because blocks are dealt round-robin.
  async collectAsync() {
    if (!this.preds.length) return this;
    const newId = ++this.engine.nextTable;
    const parts = await this.#all('collect', { newId }, true);
    return new SplitTable(this.engine, newId, parts.map((p) => p.segs), this.meta, [], {}, true);
  }

  async deriveAsync(name, expr, params = {}) {
    const newId = ++this.engine.nextTable;
    await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'derive', { id: this.id, newId, name, expr, params })));
    return new SplitTable(this.engine, newId, this.segs, { ...this.meta, names: [...this.meta.names, name] }, this.preds, this.params, true);
  }

  // The first k rows by `by` (see Table.top), as a normal table on the page:
  // every worker sends its own first k, and the page merges them. Rows that
  // tie on every key come in worker order.
  // columns: return only these (and read only these), e.g. for wide tables.
  async topAsync(k, by, { columns = null } = {}) {
    const parts = await this.#all('top', { k, by, columns });
    const keys = parts[0]?.keys ?? [];
    const cand = [];
    parts.forEach((p, w) => p.rows.forEach((row, i) => cand.push({ row, w, i, vals: p.values.map((v) => v && v[i]) })));
    const cmp = (a, b) => {
      for (let j = 0; j < keys.length; j++) {
        if (keys[j]) { // text: by label
          const x = a.row[keys[j].label], y = b.row[keys[j].label];
          if (x !== y) return (x < y ? -1 : 1) * (keys[j].desc ? -1 : 1);
          continue;
        }
        const x = a.vals[j], y = b.vals[j];
        if (x !== y) {
          if (x !== x) return 1; // NaN last
          if (y !== y) return -1;
          return x < y ? -1 : 1;
        }
      }
      return a.w - b.w || a.i - b.i;
    };
    const rows = cand.sort(cmp).slice(0, k).map((c) => c.row);
    const names = rows.length ? Object.keys(rows[0]) : (columns ?? this.meta.names);
    return this.engine.local.table(Object.fromEntries(names.map((name) => [name, rows.map((r) => r[name])])));
  }

  // Vector search (see VectorIndex): each worker searches its own rows in one
  // round trip; the page keeps the best k of all workers' hits.
  async vectorSearchAsync(query, opts) {
    const lists = await this.#all('vectorTop', { query, opts });
    return mergeHits(lists, opts.k, opts.metric);
  }

  // Each worker sends its first `limit` rows (in its order) and the matches
  // per block; walking the blocks in table order interleaves them back.
  async toRowsAsync(limit = Infinity) {
    const parts = await this.#all('toRows', { limit: Number.isFinite(limit) ? limit : undefined }, true);
    const rows = [], next = parts.map(() => 0);
    inOrder(parts.map((p) => p.segs), (w, _, n) => {
      for (let i = 0; i < n && rows.length < limit; i++) rows.push(parts[w].rows[next[w]++]);
      return rows.length < limit;
    });
    return rows;
  }

  // The rows (matching the filters) as plain columns on the page, in table
  // order (see Table.toColumns).
  async toColumnsAsync() {
    return mergeColumns(await this.#all('columns', {}, true));
  }

  // The rows (matching the filters) as Arrow IPC bytes (see Table.toArrow).
  async toArrowAsync({ file = false } = {}) { return writeArrow(await this.toColumnsAsync(), { file }); }

  async dispose() {
    if (!this.owned) return;
    this.owned = false;
    await this.#all('dispose', {});
  }
}

// A stream: queries see the rows each worker has parsed so far (call
// flushAsync() first to include rows still waiting in a batch). Rows have no
// global order across workers, so toRowsAsync returns worker 0's rows first.
export class StreamTable extends SplitTable {
  constructor(engine, id, names, preds = [], params = {}, owned = true) {
    super(engine, id, engine.workers.map(() => Float64Array.of(Infinity)), { names, dicts: {}, strings: new Set() }, preds, params, owned);
    this.partial = null;
    this.next = 0; // worker for the next pushed chunk
  }

  get length() { return NaN; } // changes all the time: use countAsync()

  // A filtered view of the live stream (still live).
  filter(expr, params = {}) {
    return new StreamTable(this.engine, this.id, this.meta.names, [...this.preds, { expr, params }], { ...this.params, ...params }, false);
  }

  async deriveAsync() {
    throw new Error('vectro: deriveAsync needs fixed rows; call snapshotAsync() (or collectAsync()) on the stream first');
  }

  // Bytes of CSV lines (no header), cut anywhere. Whole lines go to the
  // workers in turn; the promise resolves once the worker has queued them.
  push(chunk) {
    if (this.format === 'vectro' || this.format === 'arrow') return Promise.reject(new Error("vectro: binary streams take frames over connections: connect each worker to the sender (push() is for 'csv' and 'ndjson')"));
    const bytes = typeof chunk === 'string' ? encode(chunk) : chunk;
    const last = bytes.lastIndexOf(NL);
    if (last === -1) { this.partial = this.partial ? join(this.partial, bytes) : Uint8Array.prototype.slice.call(bytes); return Promise.resolve(); }
    const lines = this.partial ? join(this.partial, bytes.subarray(0, last + 1)) : Uint8Array.prototype.slice.call(bytes, 0, last + 1);
    this.partial = last + 1 < bytes.length ? Uint8Array.prototype.slice.call(bytes, last + 1) : null;
    const w = this.next;
    this.next = (this.next + 1) % this.counts.length;
    return this.engine._call(w, 'streamPush', { id: this.id, bytes: lines }, [lines.buffer]);
  }

  // Worker `worker` opens its own connection and reads lines from it:
  // { url: 'wss://…' } for a WebSocket (browsers, Node 22+; each message is
  // whole lines), or { host, port } for raw TCP (Node).
  connect(worker, { url, host = '127.0.0.1', port } = {}) {
    return this.engine._call(worker, 'streamConnect', { id: this.id, url, host, port });
  }

  // Parse whatever every worker has received; returns totals.
  async flushAsync() {
    if (this.partial) { const p = join(this.partial, Uint8Array.of(NL)); this.partial = null; await this.push(p); }
    return total(await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'streamFlush', { id: this.id }))));
  }

  async statsAsync() {
    return total(await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'streamStats', { id: this.id }))));
  }

  // A fixed copy of the rows kept right now (matching the filters, if any),
  // as a normal split table.
  async snapshotAsync() {
    if (this.preds.length) return super.collectAsync();
    const newId = ++this.engine.nextTable;
    const counts = await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'streamSnapshot', { id: this.id, newId })));
    return new SplitTable(this.engine, newId, counts.map((n) => Float64Array.of(n)), this.meta, [], {}, true);
  }

  async collectAsync() { return this.snapshotAsync(); }

  async toColumnsAsync() {
    const snap = await this.snapshotAsync();
    try { return await snap.toColumnsAsync(); } finally { await snap.dispose(); }
  }

  async toRowsAsync(limit = Infinity) {
    const parts = await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'toRows', { id: this.id, preds: this.preds, limit: Number.isFinite(limit) ? limit : undefined, segs: [Infinity] })));
    return parts.flatMap((p) => p.rows).slice(0, limit);
  }

  // Running totals over a time window, kept up to date as rows arrive:
  //   const w = await orders.windowAsync({ by: 'region', every: 1000, last: 300_000,
  //     timeColumn: 'ts', agg: { revenue: 'sum(price * qty)', orders: 'count()' } });
  //   await w.readAsync();                  // per region, over the last 5 minutes
  //   await w.readAsync({ series: true });  // per second and region, for charts
  // Workers keep one small total per (slice of `every` ms, group), so reading
  // costs nothing like a scan, and totals outlive rows dropped by keepRows.
  // The window ends at the newest time seen and covers whole slices.
  async windowAsync({ by = null, every, last, timeColumn = null, agg }) {
    const { ask, outs } = plan(agg);
    const fns = Object.keys(ask).map((k) => outs.find((o) => o.out === k)?.fn ?? (k.startsWith('__np_n_') ? 'count' : 'sum'));
    const wid = ++this.engine.nextTable;
    await Promise.all(this.counts.map((_, w) => this.engine._call(w, 'streamWindow', { id: this.id, wid, by, every, last, timeColumn, ask, fns })));
    return new LiveWindow(this, wid, { by, every, slices: Math.ceil(last / every), ask, fns, outs });
  }
}

class LiveWindow {
  constructor(stream, wid, spec) { this.stream = stream; this.wid = wid; this.spec = spec; }

  async readAsync({ series = false } = {}) {
    const { by, every, slices, ask, fns, outs } = this.spec;
    const engine = this.stream.engine, id = this.stream.id;
    const parts = await Promise.all(this.stream.counts.map((_, w) => engine._call(w, 'streamWindowRead', { id, wid: this.wid })));
    const newest = Math.max(-Infinity, ...parts.flat().map((r) => r[0]));
    const askNames = Object.keys(ask);
    const merged = new Map(); // series: slice\0group, totals: group
    for (const part of parts) {
      for (const [slice, group, ...values] of part) {
        if (slice <= newest - slices) continue;
        const key = series ? `${slice}\u0000${group}` : group;
        const acc = merged.get(key);
        if (!acc) { merged.set(key, { slice, group, values: values.slice() }); continue; }
        values.forEach((v, j) => { const f = fns[j]; acc.values[j] = f === 'min' ? Math.min(acc.values[j], v) : f === 'max' ? Math.max(acc.values[j], v) : acc.values[j] + v; });
      }
    }
    const rows = [...merged.values()].map(({ slice, group, values }) => {
      const row = series ? { t: slice * every } : {};
      if (by) row[by] = group;
      const val = (name) => values[askNames.indexOf(name)];
      for (const { out, fn } of outs) row[out] = fn === 'mean' ? val(`__np_sum_${out}`) / val(`__np_n_${out}`) : val(out);
      return row;
    });
    const label = (r) => (by ? r[by] : '');
    return rows.sort((a, b) => (series && a.t !== b.t ? a.t - b.t : label(a) < label(b) ? -1 : label(a) > label(b) ? 1 : 0));
  }

  async dispose() {
    await Promise.all(this.stream.counts.map((_, w) => this.stream.engine._call(w, 'streamWindowClose', { id: this.stream.id, wid: this.wid })));
  }
}

function join(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function total(list) {
  const out = { rows: 0, rowsIn: 0, badLines: 0, bytesIn: 0, batches: 0, unparsedBytes: 0, perWorker: list.map((s) => s.rows), errors: list.map((s) => s.error).filter(Boolean) };
  for (const s of list) for (const k of ['rows', 'rowsIn', 'badLines', 'bytesIn', 'batches', 'unparsedBytes']) out[k] += s[k];
  return out;
}
