// Datasets: tables bigger than memory, kept on disk (Node.js).
//
//   engine.csvToDataset('orders.csv', 'orders.vectro');   // a CSV of any size
//   const w = engine.createDataset('orders.vectro');      // or append tables
//   w.append(table); w.append(moreRows); w.close();
//
//   const orders = engine.openDataset('orders.vectro');
//   orders.filter("region == 'EU'").groupBy('day').agg({ revenue: 'sum(price * qty)' }).toRows();
//   sql('SELECT region, COUNT(*) FROM orders GROUP BY region', { orders }).toRows();
//
// The file holds chunks of rows (4M by default). Each column of each chunk
// is stored compressed, exactly as it sits in engine memory (pack.mjs), so
// loading a chunk is one read per column: no parsing, no decoding. A footer
// holds the schema, the category labels (one dictionary for the whole
// dataset) and the min / max of every column in every chunk.
//
// A query is the usual lazy plan (plan.mjs), run one chunk at a time:
//   - only the columns the plan uses are read, and chunks whose min / max
//     rule out the filters are not read at all;
//   - filters, derived columns and joins (the other table in memory) run on
//     each chunk with the normal kernels, on every thread;
//   - the first step that needs all rows takes partial results per chunk
//     and merges them: totals add up (a mean is a sum and a count), groups
//     are merged by key as they come (memory follows the groups, not the
//     rows), top-k keeps the best k of each chunk, limit stops reading
//     once it has its rows, collect concatenates the matching rows;
//   - everything after that step runs in memory on the merged result.
// With async calls (toRowsAsync, aggAsync...) the next chunk is read while
// the current one is computed.
//
// File layout: 'VECTRODS', version (u32), 0 (u32); column blobs (64-byte
// aligned); footer JSON; footer offset and length (f64 each); 'VECTRODS'.

import { Table, rewriteText } from './table.mjs';
import { Query, execute, findDataset, optimize, vars, namesOf, joinNames, joinCondColumns } from './plan.mjs';
import { parse, toSource, isMask } from './fuse.mjs';
import { KINDS, decodeOne } from './pack.mjs';
import { conditions, mayMatch, computeZones } from './zones.mjs';
import { STR_SHIFT, STR_ESCAPED, csvRowEnds } from './csv-kernel.mjs';
import { trackAsyncMethods } from './engine.mjs';
import { inspectCSV, loadBytes, loadBytesAsync, crOnlyToLF, checkCSVOptions } from './csv.mjs';
import { freeBuild } from './join.mjs';
import { copyRows, plainInto, remapCodes, textLength } from './rows.mjs';
export { copyRows };
import { OP } from './kernels.mjs';
import { bitPlan, packBits, unpackBits, unpackModule, seqModule } from './bitpack.mjs';

// Files: Node registers its own (files-node.mjs: paths, sync and worker-thread
// reads, writing, temporary folders for spilling). Without them (browsers,
// edge runtimes) datasets open from a File / Blob or a URL and are read
// asynchronously; writing and spilling need Node.
let files = null;
export function setFiles(f) { files = f; }
const need = (what) => {
  if (!files) throw new Error(`vectro: ${what} needs Node (here, datasets open read-only from a File, Blob or URL: openDatasetAsync)`);
  return files;
};

// Temporary files for spilling (huge group-bys, sorts, joins): Node's temp
// folder (from its files), or browser storage (files-browser.mjs: opfsTemp,
// registered by vectrojs/browser). Its calls may be async; the spilling code
// asks for them as { call } requests that the drivers resolve.
let tempStorage = null;
export function setTempStorage(t) { tempStorage = t; }
const temp = () => {
  const t = tempStorage ?? (files && { dir: files.tempDir, join: files.join, openWrite: (p) => files.openWrite(p), openRead: (p) => files.openRead(p), remove: files.remove });
  if (!t) throw new Error('vectro: this query needs to spill to disk (too many groups, a big sort or join), which needs Node or browser file storage');
  return t;
};
function* tempDataset(engine, path) { // (a spill file, written and closed)
  const file = yield { call: () => temp().openRead(path) };
  const footer = file.readSync ? readFooter(file) : yield { call: () => readFooterAsync(file) };
  return new Dataset(engine, file, footer);
}
function* tempWriter(engine, path, opts) { return new DatasetWriter(engine, yield { call: () => temp().openWrite(path) }, opts); }
function* closeWriter(w) { w.close(); yield { call: () => w.file.done?.() }; }
const removeTemp = (path) => { try { const r = tempStorage ? tempStorage.remove(path) : files?.remove(path); r?.catch?.(() => {}); } catch { /* (best effort) */ } };

const MAGIC = new TextEncoder().encode('VECTRODS');
const VERSION = 2; // (2: bit-packed columns; version 1 files read too)
const HEADER = 16, TAIL = 24, ALIGN = 64;
const SLACK = 16;
const READ_PIECE = 1 << 20; // reads are cut into pieces this big, read by several threads at once // bytes after packed codes (SIMD loads read a little past the end)
export const CHUNK_ROWS = 1 << 22;
const BLOCK = 1 << 16; // min / max per block of rows inside a chunk: a query reads only the blocks that can match
// partial groups are merged again once this many pile up (tests make it small)
// how often partial results went to disk (tests, benchmarks)
export const stats = { spills: 0, resplits: 0, joinPartitions: 0 };
export const tuning = { mergeRows: 1 << 20, denseMin: undefined, spillRows: 1 << 23, partitionRows: 1 << 24, joinRows: 1 << 24 };

// ---- writing --------------------------------------------------------------------------

export class DatasetWriter {
  // rowsPerChunk: rows per chunk (the last one may be shorter); append: add
  // to an existing dataset (same columns) instead of replacing it.
  // bitPack: false stores packed codes in whole bytes (bigger files, no unpacking).
  // path: a path (Node) or a writable file object (see createDatasetAsync).
  constructor(engine, path, { rowsPerChunk = CHUNK_ROWS, append = false, bitPack = true, footer = null } = {}) {
    this.engine = engine;
    this.bitPack = bitPack;
    this.path = typeof path === 'string' ? path : path.name;
    this.rowsPerChunk = Math.max(1, Math.floor(rowsPerChunk));
    this.parts = []; // rows waiting for a full chunk (tables this writer owns), copied together once
    this.pendingRows = 0; // (copying the waiting rows on every append made many small appends quadratic)
    this.meta = null;
    this.labels = new Map(); // category column -> { dict, codes }: one dictionary for the dataset
    this.file = typeof path === 'string' ? need('writing a dataset').openWrite(path, { append, atomic: true }) : path;
    if (this.file.existed) {
      if (!footer) try { footer = readFooter(this.file); } catch (err) { this.file.close(); throw err; }
      const { meta, footerStart } = footer;
      this.meta = meta;
      this.pos = footerStart; // new chunks overwrite the old footer
      const v = new Uint8Array(4);
      new DataView(v.buffer).setUint32(0, VERSION, true);
      this.file.write(v, 8); // (new chunks may use this version's features)
      meta.version = VERSION;
      for (const c of meta.columns) if (c.kind === 'category') this.labels.set(c.name, { dict: c.dict, codes: new Map(c.dict.map((s, i) => [s, i])) });
    } else {
      const head = new Uint8Array(HEADER);
      head.set(MAGIC);
      new DataView(head.buffer).setUint32(8, VERSION, true);
      this.file.write(head, 0);
      this.pos = HEADER;
    }
  }

  // Add rows: a Table (only the rows matching its filter; or its rows
  // lo..hi) or { name: values }.
  append(source, lo = 0, hi = Infinity) {
    const { engine } = this;
    let t = source instanceof Table ? source : engine.table(source, { compress: false });
    const temps = t === source ? [] : [t];
    try {
      if (t.pred) { t = t.collect(); temps.push(t); }
      this.#schema(t);
      const fit = this.#conform(t);
      if (fit) { t = fit; temps.push(t); }
      hi = Math.min(hi, t.n);
      while (this.pendingRows + hi - lo >= this.rowsPerChunk) { // a full chunk: the waiting rows and the first of these
        const take = this.rowsPerChunk - this.pendingRows;
        if (!this.parts.length) this.#writeChunk(t, lo, lo + take);
        else {
          const all = copyRows(engine, [...this.parts.map((p) => [p, 0, p.n]), [t, lo, lo + take]]);
          try { this.#writeChunk(all, 0, all.n); } finally { all.dispose(); this.#dropParts(); }
        }
        lo += take;
      }
      if (lo < hi || (!this.meta.chunks.length && !this.parts.length)) {
        this.parts.push(copyRows(engine, [[t, lo, hi]]));
        this.pendingRows += hi - lo;
      }
    } finally {
      for (const x of temps) x.dispose();
    }
    return this;
  }

  // Write what's left and the footer. Returns { path, rows, chunks, bytes }.
  close() {
    if (this.summary) return this.summary;
    if (this.aborted) throw new Error('vectro: this dataset writer was aborted');
    let closed = false;
    try {
      if (!this.meta) throw new Error('vectro: append at least one table to a dataset (it can have no rows)');
      if (this.parts.length && (this.pendingRows || !this.meta.chunks.length)) {
        const all = this.parts.length === 1 ? this.parts[0] : copyRows(this.engine, this.parts.map((p) => [p, 0, p.n]));
        try { this.#writeChunk(all, 0, all.n); } finally { if (all !== this.parts[0]) all.dispose(); }
      }
      for (const c of this.meta.columns) if (c.kind === 'category') c.dict = this.labels.get(c.name).dict;
      const json = new TextEncoder().encode(JSON.stringify(this.meta));
      const { off } = this.#write(json);
      const tail = new Uint8Array(TAIL), dv = new DataView(tail.buffer);
      dv.setFloat64(0, off, true);
      dv.setFloat64(8, json.length, true);
      tail.set(MAGIC, 16);
      this.file.write(tail, this.pos);
      this.pos += TAIL;
      this.file.truncate(this.pos);
      this.file.close();
      closed = true;
      this.file.commit?.(); // (a new file in Node: written beside the path, now put in its place)
      this.summary = { path: this.path, rows: this.meta.rows, chunks: this.meta.chunks.length, bytes: this.pos };
      return this.summary;
    } finally {
      this.#dropParts();
      if (!closed) { this.file.close(); this.file.discard?.(); }
    }
  }

  // Stop without a footer: a new file is discarded (in Node and browsers), an
  // existing one appended to is left unreadable as a dataset.
  abort() {
    if (this.summary || this.aborted) return;
    this.aborted = true;
    this.#dropParts();
    if (this.file.abort) this.file.abort(); else this.file.close();
  }

  // The same as append / close, then waiting until the bytes are written
  // (browser files write in the background; in Node they already are).
  async appendAsync(source, lo, hi) { this.append(source, lo, hi); await this.file.flush?.(); return this; }
  async closeAsync() { const summary = this.close(); await this.file.done?.(); return summary; }

  #dropParts() {
    for (const p of this.parts) p.dispose();
    this.parts = [];
    this.pendingRows = 0;
  }

  #schema(t) {
    const kindOf = (c) => (c.strings ? 'text' : c.dict ? 'category' : c.date ? 'date' : 'number');
    const names = t.columnNames;
    if (!this.meta) {
      this.meta = { version: VERSION, type: t.type === Float32Array ? 'f32' : 'f64', rows: 0, columns: names.map((name) => ({ name, kind: kindOf(t.cols.get(name)) })), chunks: [] };
      for (const c of this.meta.columns) if (c.kind === 'category') this.labels.set(c.name, { dict: [], codes: new Map() });
      return;
    }
    const want = this.meta.columns;
    if (names.length !== want.length || want.some((c) => !t.cols.has(c.name))) {
      throw new Error(`vectro: appended columns (${names.join(', ')}) differ from the dataset's (${want.map((c) => c.name).join(', ')})`);
    }
  }

  // Columns of t whose kind isn't the dataset's but lose nothing as its kind: text as categories or
  // free text (either way round), and a column with no values (no rows, or all missing: a plain []
  // or [null, null] makes a number column). Returns a table of the dataset's kinds, or null. (Such a
  // batch was refused: "column "note" is number here but category in the dataset".)
  #conform(t) {
    const kindOf = (c) => (c.strings ? 'text' : c.dict ? 'category' : c.date ? 'date' : 'number');
    const fix = this.meta.columns.filter((c) => kindOf(t.cols.get(c.name)) !== c.kind);
    if (!fix.length) return null;
    const { engine } = this, n = t.n, plain = t.toColumns();
    const cols = new Map(t.cols), owned = [], ownedBuffers = [];
    const out = new Table(engine, cols, n, { owned, ownedBuffers, zones: false }); // (t's other columns as they are; t outlives it)
    try {
      for (const c of fix) {
        const k = kindOf(t.cols.get(c.name)), v = plain[c.name];
        const texts = v.codes ? Array.from(v.codes, (i) => v.labels[i]) : Array.isArray(v) ? v : null;
        const empty = texts ? texts.every((s) => s === '') : v.every((x) => x !== x);
        if (!empty && !(texts && (c.kind === 'category' || c.kind === 'text'))) throw new Error(`vectro: column "${c.name}" is ${k} here but ${c.kind} in the dataset`);
        let col;
        if (c.kind === 'number' || c.kind === 'date') col = { data: engine.alloc(t.type, n).fill(NaN), dict: null, codes: null, date: c.kind === 'date' };
        else if (c.kind === 'category') {
          const codes = new Map([['', 0]]), dict = [''], data = engine.alloc(t.type, n);
          for (let i = 0; i < n; i++) {
            const s = texts ? texts[i] : '';
            let code = codes.get(s);
            if (code === undefined) { code = dict.length; dict.push(s); codes.set(s, code); }
            data[i] = code;
          }
          col = { data, dict, codes };
        } else col = freeText(engine, texts ?? new Array(n).fill(''));
        cols.set(c.name, col);
        owned.push(c.name);
        if (col.strings) ownedBuffers.push(col.strings);
      }
    } catch (err) { out.dispose(); throw err; }
    return out;
  }

  // Rows [lo, hi) of t as one chunk: plain values (text as dataset codes),
  // min / max, then packed with the kernels and written column by column.
  #writeChunk(t, lo, hi) {
    const { engine } = this;
    const n = hi - lo, T = this.meta.type === 'f32' ? Float32Array : Float64Array, f32 = T === Float32Array;
    const chunk = { rows: n, cols: {} };
    const numeric = this.meta.columns.filter((m) => m.kind !== 'text');
    const cols = new Map(numeric.map((m) => [m.name, { data: engine.alloc(T, n), dict: null, codes: null }]));
    const ct = new Table(engine, cols, n, { owned: [...cols.keys()], zones: false });
    try {
      for (const m of this.meta.columns) {
        const c = t.cols.get(m.name);
        if (m.kind === 'text') { chunk.cols[m.name] = this.#writeText(c, lo, hi); continue; }
        const out = engine.live(cols.get(m.name).data);
        plainInto(engine, c, lo, hi, out, f32);
        if (m.kind === 'category') remapCodes(out, c.dict, this.labels.get(m.name));
      }
      const dates = new Set(this.meta.columns.filter((m) => m.kind === 'date').map((m) => m.name));
      for (const [name, c] of cols) {
        c.data = engine.live(c.data);
        const date = dates.has(name);
        const [min, max, missing, offDay = 0] = n ? engine.eval(['min(v == v ? v : Infinity)', 'max(v == v ? v : -Infinity)', 'count(v != v)', ...(date ? ['count(v == v && v != floor(v / 86400000) * 86400000)'] : [])], { v: c.data }) : [Infinity, -Infinity, 0];
        chunk.cols[name] = { min: jsonNumber(min), max: jsonNumber(max), missing };
        if (date) chunk.cols[name].day = offDay === 0; // (every time at midnight: a column of days, so DATE - DATE is days)
        if (n > BLOCK) { const z = computeZones(engine.live(c.data), n, BLOCK); chunk.cols[name].blocks = { min: Array.from(z.mins, jsonNumber), max: Array.from(z.maxs, jsonNumber) }; }
      }
      if (n && cols.size) ct.compress(); // (no number or category columns: nothing to pack)
      for (const [name, c] of cols) {
        const d = engine.live(c.data);
        const seq = c.enc && !chunk.cols[name].missing ? sequence(d, n) : null;
        if (seq) { Object.assign(chunk.cols[name], { off: 0, len: 0, enc: c.enc, seq }); continue; } // (nothing to store: rebuilt on read)
        const plan = c.enc && this.bitPack ? bitPlan(d, n, KINDS[c.enc.kind].nan, KINDS[c.enc.kind].bytes * 8) : null;
        if (plan) { // each code in only the bits it needs
          Object.assign(chunk.cols[name], this.#write(packBits(d, n, plan)), { enc: c.enc, bits: plan.bits, bbase: plan.base }, plan.missing !== null ? { bmissing: plan.missing } : {});
          continue;
        }
        const bytes = c.enc ? KINDS[c.enc.kind].bytes * n : d.BYTES_PER_ELEMENT * n;
        Object.assign(chunk.cols[name], this.#write(new Uint8Array(d.buffer, d.byteOffset, bytes)), c.enc ? { enc: c.enc } : {});
      }
    } finally {
      ct.dispose();
    }
    this.meta.chunks.push(chunk);
    this.meta.rows += n;
  }

  // Free text: the chunk's own bytes (only the values it uses), and each
  // value's position in them.
  #writeText(c, lo, hi) {
    const { engine } = this;
    const ptrs = engine.live(c.data), bytes = engine.live(c.strings);
    let total = 0;
    for (let i = lo; i < hi; i++) { const v = ptrs[i]; if (v === v) total += textLength(v); }
    const outPtr = new Float64Array(hi - lo), outBytes = new Uint8Array(total);
    for (let i = lo, at = 0; i < hi; i++) {
      const v = ptrs[i];
      if (v !== v) { outPtr[i - lo] = NaN; continue; }
      const start = Math.floor(v / STR_SHIFT), field = v - start * STR_SHIFT, len = textLength(v);
      outBytes.set(bytes.subarray(start, start + len), at);
      outPtr[i - lo] = at * STR_SHIFT + field;
      at += len;
    }
    const p = this.#write(new Uint8Array(outPtr.buffer)), s = this.#write(outBytes);
    return { off: p.off, len: p.len, soff: s.off, slen: s.len };
  }

  #write(u8) {
    const pad = (ALIGN - (this.pos % ALIGN)) % ALIGN;
    if (pad) { this.file.write(new Uint8Array(pad), this.pos); this.pos += pad; }
    const off = this.pos;
    this.file.write(u8, off);
    this.pos += u8.length;
    return { off, len: u8.length };
  }
}

// Strings as a free-text column: their UTF-8 bytes, and each value's place in them ('' is missing).
function freeText(engine, texts) {
  const enc = new TextEncoder(), parts = texts.map((s) => (s === '' ? null : enc.encode(s)));
  const total = parts.reduce((a, b) => a + (b ? b.length : 0), 0);
  for (const b of parts) if (b && b.length >= STR_ESCAPED) throw new Error(`vectro: a free-text value of ${b.length} bytes is over the ${STR_ESCAPED - 1} a free-text column holds`);
  const data = engine.alloc(Float64Array, texts.length), strings = engine.alloc(Uint8Array, total);
  const d = engine.live(data), sb = engine.live(strings);
  for (let i = 0, at = 0; i < parts.length; i++) {
    const b = parts[i];
    if (!b) { d[i] = NaN; continue; }
    sb.set(b, at);
    d[i] = at * STR_SHIFT + b.length;
    at += b.length;
  }
  return { data, dict: null, codes: null, strings };
}

// Codes first, first + step, first + 2 * step... (row numbers, times at a
// fixed interval): { first, step }, else null.
function sequence(codes, n) {
  if (n < 2) return null;
  const first = codes[0], step = codes[1] - first;
  for (let i = 2; i < n; i++) if (codes[i] !== first + i * step) return null;
  return { first, step };
}

const jsonNumber = (v) => (Number.isFinite(v) ? v : String(v)); // JSON has no Infinity
const fromJson = (v) => (typeof v === 'string' ? Number(v) : v);

// ---- reading --------------------------------------------------------------------------

// The footer of a dataset file: { meta, footerStart, size }. A generator
// that yields { read: bytes, pos } (filled by readFooter / readFooterAsync).
function* footerG(file) {
  const { size, name } = file;
  const head = new Uint8Array(HEADER), tail = new Uint8Array(TAIL);
  if (size < HEADER + TAIL) throw new Error(`vectro: ${name} is not a Vectro dataset (or wasn't closed)`);
  yield { read: head, pos: 0 };
  yield { read: tail, pos: size - TAIL };
  if (!MAGIC.every((b, i) => head[i] === b && tail[16 + i] === b)) throw new Error(`vectro: ${name} is not a Vectro dataset (or wasn't closed)`);
  const version = new DataView(head.buffer).getUint32(8, true);
  if (version < 1 || version > VERSION) throw new Error(`vectro: ${name} is dataset version ${version}; this Vectro reads versions 1 to ${VERSION}`);
  const dv = new DataView(tail.buffer);
  const footerStart = dv.getFloat64(0, true), len = dv.getFloat64(8, true);
  const damaged = () => new Error(`vectro: ${name} is damaged: its footer (the schema and where each chunk is) can't be read`);
  if (!Number.isInteger(footerStart) || !Number.isInteger(len) || footerStart < HEADER || footerStart + len + TAIL !== size) throw damaged(); // (a raw "position NaN" or JSON error before)
  const json = new Uint8Array(len);
  yield { read: json, pos: footerStart };
  let meta;
  try { meta = JSON.parse(new TextDecoder().decode(json)); } catch { throw damaged(); }
  if (!Array.isArray(meta?.columns) || !Array.isArray(meta.chunks)) throw damaged();
  const place = (v) => v === undefined || (Number.isInteger(v) && v >= 0 && v <= size);
  for (const c of meta.columns) if (typeof c?.name !== 'string' || !['number', 'date', 'category', 'text'].includes(c.kind) || (c.kind === 'category' && !Array.isArray(c.dict))) throw damaged();
  for (const ch of meta.chunks) {
    if (!Number.isInteger(ch?.rows) || ch.rows < 0 || !ch.cols || typeof ch.cols !== 'object') throw damaged();
    for (const c of meta.columns) {
      const m = ch.cols[c.name];
      if (!m || typeof m !== 'object' || m.off === undefined || m.len === undefined || !place(m.off) || !place(m.len) || (m.off + m.len > size) || !place(m.soff) || !place(m.slen) || (m.enc && !KINDS[m.enc.kind])) throw damaged();
    }
  }
  return { meta, footerStart, size };
}

function readFooter(file) {
  const g = footerG(file);
  for (let step = g.next(); ; ) {
    if (step.done) return step.value;
    file.readSync(step.value.read, step.value.pos);
    step = g.next();
  }
}

async function readFooterAsync(file) {
  const g = footerG(file);
  for (let step = g.next(); ; ) {
    if (step.done) return step.value;
    await file.read(step.value.read, step.value.pos);
    step = g.next();
  }
}

// A dataset in a Blob or File (a browser's file input or drop; anything with
// slice()): read-only, async.
export function blobFile(blob, name = blob.name ?? 'dataset') {
  return {
    name,
    size: blob.size,
    async read(u8, pos) { u8.set(new Uint8Array(await blob.slice(pos, pos + u8.length).arrayBuffer())); },
    close() {},
  };
}

// A dataset at a URL, read with HTTP range requests (the server must answer
// "Range: bytes=a-b" with 206): only the parts a query needs are fetched.
export async function urlFile(url, { fetch: get = globalThis.fetch, headers = {} } = {}) {
  const probe = await get(url, { headers: { ...headers, Range: 'bytes=0-0' } });
  const range = probe.headers.get('content-range'); // bytes 0-0/12345
  await probe.body?.cancel?.();
  if (probe.status !== 206 || !range) throw new Error(`vectro: ${url} doesn't answer range requests (HTTP ${probe.status}); serve it with Range support, or download it first`);
  const size = Number(range.split('/')[1]);
  return {
    name: url,
    size,
    pieceBytes: 8 << 20, // (fewer, bigger requests)
    coalesce: 1 << 16,   // neighbouring reads closer than this become one request
    async read(u8, pos) {
      if (!u8.length) return;
      const res = await get(url, { headers: { ...headers, Range: `bytes=${pos}-${pos + u8.length - 1}` } });
      if (res.status !== 206) throw new Error(`vectro: ${url}: HTTP ${res.status} for a range request`);
      const got = new Uint8Array(await res.arrayBuffer());
      if (got.length !== u8.length) throw new Error(`vectro: ${url}: expected ${u8.length} bytes, got ${got.length}`);
      u8.set(got);
    },
    close() {},
  };
}

// A dataset from a path (Node), a URL, a File / Blob or a file object (see
// blobFile): reads its footer and returns the Dataset.
export async function openDatasetAsync(engine, source) {
  let file = source;
  if (typeof source === 'string') file = /^https?:/i.test(source) || !files ? await urlFile(source) : files.openRead(source);
  else if (typeof Blob === 'function' && source instanceof Blob) file = blobFile(source);
  let footer;
  try { footer = file.readSync ? readFooter(file) : await readFooterAsync(file); } catch (err) { file.close(); throw err; }
  return new Dataset(engine, file, footer);
}

const bytesOf = (a, len) => new Uint8Array(a.buffer, a.byteOffset, len);

export class Dataset {
  // source: a path (Node), or a file object from blobFile / urlFile (those
  // need openDatasetAsync, which reads the footer first).
  constructor(engine, source, footer = null) {
    const file = typeof source === 'string' ? need('opening a dataset by path').openRead(source) : source;
    if (!footer) {
      if (!file.readSync) throw new Error('vectro: open this dataset with openDatasetAsync (it is read asynchronously)');
      try { footer = readFooter(file); } catch (err) { file.close(); throw err; }
    }
    const { meta, size } = footer;
    this.engine = engine;
    this.file = file;
    this.path = file.name;
    this.meta = meta;
    this.bytes = size;
    this.isDataset = true;
    this.type = meta.type === 'f32' ? Float32Array : Float64Array;
    // column info the planner and SQL look at (labels, dates, free text)
    this.cols = new Map(meta.columns.map((c) => [c.name, c.kind === 'category'
      ? { dict: c.dict, codes: new Map(c.dict.map((s, i) => [s, i])) }
      : { dict: null, codes: null, date: c.kind === 'date', strings: c.kind === 'text' || undefined }]));
    this.lastRun = null; // { chunks, read, skipped, rows, bytesRead, ms } of the last query
  }

  get length() { return this.meta.rows; }
  get columnNames() { return this.meta.columns.map((c) => c.name); }
  get chunkCount() { return this.meta.chunks.length; }

  // ---- queries (see plan.mjs) -------------------------------------------------------
  lazy() { return new Query({ op: 'scan', table: this }); }
  filter(expr, params) { return this.lazy().filter(expr, params); }
  derive(name, expr, params) { return this.lazy().derive(name, expr, params); }
  select(...names) { return this.lazy().select(...names); }
  sort(by) { return this.lazy().sort(by); }
  top(k, by) { return this.lazy().top(k, by); }
  limit(n, offset) { return this.lazy().limit(n, offset); }
  join(other, opts) { return this.lazy().join(other, opts); }
  groupBy(keys) { return this.lazy().groupBy(keys); }
  agg(spec, params) { return this.lazy().agg(spec, params); }
  aggAsync(spec, params) { return this.lazy().aggAsync(spec, params); }
  count(expr, params) { return expr ? this.agg({ v: `count(${expr})` }, params).v : this.lazy().count(); }
  async countAsync(expr, params) { return expr ? (await this.aggAsync({ v: `count(${expr})` }, params)).v : this.lazy().countAsync(); }
  sum(expr, params) { return this.agg({ v: `sum(${expr})` }, params).v; }
  mean(expr, params) { return this.agg({ v: `mean(${expr})` }, params).v; }
  min(expr, params) { return this.agg({ v: `min(${expr})` }, params).v; }
  max(expr, params) { return this.agg({ v: `max(${expr})` }, params).v; }
  async sumAsync(expr, params) { return (await this.aggAsync({ v: `sum(${expr})` }, params)).v; }
  async meanAsync(expr, params) { return (await this.aggAsync({ v: `mean(${expr})` }, params)).v; }
  async minAsync(expr, params) { return (await this.aggAsync({ v: `min(${expr})` }, params)).v; }
  async maxAsync(expr, params) { return (await this.aggAsync({ v: `max(${expr})` }, params)).v; }
  // (as a table's: the rows, in a table in memory)
  sortAsync(by) { return this.lazy().sort(by).collectAsync(); }
  topAsync(k, by) { return this.lazy().top(k, by).collectAsync(); }
  limitAsync(n, offset) { return this.lazy().limit(n, offset).collectAsync(); }
  joinAsync(other, opts) { return this.lazy().join(other, opts).collectAsync(); }
  head(n) { return this.lazy().limit(n).collect(); }
  headAsync(n) { return this.lazy().limit(n).collectAsync(); }
  toRows(limit = Infinity) { return this.lazy().toRows(limit); }
  toRowsAsync(limit = Infinity) { return this.lazy().toRowsAsync(limit); }
  collect() { return this.lazy().collect(); }        // the whole dataset in memory
  collectAsync() { return this.lazy().collectAsync(); }
  explain() { return this.lazy().explain(); }
  async explainAsync() { return this.explain(); } // (nothing runs: the plan as text)

  // Called by plan.mjs for a plan that reads a dataset.
  runPlan(plan, sync) { return sync ? driveSync(runPlanG(plan)) : driveAsync(runPlanG(plan)); }

  async close() {
    if (this.file) { const f = this.file; this.file = null; f.close(); }
  }

  // ---- chunks -------------------------------------------------------------------------
  // Rows [lo, hi) of chunk i (a piece) as a table, with engine arrays ready
  // to be read into. Every column has fixed-width rows, so a row range is one
  // read per column (free text: its positions, plus the chunk's text bytes).
  #allocChunk({ i, lo, hi }, names) {
    const { engine } = this;
    const ch = i < 0 ? { rows: 0, cols: {} } : this.meta.chunks[i];
    const n = hi - lo;
    const cols = new Map(), owned = [], ownedBuffers = [], reads = [], seqs = [], unpacks = [], temps = [];
    for (const name of names) {
      const m = ch.cols[name], base = this.cols.get(name);
      if (base.strings) {
        const data = engine.alloc(Float64Array, n), strings = engine.alloc(Uint8Array, m?.slen ?? 0);
        cols.set(name, { data, dict: null, codes: null, strings });
        ownedBuffers.push(strings);
        if (m) reads.push([data, m.off + lo * 8, n * 8], [strings, m.soff, m.slen]);
      } else if (m?.enc) {
        const k = KINDS[m.enc.kind];
        const data = engine.alloc(k.Type, n + SLACK / k.bytes);
        cols.set(name, { data, dict: base.dict, codes: base.codes, date: base.date, enc: m.enc, plainType: this.type, rows: n });
        if (m.seq) seqs.push([name, m.seq]);
        else if (m.bits) { // bit-packed: the words holding rows lo..hi, unpacked after the read
          const w0 = (lo * m.bits) / 32, w1 = Math.ceil((hi * m.bits) / 32); // (lo is a multiple of 65,536: whole words)
          const words = engine.alloc(Uint8Array, (w1 - w0) * 4 + 8);
          temps.push(words);
          reads.push([words, m.off + w0 * 4, (w1 - w0) * 4]);
          unpacks.push([words, data, n, { bits: m.bits, base: m.bbase, missing: m.bmissing ?? null }, k.bytes]);
        } else reads.push([data, m.off + lo * k.bytes, n * k.bytes]);
      } else {
        const w = this.type.BYTES_PER_ELEMENT, data = engine.alloc(this.type, n);
        cols.set(name, { data, dict: base.dict, codes: base.codes, date: base.date });
        if (m) reads.push([data, m.off + lo * w, n * w]);
      }
      owned.push(name);
    }
    for (const c of cols.values()) { c.data = engine.live(c.data); if (c.strings) c.strings = engine.live(c.strings); }
    for (const [name, { first, step }] of seqs) unpacks.push(['seq', cols.get(name).data, n, { first: first + lo * step, step }, KINDS[cols.get(name).enc.kind].bytes]);
    const table = new Table(engine, cols, n, { owned, ownedBuffers, zones: false });
    return { table, reads, unpacks, temps, bytes: reads.reduce((a, r) => a + r[2], 0) };
  }

  // Bit-packed columns back to their codes, on every thread (a generator).
  *#unpackG(unpacks) {
    const { engine } = this;
    for (const [words, data, n, plan, width] of unpacks) {
      if (!n) continue;
      if (words === 'seq') { // fixed-step column: rebuilt (on every thread)
        if (!engine.jit) { const d = engine.live(data); for (let j = 0; j < n; j++) d[j] = plan.first + j * plan.step; continue; }
        engine._bitKernels ??= new Map();
        let v = engine._bitKernels.get(`seq|${width}`);
        if (!v) engine._bitKernels.set(`seq|${width}`, (v = engine._register(seqModule(width, engine.isShared))));
        yield* engine._unitsG(v, n, [engine.live(data).byteOffset, plan.first, plan.step]);
        continue;
      }
      if (!engine.jit) { unpackBits(engine.live(words), n, plan, engine.live(data)); continue; }
      engine._bitKernels ??= new Map();
      const key = `${width}|${plan.bits}`;
      let v = engine._bitKernels.get(key);
      if (!v) engine._bitKernels.set(key, (v = engine._register(unpackModule(width, plan.bits, engine.isShared))));
      yield* engine._unitsG(v, n, [engine.live(words).byteOffset, engine.live(data).byteOffset, plan.base, plan.missing !== null ? 1 : 0, plan.missing ?? 0]);
    }
  }

  // The reads in pieces of at most READ_PIECE bytes: [array, file position, bytes, offset in array].
  #pieces(reads) {
    const out = [], size = this.file.pieceBytes ?? READ_PIECE;
    for (const [arr, off, len] of reads) for (let at = 0; at < len; at += size) out.push([arr, off + at, Math.min(size, len - at), at]);
    return out;
  }

  _readSync(piece, names) {
    const { file } = this;
    if (!file?.readSync) throw new Error(file ? 'vectro: this dataset is read asynchronously (a File, Blob or URL): use toRowsAsync(), aggAsync(), collectAsync()...' : 'vectro: this dataset is closed');
    const { table, reads, unpacks, temps, bytes } = this.#allocChunk(piece, names);
    const { engine } = this;
    try {
      const pieces = this.#pieces(reads);
      if (file.fd !== undefined && engine.W > 0 && engine.canBlock !== false && pieces.length > 1) { // every thread reads pieces straight into engine memory
        const jobs = engine.alloc(Float64Array, pieces.length * 4);
        try {
          const j = engine.live(jobs);
          pieces.forEach(([arr, off, len, at], u) => { j.set([file.fd, off, len, engine.live(arr).byteOffset + at], u * 4); });
          engine._drive((function* () {
            yield { op: OP.READ, n: pieces.length, arrays: [jobs], F: (J, u) => file.readSync(new Uint8Array(engine.memory.buffer, J[u * 4 + 3], J[u * 4 + 2]), J[u * 4 + 1]) };
          })());
        } finally { engine.free(jobs); }
      } else for (const [arr, off, len] of reads) file.readSync(bytesOf(engine.live(arr), len), off);
      this.engine._drive(this.#unpackG(unpacks));
    } catch (err) { table.dispose(); throw err; } finally { for (const t of temps) this.engine.free(t); }
    if (this.lastRun) this.lastRun.bytesRead += bytes;
    return table;
  }

  async _readAsync(piece, names) {
    const { file } = this;
    if (!file) throw new Error('vectro: this dataset is closed');
    const { table, reads, unpacks, temps, bytes } = this.#allocChunk(piece, names);
    try {
      const view = (arr, at, len) => new Uint8Array(this.engine.live(arr).buffer, this.engine.live(arr).byteOffset + at, len);
      if (file.coalesce) { // (a URL: neighbouring reads as one request each)
        const sorted = reads.filter((r) => r[2]).sort((a, b) => a[1] - b[1]), groups = [];
        for (const r of sorted) {
          const g = groups[groups.length - 1];
          if (g && r[1] - g.end <= file.coalesce && r[1] + r[2] - g.start <= (file.pieceBytes ?? READ_PIECE)) { g.items.push(r); g.end = Math.max(g.end, r[1] + r[2]); } else groups.push({ start: r[1], end: r[1] + r[2], items: [r] });
        }
        await settled(groups.map(async (g) => {
          const buf = new Uint8Array(g.end - g.start);
          await file.read(buf, g.start);
          for (const [arr, off, len] of g.items) view(arr, 0, len).set(buf.subarray(off - g.start, off - g.start + len));
        }));
      } else {
        await settled(this.#pieces(reads).map(async ([arr, off, len, at]) => { // (in pieces: several reads at once)
          if (this.engine.isShared) return file.read(view(arr, at, len), off); // straight into engine memory
          const buf = new Uint8Array(len); // (plain memory may move while the read runs)
          await file.read(buf, off);
          view(arr, at, len).set(buf);
        }));
      }
      await this.engine._driveAsync(this.#unpackG(unpacks));
    } catch (err) { table.dispose(); throw err; } finally { for (const t of temps) this.engine.free(t); }
    if (this.lastRun) this.lastRun.bytesRead += bytes;
    return table;
  }

  // The pieces { i, lo, hi } (rows lo..hi of chunk i) that can match every
  // `column <op> number` condition: chunks by their min / max, then the
  // blocks from the first that can match to the last.
  _piecesFor(conds) {
    const out = [];
    this.meta.chunks.forEach((ch, i) => {
      for (const { col, op, v } of conds) {
        const z = ch.cols[col];
        if (!z) continue;
        if (z.missing === ch.rows) return; // only missing values: no comparison holds
        if (!mayMatch(op, fromJson(z.min), fromJson(z.max), v)) return;
      }
      let first = 0, last = Math.ceil(ch.rows / BLOCK) - 1;
      const zoned = conds.filter((c) => ch.cols[c.col]?.blocks);
      if (zoned.length) {
        const ok = (b) => zoned.every(({ col, op, v }) => { const z = ch.cols[col].blocks; return mayMatch(op, fromJson(z.min[b]), fromJson(z.max[b]), v); });
        while (first <= last && !ok(first)) first++;
        while (last >= first && !ok(last)) last--;
        if (first > last) return;
      }
      out.push({ i, lo: first * BLOCK, hi: Math.min(ch.rows, (last + 1) * BLOCK) });
    });
    return out;
  }
}

// ---- running plans ----------------------------------------------------------------------
// A plan runs as a generator of requests: { run: node } (a plan over tables in
// memory), { read: dataset, i, names } (a chunk as a table), { prefetch: ... }
// (start reading a chunk; async only).

function driveSync(gen) {
  let step = gen.next();
  while (!step.done) {
    const v = step.value;
    let out, err = null;
    try {
      out = v.run ? execute(v.run, true) : v.read ? v.read._readSync(v.piece, v.names) : v.call ? v.call() : undefined; // ({ flush }: nothing to wait for)
      if (typeof out?.then === 'function') throw new Error('vectro: this needs the async calls (toRowsAsync, aggAsync, writeDatasetAsync...)');
    } catch (e) { err = e; }
    step = err ? gen.throw(err) : gen.next(out);
  }
  return step.value;
}

// Promise.all that waits for every promise before failing: a read still writing into an array must
// end before the chunk's arrays are freed
async function settled(promises) {
  const all = await Promise.allSettled(promises);
  const bad = all.find((r) => r.status === 'rejected');
  if (bad) throw bad.reason;
  return all.map((r) => r.value);
}

async function driveAsync(gen) {
  const pending = new Map(); // chunk -> promise of its table
  try {
    let step = gen.next();
    while (!step.done) {
      const v = step.value;
      let out, err = null;
      try {
        if (v.run) out = await execute(v.run, false);
        else if (v.flush) await v.flush.file?.flush?.(); // (a writer's bytes on disk before more come)
        else if (v.call) out = await v.call();
        else if (v.prefetch) {
          const key = `${v.prefetch.path}\0${v.piece.i}\0${v.piece.lo}`;
          if (!pending.has(key)) { const p = v.prefetch._readAsync(v.piece, v.names); p.catch(() => {}); pending.set(key, p); }
        } else if (v.read) {
          const key = `${v.read.path}\0${v.piece.i}\0${v.piece.lo}`, p = pending.get(key);
          pending.delete(key);
          out = await (p ?? v.read._readAsync(v.piece, v.names));
        }
      } catch (e) { err = e; }
      step = err ? gen.throw(err) : gen.next(out);
    }
    return step.value;
  } finally {
    // chunks read ahead and not used (the query failed, or stopped early): wait for them before the answer
    // is given, so no work of this query is still on the engine's threads when the caller goes on
    const left = await Promise.allSettled(pending.values());
    for (const r of left) if (r.status === 'fulfilled') r.value.dispose();
  }
}

const BLOCKING = { aggregate: 'group', total: 'total', limit: 'head', sort: 'gather', output: 'collect', window: 'gather' };
const union = (a, b) => new Set([...a, ...b]);
const onKeys = (on) => (typeof on === 'string' || Array.isArray(on) ? [[on].flat(), [on].flat()] : [[on?.left].flat(), [on?.right].flat()]);

// The joins in a per-chunk plan, marked to return views (scans kept as they are).
function viewJoins(node) {
  if (node.op === 'scan' || node.op === 'unmatched') return node;
  if (node.op === 'join') return { ...node, opts: { ...node.opts, view: true }, left: viewJoins(node.left) };
  return { ...node, input: viewJoins(node.input) };
}

// node with `target` (the same object) replaced by `repl`
function substitute(node, target, repl) {
  if (node === target) return repl;
  if (node.op === 'scan' || node.op === 'unmatched') return node;
  if (node.op === 'join') return { ...node, left: substitute(node.left, target, repl), right: substitute(node.right, target, repl) };
  return { ...node, input: substitute(node.input, target, repl) };
}

// The dataset columns a plan reads (null: all of them). Mirrors what the
// executor in plan.mjs asks of its input.
// The columns of `ds` the plan reads (null: all). With stop, the columns of
// that node's rows the steps above it use instead (ALL: all of them).
const ALL = Symbol('all');
function needs(node, need, ds, stop = null) {
  if (stop && node === stop) return need ?? ALL;
  switch (node.op) {
    case 'scan': return node.table === ds ? need : undefined;
    case 'unmatched': return undefined;
    case 'filter': return needs(node.input, need && union(need, vars(node.ast)), ds, stop);
    case 'derive':
      if (!need) return needs(node.input, null, ds, stop);
      if (!need.has(node.name)) return needs(node.input, need, ds, stop);
      return needs(node.input, union([...need].filter((n) => n !== node.name), vars(node.ast)), ds, stop);
    case 'select': return needs(node.input, new Set(need ? node.names.filter((n) => need.has(n)) : node.names), ds, stop);
    case 'aggregate': return needs(node.input, new Set([...node.keys, ...Object.values(node.spec).flatMap((a) => [...vars(a)])]), ds, stop);
    case 'total': return needs(node.input, new Set(Object.values(node.spec).flatMap((a) => [...vars(a)])), ds, stop);
    case 'sort': return needs(node.input, need && union(need, node.by.flatMap((b) => [...vars(parse(String(b)))])), ds, stop);
    case 'limit': return needs(node.input, need, ds, stop);
    case 'output': return needs(node.input, null, ds, stop);
    case 'textmap': return needs(node.input, need && new Set([...[...need].filter((n) => n !== node.name), ...node.inputs]), ds, stop);
    case 'window': {
      const made = new Set(node.items.map((it) => it.name));
      const inputs = [...node.partition, ...node.order.map((o) => o.name), ...node.items.map((it) => it.arg).filter(Boolean)];
      return needs(node.input, need && new Set([...[...need].filter((n) => !made.has(n)), ...inputs]), ds, stop);
    }
    case 'join': {
      const { left } = joinNames(node);
      const needL = new Set(onKeys(node.opts.on)[0]);
      for (const [x] of joinCondColumns(node)) if (x) needL.add(x); // (a range's left key, what a where reads)
      for (const n of need ?? namesOf(node)) if (left.has(n)) needL.add(n);
      for (const n of namesOf(node.right)) if (left.has(n)) needL.add(n); // (they decide the names of right columns)
      return needs(node.left, needL, ds, stop);
    }
  }
  return null;
}

// Give `result` the columns it shares with `temp`, then free temp.
function adopt(result, temp) {
  if (!temp || result === temp) return;
  if (result?.cols) {
    for (const [name, c] of result.cols) {
      const own = [...temp.cols].find(([n, tc]) => tc.data === c.data && temp.owned.includes(n));
      if (own) { temp.owned = temp.owned.filter((n) => n !== own[0]); if (!result.owned.includes(name)) result.owned.push(name); }
      if (c.strings && temp.ownedBuffers.includes(c.strings)) { temp.ownedBuffers = temp.ownedBuffers.filter((b) => b !== c.strings); result.ownedBuffers.push(c.strings); }
    }
  }
  temp.dispose();
}

// A per-chunk result we can keep after the chunk is freed (else a copy).
function* keep(engine, r, chunk) {
  const own = r !== chunk && r.columnNames.every((n) => r.owned.includes(n)) && [...r.cols.values()].every((c) => !c.strings || r.ownedBuffers.includes(c.strings));
  if (own) return r;
  const copy = copyRows(engine, [[r, 0, r.n]]);
  if (r !== chunk) r.dispose();
  return copy;
}

// A join whose two sides are both too big for memory (a "grace" hash join):
// both sides' rows go to partition files by ranges of the (first) join key,
// with the same split points; then for each range, its right rows are held
// in memory and its left rows stream past them chunk by chunk, through the
// rest of the per-chunk plan (acc). Rows come out range by range, not in the
// left table's order.
function* partitionedJoinG(J, viewTop, ds, names, acc) {
  const { engine } = ds;
  const [lk, rk] = onKeys(J.opts.on);
  const Rds = findDataset(J.right);
  const count = Math.min(64, Math.max(2, Math.ceil((2 * J.partitioned.rows) / tuning.joinRows)));
  const splitters = (Rds.cols.has(rk[0]) ? zoneSplitters(Rds, rk[0], count) : null) ?? (ds.cols.has(lk[0]) ? zoneSplitters(ds, lk[0], count) : null) ?? [];
  const rs = yield* Spill.create(engine, rk[0], splitters);
  let ls;
  try { ls = yield* Spill.create(engine, lk[0], splitters); } catch (err) { rs.cleanup(); throw err; }
  stats.joinPartitions++;
  try {
    yield* runPlanG({ op: 'output', input: J.right }, rs);
    const keep = namesOf(J.left).filter((n) => names.includes(n) || !ds.cols.has(n)); // (the left columns used, and derived ones)
    yield* runPlanG({ op: 'output', input: { op: 'select', names: keep, input: J.left } }, ls);
    const rp = yield* rs.closeG(), lp = yield* ls.closeG();
    let ran = false;
    for (let j = 0; j < lp.length && !acc.done; j++) {
      if (!lp[j] || (!rp[j] && (J.opts.how ?? 'inner') !== 'left')) continue; // (no rows can come out of this range)
      const R = rp[j] ? yield* readAll(engine, rp[j]) : emptyLike(engine, rs);
      const L = yield* tempDataset(engine, lp[j]);
      try {
        for (let i = 0; i < L.chunkCount && !acc.done; i++) {
          const c = yield { read: L, piece: { i, lo: 0, hi: L.meta.chunks[i].rows }, names: L.columnNames };
          try {
            ds.lastRun.rows += c.n;
            ran = true;
            yield* acc.chunk(substitute(viewTop, J, { ...J, left: { op: 'scan', table: c }, right: { op: 'scan', table: R } }), c);
          } finally { c.dispose(); }
        }
      } finally {
        L.close();
        const builds = engine._joinBuilds?.get(R.cols.get(rk[0])?.data); // (this range's hash table: freed now)
        if (builds) { for (const b of builds.values()) freeBuild(engine, b); engine._joinBuilds.delete(R.cols.get(rk[0]).data); }
        R.dispose();
      }
    }
    if (!ran) { // no rows on one side: the plan still runs once, on no rows, for the result's shape
      const scan = (function find(n) { return n.op === 'scan' ? n : find(n.op === 'join' ? n.left : n.input); })(J.left);
      const empty = yield { read: ds, piece: { i: -1, lo: 0, hi: 0 }, names };
      const R = emptyLike(engine, rs);
      try { yield* acc.chunk(substitute(substitute(viewTop, J, { ...J, right: { op: 'scan', table: R } }), scan, { op: 'scan', table: empty }), empty); } finally { empty.dispose(); R.dispose(); }
    }
  } finally { rs.cleanup(); ls.cleanup(); }
}

// A dataset's rows as one table in memory.
function* readAll(engine, path) {
  const D = yield* tempDataset(engine, path), parts = [];
  try {
    yield* eachChunk(D, function* (t) { parts.push(t); });
    return copyRows(engine, parts.map((t) => [t, 0, t.n]));
  } finally { for (const t of parts) t.dispose(); D.close(); }
}

// A table with no rows and the columns a spill was given.
function emptyLike(engine, spill) {
  const w = spill.writers.find((x) => x.meta);
  const cols = new Map(), owned = [];
  for (const c of w.meta.columns) {
    const labels = c.kind === 'category' ? w.labels.get(c.name).dict : null;
    cols.set(c.name, c.kind === 'text' ? { data: engine.alloc(Float64Array, 0), dict: null, codes: null, strings: engine.alloc(Uint8Array, 0) } : { data: engine.alloc(Float64Array, 0), dict: labels, codes: labels && new Map(labels.map((s, i) => [s, i])), date: c.kind === 'date' });
    owned.push(c.name);
  }
  return new Table(engine, cols, 0, { owned, ownedBuffers: [...cols.values()].filter((c) => c.strings).map((c) => c.strings) });
}

// Every join's right side (the build side) as one plain table, made once:
// a dataset there is read in full (its matching rows and used columns), a
// filtered or packed table is collected / decoded, so each chunk's join finds
// the same table (and its hash table, kept in engine._joinBuilds).
function* materializeRights(node, temps) {
  if (node.op === 'scan' || node.op === 'unmatched') return node; // (unmatched: a FULL JOIN's right rows, made by its join)
  if (node.op === 'union') { // each input with a dataset in it: read (its matching rows) into memory
    const inputs = [];
    for (const input of node.inputs) {
      if (!findDataset(input)) { inputs.push(yield* materializeRights(input, temps)); continue; }
      const t = yield* runPlanG({ op: 'output', input });
      temps.push(t);
      inputs.push({ op: 'scan', table: t });
    }
    return { ...node, inputs };
  }
  if (node.op === 'join') {
    const left = yield* materializeRights(node.left, temps);
    let right = node.right;
    const L = findDataset(left), Rds = findDataset(right);
    // (each chunk is joined on its own, so the right rows no chunk matched aren't known: they came out once per chunk)
    if (L && (node.opts.how === 'right' || node.opts.how === 'full')) throw new Error(`vectro: join how: '${node.opts.how}' needs a table on the left, not a dataset: collect() the dataset's rows first, or use SQL ${node.opts.how.toUpperCase()} JOIN ... ON keys, which works on datasets`);
    if (L && Rds &&!node.opts.range && !node.opts.where && !node.opts.cross && onKeys(node.opts.on)[0].length) { // both on disk: too many right rows to hold? then both sides go to disk by key range
      const [lk, rk] = onKeys(node.opts.on);
      const textKey = L.cols.get(lk[0])?.dict || Rds.cols.get(rk[0])?.dict; // (codes differ between files: not by range)
      const rows = textKey ? 0 : (yield* runPlanG({ op: 'total', spec: { n: parse('count()') }, input: right })).n;
      if (rows > tuning.joinRows) return { ...node, left, right, partitioned: { rows } };
    }
    const plain = right.op === 'scan' && !right.table.isDataset && !right.table.pred && ![...right.table.cols.values()].some((c) => c.enc);
    if (!plain) {
      let t = findDataset(right) ? yield* runPlanG({ op: 'output', input: right }) : yield { run: { op: 'output', input: right } };
      if (t.pred || [...t.cols.values()].some((c) => c.enc) || t.owned.length < t.cols.size) { const copy = copyRows(t.engine, [[t, 0, t.n]]); adopt(null, t); t = copy; }
      temps.push(t);
      right = { op: 'scan', table: t };
    }
    return { ...node, left, right };
  }
  return { ...node, input: yield* materializeRights(node.input, temps) };
}

// sink: a DatasetWriter that takes the result (matching rows go straight to
// it, chunk by chunk, so the result needn't fit in memory either).
export function* runPlanG(plan, sink = null) {
  const temps = [];
  let result;
  try {
    plan = yield* materializeRights(plan, temps);
    const path = [plan];
    for (let n = plan; n.op !== 'scan' && n.op !== 'union'; ) { n = n.op === 'join' ? n.left : n.input; path.push(n); }
    const scan = path[path.length - 1];
    if (!scan.table?.isDataset) result = yield { run: plan };
    else {
      try { result = yield* runChunksG(plan, path, sink); } catch (err) { // (chunks are read with only the columns used: name them all)
        if (/^vectro: unknown column /.test(err?.message)) err.message = err.message.replace(/\(columns: [^)]*\)$/, `(columns: ${scan.table.columnNames.join(', ')})`);
        throw err;
      }
    }
    if (sink && result) { // (the result itself was built in memory)
      if (result.cols) sink.append(result);
      else sink.append(Object.fromEntries(Object.entries(result).map(([k, v]) => [k, [v]])));
    }
    return sink ? null : result;
  } finally {
    for (const t of temps) adopt(sink ? null : result, t);
    if (sink && result?.cols) result.dispose();
  }
}

function* runChunksG(plan, path, sink) {
  const scan = path[path.length - 1], ds = scan.table, { engine } = ds;
  const t0 = performance.now();
  ds.lastRun = { chunks: ds.chunkCount, read: 0, skipped: 0, rows: 0, bytesRead: 0, ms: 0 };
  // the first step above the dataset that needs all rows
  let bi = path.length - 2;
  while (!BLOCKING[path[bi].op]) bi--;
  let B = path[bi], kind = BLOCKING[B.op];
  const below = path.slice(bi + 1, -1); // (the steps between the dataset and B: what each chunk runs)
  if (B.op === 'sort' && bi > 0 && path[bi - 1].op === 'limit' && path[bi - 1].input === B && path[bi - 1].n !== Infinity) { B = path[bi - 1]; kind = 'top'; }
  const top = kind === 'top' ? B.input.input : B.input; // what runs on each chunk
  // columns to read, and the chunks the filters right above the dataset allow
  const need = needs(plan, null, ds);
  let names = need ? ds.columnNames.filter((n) => need.has(n)) : ds.columnNames;
  if (!names.length) names = [smallestColumn(ds)]; // (count() still needs the row count)
  const conds = [];
  for (let k = path.length - 2; k >= 0 && path[k].op === 'filter'; k--) {
    let rewritten;
    try { rewritten = toSource(rewriteText(path[k].ast, ds.cols, toSource(path[k].ast))); } catch { continue; } // (the run reports it)
    const round = ds.type === Float32Array ? Math.fround : (v) => v;
    for (const c of conditions(rewritten, ds.cols, {})) conds.push({ ...c, v: round(c.v) });
  }
  const viewTop = viewJoins(top); // (joins on each chunk: unique right keys as views, see joinViewG)
  const pieces = ds._piecesFor(conds);
  ds.lastRun.skipped = ds.chunkCount - pieces.length;
  const list = pieces.length ? pieces : [{ i: -1, lo: 0, hi: 0 }]; // (no rows, but the plan still runs once for the result's shape)
  // columns made below B (names made above it, such as a result's renames, are not: B sees the dataset's)
  const derived = new Set(below.filter((n) => n.op === 'derive' || n.op === 'textmap' || n.op === 'join').flatMap((n) => (n.op === 'join' ? [...namesOf(n.right), ...joinNames(n).fromRight.keys()] : [n.name]))); // (a join's right columns under their output names too: s_right)
  // derived columns that are arithmetic on number columns of the dataset (a top-k cutoff can compare them)
  derived.numeric = new Set(below.filter((n) => n.op === 'derive' && arithmetic(n.ast, ds)).map((n) => n.name));
  // the columns of each chunk's rows the steps from B up use: a chunk's plan is cut to them, so a column it
  // makes (a derive, a select's) that nothing uses isn't made from columns that were never read
  const above = needs(plan, null, ds, top);
  if (above instanceof Set) derived.keep = above;
  const acc = accumulator(kind, B, engine, sink, ds, derived);
  let finalTable = null;
  engine._joinRuns = (engine._joinRuns ?? 0) + 1; // joins keep their right side's build while a dataset runs
  engine._joinBuilds ??= new Map();
  try {
    let pj = viewTop;
    while (pj.op !== 'scan' && !(pj.op === 'join' && pj.partitioned)) pj = pj.op === 'join' ? pj.left : pj.input;
    if (pj.op === 'join') yield* partitionedJoinG(pj, viewTop, ds, names, acc);
    else {
      // top-k: each chunk is narrowed to the blocks that can still beat the
      // k-th row so far (acc.prune), just before it is read or prefetched
      let at = 0, pruned = 0;
      const next = () => { // the next piece to read (null: none left)
        while (at < list.length) {
          const p = list[at++], q = acc.prune && p.i >= 0 ? acc.prune(p) : p;
          if (q) return q;
          pruned++;
        }
        return null;
      };
      let piece = next(), k = 0;
      while (piece) {
        if (k++ > 0 && acc.done) break;
        const chunk = yield { read: ds, piece, names };
        // (top-k: no prefetch during the first chunk, so the next one is narrowed by its result)
        const ahead = acc.done || (acc.prune && !acc.ready() && k === 1) ? null : next();
        if (ahead) yield { prefetch: ds, piece: ahead, names };
        ds.lastRun.read += piece.i >= 0 ? 1 : 0;
        ds.lastRun.rows += chunk.n;
        try {
          yield* acc.chunk(substitute(viewTop, scan, { op: 'scan', table: chunk }), chunk);
        } finally {
          chunk.dispose();
        }
        piece = ahead ?? (acc.done ? null : next());
      }
      ds.lastRun.skipped += pruned;
    }
    const merged = yield* acc.finish();
    if (kind === 'total' || merged === null) return merged;
    if (merged.isDataset) { // spilled groups: the rest of the plan reads them from disk
      try { return yield* runPlanG(substitute(plan, B, { op: 'scan', table: merged }), sink); } finally { merged.close(); removeTemp(merged.temporary); }
    }
    finalTable = merged;
    const rest = substitute(plan, kind === 'group' ? B : top, { op: 'scan', table: merged });
    const result = yield { run: rest };
    adopt(result, merged);
    finalTable = null;
    return result;
  } finally {
    finalTable?.dispose();
    acc.dispose();
    if (--engine._joinRuns === 0) {
      for (const builds of engine._joinBuilds.values()) for (const b of builds.values()) freeBuild(engine, b);
      engine._joinBuilds = null;
    }
    ds.lastRun.ms = performance.now() - t0;
  }
}

function smallestColumn(ds) {
  const ch = ds.meta.chunks[0];
  if (!ch) return ds.columnNames[0];
  return ds.columnNames.filter((n) => !ds.cols.get(n).strings).sort((a, b) => (ch.cols[a]?.len ?? 0) - (ch.cols[b]?.len ?? 0))[0] ?? ds.columnNames[0];
}

// How per-chunk results are kept and merged, per kind of step:
//   chunk(P, table): run the step on one chunk's plan P (may yield runs)
//   finish(): the merged result (a table, or totals for 'total')
function accumulator(kind, B, engine, sink, ds, derived) {
  if (kind === 'group' || kind === 'total') return groupAccumulator(kind, B, engine, ds, derived);
  if (kind === 'gather' && sink && B.op === 'sort') { const first = firstSortKey(B.by[0], ds, derived); if (first && /^[\p{L}_][\p{L}\p{M}\p{N}_]*$/u.test(first.expr)) return sortSpillAccumulator(B, engine, ds, derived, first); }
  const parts = [];
  let rows = 0;
  // top-k: once k rows are kept, a row whose first key is worse than the
  // k-th can't make it, so later chunks get that cutoff as a filter (ties pass)
  const k = B.n + (B.offset ?? 0);
  const first = kind === 'top' && k > 0 ? firstSortKey(B.input.by[0], ds, derived, true) : null;
  let cutoff = null;
  // the leading sort keys that are plain number columns of the dataset (their
  // min / max per block are in the footer), and the k-th row's values of them
  const zkeys = [];
  if (first) for (const by of B.input.by) {
    const key = firstSortKey(by, ds, derived, true);
    const c = key && /^[\p{L}_][\p{L}\p{M}\p{N}_]*$/u.test(key.expr) && !derived.has(key.expr) && ds.cols.get(key.expr);
    if (!c || c.dict || c.strings) break;
    zkeys.push(key);
  }
  // the leading sort keys that sort by value (the k-th row's values of them are found)
  const ckeys = [];
  if (first) for (const by of B.input.by) { const key = firstSortKey(by, ds, derived, true); if (!key) break; ckeys.push(key); }
  let bound = null, dense = false, cutFirst = null;
  const acc = {
    done: false,
    ready: () => bound !== null,
    // piece narrowed to the blocks that may hold a row at least as good as
    // the k-th so far (from the first such block to the last; null: none): a
    // block whose max (desc) / min (asc) of the first key is worse can't, nor
    // one that at best ties it there and is worse on the next key, and so on
    // (missing values sort last: never better)
    prune: zkeys.length ? (piece) => {
      if (!bound) return piece;
      const ch = ds.meta.chunks[piece.i];
      const b0 = Math.floor(piece.lo / BLOCK), b1 = Math.ceil(piece.hi / BLOCK);
      const zone = (z, b, which) => fromJson(z.blocks && b1 - b0 > 0 ? z.blocks[which][b] : z[which]);
      const may = (b) => {
        for (let j = 0; j < zkeys.length; j++) {
          const z = ch.cols[zkeys[j].expr], v = bound[j];
          if (!z || v !== v) return true;
          if (z.missing === ch.rows) return false; // (only missing values: last)
          const lo = zone(z, b, 'min'), hi = zone(z, b, 'max');
          if (zkeys[j].desc ? hi > v : lo < v) return true;   // may be better on this key
          if (zkeys[j].desc ? hi < v : lo > v) return false;  // worse on this key
          if (!(lo <= v && v <= hi)) return false;            // (an all-missing block)
          // at best a tie on this key: the next key decides
        }
        return true; // (ties with the k-th pass: the merge picks)
      };
      let first = b0, last = b1 - 1;
      while (first <= last && !may(first)) first++;
      while (last > first && !may(last)) last--;
      if (first > last) return null;
      return { i: piece.i, lo: Math.max(piece.lo, first * BLOCK), hi: Math.min(piece.hi, (last + 1) * BLOCK) };
    } : null,
    *chunk(P, chunk) {
      let node;
      if (derived.keep && (kind === 'top' || kind === 'head' || kind === 'gather')) {
        const have = namesOf(P);
        let keepNames = have.filter((n) => derived.keep.has(n));
        if (!keepNames.length) keepNames = [have.find((n) => chunk.cols.has(n)) ?? have[0]]; // (count(): one column read, for the rows)
        if (keepNames.length < have.length) P = { op: 'select', names: keepNames, input: P };
      }
      if (kind === 'top') {
        node = { op: 'output', input: { op: 'limit', n: k, offset: 0, input: { ...B.input, input: P } } };
        if (cutoff !== null) { // only rows at least as good as the k-th so far: keep them all (the merge picks the best)
          const F = { op: 'filter', ast: cutFirst, input: P };
          node = dense ? { op: 'output', input: { op: 'limit', n: k, offset: 0, input: { ...B.input, input: F } } } : { op: 'output', input: F };
        }
      }
      else if (kind === 'head') node = { op: 'output', input: { op: 'limit', n: B.n + (B.offset ?? 0) - rows, offset: 0, input: P } };
      else node = { op: 'output', input: P };
      let r = yield { run: node };
      if (kind === 'top' && cutoff !== null && !dense && r.n > Math.max(8 * k, 1 << 14)) {
        // many rows beat the cutoff (e.g. newest first over time-ordered chunks):
        // the best k of them now, and top-k straight away on the chunks after
        dense = true;
        const own = yield* keep(engine, r, chunk);
        const best = yield { run: { op: 'output', input: { op: 'limit', n: k, offset: 0, input: { ...B.input, input: { op: 'scan', table: own } } } } };
        r = yield* keep(engine, best, own);
        if (r !== own) own.dispose();
      }
      if (sink && kind === 'collect') { // straight to the dataset being written (or to spill partitions)
        if (sink.appendG) { yield* sink.appendG(yield* keep(engine, r, chunk)); return; }
        try { sink.append(r); } finally { if (r !== chunk) r.dispose(); }
        yield { flush: sink };
        return;
      }
      parts.push(yield* keep(engine, r, chunk));
      rows += r.n;
      if (kind === 'head' && rows >= B.n + (B.offset ?? 0)) acc.done = true;
      if (kind === 'top' && (parts.length >= 16 || (first && rows >= k && parts.length > 1))) { // keep the best k of what we have
        const all = merge();
        const best = yield { run: { op: 'output', input: { op: 'limit', n: k, offset: 0, input: { ...B.input, input: { op: 'scan', table: all } } } } };
        parts.push(yield* keep(engine, best, all));
        all.dispose();
        rows = parts[0].n;
      }
      if (first && rows >= k && parts.length === 1) { // the k-th row's first key
        const spec = {};
        ckeys.forEach((c, j) => { spec[`c${j}`] = parse(`min(${c.expr})`); });
        const r = yield { run: { op: 'total', spec, input: { op: 'limit', n: 1, offset: k - 1, input: { op: 'scan', table: parts[0] } } } };
        const num = (x) => (x instanceof Date ? x.getTime() : typeof x === 'number' ? x : NaN);
        const vals = ckeys.map((_, j) => num(r[`c${j}`]));
        if (vals[0] === vals[0]) {
          cutoff = vals[0];
          cutFirst = parse(`(${first.expr}) ${first.desc ? '>=' : '<='} ${toSource({ t: 'num', v: cutoff })}`);
        }
        if (zkeys.length) bound = vals.slice(0, zkeys.length);
      }
    },
    *finish() {
      if (sink && kind === 'collect') return null;
      return merge();
    },
    dispose() { for (const p of parts.splice(0)) p.dispose(); },
  };
  // all parts as one table (parts emptied)
  const merge = () => {
    const ps = parts.splice(0);
    if (ps.length === 1) return ps[0];
    try { return copyRows(engine, ps.map((p) => [p, 0, p.n])); } finally { for (const p of ps) p.dispose(); }
  };
  return acc;
}

// ---- spilling: partial results on disk, split by key ranges --------------------
// Rows go to partition files by their first key: partition j holds the keys
// in [splitters[j-1], splitters[j]), and the last one the missing keys, so
// partitions in order hold the keys in order, and each can be finished on
// its own.
const PART = '__vectro_part';
class Spill {
  // (made with yield* Spill.create(...): the files may open asynchronously)
  constructor(engine, key, splitters, dir, paths, writers) {
    const P = splitters.length + 2;
    Object.assign(this, { engine, dir, paths, writers, rows: 0 });
    this.partAst = parse(`(${key}) != (${key}) ? ${P - 1} : (0${splitters.map((v) => ` + ((${key}) >= ${toSource({ t: 'num', v })})`).join('')})`);
  }

  static *create(engine, key, splitters) {
    const store = temp();
    const dir = yield { call: () => store.dir('vectro-spill-') };
    const paths = Array.from({ length: splitters.length + 2 }, (_, j) => store.join(dir, `part-${j}.vectro`));
    const writers = [];
    try { for (const p of paths) writers.push(yield* tempWriter(engine, p, { rowsPerChunk: 1 << 18 })); } catch (err) { for (const w of writers) w.abort(); removeTemp(dir); throw err; }
    return new Spill(engine, key, splitters, dir, paths, writers);
  }

  *appendG(t) { yield* this.add(t); } // (as a sink: runPlanG writes a query's rows here)

  // t's rows, each to its partition (t is freed)
  *add(t) {
    try {
      if (!t.n) return;
      this.rows += t.n;
      const S = yield { run: { op: 'output', input: { op: 'sort', by: [PART], input: { op: 'derive', name: PART, ast: this.partAst, input: { op: 'scan', table: t } } } } };
      const touched = new Set();
      try {
        const part = this.engine.live(S.cols.get(PART).data), view = S.select(...t.columnNames);
        for (let a = 0; a < S.n; ) {
          const j = part[a];
          let b = a;
          while (b < S.n && part[b] === j) b++;
          this.writers[j].append(view, a, b);
          touched.add(this.writers[j]);
          a = b;
        }
      } finally { S.dispose(); }
      for (const w of touched) yield { flush: w }; // (browser files: written before more comes)
    } finally { t.dispose(); }
  }

  // the partition files in key order (null: no rows), closed
  *closeG() {
    for (const w of this.writers) { if (!w.meta) w.abort(); else yield* closeWriter(w); }
    return this.paths.map((p, j) => (this.writers[j].meta ? p : null));
  }

  cleanup() { for (const w of this.writers) w.abort(); removeTemp(this.dir); }
}

// Up to P - 1 split points for a column, from the dataset's min / max per
// block (an even spread of the values), or null if they're all the same.
function zoneSplitters(ds, name, P) {
  const vals = [];
  for (const ch of ds.meta.chunks) {
    const z = ch.cols[name];
    if (!z || z.missing === ch.rows) continue;
    const add = (v) => { v = fromJson(v); if (Number.isFinite(v)) vals.push(v); };
    if (z.blocks) for (let b = 0; b < z.blocks.min.length; b++) { add(z.blocks.min[b]); add(z.blocks.max[b]); }
    else { add(z.min); add(z.max); }
  }
  return quantiles(vals.sort((a, b) => a - b), P);
}

function quantiles(sorted, P) {
  const out = [];
  for (let i = 1; i < P; i++) {
    const v = sorted[Math.floor((i * sorted.length) / P)];
    if (v !== undefined && v > sorted[0] && (!out.length || v > out[out.length - 1])) out.push(v);
  }
  return out.length ? out : null;
}

// Every chunk of a dataset as tables (all columns), one at a time.
function* eachChunk(D, fn) {
  for (let i = 0; i < D.chunkCount; i++) {
    const t = yield { read: D, piece: { i, lo: 0, hi: D.meta.chunks[i].rows }, names: D.columnNames };
    yield* fn(t);
  }
}

// A full sort whose result goes to a dataset (it needn't fit in memory):
// the rows go to partitions by the first sort key, each partition is sorted
// in memory, and the partitions are written in order (missing keys last).
function sortSpillAccumulator(B, engine, ds, derived, first) {
  let spill = null, empty = null; // (empty: the result's columns, if no rows match)
  return {
    done: false,
    *chunk(P, chunk) {
      const t = yield* keep(engine, yield { run: { op: 'output', input: P } }, chunk);
      if (!t.n && !empty) { empty = t; return; }
      if (!spill) {
        const count = Math.min(64, Math.max(2, Math.ceil((2 * ds.length) / tuning.partitionRows)));
        let splitters = !derived.has(first.expr) && ds.cols.has(first.expr) ? zoneSplitters(ds, first.expr, count) : null;
        if (!splitters && t.cols.has(first.expr)) { // from the first rows
          const v = engine.live(t.cols.get(first.expr).data), vals = [];
          for (let i = 0; i < t.n; i++) if (v[i] === v[i]) vals.push(v[i]);
          splitters = quantiles(vals.sort((a, b) => a - b), count);
        }
        spill = yield* Spill.create(engine, first.expr, splitters ?? []);
        stats.spills++;
      }
      yield* spill.add(t);
    },
    *finish() {
      spill ??= yield* Spill.create(engine, first.expr, []);
      const paths = yield* spill.closeG();
      const ordered = first.desc ? [...paths.slice(0, -1).reverse(), paths[paths.length - 1]] : paths;
      const outPath = temp().join(spill.dir, 'sorted.vectro');
      const out = yield* tempWriter(engine, outPath);
      let ok = false;
      try {
        for (const p of ordered) if (p) yield* sortPartition(p, 0, out);
        if (!out.meta) out.append(empty); // (no rows at all)
        yield* closeWriter(out);
        ok = true;
      } finally { if (!ok) out.abort(); }
      const result = yield* tempDataset(engine, outPath);
      result.temporary = spill.dir;
      spill = null;
      return result;
    },
    dispose() { spill?.cleanup(); empty?.dispose(); },
  };

  function* sortPartition(path, depth, out) {
    const D = yield* tempDataset(engine, path);
    try {
      const splitters = D.length > tuning.partitionRows && depth < 4 ? zoneSplitters(D, first.expr, 16) : null;
      if (splitters) { // split again
        const sub = yield* Spill.create(engine, first.expr, splitters);
        stats.resplits++;
        try {
          yield* eachChunk(D, (t) => sub.add(t));
          const ps = yield* sub.closeG();
          for (const p of first.desc ? [...ps.slice(0, -1).reverse(), ps[ps.length - 1]] : ps) if (p) yield* sortPartition(p, depth + 1, out);
        } finally { sub.cleanup(); }
        return;
      }
      const parts = [];
      let all;
      try {
        yield* eachChunk(D, function* (t) { parts.push(t); });
        all = copyRows(engine, parts.map((t) => [t, 0, t.n]));
      } finally { for (const t of parts) t.dispose(); }
      let sorted;
      try { sorted = yield* keep(engine, yield { run: { op: 'output', input: { op: 'sort', by: B.by, input: { op: 'scan', table: all } } } }, all); } finally { all.dispose(); }
      try { out.append(sorted); } finally { sorted.dispose(); }
      yield { flush: out };
    } finally { D.close(); }
  }
}

// The first sort key as an expression and its direction, if it sorts by
// value (a text column sorts by label: no cutoff then).
// (numeric: a derived column that is arithmetic on number columns counts as a value too)
function firstSortKey(by, ds, derived, numeric = false) {
  const src = String(by).trim();
  const desc = src.startsWith('-');
  const expr = desc ? src.slice(1).trim() : src;
  const ident = /^[\p{L}_][\p{L}\p{M}\p{N}_]*$/u.test(expr);
  if (ident && !derived.has(expr) && (ds.cols.get(expr)?.dict || ds.cols.get(expr)?.strings)) return null;
  if (ident && derived.has(expr) && !(numeric && derived.numeric?.has(expr))) return null; // (a join's text column, or a derived label: be safe)
  return { expr, desc };
}

// numbers, number columns of ds, + - * / and minus only
function arithmetic(ast, ds) {
  if (ast.t === 'num') return true;
  if (ast.t === 'var') { const c = ds.cols.get(ast.name); return !!c && !c.dict && !c.strings; }
  if (ast.t === 'un') return ast.op === '-' && arithmetic(ast.x, ds);
  if (ast.t === 'bin') return ['+', '-', '*', '/'].includes(ast.op) && arithmetic(ast.l, ds) && arithmetic(ast.r, ds);
  return false;
}

// totals and groups: each aggregation as partial values per chunk, merged
//   sum, count -> added; min, max -> min, max (missing skipped); mean -> a
//   sum and a count of the values present; countDistinct / approxDistinct ->
//   the distinct (key, value) pairs, merged as groups, counted at the end
//   (exact on datasets).
const DV = '__vectro_dv';
function groupAccumulator(kind, B, engine, ds, derived) {
  const keys = kind === 'group' ? B.keys : [];
  const partial = {}, merge = {}, outs = [], distinct = [];
  Object.entries(B.spec).forEach(([out, ast], j) => {
    const p = `__vectro_p${j}`;
    if (ast.fn === 'countDistinct' || ast.fn === 'approxDistinct') {
      // (a category column: its missing values, '', are not counted)
      const a0 = ast.args[0], blankable = a0.t === 'var' && !derived.has(a0.name) && ds.cols.get(a0.name)?.dict?.includes('');
      const arg = blankable ? parse(`${a0.name} != '' ? ${a0.name} : 0 / 0`) : a0;
      distinct.push({ out, arg, parts: [], rows: 0, last: 0, bitmap: bitmapPlan(ds, derived, keys, a0) });
      outs.push({ out, distinct: distinct.length - 1 });
      return;
    }
    if (ast.fn === 'mean') {
      const src = toSource(ast.args[0]);
      partial[`${p}s`] = { t: 'call', fn: 'sum', args: [ast.args[0]] };
      partial[`${p}n`] = parse(`count((${src}) == (${src}))`);
      merge[`${p}s`] = parse(`sum(${p}s)`);
      merge[`${p}n`] = parse(`sum(${p}n)`);
      outs.push({ out, mean: p });
      return;
    }
    partial[p] = ast;
    merge[p] = parse(`${ast.fn === 'count' ? 'sum' : ast.fn}(${p})`);
    outs.push({ out, col: p });
  });
  // A group's sum over a chunk is NaN where its values hold +Infinity and -Infinity. Summing partial sums skips
  // NaN (it is missing), so each one's groups where it is NaN are counted in a column of their own, summed
  // too, and the answer is NaN there
  const nanOf = (k) => `${k}__nan`;
  const sums = Object.keys(partial).filter((k) => partial[k].fn === 'sum');
  for (const k of sums) merge[nanOf(k)] = parse(`sum(${nanOf(k)})`);
  const markNaN = (t) => {
    for (const k of sums) {
      const d = engine.live(t.cols.get(k).data), flag = engine.alloc(Float64Array, t.n), f = engine.live(flag);
      for (let i = 0; i < t.n; i++) f[i] = d[i] === d[i] ? 0 : 1;
      t.cols.set(nanOf(k), { data: f, dict: null, codes: null });
      t.owned = [...t.owned, nanOf(k)];
    }
  };
  const nanBack = (G, k, data) => { // (NaN in the answer where a chunk's sum was)
    const flag = G.cols.get(nanOf(k));
    if (!flag) return;
    const f = engine.live(flag.data), d = engine.live(data);
    for (let i = 0; i < G.n; i++) if (f[i] > 0) d[i] = NaN;
  };
  if (!Object.keys(partial).length) { partial.__vectro_rows = parse('count()'); merge.__vectro_rows = parse('sum(__vectro_rows)'); }
  const main = { parts: [], rows: 0, last: 0 };
  const dense = kind === 'group' ? densePlan(ds, derived, keys, partial, engine) : null;
  // too many groups to keep: partial groups go to disk by key range
  const canSpill = kind === 'group' && !dense && !distinct.length;
  let spill = null, chunksDone = 0;
  let totals = null; // kind 'total': partial values so far
  const combine = (fn, a, b) => (fn === 'min' ? (b < a || a !== a ? b : a) : fn === 'max' ? (b > a || a !== a ? b : a) : a + b);
  const mergeFn = Object.fromEntries(Object.entries(merge).map(([k, a]) => [k, a.fn]));

  // grouped partial tables of one kind, merged again when they pile up
  function* compact(state, groupKeys, spec, force) {
    if (!force && (state.parts.length < 2 || state.rows < Math.max(tuning.mergeRows, 2 * state.last))) return;
    if (state.parts.length < 2 && force) return;
    const ps = state.parts.splice(0);
    let all;
    try { all = copyRows(engine, ps.map((p) => [p, 0, p.n])); } finally { for (const p of ps) p.dispose(); }
    try {
      const g = yield { run: { op: 'output', input: { op: 'aggregate', keys: groupKeys, spec, input: { op: 'scan', table: all } } } };
      state.parts.push(yield* keep(engine, g, all));
    } finally { all.dispose(); }
    state.rows = state.last = state.parts[0].n;
  }

  const distinctKeys = [...keys, DV];
  const distinctMerge = { __vectro_dn: parse('sum(__vectro_dn)') };
  return {
    done: false,
    *chunk(P, chunk) {
      if (kind === 'total') {
        const r = yield { run: { op: 'total', input: P, spec: partial } };
        totals = totals ? Object.fromEntries(Object.entries(r).map(([k, v]) => [k, combine(mergeFn[k], totals[k], v)])) : r;
      } else if (dense?.kernel) { // the matching rows added into the running totals, on every thread
        yield { run: { op: 'groupInto', input: P, group: dense.code, K: dense.groups, sources: dense.sources, state: dense.state } };
      } else if (dense) { // group numbers and values of the matching rows, added into the arrays
        let node = P;
        node = { op: 'derive', name: DV, ast: dense.code, input: node };
        for (const it of dense.items) node = { op: 'derive', name: it.e, ast: it.ast, input: node };
        const r = yield { run: { op: 'output', input: { op: 'select', names: [DV, ...dense.items.map((it) => it.e)], input: node } } };
        try {
          denseAdd(engine.live(r.cols.get(DV).data), dense.items.map((it) => engine.live(r.cols.get(it.e).data)), dense.items.map((it) => it.fn), dense.accs, dense.seen, r.n);
        } finally { if (r !== chunk) r.dispose(); }
      } else {
        const r = yield { run: { op: 'output', input: { op: 'aggregate', keys, spec: partial, input: P } } };
        const kept = yield* keep(engine, r, chunk);
        markNaN(kept);
        main.parts.push(kept);
        main.rows += kept.n;
        chunksDone++;
        if (!spill) yield* compact(main, keys, merge, false);
        if (canSpill && !spill && main.rows > tuning.spillRows) {
          const estimate = (main.rows * Math.max(1, ds.chunkCount)) / chunksDone;
          const P = Math.min(64, Math.max(4, Math.ceil((2 * estimate) / tuning.partitionRows)));
          const k0 = keys[0];
          let splitters = !derived.has(k0) && ds.cols.has(k0) ? zoneSplitters(ds, k0, P) : null;
          if (!splitters) { // from the groups so far (they are sorted by key)
            const g = main.parts[0], v = engine.live(g.cols.get(k0).data);
            const vals = [];
            for (let i = 0; i < g.n; i++) if (v[i] === v[i]) vals.push(v[i]);
            splitters = quantiles(vals.sort((a, b) => a - b), P);
          }
          if (splitters) { spill = yield* Spill.create(engine, k0, splitters); stats.spills++; }
        }
        if (spill) { for (const t of main.parts.splice(0)) yield* spill.add(t); main.rows = 0; }
      }
      for (const d of distinct) {
        if (d.bitmap) { // one code per matching row: its group and value, as a bit
          const r = yield { run: { op: 'output', input: { op: 'select', names: [DV], input: { op: 'derive', name: DV, ast: d.bitmap.code, input: P } } } };
          try { setBits(d.bitmap.bits, engine.live(r.cols.get(DV).data), r.n); } finally { if (r !== chunk) r.dispose(); }
          continue;
        }
        const r = yield { run: { op: 'output', input: { op: 'aggregate', keys: distinctKeys, spec: { __vectro_dn: parse('count()') }, input: { op: 'derive', name: DV, ast: d.arg, input: P } } } };
        const kept = yield* keep(engine, r, chunk);
        d.parts.push(kept);
        d.rows += kept.n;
        yield* compact(d, distinctKeys, distinctMerge, false);
      }
    },
    *finish() {
      // distinct counts: per group, the distinct values present
      const counts = [];
      for (const d of distinct) {
        if (d.bitmap) { counts.push(d.bitmap); continue; } // (counted below, per group)
        yield* compact(d, distinctKeys, distinctMerge, true);
        const pairs = d.parts[0];
        if (kind === 'total') counts.push((yield { run: { op: 'total', input: { op: 'scan', table: pairs }, spec: { c: parse(`count(${DV})`) } } }).c);
        else counts.push(yield* keep(engine, yield { run: { op: 'output', input: { op: 'aggregate', keys, spec: { c: parse(`count(${DV})`) }, input: { op: 'scan', table: pairs } } } }, pairs));
      }
      if (kind === 'total') {
        const out = {};
        const value = (c) => (c.bits ? countBits(c.bits, 0, c.range) - blankBit(c, 0) : c);
        for (const o of outs) out[o.out] = o.distinct !== undefined ? value(counts[o.distinct]) : o.mean ? totals[`${o.mean}s`] / totals[`${o.mean}n`] : totals[o.col];
        return out;
      }
      if (spill) return yield* finishSpilled();
      yield* compact(main, keys, merge, true);
      return finalize(dense ? denseTable(engine, dense, keys, ds) : main.parts.pop(), counts);
    },
    dispose() {
      if (dense?.state.acc) { engine.free(dense.state.acc); dense.state.acc = null; }
      for (const p of main.parts.splice(0)) p.dispose();
      for (const d of distinct) for (const p of d.parts.splice(0)) p.dispose();
      spill?.cleanup();
    },
  };

  // Each partition's groups merged and finished, in key order, into a
  // dataset of the result (the rest of the plan reads it chunk by chunk).
  // A partition too big to merge in memory is split again by its first key.
  function* finishSpilled() {
    const paths = yield* spill.closeG();
    const outPath = temp().join(spill.dir, 'groups.vectro');
    const out = yield* tempWriter(engine, outPath);
    let ok = false;
    try {
      for (const p of paths) if (p) yield* mergePartition(p, 0, out);
      yield* closeWriter(out);
      ok = true;
    } finally { if (!ok) out.abort(); }
    const result = yield* tempDataset(engine, outPath);
    const dir = spill.dir;
    spill = null;
    result.temporary = dir; // (deleted after the rest of the plan ran)
    return result;
  }

  function* mergePartition(path, depth, out) {
    const D = yield* tempDataset(engine, path);
    try {
      if (!D.length) return;
      const splitters = D.length > tuning.partitionRows && depth < 4 ? zoneSplitters(D, keys[0], 16) : null;
      if (splitters) { // split again
        const sub = yield* Spill.create(engine, keys[0], splitters);
        stats.resplits++;
        try {
          yield* eachChunk(D, (t) => sub.add(t));
          for (const p of yield* sub.closeG()) if (p) yield* mergePartition(p, depth + 1, out);
        } finally { sub.cleanup(); }
        return;
      }
      const parts = [];
      let all;
      try {
        yield* eachChunk(D, function* (t) { parts.push(t); });
        all = copyRows(engine, parts.map((t) => [t, 0, t.n]));
      } finally { for (const t of parts) t.dispose(); }
      let G;
      try { G = yield* keep(engine, yield { run: { op: 'output', input: { op: 'aggregate', keys, spec: merge, input: { op: 'scan', table: all } } } }, all); } finally { all.dispose(); }
      const F = finalize(G, []);
      try { out.append(F); } finally { F.dispose(); }
      yield { flush: out };
    } finally { D.close(); }
  }

  // Merged partial groups G -> the result: keys, then each aggregation.
  function finalize(G0, counts) {
      const G = G0._plain(); // (packed partial columns: their values, not their codes)
      try {
        const cols = new Map(), owned = [];
        const take = (t, name) => { t.owned = t.owned.filter((n) => n !== name); return t.cols.get(name); };
        for (const k of keys) { cols.set(k, take(G, k)); owned.push(k); }
        for (const o of outs) {
          if (o.col) { cols.set(o.out, take(G, o.col)); nanBack(G, o.col, cols.get(o.out).data); }
          else if (o.mean) {
            const data = engine.alloc(Float64Array, G.n);
            if (G.n) engine.eval('s / n', { s: G.cols.get(`${o.mean}s`).data, n: G.cols.get(`${o.mean}n`).data }, data);
            nanBack(G, `${o.mean}s`, data);
            cols.set(o.out, { data: engine.live(data), dict: null, codes: null });
          } else if (counts[o.distinct].bits) { // each group's bits
            const bm = counts[o.distinct], data = engine.alloc(Float64Array, G.n);
            const keyData = keys.map((k) => engine.live(G.cols.get(k).data)), out = engine.live(data);
            for (let r = 0; r < G.n; r++) {
              let g = 0;
              for (let k = 0; k < keys.length; k++) g += (keyData[k][r] - bm.mins[k]) * bm.strides[k];
              out[r] = countBits(bm.bits, g * bm.range, (g + 1) * bm.range) - blankBit(bm, g);
            }
            cols.set(o.out, { data: engine.live(data), dict: null, codes: null });
          } else {
            const C = counts[o.distinct];
            if (C.n !== G.n) throw new Error(`vectro: internal: ${C.n} distinct-count groups for ${G.n} groups`);
            cols.set(o.out, take(C, 'c'));
          }
          owned.push(o.out);
        }
        for (const c of cols.values()) c.data = engine.live(c.data);
        return new Table(engine, cols, G.n, { owned });
      } finally {
        G.dispose();
        if (G0 !== G) G0.dispose();
        for (const C of counts) C.dispose?.();
      }
  }
}

// Exact distinct counts as one bitmap: when the counted column and the group
// keys are dataset columns of whole numbers (or labels) with known ranges,
// bit (group * range + value - min) marks that a group has that value.
// Null when it doesn't apply (the distinct pairs are merged instead).
const MAX_BITS = 2 ** 31; // 256 MB
function bitmapPlan(ds, derived, keys, arg) {
  if (arg.t !== 'var') return null;
  const v = wholeRange(ds, derived, arg.name), g = v && denseKeys(ds, derived, keys);
  if (!g || g.groups * v[1] > MAX_BITS) return null;
  const blank = ds.cols.get(arg.name).dict?.indexOf('') ?? -1; // (the blank label's code: missing text)
  return { code: parse(`(${g.src}) * ${v[1]} + ((${arg.name}) - (${v[0]}))`), range: v[1], blank, mins: g.mins, strides: g.strides, bits: new Uint32Array(Math.ceil((g.groups * v[1]) / 32)) };
}

// [min, size, missing] of a dataset column's values when they are whole
// numbers one apart (counts, IDs, codes) or labels, from the chunk
// statistics; else null.
function wholeRange(ds, derived, name) {
  if (!ds.cols.has(name) || derived.has(name)) return null;
  const base = ds.cols.get(name);
  if (base.strings) return null;
  if (base.dict) return [0, Math.max(1, base.dict.length), 0];
  let lo = Infinity, hi = -Infinity, missing = 0;
  for (const ch of ds.meta.chunks) {
    const z = ch.cols[name];
    missing += z.missing;
    if (z.missing === ch.rows) continue;
    if (!z.enc || z.enc.div || z.enc.mul !== 1) return null;
    lo = Math.min(lo, fromJson(z.min));
    hi = Math.max(hi, fromJson(z.max));
  }
  return lo === Infinity ? [0, 1, missing] : [lo, hi - lo + 1, missing];
}

// Group keys as one dense group number: (k1 - min1) * stride1 + ..., in key
// order (labels in dictionary order). Null unless every key qualifies.
function denseKeys(ds, derived, keys) {
  const mins = [], sizes = [];
  for (const k of keys) {
    const r = wholeRange(ds, derived, k);
    if (!r || r[2]) return null; // (missing keys form a group of their own: not here)
    mins.push(r[0]);
    sizes.push(r[1]);
  }
  const groups = sizes.reduce((a, b) => a * b, 1);
  const strides = sizes.map((_, k) => sizes.slice(k + 1).reduce((a, b) => a * b, 1));
  let src = '0';
  keys.forEach((k, j) => { src = `${src} + ((${k}) - (${mins[j]})) * ${strides[j]}`; });
  return { src, groups, mins, sizes, strides };
}

// Many groups with dense keys: each chunk's values are added straight into
// one array per partial value, indexed by group number (no partial tables
// to merge). Null when it doesn't apply or the groups are few (the group
// kernels are faster there).
// With compiled kernels the rows are added on every thread (each thread its
// own range of groups, see Table._groupInto); else in plain JS.
const DENSE_MIN = 1 << 16, DENSE_MAX = 1 << 24;
function densePlan(ds, derived, keys, partial, engine) {
  const g = keys.length && denseKeys(ds, derived, keys);
  if (!g || g.groups < (tuning.denseMin ?? DENSE_MIN) || g.groups > DENSE_MAX) return null;
  const names = Object.keys(partial);
  const items = names.map((name, j) => {
    const { fn, args } = partial[name], arg = args[0], a = arg && toSource(arg);
    const e = `__vectro_e${j}`;
    if (fn === 'count') return { name, fn: 'add', e, ast: arg ? parse(isMask(arg) ? `(${a}) ? 1 : 0` : `(${a}) == (${a}) ? 1 : 0`) : parse('1') };
    return { name, fn, e, ast: arg }; // sum, min, max
  });
  // the kernel's reductions: rows per group first, then each item (min / max with a count of values present)
  const sources = ['count(1)'], at = (src) => { let i = sources.indexOf(src); if (i < 0) { i = sources.length; sources.push(src); } return i; };
  for (const it of items) {
    const a = toSource(it.ast), present = `(${a}) == (${a})`;
    if (it.fn === 'add') it.slot = a === '1' ? 0 : at(`sum(${a})`);
    else if (it.fn === 'sum') it.slot = at(`sum(${present} ? (${a}) : 0)`);
    else { it.slot = at(`${it.fn}(${present} ? (${a}) : ${it.fn === 'min' ? 'Infinity' : '-Infinity'})`); it.presentSlot = at(`sum(${present} ? 1 : 0)`); }
  }
  const kernel = engine.jit && sources.length <= 8;
  if (g.groups * (kernel ? sources.length : names.length + 1) * 8 > 2 ** 30) return null;
  return {
    ...g, items, code: parse(g.src), kernel, sources: sources.map((x) => parse(x)), state: { acc: null },
    seen: kernel ? null : new Float64Array(g.groups),
    accs: kernel ? null : items.map((it) => new Float64Array(g.groups).fill(it.fn === 'min' || it.fn === 'max' ? NaN : 0)),
  };
}

// The groups that have rows, in group-number (= key) order, as a table of
// the keys and the partial values (as merged partial tables would be).
function denseTable(engine, dense, keys, ds) {
  const { items, mins, sizes, strides, groups: K } = dense;
  let { seen, accs } = dense;
  if (dense.kernel) { // (the kernel's totals: per item its slot; min / max are missing where no value was present)
    const acc = dense.state.acc ? engine.live(dense.state.acc) : new Float64Array(K * dense.sources.length);
    const slot = (j) => acc.subarray(j * K, (j + 1) * K);
    seen = slot(0);
    accs = items.map((it) => {
      const v = slot(it.slot);
      if (it.presentSlot === undefined) return v;
      const p = slot(it.presentSlot), out = new Float64Array(K);
      for (let g = 0; g < K; g++) out[g] = p[g] ? v[g] : NaN;
      return out;
    });
  }
  let n = 0;
  for (let g = 0; g < seen.length; g++) if (seen[g]) n++;
  const cols = new Map(), owned = [];
  const add = (name, desc) => { cols.set(name, desc); owned.push(name); };
  keys.forEach((k) => { const base = ds.cols.get(k); add(k, { data: engine.alloc(Float64Array, n), dict: base.dict, codes: base.codes, date: base.date }); });
  items.forEach((it) => add(it.name, { data: engine.alloc(Float64Array, n), dict: null, codes: null }));
  const keyOut = keys.map((k) => engine.live(cols.get(k).data)), accOut = items.map((it) => engine.live(cols.get(it.name).data));
  for (let g = 0, r = 0; g < seen.length; g++) {
    if (!seen[g]) continue;
    for (let k = 0; k < keys.length; k++) keyOut[k][r] = mins[k] + (Math.floor(g / strides[k]) % sizes[k]);
    for (let j = 0; j < items.length; j++) accOut[j][r] = accs[j][g];
    r++;
  }
  for (const c of cols.values()) c.data = engine.live(c.data);
  return new Table(engine, cols, n, { owned });
}

// (a plain function: the hot loop)
function denseAdd(gs, values, fns, accs, seen, n) {
  for (let i = 0; i < n; i++) seen[gs[i]]++;
  for (let j = 0; j < values.length; j++) {
    const v = values[j], acc = accs[j], fn = fns[j];
    if (fn === 'add') for (let i = 0; i < n; i++) acc[gs[i]] += v[i];
    else if (fn === 'sum') for (let i = 0; i < n; i++) { const x = v[i]; if (x === x) acc[gs[i]] += x; }
    else if (fn === 'min') for (let i = 0; i < n; i++) { const x = v[i], g = gs[i], a = acc[g]; if (x < a || (a !== a && x === x)) acc[g] = x; }
    else for (let i = 0; i < n; i++) { const x = v[i], g = gs[i], a = acc[g]; if (x > a || (a !== a && x === x)) acc[g] = x; }
  }
}

function setBits(bits, codes, n) {
  for (let i = 0; i < n; i++) {
    const c = codes[i];
    if (c === c) bits[c >>> 5] |= 1 << (c & 31); // (missing values: NaN, not counted)
  }
}

// 1 when group g of a bitmap saw the blank label (missing text: not counted), else 0
function blankBit(bm, g) {
  if (bm.blank < 0) return 0;
  const i = g * bm.range + bm.blank;
  return (bm.bits[i >>> 5] >>> (i & 31)) & 1;
}

function countBits(bits, from, to) {
  let n = 0;
  const pop = (x) => { x -= (x >>> 1) & 0x55555555; x = (x & 0x33333333) + ((x >>> 2) & 0x33333333); return (((x + (x >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24; };
  for (let b = from; b < to; ) {
    const w = b >>> 5, off = b & 31, take = Math.min(32 - off, to - b);
    const mask = take === 32 ? 0xffffffff : ((1 << take) - 1) << off;
    n += pop((bits[w] & mask) >>> 0);
    b += take;
  }
  return n;
}

// ---- CSV of any size -> dataset ------------------------------------------------------------
// Read in pieces (pieceBytes, cut at row ends), each parsed on every thread
// with the column types found in the first piece. Returns the writer's summary.
export function csvToDataset(engine, csvPath, outPath, opts = {}) {
  const file = need('csvToDataset').openRead(csvPath);
  try {
    const writer = new DatasetWriter(engine, outPath, opts);
    const gen = csvPiecesG(engine, file.size, writer, opts);
    let step = gen.next();
    while (!step.done) {
      const v = step.value;
      let out, err = null;
      try { out = v.read ? file.readSync(v.read, v.pos) : v.parse ? loadBytes(engine, v.parse, v.opts) : undefined; } catch (e) { err = e; }
      step = err ? gen.throw(err) : gen.next(out);
    }
    return writer.close();
  } finally { file.close(); }
}

// The same without blocking: reads with async I/O, parses on the worker
// threads. csv: a path (Node), a URL, or a File / Blob; out: a path (Node) or
// a writable file object (browsers: see files-browser.mjs).
export async function csvToDatasetAsync(engine, csv, out, opts = {}) {
  const file = typeof csv === 'string' && files && !/^https?:/i.test(csv) ? files.openRead(csv)
    : typeof csv === 'string' ? await urlFile(csv) : typeof Blob === 'function' && csv instanceof Blob ? blobFile(csv) : csv;
  try {
    const writer = await createDatasetAsync(engine, out, opts);
    try {
      const gen = csvPiecesG(engine, file.size, writer, opts);
      let step = gen.next();
      while (!step.done) {
        const v = step.value;
        let got, err = null;
        try { got = v.read ? await file.read(v.read, v.pos) : v.parse ? await loadBytesAsync(engine, v.parse, v.opts) : await writer.file.flush?.(); } catch (e) { err = e; }
        step = err ? gen.throw(err) : gen.next(got);
      }
    } catch (err) { writer.abort(); throw err; }
    return await writer.closeAsync();
  } finally { file.close(); }
}

// Yields { read: bytes, pos } (fill bytes from the file at pos), { parse:
// engine bytes, opts } (a table) and { flush } (wait for the writer); appends
// each piece's rows to the writer (aborted on an error).
// opts.onProgress({ bytes, size, rows }) is called after each piece.
function* csvPiecesG(engine, size, writer, opts = {}) {
  let { pieceBytes = 64 << 20, header = true, delimiter = ',', types = {}, sampleRows, rowsPerChunk, bitPack, append, onProgress, ...rest } = opts;
  try {
    checkCSVOptions(opts);
    if (!(pieceBytes >= 1)) throw new Error('vectro: pieceBytes must be 1 or more'); // (0 read nothing, forever)
    pieceBytes = Math.floor(pieceBytes);
    let pos = 0, carry = new Uint8Array(0), head = new Uint8Array(0), kinds = null, crOnly = null;
    const last0 = (at) => at >= size;
    while (true) {
      const want = Math.min(pieceBytes, size - pos);
      const buf = new Uint8Array(carry.length + want);
      buf.set(carry, 0);
      yield { read: buf.subarray(carry.length), pos };
      // (CR-only line ends, decided on the first piece: every piece's CRs become LFs, see crOnlyToLF)
      const fresh = buf.subarray(carry.length);
      if (crOnly === null) { // (on the first piece with an LF, or a CR that isn't its last byte: an LF may follow it)
        const cr = fresh.indexOf(13);
        if (fresh.includes(10) || (cr !== -1 && cr < fresh.length - 1) || last0(pos + want)) crOnly = crOnlyToLF(buf); // (the carry too: nothing was parsed yet)
      }
      else if (crOnly) for (let i = fresh.indexOf(13); i !== -1; i = fresh.indexOf(13, i + 1)) fresh[i] = 10;
      pos += want;
      const last = pos >= size;
      let start = 0;
      // the first piece needs the header and the rows types are guessed from (as readCSV guesses them): read on
      if (!kinds && !last && !hasRows(buf, (header ? 1 : 0) + (sampleRows ?? 1000), delimiter)) { carry = buf; continue; }
      if (!kinds) { // the first piece: column names and types, from its complete rows
        const info = inspectCSV(last ? buf : buf.subarray(0, rowsEnd(buf, 0, delimiter)), { delimiter, header, types, sampleRows, trimEnd: last });
        // (a guessed free-text column in a Float32Array table: a category, as readCSV loads it)
        kinds = Object.fromEntries(info.names.map((n, i) => [n, info.kinds[i] === 'string' && rest.type === Float32Array && !(n in types) ? 'category' : info.kinds[i]]));
        head = header ? buf.slice(0, info.start) : new Uint8Array(0);
        start = info.start;
      }
      let end = last ? buf.length : rowsEnd(buf, start, delimiter);
      if (!last) { // (blank lines at a piece's end may be the file's end, which has no rows: they go with the next piece)
        let t = end;
        while (t > start && (buf[t - 1] === 10 || buf[t - 1] === 13)) t--;
        if (t < end) end = t === start ? start : t + (buf[t] === 13 && buf[t + 1] === 10 ? 2 : 1);
      }
      if (end > start || (!writer.meta && last)) { // (a header and no rows: still the columns)
        const rows = end > start ? buf.subarray(start, end) : new Uint8Array(0);
        const bytes = engine.alloc(Uint8Array, head.length + rows.length);
        bytes.set(head, 0);
        bytes.set(rows, head.length);
        const t = yield { parse: bytes, opts: { delimiter, header, types: kinds, ...rest, checkTypes: Object.keys(kinds).filter((nm) => !(nm in types)), badType: 'throw', trimEnd: last } };
        try { writer.append(t); } finally { t.dispose(); }
        yield { flush: writer };
        onProgress?.({ bytes: pos, size, rows: writer.meta.rows + writer.pendingRows });
      }
      if (last) break;
      carry = buf.slice(Math.max(start, end));
    }
  } catch (err) {
    writer.abort();
    throw err;
  }
}

// Whether buf holds at least k complete rows.
function hasRows(buf, k, delimiter) {
  let n = 0;
  csvRowEnds(buf, 0, buf.length, delimiter.charCodeAt(0), () => ++n < k);
  return n >= k;
}

// Where the last complete row of buf[start..] ends (quotes may hold newlines;
// the parser's quote rules, csv-kernel.mjs).
function rowsEnd(buf, start, delimiter) {
  const NL = 10, QUOTE = 34;
  if (buf.indexOf(QUOTE, start) === -1) return buf.lastIndexOf(NL) + 1;
  let end = start;
  csvRowEnds(buf, start, buf.length, delimiter.charCodeAt(0), (i) => { end = i; });
  return end;
}

// Write a table, a query or a dataset as a dataset. A query over a dataset
// that ends in matching rows is written chunk by chunk.
export function writeDataset(engine, path, source, opts) {
  const writer = new DatasetWriter(engine, path, opts);
  try {
    if (source instanceof Query || source?.isDataset) {
      const plan = optimize({ op: 'output', input: (source.isDataset ? source.lazy() : source).node });
      if (findDataset(plan)) driveSync(runPlanG(plan, writer));
      else { const t = execute(plan, true); try { writer.append(t); } finally { t.dispose(); } }
    } else writer.append(source);
  } catch (err) {
    writer.abort();
    throw err;
  }
  return writer.close();
}

// A writer for a path (Node) or a writable file object (browsers), reading
// the footer first when appending to a file that has one.
export async function createDatasetAsync(engine, target, opts = {}) {
  const file = typeof target === 'string' ? need('writing a dataset').openWrite(target, { append: opts.append, atomic: true }) : target;
  let footer = null;
  if (file.existed) try { footer = file.readSync ? readFooter(file) : await readFooterAsync(file); } catch (err) { file.close(); throw err; }
  return new DatasetWriter(engine, file, { ...opts, footer });
}

// writeDataset without blocking: queries run with the async calls, and the
// writer's bytes are on disk before more are made.
export async function writeDatasetAsync(engine, target, source, opts) {
  const writer = await createDatasetAsync(engine, target, opts);
  try {
    if (source instanceof Query || source?.isDataset) {
      const plan = optimize({ op: 'output', input: (source.isDataset ? source.lazy() : source).node });
      if (findDataset(plan)) await driveAsync(runPlanG(plan, writer));
      else { const t = await execute(plan, false); try { await writer.appendAsync(t); } finally { t.dispose(); } }
    } else await writer.appendAsync(source);
  } catch (err) {
    writer.abort();
    throw err;
  }
  return writer.closeAsync();
}

trackAsyncMethods(Dataset, (d) => d.engine);
trackAsyncMethods(DatasetWriter, (w) => w.engine);
