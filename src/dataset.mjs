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

import { openSync, readSync, writeSync, closeSync, fstatSync, ftruncateSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join as joinPath } from 'node:path';
import { open as openFile } from 'node:fs/promises';
import { Table, rewriteText } from './table.mjs';
import { Query, execute, findDataset, optimize, vars, namesOf, joinNames } from './plan.mjs';
import { parse, toSource, isMask } from './fuse.mjs';
import { KINDS, decodeOne } from './pack.mjs';
import { conditions, mayMatch, computeZones } from './zones.mjs';
import { STR_SHIFT, STR_ESCAPED } from './csv-kernel.mjs';
import { inspectCSV, loadBytes } from './csv.mjs';
import { freeBuild } from './join.mjs';
import { OP } from './kernels.mjs';
import { bitPlan, packBits, unpackBits, unpackModule, seqModule } from './bitpack.mjs';

const MAGIC = new TextEncoder().encode('VECTRODS');
const VERSION = 2; // (2: bit-packed columns; version 1 files read too)
const HEADER = 16, TAIL = 24, ALIGN = 64;
const SLACK = 16;
const READ_PIECE = 1 << 20; // reads are cut into pieces this big, read by several threads at once // bytes after packed codes (SIMD loads read a little past the end)
export const CHUNK_ROWS = 1 << 22;
const BLOCK = 1 << 16; // min / max per block of rows inside a chunk: a query reads only the blocks that can match
// partial groups are merged again once this many pile up (tests make it small)
// how often partial results went to disk (tests, benchmarks)
export const stats = { spills: 0, resplits: 0 };
export const tuning = { mergeRows: 1 << 20, denseMin: undefined, spillRows: 1 << 23, partitionRows: 1 << 24 };

// ---- writing --------------------------------------------------------------------------

export class DatasetWriter {
  // rowsPerChunk: rows per chunk (the last one may be shorter); append: add
  // to an existing dataset (same columns) instead of replacing it.
  // bitPack: false stores packed codes in whole bytes (bigger files, no unpacking).
  constructor(engine, path, { rowsPerChunk = CHUNK_ROWS, append = false, bitPack = true } = {}) {
    this.engine = engine;
    this.bitPack = bitPack;
    this.path = path;
    this.rowsPerChunk = Math.max(1, Math.floor(rowsPerChunk));
    this.pending = null; // rows waiting for a full chunk (a table this writer owns)
    this.meta = null;
    this.labels = new Map(); // category column -> { dict, codes }: one dictionary for the dataset
    if (append && existsSync(path)) {
      const { fd, meta, footerStart } = readFooter(path, 'r+');
      this.fd = fd;
      this.meta = meta;
      this.pos = footerStart; // new chunks overwrite the old footer
      const v = new Uint8Array(4);
      new DataView(v.buffer).setUint32(0, VERSION, true);
      writeSync(fd, v, 0, 4, 8); // (new chunks may use this version's features)
      meta.version = VERSION;
      for (const c of meta.columns) if (c.kind === 'category') this.labels.set(c.name, { dict: c.dict, codes: new Map(c.dict.map((s, i) => [s, i])) });
    } else {
      this.fd = openSync(path, 'w');
      const head = new Uint8Array(HEADER);
      head.set(MAGIC);
      new DataView(head.buffer).setUint32(8, VERSION, true);
      writeSync(this.fd, head, 0, HEADER, 0);
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
      hi = Math.min(hi, t.n);
      if (this.pending) {
        const all = copyRows(engine, [[this.pending, 0, this.pending.n], [t, lo, hi]]);
        this.pending.dispose();
        this.pending = null;
        temps.push(all);
        t = all;
        lo = 0;
        hi = all.n;
      }
      for (; hi - lo >= this.rowsPerChunk; lo += this.rowsPerChunk) this.#writeChunk(t, lo, lo + this.rowsPerChunk);
      if (lo < hi || !this.meta.chunks.length) this.pending = copyRows(engine, [[t, lo, hi]]);
    } finally {
      for (const x of temps) x.dispose();
    }
    return this;
  }

  // Write what's left and the footer. Returns { path, rows, chunks, bytes }.
  close() {
    if (this.summary) return this.summary;
    if (this.aborted) throw new Error('vectro: this dataset writer was aborted');
    try {
      if (!this.meta) throw new Error('vectro: append at least one table to a dataset (it can have no rows)');
      if (this.pending && (this.pending.n || !this.meta.chunks.length)) this.#writeChunk(this.pending, 0, this.pending.n);
      for (const c of this.meta.columns) if (c.kind === 'category') c.dict = this.labels.get(c.name).dict;
      const json = new TextEncoder().encode(JSON.stringify(this.meta));
      const { off } = this.#write(json);
      const tail = new Uint8Array(TAIL), dv = new DataView(tail.buffer);
      dv.setFloat64(0, off, true);
      dv.setFloat64(8, json.length, true);
      tail.set(MAGIC, 16);
      writeSync(this.fd, tail, 0, TAIL, this.pos);
      this.pos += TAIL;
      ftruncateSync(this.fd, this.pos);
      this.summary = { path: this.path, rows: this.meta.rows, chunks: this.meta.chunks.length, bytes: this.pos };
      return this.summary;
    } finally {
      this.pending?.dispose();
      this.pending = null;
      closeSync(this.fd);
    }
  }

  // Stop without a footer (the file is left unreadable as a dataset).
  abort() {
    if (this.summary || this.aborted) return;
    this.aborted = true;
    this.pending?.dispose();
    this.pending = null;
    closeSync(this.fd);
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
    for (const c of want) {
      const k = kindOf(t.cols.get(c.name));
      if (k !== c.kind) throw new Error(`vectro: column "${c.name}" is ${k} here but ${c.kind} in the dataset`);
    }
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
      for (const [name, c] of cols) {
        c.data = engine.live(c.data);
        const [min, max, missing] = n ? engine.eval(['min(v == v ? v : Infinity)', 'max(v == v ? v : -Infinity)', 'count(v != v)'], { v: c.data }) : [Infinity, -Infinity, 0];
        chunk.cols[name] = { min: jsonNumber(min), max: jsonNumber(max), missing };
        if (n > BLOCK) { const z = computeZones(engine.live(c.data), n, BLOCK); chunk.cols[name].blocks = { min: Array.from(z.mins, jsonNumber), max: Array.from(z.maxs, jsonNumber) }; }
      }
      if (n) ct.compress();
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
    if (pad) { writeSync(this.fd, new Uint8Array(pad), 0, pad, this.pos); this.pos += pad; }
    const off = this.pos;
    for (let done = 0; done < u8.length; ) done += writeSync(this.fd, u8, done, u8.length - done, off + done);
    this.pos += u8.length;
    return { off, len: u8.length };
  }
}

// Codes first, first + step, first + 2 * step... (row numbers, times at a
// fixed interval): { first, step }, else null.
function sequence(codes, n) {
  if (n < 2) return null;
  const first = codes[0], step = codes[1] - first;
  for (let i = 2; i < n; i++) if (codes[i] !== first + i * step) return null;
  return { first, step };
}

const textLength = (v) => { const field = v - Math.floor(v / STR_SHIFT) * STR_SHIFT; return field >= STR_ESCAPED ? field - STR_ESCAPED : field; };
const jsonNumber = (v) => (Number.isFinite(v) ? v : String(v)); // JSON has no Infinity
const fromJson = (v) => (typeof v === 'string' ? Number(v) : v);

// Rows [lo, hi) of column c as plain numbers in out (packed: decoded).
function plainInto(engine, c, lo, hi, out, f32) {
  const src = engine.live(c.data);
  if (!c.enc) { out.set(src.subarray(lo, hi)); return; }
  for (let i = lo; i < hi; i++) out[i - lo] = decodeOne(c.enc, src[i], f32);
}

// A table's category codes as codes of the dataset's dictionary (new labels added).
function remapCodes(data, dict, labels) {
  if (dict === labels.dict) return;
  const lut = new Float64Array(dict.length);
  let same = dict.length <= labels.dict.length;
  dict.forEach((s, j) => {
    let g = labels.codes.get(s);
    if (g === undefined) { g = labels.dict.length; labels.dict.push(s); labels.codes.set(s, g); }
    lut[j] = g;
    if (g !== j) same = false;
  });
  if (same) return;
  for (let i = 0; i < data.length; i++) { const v = data[i]; data[i] = v === v ? lut[v] : NaN; }
}

const sameDict = (a, b) => a === b || (a.length === b.length && a.every((s, i) => s === b[i]));

// The rows [lo, hi) of each part, one after another, as a new table (owned,
// plain columns; categories share a dictionary; free text gets its own bytes).
export function copyRows(engine, parts) {
  const first = parts[0][0];
  const T = first.type;
  const n = parts.reduce((a, [, lo, hi]) => a + (hi - lo), 0);
  const cols = new Map(), owned = [], ownedBuffers = [];
  const table = new Table(engine, cols, n, { owned, ownedBuffers });
  try {
    for (const name of first.columnNames) {
      const c0 = first.cols.get(name);
      if (c0.strings) {
        let total = 0;
        for (const [t, lo, hi] of parts) { const p = engine.live(t.cols.get(name).data); for (let i = lo; i < hi; i++) if (p[i] === p[i]) total += textLength(p[i]); }
        const data = engine.alloc(Float64Array, n), strings = engine.alloc(Uint8Array, total);
        cols.set(name, { data, dict: null, codes: null, strings });
        owned.push(name);
        ownedBuffers.push(strings);
        const o = engine.live(data), sb = engine.live(strings);
        let at = 0, row = 0;
        for (const [t, lo, hi] of parts) {
          const c = t.cols.get(name), p = engine.live(c.data), b = engine.live(c.strings);
          for (let i = lo; i < hi; i++, row++) {
            const v = p[i];
            if (v !== v) { o[row] = NaN; continue; }
            const start = Math.floor(v / STR_SHIFT), len = textLength(v);
            sb.set(b.subarray(start, start + len), at);
            o[row] = at * STR_SHIFT + (v - start * STR_SHIFT);
            at += len;
          }
        }
        continue;
      }
      const data = engine.alloc(T, n);
      let dict = null, codes = null;
      if (c0.dict) {
        dict = c0.dict;
        if (parts.some(([t]) => !sameDict(t.cols.get(name).dict, dict))) dict = [...dict];
        codes = dict === c0.dict ? c0.codes : new Map(dict.map((s, i) => [s, i]));
      }
      cols.set(name, { data, dict, codes, date: c0.date });
      owned.push(name);
      let row = 0;
      for (const [t, lo, hi] of parts) {
        const c = t.cols.get(name), o = engine.live(data).subarray(row, row + hi - lo);
        plainInto(engine, c, lo, hi, o, T === Float32Array);
        if (dict && c.dict !== dict && !sameDict(c.dict, dict)) remapCodes(o, c.dict, { dict, codes });
        row += hi - lo;
      }
    }
    for (const c of cols.values()) { c.data = engine.live(c.data); if (c.strings) c.strings = engine.live(c.strings); }
    return table;
  } catch (err) {
    table.dispose();
    throw err;
  }
}

// ---- reading --------------------------------------------------------------------------

function readFooter(path, flags) {
  const fd = openSync(path, flags);
  try {
    const size = fstatSync(fd).size;
    const head = new Uint8Array(HEADER), tail = new Uint8Array(TAIL);
    if (size < HEADER + TAIL) throw new Error(`vectro: ${path} is not a Vectro dataset (or wasn't closed)`);
    readFully(fd, head, 0);
    readFully(fd, tail, size - TAIL);
    if (!MAGIC.every((b, i) => head[i] === b && tail[16 + i] === b)) throw new Error(`vectro: ${path} is not a Vectro dataset (or wasn't closed)`);
    const version = new DataView(head.buffer).getUint32(8, true);
    if (version < 1 || version > VERSION) throw new Error(`vectro: ${path} is dataset version ${version}; this Vectro reads versions 1 to ${VERSION}`);
    const dv = new DataView(tail.buffer);
    const footerStart = dv.getFloat64(0, true), len = dv.getFloat64(8, true);
    const json = new Uint8Array(len);
    readFully(fd, json, footerStart);
    return { fd, meta: JSON.parse(new TextDecoder().decode(json)), footerStart, size };
  } catch (err) {
    closeSync(fd);
    throw err;
  }
}

function readFully(fd, u8, pos) {
  for (let done = 0; done < u8.length; ) {
    const got = readSync(fd, u8, done, u8.length - done, pos + done);
    if (got === 0) throw new Error('vectro: dataset file ended early');
    done += got;
  }
}

async function readFullyAsync(fh, u8, pos) {
  for (let done = 0; done < u8.length; ) {
    const { bytesRead } = await fh.read(u8, done, u8.length - done, pos + done);
    if (bytesRead === 0) throw new Error('vectro: dataset file ended early');
    done += bytesRead;
  }
}

const bytesOf = (a, len) => new Uint8Array(a.buffer, a.byteOffset, len);

export class Dataset {
  constructor(engine, path) {
    const { fd, meta, size } = readFooter(path, 'r');
    this.engine = engine;
    this.path = path;
    this.fd = fd;
    this.meta = meta;
    this.bytes = size;
    this.isDataset = true;
    this.type = meta.type === 'f32' ? Float32Array : Float64Array;
    // column info the planner and SQL look at (labels, dates, free text)
    this.cols = new Map(meta.columns.map((c) => [c.name, c.kind === 'category'
      ? { dict: c.dict, codes: new Map(c.dict.map((s, i) => [s, i])) }
      : { dict: null, codes: null, date: c.kind === 'date', strings: c.kind === 'text' || undefined }]));
    this.lastRun = null; // { chunks, read, skipped, rows, bytesRead, ms } of the last query
    this.fh = null;
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
  count(expr) { return expr ? this.agg({ v: `count(${expr})` }).v : this.lazy().count(); }
  async countAsync(expr) { return expr ? (await this.aggAsync({ v: `count(${expr})` })).v : this.lazy().countAsync(); }
  sum(expr, params) { return this.agg({ v: `sum(${expr})` }, params).v; }
  mean(expr, params) { return this.agg({ v: `mean(${expr})` }, params).v; }
  min(expr, params) { return this.agg({ v: `min(${expr})` }, params).v; }
  max(expr, params) { return this.agg({ v: `max(${expr})` }, params).v; }
  head(n) { return this.lazy().limit(n).collect(); }
  headAsync(n) { return this.lazy().limit(n).collectAsync(); }
  toRows(limit = Infinity) { return this.lazy().toRows(limit); }
  toRowsAsync(limit = Infinity) { return this.lazy().toRowsAsync(limit); }
  collect() { return this.lazy().collect(); }        // the whole dataset in memory
  collectAsync() { return this.lazy().collectAsync(); }
  explain() { return this.lazy().explain(); }

  // Called by plan.mjs for a plan that reads a dataset.
  runPlan(plan, sync) { return sync ? driveSync(runPlanG(plan)) : driveAsync(runPlanG(plan)); }

  async close() {
    if (this.fd !== null) { closeSync(this.fd); this.fd = null; }
    if (this.fh) { const fh = this.fh; this.fh = null; await (await fh).close(); }
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
    const out = [];
    for (const [arr, off, len] of reads) for (let at = 0; at < len; at += READ_PIECE) out.push([arr, off + at, Math.min(READ_PIECE, len - at), at]);
    return out;
  }

  _readSync(piece, names) {
    const { table, reads, unpacks, temps, bytes } = this.#allocChunk(piece, names);
    const { engine } = this;
    try {
      const pieces = this.#pieces(reads);
      if (engine.W > 0 && engine.canBlock !== false && pieces.length > 1) { // every thread reads pieces straight into engine memory
        const jobs = engine.alloc(Float64Array, pieces.length * 4);
        try {
          const j = engine.live(jobs);
          pieces.forEach(([arr, off, len, at], u) => { j.set([this.fd, off, len, engine.live(arr).byteOffset + at], u * 4); });
          engine._drive((function* () {
            yield { op: OP.READ, n: pieces.length, arrays: [jobs], F: (J, u) => readFully(J[u * 4], new Uint8Array(engine.memory.buffer, J[u * 4 + 3], J[u * 4 + 2]), J[u * 4 + 1]) };
          })());
        } finally { engine.free(jobs); }
      } else for (const [arr, off, len] of reads) readFully(this.fd, bytesOf(engine.live(arr), len), off);
      this.engine._drive(this.#unpackG(unpacks));
    } catch (err) { table.dispose(); throw err; } finally { for (const t of temps) this.engine.free(t); }
    if (this.lastRun) this.lastRun.bytesRead += bytes;
    return table;
  }

  async _readAsync(piece, names) {
    const { table, reads, unpacks, temps, bytes } = this.#allocChunk(piece, names);
    try {
      this.fh ??= openFile(this.path, 'r');
      const fh = await this.fh;
      await Promise.all(this.#pieces(reads).map(async ([arr, off, len, at]) => { // (in pieces: Node's I/O threads read them at once)
        const view = () => new Uint8Array(this.engine.live(arr).buffer, this.engine.live(arr).byteOffset + at, len);
        if (this.engine.isShared) return readFullyAsync(fh, view(), off); // straight into engine memory
        const buf = new Uint8Array(len); // (plain memory may move while the read runs)
        await readFullyAsync(fh, buf, off);
        view().set(buf);
      }));
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
    try { out = v.run ? execute(v.run, true) : v.read ? v.read._readSync(v.piece, v.names) : undefined; } catch (e) { err = e; }
    step = err ? gen.throw(err) : gen.next(out);
  }
  return step.value;
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
    for (const p of pending.values()) p.then((t) => t.dispose(), () => {});
  }
}

const BLOCKING = { aggregate: 'group', total: 'total', limit: 'head', sort: 'gather', output: 'collect' };
const union = (a, b) => new Set([...a, ...b]);
const onKeys = (on) => (typeof on === 'string' || Array.isArray(on) ? [[on].flat(), [on].flat()] : [[on?.left].flat(), [on?.right].flat()]);

// The joins in a per-chunk plan, marked to return views (scans kept as they are).
function viewJoins(node) {
  if (node.op === 'scan') return node;
  if (node.op === 'join') return { ...node, opts: { ...node.opts, view: true }, left: viewJoins(node.left) };
  return { ...node, input: viewJoins(node.input) };
}

// node with `target` (the same object) replaced by `repl`
function substitute(node, target, repl) {
  if (node === target) return repl;
  if (node.op === 'scan') return node;
  if (node.op === 'join') return { ...node, left: substitute(node.left, target, repl), right: substitute(node.right, target, repl) };
  return { ...node, input: substitute(node.input, target, repl) };
}

// The dataset columns a plan reads (null: all of them). Mirrors what the
// executor in plan.mjs asks of its input.
function needs(node, need, ds) {
  switch (node.op) {
    case 'scan': return node.table === ds ? need : undefined;
    case 'filter': return needs(node.input, need && union(need, vars(node.ast)), ds);
    case 'derive':
      if (!need) return needs(node.input, null, ds);
      if (!need.has(node.name)) return needs(node.input, need, ds);
      return needs(node.input, union([...need].filter((n) => n !== node.name), vars(node.ast)), ds);
    case 'select': return needs(node.input, new Set(need ? node.names.filter((n) => need.has(n)) : node.names), ds);
    case 'aggregate': return needs(node.input, new Set([...node.keys, ...Object.values(node.spec).flatMap((a) => [...vars(a)])]), ds);
    case 'total': return needs(node.input, new Set(Object.values(node.spec).flatMap((a) => [...vars(a)])), ds);
    case 'sort': return needs(node.input, need && union(need, node.by.flatMap((b) => [...vars(parse(String(b)))])), ds);
    case 'limit': return needs(node.input, need, ds);
    case 'output': return needs(node.input, null, ds);
    case 'join': {
      const { left } = joinNames(node);
      const needL = new Set(onKeys(node.opts.on)[0]);
      for (const n of need ?? namesOf(node)) if (left.has(n)) needL.add(n);
      for (const n of namesOf(node.right)) if (left.has(n)) needL.add(n); // (they decide the names of right columns)
      return needs(node.left, needL, ds);
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

// Every join's right side (the build side) as one plain table, made once:
// a dataset there is read in full (its matching rows and used columns), a
// filtered or packed table is collected / decoded, so each chunk's join finds
// the same table (and its hash table, kept in engine._joinBuilds).
function* materializeRights(node, temps) {
  if (node.op === 'scan') return node;
  if (node.op === 'join') {
    const left = yield* materializeRights(node.left, temps);
    let right = node.right;
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
    for (let n = plan; n.op !== 'scan'; ) { n = n.op === 'join' ? n.left : n.input; path.push(n); }
    const scan = path[path.length - 1];
    if (!scan.table.isDataset) result = yield { run: plan };
    else result = yield* runChunksG(plan, path, sink);
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
  if (B.op === 'sort' && bi > 0 && path[bi - 1].op === 'limit' && path[bi - 1].input === B) { B = path[bi - 1]; kind = 'top'; }
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
  const derived = new Set(path.slice(0, -1).filter((n) => n.op === 'derive' || n.op === 'join').flatMap((n) => (n.op === 'derive' ? [n.name] : namesOf(n.right))));
  const acc = accumulator(kind, B, engine, sink, ds, derived);
  let finalTable = null;
  engine._joinRuns = (engine._joinRuns ?? 0) + 1; // joins keep their right side's build while a dataset runs
  engine._joinBuilds ??= new Map();
  try {
    for (let k = 0; k < list.length; k++) {
      if (k > 0 && acc.done) break;
      const chunk = yield { read: ds, piece: list[k], names };
      if (k + 1 < list.length && !acc.done) yield { prefetch: ds, piece: list[k + 1], names };
      ds.lastRun.read += list[k].i >= 0 ? 1 : 0;
      ds.lastRun.rows += chunk.n;
      try {
        yield* acc.chunk(substitute(viewTop, scan, { op: 'scan', table: chunk }), chunk);
      } finally {
        chunk.dispose();
      }
    }
    const merged = yield* acc.finish();
    if (kind === 'total' || merged === null) return merged;
    if (merged.isDataset) { // spilled groups: the rest of the plan reads them from disk
      try { return yield* runPlanG(substitute(plan, B, { op: 'scan', table: merged }), sink); } finally { merged.close(); rmSync(merged.temporary, { recursive: true, force: true }); }
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
  if (kind === 'gather' && sink) { const first = firstSortKey(B.by[0], ds, derived); if (first && /^[A-Za-z_]\w*$/.test(first.expr)) return sortSpillAccumulator(B, engine, ds, derived, first); }
  const parts = [];
  let rows = 0;
  // top-k: once k rows are kept, a row whose first key is worse than the
  // k-th can't make it, so later chunks get that cutoff as a filter (ties pass)
  const k = B.n + (B.offset ?? 0);
  const first = kind === 'top' && k > 0 ? firstSortKey(B.input.by[0], ds, derived) : null;
  let cutoff = null;
  const acc = {
    done: false,
    *chunk(P, chunk) {
      let node;
      if (kind === 'top') {
        node = { op: 'output', input: { op: 'limit', n: k, offset: 0, input: { ...B.input, input: P } } };
        if (cutoff !== null) { // only rows at least as good as the k-th so far: keep them all (the merge picks the best)
          node = { op: 'output', input: { op: 'filter', ast: parse(`(${first.expr}) ${first.desc ? '>=' : '<='} ${toSource({ t: 'num', v: cutoff })}`), input: P } };
        }
      }
      else if (kind === 'head') node = { op: 'output', input: { op: 'limit', n: B.n + (B.offset ?? 0) - rows, offset: 0, input: P } };
      else node = { op: 'output', input: P };
      const r = yield { run: node };
      if (sink && kind === 'collect') { // straight to the dataset being written
        try { sink.append(r); } finally { if (r !== chunk) r.dispose(); }
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
        const v = (yield { run: { op: 'total', spec: { v: parse(`min(${first.expr})`) }, input: { op: 'limit', n: 1, offset: k - 1, input: { op: 'scan', table: parts[0] } } } }).v;
        if (v === v) cutoff = v;
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
  constructor(engine, key, splitters) {
    this.engine = engine;
    this.dir = mkdtempSync(joinPath(tmpdir(), 'vectro-spill-'));
    const P = splitters.length + 2;
    this.partAst = parse(`(${key}) != (${key}) ? ${P - 1} : (0${splitters.map((v) => ` + ((${key}) >= ${toSource({ t: 'num', v })})`).join('')})`);
    this.paths = Array.from({ length: P }, (_, j) => joinPath(this.dir, `part-${j}.vectro`));
    this.writers = this.paths.map((p) => new DatasetWriter(engine, p, { rowsPerChunk: 1 << 18 }));
    this.rows = 0;
  }

  // t's rows, each to its partition (t is freed)
  *add(t) {
    try {
      if (!t.n) return;
      this.rows += t.n;
      const S = yield { run: { op: 'output', input: { op: 'sort', by: [PART], input: { op: 'derive', name: PART, ast: this.partAst, input: { op: 'scan', table: t } } } } };
      try {
        const part = this.engine.live(S.cols.get(PART).data), view = S.select(...t.columnNames);
        for (let a = 0; a < S.n; ) {
          const j = part[a];
          let b = a;
          while (b < S.n && part[b] === j) b++;
          this.writers[j].append(view, a, b);
          a = b;
        }
      } finally { S.dispose(); }
    } finally { t.dispose(); }
  }

  // the partition files in key order (null: no rows)
  close() {
    for (const w of this.writers) { if (!w.meta) w.abort(); else w.close(); }
    return this.paths.map((p, j) => (this.writers[j].meta ? p : null));
  }

  cleanup() { for (const w of this.writers) w.abort(); rmSync(this.dir, { recursive: true, force: true }); }
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
        spill = new Spill(engine, first.expr, splitters ?? []);
        stats.spills++;
      }
      yield* spill.add(t);
    },
    *finish() {
      spill ??= new Spill(engine, first.expr, []);
      const paths = spill.close();
      const ordered = first.desc ? [...paths.slice(0, -1).reverse(), paths[paths.length - 1]] : paths;
      const outPath = joinPath(spill.dir, 'sorted.vectro');
      const out = new DatasetWriter(engine, outPath);
      let ok = false;
      try {
        for (const p of ordered) if (p) yield* sortPartition(p, 0, out);
        if (!out.meta) out.append(empty); // (no rows at all)
        out.close();
        ok = true;
      } finally { if (!ok) out.abort(); }
      const result = new Dataset(engine, outPath);
      result.temporary = spill.dir;
      spill = null;
      return result;
    },
    dispose() { spill?.cleanup(); empty?.dispose(); },
  };

  function* sortPartition(path, depth, out) {
    const D = new Dataset(engine, path);
    try {
      const splitters = D.length > tuning.partitionRows && depth < 4 ? zoneSplitters(D, first.expr, 16) : null;
      if (splitters) { // split again
        const sub = new Spill(engine, first.expr, splitters);
        stats.resplits++;
        try {
          yield* eachChunk(D, (t) => sub.add(t));
          const ps = sub.close();
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
    } finally { D.close(); }
  }
}

// The first sort key as an expression and its direction, if it sorts by
// value (a text column sorts by label: no cutoff then).
function firstSortKey(by, ds, derived) {
  const src = String(by).trim();
  const desc = src.startsWith('-');
  const expr = desc ? src.slice(1).trim() : src;
  const ident = /^[A-Za-z_]\w*$/.test(expr);
  if (ident && !derived.has(expr) && (ds.cols.get(expr)?.dict || ds.cols.get(expr)?.strings)) return null;
  if (ident && derived.has(expr)) return null; // (a join's text column, or a derived label: be safe)
  return { expr, desc };
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
      distinct.push({ out, arg: ast.args[0], parts: [], rows: 0, last: 0, bitmap: bitmapPlan(ds, derived, keys, ast.args[0]) });
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
          if (splitters) { spill = new Spill(engine, k0, splitters); stats.spills++; }
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
        const value = (c) => (c.bits ? countBits(c.bits, 0, c.range) : c);
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
    const paths = spill.close();
    const outPath = joinPath(spill.dir, 'groups.vectro');
    const out = new DatasetWriter(engine, outPath);
    let ok = false;
    try {
      for (const p of paths) if (p) yield* mergePartition(p, 0, out);
      out.close();
      ok = true;
    } finally { if (!ok) out.abort(); }
    const result = new Dataset(engine, outPath);
    const dir = spill.dir;
    spill = null;
    result.temporary = dir; // (deleted after the rest of the plan ran)
    return result;
  }

  function* mergePartition(path, depth, out) {
    const D = new Dataset(engine, path);
    try {
      if (!D.length) return;
      const splitters = D.length > tuning.partitionRows && depth < 4 ? zoneSplitters(D, keys[0], 16) : null;
      if (splitters) { // split again
        const sub = new Spill(engine, keys[0], splitters);
        stats.resplits++;
        try {
          yield* eachChunk(D, (t) => sub.add(t));
          for (const p of sub.close()) if (p) yield* mergePartition(p, depth + 1, out);
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
    } finally { D.close(); }
  }

  // Merged partial groups G -> the result: keys, then each aggregation.
  function finalize(G, counts) {
      try {
        const cols = new Map(), owned = [];
        const take = (t, name) => { t.owned = t.owned.filter((n) => n !== name); return t.cols.get(name); };
        for (const k of keys) { cols.set(k, take(G, k)); owned.push(k); }
        for (const o of outs) {
          if (o.col) cols.set(o.out, take(G, o.col));
          else if (o.mean) {
            const data = engine.alloc(Float64Array, G.n);
            if (G.n) engine.eval('s / n', { s: G.cols.get(`${o.mean}s`).data, n: G.cols.get(`${o.mean}n`).data }, data);
            cols.set(o.out, { data: engine.live(data), dict: null, codes: null });
          } else if (counts[o.distinct].bits) { // each group's bits
            const bm = counts[o.distinct], data = engine.alloc(Float64Array, G.n);
            const keyData = keys.map((k) => engine.live(G.cols.get(k).data)), out = engine.live(data);
            for (let r = 0; r < G.n; r++) {
              let g = 0;
              for (let k = 0; k < keys.length; k++) g += (keyData[k][r] - bm.mins[k]) * bm.strides[k];
              out[r] = countBits(bm.bits, g * bm.range, (g + 1) * bm.range);
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
  return { code: parse(`(${g.src}) * ${v[1]} + ((${arg.name}) - (${v[0]}))`), range: v[1], mins: g.mins, strides: g.strides, bits: new Uint32Array(Math.ceil((g.groups * v[1]) / 32)) };
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
export function csvToDataset(engine, csvPath, outPath, { rowsPerChunk, pieceBytes = 64 << 20, header = true, delimiter = ',', types = {}, sampleRows, ...rest } = {}) {
  const fd = openSync(csvPath, 'r');
  const writer = new DatasetWriter(engine, outPath, { rowsPerChunk });
  try {
    const size = fstatSync(fd).size;
    let pos = 0, carry = new Uint8Array(0), head = new Uint8Array(0), kinds = null;
    while (true) {
      const want = Math.min(pieceBytes, size - pos);
      const buf = new Uint8Array(carry.length + want);
      buf.set(carry, 0);
      readFully(fd, buf.subarray(carry.length), pos);
      pos += want;
      const last = pos >= size;
      let start = 0;
      if (!kinds) { // the first piece: column names and types
        const info = inspectCSV(buf, { delimiter, header, types, sampleRows });
        kinds = Object.fromEntries(info.names.map((n, i) => [n, info.kinds[i]]));
        head = header ? buf.slice(0, info.start) : new Uint8Array(0);
        start = info.start;
      }
      const end = last ? buf.length : rowsEnd(buf, start);
      if (end > start) {
        const bytes = engine.alloc(Uint8Array, head.length + end - start);
        bytes.set(head, 0);
        bytes.set(buf.subarray(start, end), head.length);
        const t = loadBytes(engine, bytes, { delimiter, header, types: kinds, ...rest });
        try { writer.append(t); } finally { t.dispose(); }
      } else if (!writer.meta && last) { // a header and no rows
        const bytes = engine.alloc(Uint8Array, head.length);
        bytes.set(head, 0);
        const t = loadBytes(engine, bytes, { delimiter, header, types: kinds, ...rest });
        try { writer.append(t); } finally { t.dispose(); }
      }
      if (last) break;
      carry = buf.slice(Math.max(start, end));
    }
  } catch (err) {
    writer.abort();
    throw err;
  } finally {
    closeSync(fd);
  }
  return writer.close();
}

// Where the last complete row of buf[start..] ends (quotes may hold newlines).
function rowsEnd(buf, start) {
  const NL = 10, QUOTE = 34;
  if (buf.indexOf(QUOTE, start) === -1) return buf.lastIndexOf(NL) + 1;
  let inQuotes = false, end = start;
  for (let i = start; i < buf.length; i++) {
    const c = buf[i];
    if (c === QUOTE) inQuotes = !inQuotes;
    else if (c === NL && !inQuotes) end = i + 1;
  }
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
