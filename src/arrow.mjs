// Apache Arrow IPC: the binary table format pandas, Polars, DuckDB, pyarrow
// and Arrow Flight use to move data. Reads the stream format and the file
// format (Feather v2), including LZ4-compressed bodies, and writes both.
//
// Arrow types map onto Vectro's two column kinds:
//   number:   ints, floats, bools (0/1), dates, times, timestamps and
//             durations (as milliseconds); null -> NaN
//   category: strings and dictionary-encoded strings; null -> ''
// Other types (lists, structs, decimals, binary...) are refused unless left
// out with `columns`. 64-bit integers are exact up to 2^53.
//
// Layout of the stream format: messages, each
//   0xFFFFFFFF, int32 metadata length, flatbuffer metadata (padded to 8), body
// then 0xFFFFFFFF 0x00000000 at the end. The file format wraps the stream in
// "ARROW1" magic and adds a footer.

import { ByteDict } from './csv-kernel.mjs';

const enc = new TextEncoder(), dec = new TextDecoder();
const pad8 = (n) => (n + 7) & ~7;
const MAGIC = enc.encode('ARROW1');
const isMagic = (b, at) => MAGIC.every((x, i) => b[at + i] === x);
const EOS = Uint8Array.of(255, 255, 255, 255, 0, 0, 0, 0);
const MESSAGE = { schema: 1, dictionary: 2, batch: 3 };
const V5 = 4;

// Arrow type ids (Schema.fbs, union Type)
const T = { Null: 1, Int: 2, FloatingPoint: 3, Binary: 4, Utf8: 5, Bool: 6, Decimal: 7, Date: 8, Time: 9, Timestamp: 10, Interval: 11, List: 12, Struct: 13, Union: 14, FixedSizeBinary: 15, FixedSizeList: 16, Map: 17, Duration: 18, LargeBinary: 19, LargeUtf8: 20, LargeList: 21, RunEndEncoded: 22, BinaryView: 23, Utf8View: 24, ListView: 25, LargeListView: 26 };
const TYPE_NAMES = Object.fromEntries(Object.entries(T).map(([k, v]) => [v, k]));
const NUMBER_TYPES = new Set([T.Null, T.Int, T.FloatingPoint, T.Bool, T.Date, T.Time, T.Timestamp, T.Duration]);
const TO_MS = [[1000, 1], [1, 1], [1, 1000], [1, 1e6]]; // TimeUnit s, ms, us, ns -> [multiply, divide]

// ---- flatbuffers, reading -----------------------------------------------------

class FlatReader {
  constructor(bytes) {
    this.b = bytes;
    this.dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  root() { return this.dv.getUint32(0, true); }
  #field(t, slot) {
    const vt = t - this.dv.getInt32(t, true);
    const at = 4 + 2 * slot;
    if (at >= this.dv.getUint16(vt, true)) return 0;
    const off = this.dv.getUint16(vt + at, true);
    return off ? t + off : 0;
  }
  u8(t, slot, d = 0) { const p = this.#field(t, slot); return p ? this.b[p] : d; }
  i16(t, slot, d = 0) { const p = this.#field(t, slot); return p ? this.dv.getInt16(p, true) : d; }
  i32(t, slot, d = 0) { const p = this.#field(t, slot); return p ? this.dv.getInt32(p, true) : d; }
  i64(t, slot, d = 0) { const p = this.#field(t, slot); return p ? this.long(p) : d; }
  long(p) { return this.dv.getUint32(p, true) + this.dv.getInt32(p + 4, true) * 4294967296; }
  table(t, slot) { const p = this.#field(t, slot); return p ? p + this.dv.getUint32(p, true) : 0; }
  vector(t, slot) {
    const p = this.#field(t, slot);
    if (!p) return { at: 0, n: 0 };
    const v = p + this.dv.getUint32(p, true);
    return { at: v + 4, n: this.dv.getUint32(v, true) };
  }
  string(t, slot) { const v = this.vector(t, slot); return v.n ? dec.decode(this.b.subarray(v.at, v.at + v.n)) : ''; }
  tables(t, slot) {
    const v = this.vector(t, slot);
    return Array.from({ length: v.n }, (_, i) => { const p = v.at + 4 * i; return p + this.dv.getUint32(p, true); });
  }
}

function readField(fb, f) {
  const typeId = fb.u8(f, 2), type = fb.table(f, 3), dict = fb.table(f, 4);
  const field = { name: fb.string(f, 0), typeId, children: fb.tables(f, 5).map((c) => readField(fb, c)), dict: null };
  if (typeId === T.Int) { field.bits = fb.i32(type, 0); field.signed = !!fb.u8(type, 1); }
  else if (typeId === T.FloatingPoint) field.precision = fb.i16(type, 0);
  else if (typeId === T.Date) field.unit = fb.i16(type, 0, 1);
  else if (typeId === T.Time) { field.unit = fb.i16(type, 0, 1); field.bits = fb.i32(type, 1, 32); }
  else if (typeId === T.Timestamp) field.unit = fb.i16(type, 0, 0);
  else if (typeId === T.Duration) field.unit = fb.i16(type, 0, 1);
  else if (typeId === T.Union) field.mode = fb.i16(type, 0, 0);
  if (dict) {
    const index = fb.table(dict, 1); // absent: int32
    field.dict = { id: fb.i64(dict, 0), bits: index ? fb.i32(index, 0) : 32, signed: index ? !!fb.u8(index, 1) : true };
  }
  return field;
}

const valueKind = (t) => (t === T.Utf8 || t === T.LargeUtf8 ? 'category' : NUMBER_TYPES.has(t) ? 'number' : null);
const kindOf = (f) => (f.typeId === T.FloatingPoint && f.precision === 0 && !f.dict ? 'number' : valueKind(f.typeId));
const typeName = (f) => (f.dict ? `dictionary of ${TYPE_NAMES[f.typeId] ?? f.typeId}` : TYPE_NAMES[f.typeId] ?? `type ${f.typeId}`);

// A record batch's nodes (rows, nulls per column) and buffers (decompressed
// on demand).
function batchMeta(fb, rb, body) {
  const nodes = fb.vector(rb, 1), buffers = fb.vector(rb, 2), comp = fb.table(rb, 3), variadic = fb.vector(rb, 4);
  const codec = comp ? fb.u8(comp, 0, 0) : -1; // 0 = LZ4 frame, 1 = ZSTD
  return {
    length: fb.i64(rb, 0),
    node: (i) => [fb.long(nodes.at + 16 * i), fb.long(nodes.at + 16 * i + 8)],
    variadic: (i) => fb.long(variadic.at + 8 * i),
    buffer(i) {
      if (i >= buffers.n) throw new Error('vectro: an Arrow record batch has fewer buffers than its schema needs');
      const off = fb.long(buffers.at + 16 * i), len = fb.long(buffers.at + 16 * i + 8);
      if (off + len > body.length) throw new Error('vectro: an Arrow buffer runs past the end of its message (truncated data?)');
      const raw = body.subarray(off, off + len);
      if (codec < 0 || len === 0) return raw;
      const dv = new DataView(raw.buffer, raw.byteOffset, 8);
      const full = dv.getUint32(0, true) + dv.getInt32(4, true) * 4294967296;
      if (full === -1) return raw.subarray(8); // stored uncompressed
      if (codec !== 0) throw new Error('vectro: ZSTD-compressed Arrow data is not supported yet (write it uncompressed or with LZ4)');
      return lz4Frame(raw.subarray(8), full);
    },
  };
}

function view(Type, bytes, n) {
  const need = n * Type.BYTES_PER_ELEMENT;
  if (bytes.length < need) throw new Error('vectro: an Arrow buffer is shorter than its column (truncated data?)');
  if (bytes.byteOffset % Type.BYTES_PER_ELEMENT) bytes = bytes.slice(0, need); // misaligned: copy
  return new Type(bytes.buffer, bytes.byteOffset, n);
}

const INT_TYPES = { 8: [Uint8Array, Int8Array], 16: [Uint16Array, Int16Array], 32: [Uint32Array, Int32Array] };

function ints(data, n, bits, signed, [mul, div]) {
  let out;
  if (bits === 64) {
    out = new Float64Array(n);
    const lo = view(Uint32Array, data, 2 * n), hi = signed ? view(Int32Array, data, 2 * n) : lo;
    for (let i = 0; i < n; i++) out[i] = hi[2 * i + 1] * 4294967296 + lo[2 * i];
  } else {
    const Type = INT_TYPES[bits]?.[signed ? 1 : 0];
    if (!Type) throw new Error(`vectro: unsupported Arrow integer width ${bits}`);
    out = new Float64Array(n);
    out.set(view(Type, data, n));
  }
  if (mul !== 1) for (let i = 0; i < n; i++) out[i] *= mul;
  if (div !== 1) for (let i = 0; i < n; i++) out[i] /= div;
  return out;
}

function half(h) {
  const out = new Float64Array(h.length);
  for (let i = 0; i < h.length; i++) {
    const x = h[i], e = (x >> 10) & 31, m = x & 1023, s = x & 0x8000 ? -1 : 1;
    out[i] = e === 0 ? s * m * 2 ** -24 : e === 31 ? (m ? NaN : s * Infinity) : s * (1 + m / 1024) * 2 ** (e - 15);
  }
  return out;
}

// Number column values (a view into `data` when it is float64 already).
function numbers(f, n, data) {
  switch (f.typeId) {
    case T.FloatingPoint:
      if (f.precision === 2) return view(Float64Array, data, n);
      if (f.precision === 1) { const out = new Float64Array(n); out.set(view(Float32Array, data, n)); return out; }
      return half(view(Uint16Array, data, n));
    case T.Int: return ints(data, n, f.bits, f.signed, [1, 1]);
    case T.Bool: { const out = new Float64Array(n); for (let i = 0; i < n; i++) out[i] = (data[i >> 3] >> (i & 7)) & 1; return out; }
    case T.Date: return f.unit === 0 ? ints(data, n, 32, true, [86_400_000, 1]) : ints(data, n, 64, true, [1, 1]);
    case T.Time: return ints(data, n, f.bits, true, TO_MS[f.unit]);
    default: return ints(data, n, 64, true, TO_MS[f.unit]); // Timestamp, Duration
  }
}

const isNull = (valid, i) => !((valid[i >> 3] >> (i & 7)) & 1);

function indexArray(d, bytes, n) {
  if (d.bits === 32) return view(Uint32Array, bytes, n); // non-negative int32: same bits
  const out = new Uint32Array(n);
  if (d.bits === 64) { const w = view(Uint32Array, bytes, 2 * n); for (let i = 0; i < n; i++) out[i] = w[2 * i]; return out; }
  out.set(view(INT_TYPES[d.bits][d.signed ? 1 : 0], bytes, n));
  return out;
}

// Strings as codes into the distinct values of this batch; a repeated value
// never becomes a JS string twice. Strings of up to 7 bytes (most labels) are
// packed into two ints and found in a small hash table; longer ones go
// through a byte-keyed dictionary.
const MASK_A = Int32Array.from({ length: 8 }, (_, n) => (n >= 4 ? -1 : (1 << (8 * n)) - 1));
const MASK_B = Int32Array.from({ length: 8 }, (_, n) => (n <= 4 ? 0 : (1 << (8 * (n - 4))) - 1));

function strings(offsets, data, n, large) {
  const o = large ? view(Uint32Array, offsets, 2 * (n + 1)) : view(Int32Array, offsets, n + 1);
  const step = large ? 2 : 1; // (large: the low half of each int64 offset)
  const local = new Uint32Array(n), strs = [], longCodes = [];
  const long = new ByteDict(data, (x) => x);
  let cap = 256, lo = new Int32Array(cap), hi = new Int32Array(cap), code = new Int32Array(cap).fill(-1);
  const slotOf = (a, b, mask) => {
    const h = Math.imul(a, 0x9e3779b1) ^ Math.imul(b ^ (b >>> 16), 0x85ebca6b);
    return (h ^ (h >>> 15)) & mask;
  };
  for (let i = 0; i < n; i++) {
    const s = o[i * step], e = o[(i + 1) * step], len = e - s;
    if (len > 7) {
      const c = long.code(s, e, false);
      let g = longCodes[c];
      if (g === undefined) { g = longCodes[c] = strs.length; strs.push(long.strings[c]); }
      local[i] = g;
      continue;
    }
    // read 7 bytes regardless (no branch on the length), keep `len` of them
    const a = (data[s] | (data[s + 1] << 8) | (data[s + 2] << 16) | (data[s + 3] << 24)) & MASK_A[len];
    const b = (len << 24) | ((data[s + 4] | (data[s + 5] << 8) | (data[s + 6] << 16)) & MASK_B[len]);
    const mask = cap - 1;
    let slot = slotOf(a, b, mask);
    while (code[slot] !== -1 && (lo[slot] !== a || hi[slot] !== b)) slot = (slot + 1) & mask;
    if (code[slot] === -1) {
      code[slot] = strs.length; lo[slot] = a; hi[slot] = b;
      strs.push(dec.decode(data.subarray(s, e)));
      if (strs.length * 2 > cap) { // grow and rehash
        const old = [lo, hi, code];
        cap *= 2; lo = new Int32Array(cap); hi = new Int32Array(cap); code = new Int32Array(cap).fill(-1);
        for (let k = 0; k < old[2].length; k++) {
          if (old[2][k] === -1) continue;
          let t = slotOf(old[0][k], old[1][k], cap - 1);
          while (code[t] !== -1) t = (t + 1) & (cap - 1);
          lo[t] = old[0][k]; hi[t] = old[1][k]; code[t] = old[2][k];
        }
        local[i] = strs.length - 1;
        continue;
      }
    }
    local[i] = code[slot];
  }
  return { local, strings: strs };
}

// One top-level column: { values } | { local, strings, valid } | { indices, valid }.
function decodeColumn(f, meta, st) {
  const [n, nulls] = meta.node(st.node++);
  const take = () => meta.buffer(st.buf++);
  if (f.dict) {
    const valid = take(), idx = take();
    return { n, valid: nulls && valid.length ? valid : null, indices: indexArray(f.dict, idx, n) };
  }
  if (f.typeId === T.Null) return { n, values: new Float64Array(n).fill(NaN) };
  const valid = take();
  if (f.typeId === T.Utf8 || f.typeId === T.LargeUtf8) {
    const offsets = take(), data = take();
    return { n, valid: nulls && valid.length ? valid : null, ...strings(offsets, data, n, f.typeId === T.LargeUtf8) };
  }
  const data = take();
  let values = numbers(f, n, data);
  if (nulls && valid.length) {
    if (values.buffer === data.buffer) values = values.slice(); // don't write into the caller's bytes
    for (let i = 0; i < n; i++) if (isNull(valid, i)) values[i] = NaN;
  }
  return { n, values };
}

// Step over a column that isn't read (any type, nested or not).
function skipColumn(f, meta, st) {
  st.node++;
  if (f.dict) { st.buf += 2; return; }
  const t = f.typeId;
  if (t === T.Null || t === T.RunEndEncoded) st.buf += 0;
  else if (t === T.Struct || t === T.FixedSizeList) st.buf += 1;
  else if (t === T.Union) st.buf += f.mode === 1 ? 2 : 1;
  else if (t === T.Binary || t === T.Utf8 || t === T.LargeBinary || t === T.LargeUtf8 || t === T.ListView || t === T.LargeListView) st.buf += 3;
  else if (t === T.BinaryView || t === T.Utf8View) st.buf += 2 + meta.variadic(st.view++);
  else st.buf += 2;
  for (const c of f.children) skipColumn(c, meta, st);
}

// ---- reading, incrementally -----------------------------------------------------

// Reads Arrow IPC bytes that arrive in pieces cut anywhere (the file format
// works too: its footer is ignored). Calls
//   onSchema(kinds, fields): kinds = { name: 'number' | 'category' | null }
//     (null: a type Vectro can't read); may return the names to read
//     (default: every column it can read);
//   onBatch(rows, columns): columns[k] (schema order) = { values: Float64Array }
//     or { codes: Uint32Array, labels: string[] } (all labels of column k so
//     far; codes stay valid as labels are added), or null if not read;
//   onEnd() at the end-of-stream marker.
// Views may point into the bytes pushed: copy what you keep.
export class ArrowReader {
  constructor({ onSchema, onBatch, onEnd = () => {} }) {
    this.on = { onSchema, onBatch, onEnd };
    this.buf = new Uint8Array(1 << 16);
    this.len = 0;
    this.started = false;
    this.ended = false;
    this.fields = null;
    this.cols = null;          // per field: { labels, index, lut, version } or null
    this.dicts = new Map();    // dictionary id -> { field, values, numeric, version }
  }

  push(chunk) {
    if (this.ended) return;
    if (!this.len) { // nothing buffered: read straight from the chunk
      const used = this.#scan(chunk, chunk.length);
      this.#keep(chunk.subarray(used));
      return;
    }
    this.#keep(chunk);
    const used = this.#scan(this.buf, this.len);
    this.buf.copyWithin(0, used, this.len);
    this.len -= used;
  }

  #keep(bytes) {
    if (!bytes.length || this.ended) return;
    if (this.len + bytes.length > this.buf.length) {
      const bigger = new Uint8Array(Math.max(this.buf.length * 2, this.len + bytes.length));
      bigger.set(this.buf.subarray(0, this.len));
      this.buf = bigger;
    }
    this.buf.set(bytes, this.len);
    this.len += bytes.length;
  }

  #scan(b, len) {
    const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
    let at = 0;
    if (!this.started) {
      if (len < 8) return 0;
      this.started = true;
      if (isMagic(b, 0)) at = 8; // file format
    }
    while (len - at >= 4) {
      let p = at, size = dv.getInt32(p, true);
      if (size === -1) { if (len - at < 8) break; size = dv.getInt32(p + 4, true); p += 8; } else p += 4; // (old streams have no 0xFFFFFFFF)
      if (size === 0) { at = p; this.ended = true; this.on.onEnd(); break; }
      if (size < 0) throw new Error('vectro: this is not Arrow IPC data (bad message length)');
      if (len - p < size) break;
      const fb = new FlatReader(b.subarray(p, p + size));
      const msg = fb.root();
      const bodyLength = fb.i64(msg, 3);
      if (len - p - size < bodyLength) break;
      this.#message(fb, msg, b.subarray(p + size, p + size + bodyLength));
      at = p + size + bodyLength;
    }
    return at;
  }

  #message(fb, msg, body) {
    const version = fb.i16(msg, 0);
    if (version < 3) throw new Error(`vectro: Arrow IPC metadata V${version + 1} is too old (V4 or later)`);
    const kind = fb.u8(msg, 1), h = fb.table(msg, 2);
    if (kind === MESSAGE.schema) return this.#schema(fb, h);
    if (!this.fields) throw new Error('vectro: Arrow data arrived before the schema');
    if (kind === MESSAGE.dictionary) return this.#dictionary(fb, h, body);
    if (kind === MESSAGE.batch) return this.#batch(fb, h, body);
    throw new Error(`vectro: unsupported Arrow message type ${kind}`);
  }

  #schema(fb, h) {
    if (this.fields) throw new Error('vectro: a second Arrow schema arrived on one stream');
    if (fb.i16(h, 0) === 1) throw new Error('vectro: big-endian Arrow data is not supported');
    this.fields = fb.tables(h, 1).map((f) => readField(fb, f));
    const kinds = Object.fromEntries(this.fields.map((f) => [f.name, kindOf(f)]));
    const info = this.fields.map((f) => ({ name: f.name, kind: kindOf(f), type: typeName(f) }));
    const want = new Set(this.on.onSchema(kinds, info) ?? info.filter((f) => f.kind).map((f) => f.name));
    this.cols = this.fields.map((f) => (want.has(f.name) && kindOf(f) ? { labels: [], index: new Map(), lut: [], identity: true, version: -1 } : null));
    this.fields.forEach((f, k) => {
      if (!this.cols[k] || !f.dict) return;
      if (!this.dicts.has(f.dict.id)) this.dicts.set(f.dict.id, { field: { ...f, dict: null }, values: null, numeric: kindOf(f) === 'number', version: 0 });
    });
  }

  #dictionary(fb, h, body) {
    const d = this.dicts.get(fb.i64(h, 0));
    if (!d) return; // a dictionary for a column not read
    const c = decodeColumn(d.field, batchMeta(fb, fb.table(h, 1), body), { node: 0, buf: 0, view: 0 });
    const delta = !!fb.u8(h, 2);
    if (!delta || !d.values) { d.values = []; d.version++; }
    if (c.values) { for (const v of c.values) d.values.push(v); return; }
    for (let i = 0; i < c.n; i++) d.values.push(c.valid && isNull(c.valid, i) ? '' : c.strings[c.local[i]]);
  }

  #label(col, s) {
    let g = col.index.get(s);
    if (g === undefined) { g = col.labels.length; col.labels.push(s); col.index.set(s, g); }
    return g;
  }

  #batch(fb, h, body) {
    const meta = batchMeta(fb, h, body);
    const st = { node: 0, buf: 0, view: 0 };
    const out = this.fields.map((f, k) => {
      const col = this.cols[k];
      if (!col) { skipColumn(f, meta, st); return null; }
      const c = decodeColumn(f, meta, st);
      if (c.values) return { values: c.values };
      if (c.indices) return this.#fromDictionary(f, col, c);
      const lut = c.strings.map((s) => this.#label(col, s));
      const codes = new Uint32Array(c.n);
      for (let i = 0; i < c.n; i++) codes[i] = lut[c.local[i]];
      if (c.valid) { const e = this.#label(col, ''); for (let i = 0; i < c.n; i++) if (isNull(c.valid, i)) codes[i] = e; }
      return { codes, labels: col.labels };
    });
    this.on.onBatch(meta.length, out);
  }

  #fromDictionary(f, col, c) {
    const d = this.dicts.get(f.dict.id), idx = c.indices, n = c.n;
    if (!d?.values) throw new Error(`vectro: Arrow column "${f.name}" arrived before its dictionary`);
    if (d.numeric) {
      const values = new Float64Array(n);
      for (let i = 0; i < n; i++) values[i] = c.valid && isNull(c.valid, i) ? NaN : d.values[idx[i]];
      return { values };
    }
    if (col.version !== d.version) { col.version = d.version; col.lut = []; col.identity = true; } // dictionary replaced
    for (let j = col.lut.length; j < d.values.length; j++) {
      const g = this.#label(col, d.values[j]);
      col.lut.push(g);
      if (g !== j) col.identity = false;
    }
    let codes = idx;
    if (!col.identity || !(idx instanceof Uint32Array)) {
      const lut = col.lut;
      codes = new Uint32Array(n);
      for (let i = 0; i < n; i++) codes[i] = lut[idx[i]];
    }
    if (c.valid) {
      const e = this.#label(col, '');
      if (codes === idx) codes = codes.slice();
      for (let i = 0; i < n; i++) if (isNull(c.valid, i)) codes[i] = e;
    }
    return { codes, labels: col.labels };
  }
}

// Read a whole Arrow stream or file. Returns { rows, columns } with columns
// ready for engine.table(): { name: Float64Array | { codes, labels } }.
// columns: the names to read (default: all; a type Vectro can't read is an
// error unless left out).
export function readArrow(bytes, { columns: pick } = {}) {
  if (!(bytes instanceof Uint8Array)) bytes = ArrayBuffer.isView(bytes) ? new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength) : new Uint8Array(bytes);
  let names = null;
  const parts = [];
  const reader = new ArrowReader({
    onSchema(kinds, info) {
      const want = pick ?? Object.keys(kinds);
      for (const name of want) {
        const f = info.find((x) => x.name === name);
        if (!f) throw new Error(`vectro: the Arrow data has no column "${name}" (columns: ${info.map((x) => x.name).join(', ')})`);
        if (!f.kind) throw new Error(`vectro: column "${name}" has Arrow type ${f.type}, which Vectro can't read yet; leave it out with columns: [...]`);
      }
      names = info.map((x) => x.name);
      return want;
    },
    onBatch(rows, cols) { parts.push([rows, cols]); }, // views stay valid: all bytes are pushed at once
  });
  reader.push(bytes);
  if (!names) throw new Error('vectro: no Arrow schema found (is this Arrow IPC data?)');
  if (reader.len && !reader.ended) throw new Error('vectro: the Arrow data ends in the middle of a message (truncated?)');
  const rows = parts.reduce((a, [n]) => a + n, 0);
  const columns = {};
  const want = pick ?? names;
  for (const name of want) {
    const k = names.indexOf(name);
    const pieces = parts.map(([, cols]) => cols[k]);
    const first = pieces[0];
    if (!first) { columns[name] = new Float64Array(0); continue; }
    if (first.values) {
      columns[name] = pieces.length === 1 ? first.values : concatTyped(Float64Array, pieces.map((p) => p.values), rows);
    } else {
      const codes = pieces.length === 1 ? first.codes : concatTyped(Uint32Array, pieces.map((p) => p.codes), rows);
      columns[name] = { codes, labels: pieces[pieces.length - 1].labels };
    }
  }
  return { rows, columns };
}

function concatTyped(Type, list, n) {
  const out = new Type(n);
  let at = 0;
  for (const a of list) { out.set(a, at); at += a.length; }
  return out;
}

// ---- LZ4 (frame format), for compressed Arrow bodies -----------------------------

function lz4Frame(src, size) {
  const dv = new DataView(src.buffer, src.byteOffset, src.byteLength);
  if (dv.getUint32(0, true) !== 0x184d2204) throw new Error('vectro: bad LZ4 frame in Arrow data');
  const out = new Uint8Array(size);
  const flg = src[4];
  let p = 6 + (flg & 8 ? 8 : 0) + (flg & 1 ? 4 : 0) + 1; // header: magic, FLG, BD, [size], [dict id], checksum
  let o = 0;
  for (;;) {
    const word = dv.getUint32(p, true);
    p += 4;
    if (word === 0) break;
    const len = word & 0x7fffffff;
    if (word & 0x80000000) { out.set(src.subarray(p, p + len), o); o += len; }
    else o = lz4Block(src, p, p + len, out, o);
    p += len + (flg & 0x10 ? 4 : 0);
  }
  if (o !== size) throw new Error('vectro: an LZ4 buffer in the Arrow data has the wrong size');
  return out;
}

function lz4Block(src, p, end, out, o) {
  while (p < end) {
    const token = src[p++];
    let lit = token >> 4;
    if (lit === 15) { let b; do { b = src[p++]; lit += b; } while (b === 255); }
    if (lit < 32) for (let i = 0; i < lit; i++) out[o++] = src[p++];
    else { out.set(src.subarray(p, p + lit), o); o += lit; p += lit; }
    if (p >= end) break;
    const off = src[p] | (src[p + 1] << 8);
    p += 2;
    let m = token & 15;
    if (m === 15) { let b; do { b = src[p++]; m += b; } while (b === 255); }
    m += 4;
    let from = o - off;
    if (m < 32) for (let i = 0; i < m; i++) out[o++] = out[from++]; // short (or overlapping) copy
    else { // long: the repeating part doubles with each copy
      const stop = o + m;
      while (o < stop) { const k = Math.min(stop - o, o - from); out.copyWithin(o, from, from + k); o += k; }
    }
  }
  return o;
}

// ---- flatbuffers, writing -----------------------------------------------------------
// Objects are written front to back (a table, then what it points to), so
// every offset points forward as flatbuffers require.
// A table is { table: [slot, ...] }, each slot null or [kind, value] with kind
// u8 | bool | i16 | i32 | i64 | off (value: a table, string, { vec: [...] } of
// tables, or { structs: bytes, count } with 8-byte-aligned elements).

const SIZE = { u8: 1, bool: 1, i16: 2, i32: 4, off: 4, i64: 8 };
const tbl = (...slots) => ({ table: slots });

class FlatWriter {
  constructor() { this.b = new Uint8Array(512); this.dv = new DataView(this.b.buffer); this.n = 0; }
  #room(k) {
    if (this.n + k <= this.b.length) return;
    const bigger = new Uint8Array(Math.max(this.b.length * 2, this.n + k));
    bigger.set(this.b);
    this.b = bigger;
    this.dv = new DataView(bigger.buffer);
  }
  #align(a) { const k = (a - (this.n % a)) % a; this.#room(k); this.n += k; }
  #u16(v) { this.#room(2); this.dv.setUint16(this.n, v, true); this.n += 2; }
  #u32(v) { this.#room(4); this.dv.setUint32(this.n, v, true); this.n += 4; }
  #bytes(x) { this.#room(x.length); this.b.set(x, this.n); this.n += x.length; }
  finish(root) {
    this.#u32(0);
    const at = this.#write(root); // (may grow the buffer: write the offset after)
    this.dv.setUint32(0, at, true);
    this.#align(8);
    return this.b.slice(0, this.n);
  }
  #write(x) {
    if (typeof x === 'string') {
      this.#align(4);
      const at = this.n, s = enc.encode(x);
      this.#u32(s.length); this.#bytes(s); this.#bytes([0]);
      return at;
    }
    if (x.vec) {
      this.#align(4);
      const at = this.n;
      this.#u32(x.vec.length);
      const slots = x.vec.map(() => { const p = this.n; this.#u32(0); return p; });
      x.vec.forEach((child, i) => { const c = this.#write(child); this.dv.setUint32(slots[i], c - slots[i], true); });
      return at;
    }
    if (x.structs) {
      this.#align(4);
      if ((this.n + 4) % 8) this.#u32(0);
      const at = this.n;
      this.#u32(x.count); this.#bytes(x.structs);
      return at;
    }
    const slots = x.table;
    const order = slots.map((s, i) => [i, s]).filter(([, s]) => s).sort((a, b) => SIZE[b[1][0]] - SIZE[a[1][0]]);
    const pos = [];
    let cur = 4; // after the vtable offset
    for (const [i, [kind]] of order) { cur = (cur + SIZE[kind] - 1) & -SIZE[kind]; pos[i] = cur; cur += SIZE[kind]; }
    this.#align(2);
    const vt = this.n;
    this.#u16(4 + 2 * slots.length); this.#u16(cur);
    for (let i = 0; i < slots.length; i++) this.#u16(slots[i] ? pos[i] : 0);
    this.#align(order.some(([, [k]]) => k === 'i64') ? 8 : 4);
    const t = this.n;
    this.#room(cur);
    this.n += cur;
    this.dv.setInt32(t, t - vt, true);
    const later = [];
    for (const [i, [kind, v]] of order) {
      const p = t + pos[i];
      if (kind === 'u8' || kind === 'bool') this.b[p] = +v;
      else if (kind === 'i16') this.dv.setInt16(p, v, true);
      else if (kind === 'i32') this.dv.setInt32(p, v, true);
      else if (kind === 'i64') this.dv.setBigInt64(p, BigInt(v), true);
      else later.push([p, v]);
    }
    for (const [p, child] of later) { const c = this.#write(child); this.dv.setUint32(p, c - p, true); }
    return t;
  }
}

// ---- writing --------------------------------------------------------------------------

const KIND_TYPES = { number: [T.FloatingPoint, 2], float32: [T.FloatingPoint, 1], category: [T.Utf8] };

function schemaTable(columns) {
  const fields = columns.map(([name, kind], k) => {
    const [typeId, precision] = KIND_TYPES[kind];
    const dict = kind === 'category' ? ['off', tbl(['i64', k], ['off', tbl(['i32', 32], ['bool', 1])], ['bool', 0])] : null;
    return tbl(['off', name], ['bool', 1], ['u8', typeId], ['off', precision ? tbl(['i16', precision]) : tbl()], dict, ['off', { vec: [] }]);
  });
  return tbl(['i16', 0], ['off', { vec: fields }]);
}

function longs(pairs) { // [[a, b], ...] as int64 structs
  const dv = new DataView(new ArrayBuffer(16 * pairs.length));
  pairs.forEach(([a, b], i) => { dv.setBigInt64(16 * i, BigInt(a), true); dv.setBigInt64(16 * i + 8, BigInt(b), true); });
  return { structs: new Uint8Array(dv.buffer), count: pairs.length };
}

function recordBatch(rows, nodes, buffers) {
  let at = 0;
  const specs = buffers.map((b) => { const s = [at, b.length]; at += pad8(b.length); return s; });
  return { header: tbl(['i64', rows], ['off', longs(nodes)], ['off', longs(specs)]), buffers, bodyLength: at };
}

// One message: { bytes, metaLength (with its 8-byte prefix), bodyLength }.
function message(type, header, buffers = [], bodyLength = 0) {
  const meta = new FlatWriter().finish(tbl(['i16', V5], ['u8', type], ['off', header], ['i64', bodyLength]));
  const bytes = alloc(8 + meta.length + bodyLength); // not zero-filled: every byte is written below
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.length);
  dv.setUint32(0, 0xffffffff, true);
  dv.setInt32(4, meta.length, true);
  bytes.set(meta, 8);
  let at = 8 + meta.length;
  for (const b of buffers) {
    bytes.set(b, at);
    const end = at + pad8(b.length);
    bytes.fill(0, at + b.length, end);
    at = end;
  }
  return { bytes, metaLength: 8 + meta.length, bodyLength };
}

// Node's Buffer.allocUnsafeSlow skips zero-filling; elsewhere a Uint8Array.
const alloc = globalThis.Buffer?.allocUnsafeSlow ? (n) => globalThis.Buffer.allocUnsafeSlow(n) : (n) => new Uint8Array(n);
const asBytes = (a) => new Uint8Array(a.buffer, a.byteOffset, a.byteLength);

// Builds Arrow IPC stream messages for one connection or file: number columns
// as float64 (or float32), category columns dictionary-encoded with int32
// indices; labels new since the last batch go out as a delta dictionary.
// columns: { name: 'number' | 'float32' | 'category' }.
export class ArrowEncoder {
  constructor(columns) {
    this.columns = Object.entries(columns);
    for (const [name, kind] of this.columns) if (!KIND_TYPES[kind]) throw new Error(`vectro: Arrow column "${name}" must be 'number' or 'category', got "${kind}"`);
    this.labels = this.columns.map(([, kind]) => (kind === 'category' ? new Map() : null));
    this.sent = this.columns.map(() => false);
  }

  schema() { return message(MESSAGE.schema, schemaTable(this.columns)).bytes; }
  end() { return EOS.slice(); }
  encode(batch) {
    const parts = this.messages(batch);
    return parts.length === 1 ? parts[0].bytes : concatBytes(parts.map((p) => p.bytes));
  }

  // batch: { name: Float64Array | number[] | string[] | { codes, labels } }.
  // Returns the messages: dictionaries first, then the record batch.
  messages(batch) {
    let rows = -1;
    const out = [], nodes = [], buffers = [];
    this.columns.forEach(([name, kind], k) => {
      const values = batch[name];
      if (!values) throw new Error(`vectro: batch is missing column "${name}"`);
      const len = values.codes ? values.codes.length : values.length;
      if (rows === -1) rows = len;
      else if (len !== rows) throw new RangeError(`vectro: column "${name}" has ${len} values, expected ${rows}`);
      nodes.push([len, 0]);
      if (kind !== 'category') {
        const Type = kind === 'float32' ? Float32Array : Float64Array;
        const f = values instanceof Type ? values : Type.from(values);
        buffers.push(new Uint8Array(0), asBytes(f));
        return;
      }
      const map = this.labels[k], fresh = [];
      let codes;
      const code = (label) => {
        let c = map.get(label);
        if (c === undefined) { c = map.size; map.set(label, c); fresh.push(String(label)); }
        return c;
      };
      if (values.codes) {
        const lut = values.labels.map(code), src = values.codes;
        if ((src instanceof Uint32Array || src instanceof Int32Array) && lut.every((c, j) => c === j)) codes = src; // same numbering: no copy
        else { codes = new Int32Array(len); for (let i = 0; i < len; i++) codes[i] = lut[src[i]]; }
      } else {
        codes = new Int32Array(len);
        for (let i = 0; i < len; i++) codes[i] = code(values[i]);
      }
      buffers.push(new Uint8Array(0), asBytes(codes));
      if (fresh.length || !this.sent[k]) out.push(this.#dictionary(k, fresh));
    });
    const rb = recordBatch(Math.max(rows, 0), nodes, buffers);
    out.push({ ...message(MESSAGE.batch, rb.header, rb.buffers, rb.bodyLength), dictionary: false });
    return out;
  }

  #dictionary(k, labels) {
    const parts = labels.map((s) => enc.encode(s));
    const offsets = new Int32Array(labels.length + 1);
    parts.forEach((p, i) => { offsets[i + 1] = offsets[i] + p.length; });
    const rb = recordBatch(labels.length, [[labels.length, 0]], [new Uint8Array(0), asBytes(offsets), concatBytes(parts)]);
    const header = tbl(['i64', k], ['off', rb.header], ['bool', this.sent[k]]);
    this.sent[k] = true;
    return { ...message(MESSAGE.dictionary, header, rb.buffers, rb.bodyLength), dictionary: true };
  }
}

function concatBytes(list) {
  const out = new Uint8Array(list.reduce((a, b) => a + b.length, 0));
  let at = 0;
  for (const b of list) { out.set(b, at); at += b.length; }
  return out;
}

const isCoded = (v) => v != null && !ArrayBuffer.isView(v) && !Array.isArray(v) && v.codes != null && Array.isArray(v.labels);

// Write columns as Arrow IPC: the stream format, or with file: true the file
// format (.arrow / .feather, what pandas.read_feather and pyarrow open).
// columns: { name: Float64Array | Float32Array | number[] | string[] | { codes, labels } }.
export function writeArrow(columns, { file = false } = {}) {
  const kinds = {};
  for (const [name, v] of Object.entries(columns)) {
    kinds[name] = isCoded(v) || (Array.isArray(v) && typeof v[0] === 'string') ? 'category' : v instanceof Float32Array ? 'float32' : 'number';
  }
  const e = new ArrowEncoder(kinds);
  const schema = e.schema(), parts = e.messages(columns);
  if (!file) return concatBytes([schema, ...parts.map((p) => p.bytes), EOS]);
  const head = new Uint8Array(8);
  head.set(MAGIC);
  let at = head.length + schema.length;
  const dicts = [], batches = [];
  for (const p of parts) {
    (p.dictionary ? dicts : batches).push([at, p.metaLength, p.bodyLength]);
    at += p.bytes.length;
  }
  const blocks = (list) => { // struct Block { offset: long; metaDataLength: int; bodyLength: long }
    const dv = new DataView(new ArrayBuffer(24 * list.length));
    list.forEach(([o, m, b], i) => { dv.setBigInt64(24 * i, BigInt(o), true); dv.setInt32(24 * i + 8, m, true); dv.setBigInt64(24 * i + 16, BigInt(b), true); });
    return { structs: new Uint8Array(dv.buffer), count: list.length };
  };
  const footer = new FlatWriter().finish(tbl(['i16', V5], ['off', schemaTable(e.columns)], ['off', blocks(dicts)], ['off', blocks(batches)]));
  const tail = new Uint8Array(10);
  new DataView(tail.buffer).setInt32(0, footer.length, true);
  tail.set(MAGIC, 4);
  return concatBytes([head, schema, ...parts.map((p) => p.bytes), EOS, footer, tail]);
}
