// Vectro frames: a binary format for streaming column batches. The receiver
// copies number columns straight into memory: no text, no parsing.
//
// A connection carries frames, each one:
//   u32  length of the rest of the frame (little-endian)
//   u8   kind: 1 = schema, 2 = batch, 3 = end
//   u8   0 (reserved)  u16 0 (reserved)
// schema: u32 byte length + UTF-8 JSON { v: 1, columns: [[name, 'number' | 'category'], ...] }
// batch:  u32 rows, u32 0, then for each column, in schema order, padded to 8 bytes:
//   number:   rows x float64
//   category: u32 count of new labels, then each label as u32 byte length +
//             UTF-8 bytes, padded to 8; then rows x u32 codes. Codes number
//             the labels in order of first appearance on this connection, so
//             a label is sent once per connection.
// end:    nothing more.
// Every section starts at a multiple of 8 from the frame start, so a frame
// copied to an aligned buffer can be read through typed-array views.
export const SCHEMA = 1, BATCH = 2, END = 3;
const enc = new TextEncoder(), dec = new TextDecoder();
const pad8 = (n) => (n + 7) & ~7;

export function encodeSchema(columns) {
  const json = enc.encode(JSON.stringify({ v: 1, columns: Object.entries(columns) }));
  const out = new Uint8Array(8 + pad8(4 + json.length));
  const dv = new DataView(out.buffer);
  dv.setUint32(0, out.length - 4, true);
  out[4] = SCHEMA;
  dv.setUint32(8, json.length, true);
  out.set(json, 12);
  return out;
}

export function encodeEnd() {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setUint32(0, 4, true);
  out[4] = END;
  return out;
}

// Builds batch frames for one connection (it remembers which labels that
// connection has already received).
export class BatchEncoder {
  constructor(columns) {
    this.columns = Object.entries(columns);
    for (const [name, kind] of this.columns) if (kind !== 'number' && kind !== 'category') throw new Error(`vectro: emitter column "${name}" must be 'number' or 'category', got "${kind}"`);
    this.codes = this.columns.map(([, kind]) => (kind === 'category' ? new Map() : null));
  }

  // batch: { name: Float64Array | number[] | string[] | { codes, labels } }
  // (every column the same length). A category column can be given as codes
  // (Uint32Array, Int32Array or number[]) into a labels array, which skips
  // the per-row label lookup.
  encode(batch) {
    let rows = -1;
    const parts = this.columns.map(([name, kind], k) => {
      const values = batch[name];
      if (!values) throw new Error(`vectro: batch is missing column "${name}"`);
      const len = values.codes ? values.codes.length : values.length;
      if (kind === 'number' && values.codes) throw new Error(`vectro: column "${name}" is a number column; codes are for category columns`);
      if (rows === -1) rows = len;
      else if (len !== rows) throw new RangeError(`vectro: column "${name}" has ${len} values, expected ${rows}`);
      if (kind === 'number') {
        const f = values instanceof Float64Array ? values : Float64Array.from(values);
        return { bytes: new Uint8Array(f.buffer, f.byteOffset, f.byteLength) };
      }
      // category: codes on this connection, new labels sent once
      const map = this.codes[k], fresh = [];
      const code = (label) => {
        let c = map.get(label);
        if (c === undefined) { c = map.size; map.set(label, c); fresh.push(enc.encode(String(label))); }
        return c;
      };
      let codes;
      if (values.codes) { // codes + labels: map each label once, then the codes
        const lut = Uint32Array.from(values.labels, code), src = values.codes;
        if (src instanceof Uint32Array && lut.every((c, j) => c === j)) codes = src; // same numbering: no copy
        else { codes = new Uint32Array(rows); for (let i = 0; i < rows; i++) codes[i] = lut[src[i]]; }
      } else {
        codes = new Uint32Array(rows);
        for (let i = 0; i < rows; i++) codes[i] = code(values[i]);
      }
      return { fresh, bytes: new Uint8Array(codes.buffer, codes.byteOffset, codes.byteLength) };
    });
    let size = 16; // header + rows + reserved
    for (const p of parts) size += (p.fresh ? 8 + p.fresh.reduce((a, l) => a + pad8(4 + l.length), 0) : 0) + pad8(p.bytes.length);
    const out = alloc(size); // not zero-filled: every byte is written below
    const dv = new DataView(out.buffer, out.byteOffset, size);
    dv.setUint32(0, size - 4, true);
    dv.setUint32(4, BATCH, true); // kind + 3 reserved zero bytes
    dv.setUint32(8, rows, true);
    dv.setUint32(12, 0, true);
    let at = 16;
    const put = (bytes) => { out.set(bytes, at); const end = at + pad8(bytes.length); out.fill(0, at + bytes.length, end); at = end; };
    for (const p of parts) {
      if (p.fresh) {
        dv.setUint32(at, p.fresh.length, true);
        dv.setUint32(at + 4, 0, true);
        at += 8;
        for (const l of p.fresh) {
          const end = at + pad8(4 + l.length);
          dv.setUint32(at, l.length, true);
          out.set(l, at + 4);
          out.fill(0, at + 4 + l.length, end);
          at = end;
        }
      }
      put(p.bytes);
    }
    return out;
  }
}

// Frame memory: Node's Buffer.allocUnsafeSlow skips zero-filling (the
// encoder writes every byte, padding included); elsewhere a Uint8Array.
const alloc = globalThis.Buffer?.allocUnsafeSlow ? (n) => globalThis.Buffer.allocUnsafeSlow(n) : (n) => new Uint8Array(n);

// Reads frames from bytes that arrive in pieces cut anywhere. Calls
// onSchema(columns), onBatch(rows, columns) with columns[k] =
// { values: Float64Array } or { codes: Uint32Array, labels: string[] (all
// labels of this connection so far) }, and onEnd(). Views point into an
// internal buffer that is reused: copy what you keep before returning.
export class FrameReader {
  constructor({ onSchema, onBatch, onEnd = () => {} }) {
    this.on = { onSchema, onBatch, onEnd };
    this.buf = new Uint8Array(1 << 16); // aligned (offset 0 of its own ArrayBuffer)
    this.len = 0;
    this.columns = null;
    this.labels = null;
  }

  push(chunk) {
    if (this.len + chunk.length > this.buf.length) {
      const bigger = new Uint8Array(Math.max(this.buf.length * 2, this.len + chunk.length));
      bigger.set(this.buf.subarray(0, this.len));
      this.buf = bigger;
    }
    this.buf.set(chunk, this.len);
    this.len += chunk.length;
    let at = 0;
    while (this.len - at >= 4) {
      const size = this.buf[at] | (this.buf[at + 1] << 8) | (this.buf[at + 2] << 16) | (this.buf[at + 3] * 0x1000000);
      if (this.len - at < 4 + size) break;
      if (at % 8) { // keep frames aligned: move the rest to the front first
        this.buf.copyWithin(0, at, this.len);
        this.len -= at;
        at = 0;
      }
      this.#frame(at, 4 + size);
      at += 4 + size;
    }
    if (at) { this.buf.copyWithin(0, at, this.len); this.len -= at; }
  }

  #frame(at, size) {
    const dv = new DataView(this.buf.buffer, at, size);
    const kind = this.buf[at + 4];
    if (kind === SCHEMA) {
      const n = dv.getUint32(8, true);
      const { v, columns } = JSON.parse(dec.decode(this.buf.subarray(at + 12, at + 12 + n)));
      if (v !== 1) throw new Error(`vectro: unsupported frame version ${v}`);
      this.columns = columns;
      this.labels = columns.map(([, kind]) => (kind === 'category' ? [] : null));
      this.on.onSchema(Object.fromEntries(columns));
      return;
    }
    if (kind === END) { this.on.onEnd(); return; }
    if (kind !== BATCH) throw new Error(`vectro: unknown frame kind ${kind}`);
    if (!this.columns) throw new Error('vectro: a batch arrived before the schema');
    const rows = dv.getUint32(8, true);
    let p = 16;
    const out = this.columns.map(([, kind], k) => {
      if (kind === 'number') {
        const values = new Float64Array(this.buf.buffer, at + p, rows);
        p += pad8(rows * 8);
        return { values };
      }
      const fresh = dv.getUint32(p, true);
      p += 8;
      const labels = this.labels[k];
      for (let i = 0; i < fresh; i++) {
        const len = dv.getUint32(p, true);
        labels.push(dec.decode(this.buf.subarray(at + p + 4, at + p + 4 + len)));
        p += pad8(4 + len);
      }
      const codes = new Uint32Array(this.buf.buffer, at + p, rows);
      p += pad8(rows * 4);
      return { codes, labels };
    });
    this.on.onBatch(rows, out);
  }
}
