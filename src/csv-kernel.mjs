// CSV parsing that runs on every thread. The file's bytes are in shared
// memory; each unit of work is one chunk that starts at a row boundary.

import { decode, countByte } from './bytes.mjs';

const NL = 10, CR = 13, QUOTE = 34, SPACE = 32, MINUS = 45, PLUS = 43, DOT = 46;

// Free-text values are stored as one number: byte offset * 2^20 + length,
// with bit 19 marking a quoted value that contains "" escapes.
export const STR_SHIFT = 2 ** 20, STR_ESCAPED = 2 ** 19, STR_MAX_LEN = 2 ** 19 - 1;
export function decodeString(bytes, v) {
  if (Number.isNaN(v)) return '';
  const start = Math.floor(v / STR_SHIFT);
  let len = v - start * STR_SHIFT;
  const escaped = len >= STR_ESCAPED;
  if (escaped) len -= STR_ESCAPED;
  const s = decode(bytes, start, start + len);
  return escaped ? s.replaceAll('""', '"') : s;
}

// Dictionary keyed by raw bytes, so repeated values never become JS strings:
// a string is made once per distinct value per chunk. `unescape` turns the
// raw bytes of an escaped value into its text (CSV: "" -> ").
export class ByteDict {
  constructor(bytes, unescape = (str) => str.replaceAll('""', '"')) {
    this.bytes = bytes;
    this.unescape = unescape;
    this.cap = 64;
    this.slots = new Int32Array(this.cap).fill(-1);
    this.hashes = new Uint32Array(this.cap);
    this.starts = [];
    this.lens = [];
    this.strings = [];
  }
  code(s, e, escaped) {
    const b = this.bytes;
    let h = 2166136261;
    for (let i = s; i < e; i++) h = Math.imul(h ^ b[i], 16777619);
    h >>>= 0;
    const mask = this.cap - 1;
    for (let slot = h & mask; ; slot = (slot + 1) & mask) {
      const c = this.slots[slot];
      if (c === -1) return this.#insert(slot, h, s, e, escaped);
      if (this.hashes[slot] === h && this.lens[c] === e - s && this.#same(this.starts[c], s, e - s)) return c;
    }
  }
  #same(a, b, len) {
    const x = this.bytes;
    for (let i = 0; i < len; i++) if (x[a + i] !== x[b + i]) return false;
    return true;
  }
  #insert(slot, h, s, e, escaped) {
    const code = this.strings.length;
    const str = decode(this.bytes, s, e);
    this.strings.push(escaped ? this.unescape(str) : str);
    this.starts.push(s);
    this.lens.push(e - s);
    this.slots[slot] = code;
    this.hashes[slot] = h;
    if (this.strings.length * 2 > this.cap) this.#grow();
    return code;
  }
  #grow() {
    const old = { slots: this.slots, hashes: this.hashes };
    this.cap *= 2;
    this.slots = new Int32Array(this.cap).fill(-1);
    this.hashes = new Uint32Array(this.cap);
    const mask = this.cap - 1;
    for (let i = 0; i < old.slots.length; i++) {
      const c = old.slots[i];
      if (c === -1) continue;
      let slot = old.hashes[i] & mask;
      while (this.slots[slot] !== -1) slot = (slot + 1) & mask;
      this.slots[slot] = c;
      this.hashes[slot] = old.hashes[i];
    }
  }
}
const POW10 = Array.from({ length: 23 }, (_, i) => 10 ** i);

// Count quote characters (mode 0) or rows (mode 1) in chunk u.
// Rows are newlines outside quotes, plus a final line without a newline.
export function csvScan(u, A, sc) {
  const bytes = A[0], bounds = A[1];
  const start = bounds[u], end = bounds[u + 1];
  if (end <= start) return 0;
  const mode = sc[0], hasQuotes = sc[1];
  if (mode === 0) return countByte(bytes, QUOTE, start, end);
  let rows;
  if (!hasQuotes) {
    rows = countByte(bytes, NL, start, end); // native indexOf speed
  } else {
    rows = 0;
    let inQuotes = false;
    for (let i = start; i < end; i++) {
      const c = bytes[i];
      if (c === QUOTE) inQuotes = !inQuotes;
      else if (c === NL && !inQuotes) rows++;
    }
  }
  return bytes[end - 1] === NL ? rows : rows + 1;
}

// Parse chunk u straight into the output columns.
// A = [bytes, bounds, rowStarts, fieldMap, kinds, ...columns]
//   fieldMap[f] = output column for field f (-1 = skip)
//   kinds[o] = 0 number, 1 category (dictionary codes), 2 free text (byte refs)
// sc = [delimiter, number of fields]
// Text columns get chunk-local codes; emit() reports each chunk's dictionary
// so the caller can renumber them into one shared dictionary.
export function csvParse(u, A, sc, emit) {
  const bytes = A[0], bounds = A[1], rowStarts = A[2], fieldMap = A[3], kinds = A[4];
  const cols = [];
  for (let o = 0; o < kinds.length; o++) cols.push(A[5 + o]);
  const delim = sc[0], nFields = sc[1];
  const dicts = Array.from(kinds, (k) => (k === 1 ? new ByteDict(bytes) : null));
  const end = bounds[u + 1];
  let i = bounds[u], row = rowStarts[u];

  while (i < end) {
    let f = 0;
    for (;;) {
      let s, e, escaped = false;
      if (i < end && bytes[i] === QUOTE) {
        s = ++i;
        for (;;) {
          if (i >= end) { e = i; break; }
          if (bytes[i] === QUOTE) {
            if (bytes[i + 1] === QUOTE) { escaped = true; i += 2; continue; }
            e = i++;
            break;
          }
          i++;
        }
        while (i < end && bytes[i] !== delim && bytes[i] !== NL) i++;
      } else {
        s = i;
        while (i < end) { const c = bytes[i]; if (c === delim || c === NL) break; i++; }
        e = i;
        if (e > s && bytes[e - 1] === CR) e--;
      }
      const o = f < nFields ? fieldMap[f] : -1;
      if (o >= 0) {
        const kind = kinds[o];
        if (kind === 0) cols[o][row] = parseNumber(bytes, s, e);
        else if (kind === 1) cols[o][row] = dicts[o].code(s, e, escaped);
        else {
          if (e - s > STR_MAX_LEN) throw new RangeError(`vectro: a text value in row ${row + 1} is longer than ${STR_MAX_LEN} bytes`);
          cols[o][row] = s * STR_SHIFT + (e - s) + (escaped ? STR_ESCAPED : 0);
        }
      }
      f++;
      if (i < end && bytes[i] === delim) { i++; continue; }
      break;
    }
    // missing trailing fields
    for (; f < nFields; f++) {
      const o = fieldMap[f];
      if (o < 0) continue;
      if (kinds[o] === 1) cols[o][row] = dicts[o].code(0, 0, false); // empty text
      else cols[o][row] = NaN; // number, or free text (decodes to '')
    }
    i++; // past the newline
    row++;
  }
  dicts.forEach((d, o) => { if (d) emit({ kind: 'csvdict', unit: u, col: o, strings: d.strings }); });
}

// Exact decimal parsing for plain numbers (Clinger's fast path: an integer of
// at most 2^53 times or divided by an exact power of ten is correctly rounded,
// so the result is identical to Number()). Anything else goes to Number().
export function parseNumber(b, s, e) {
  while (s < e && b[s] === SPACE) s++;
  while (e > s && (b[e - 1] === SPACE || b[e - 1] === CR)) e--;
  if (s === e) return NaN;
  let i = s, neg = false;
  if (b[i] === MINUS) { neg = true; i++; } else if (b[i] === PLUS) i++;
  let m = 0, digits = 0, exp = 0, c;
  while (i < e && (c = b[i] - 48) >= 0 && c <= 9) { m = m * 10 + c; digits++; i++; }
  if (i < e && b[i] === DOT) {
    i++;
    while (i < e && (c = b[i] - 48) >= 0 && c <= 9) { m = m * 10 + c; digits++; exp--; i++; }
  }
  if (i < e && (b[i] | 32) === 101) { // e / E
    i++;
    let eneg = false, ev = 0, edigits = 0;
    if (b[i] === MINUS) { eneg = true; i++; } else if (b[i] === PLUS) i++;
    while (i < e && (c = b[i] - 48) >= 0 && c <= 9) { ev = ev * 10 + c; edigits++; i++; }
    if (!edigits) return slowNumber(b, s, e);
    exp += eneg ? -ev : ev;
  }
  if (i !== e || digits === 0 || m > 9007199254740991) return slowNumber(b, s, e);
  let v;
  if (exp === 0) v = m;
  else if (exp > 0 && exp <= 22) v = m * POW10[exp];
  else if (exp < 0 && exp >= -22) v = m / POW10[-exp];
  else return slowNumber(b, s, e);
  return neg ? -v : v;
}

function slowNumber(b, s, e) {
  const str = decode(b, s, e, 'latin1');
  const v = Number(str);
  return str.trim() === '' ? NaN : v;
}
