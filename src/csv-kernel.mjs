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

// The parser's quote rules as a state machine, so cuts and row counts agree
// with it: a quote opens a quoted field only at the start of a field (a quote
// inside an unquoted field, 27" or O"Brien, is a plain character); in a
// quoted field "" is a quote and a lone quote closes it.
//   0 field start, 1 in an unquoted field (or after a closing quote),
//   2 in a quoted field, 3 a quote seen in a quoted field (closing, or half of "")
export const CSV_FIELD_START = 0;
function csvStep(state, c, delim) {
  switch (state) {
    case 0: return c === QUOTE ? 2 : c === NL || c === delim ? 0 : 1;
    case 1: return c === NL || c === delim ? 0 : 1;
    case 2: return c === QUOTE ? 3 : 2;
    default: return c === QUOTE ? 2 : c === NL || c === delim ? 0 : 1;
  }
}
// a row ends at a newline read in state 0, 1 or 3 (not inside a quoted field)
const ROW_END = [true, true, false, true];

// Chunk u: count quotes (mode 0), count rows from a field start (mode 1), or
// (mode 2) run from each of the 4 states, writing into A[2] for each start
// state s: [u * 8 + 2s] = the state at the chunk's end, [u * 8 + 2s + 1] = the
// byte after the first row end (or -1). The caller then follows the real
// state from chunk to chunk and cuts at row starts, all chunks scanned at once.
// Rows are newlines outside quotes, plus a final line without a newline.
export function csvScan(u, A, sc) {
  const bytes = A[0], bounds = A[1];
  const start = bounds[u], end = bounds[u + 1];
  const mode = sc[0], hasQuotes = sc[1], delim = sc[2];
  if (mode === 2) {
    const info = A[2];
    for (let s0 = 0; s0 < 4; s0++) {
      let st = s0, first = -1;
      for (let i = start; i < end; i++) {
        const c = bytes[i];
        if (c === NL && ROW_END[st]) { first = i + 1; break; }
        st = csvStep(st, c, delim);
      }
      if (first >= 0) st = csvEnd(bytes, first, end, delim); // (a row ended: on from a field start)
      info[u * 8 + 2 * s0] = st;
      info[u * 8 + 2 * s0 + 1] = first;
    }
    return 0;
  }
  if (end <= start) return 0;
  if (mode === 0) return countByte(bytes, QUOTE, start, end);
  let rows;
  if (!hasQuotes) {
    rows = countByte(bytes, NL, start, end); // native indexOf speed
  } else {
    rows = 0;
    let st = 0;
    for (let i = start; i < end; i++) {
      const c = bytes[i];
      if (c === NL && ROW_END[st]) { rows++; st = 0; continue; }
      st = csvStep(st, c, delim);
    }
  }
  return bytes[end - 1] === NL ? rows : rows + 1;
}
// Row starts in [from, end) for one thread (from = a row start), by the same
// rules: rowEnds(bytes, from, end, delim, at) calls at(i) with the byte after
// each row's newline, in order; return false from at() to stop.
export function csvRowEnds(bytes, from, end, delim, at) {
  let st = 0;
  for (let i = from; i < end; i++) {
    const c = bytes[i];
    if (c === NL && ROW_END[st]) { st = 0; if (at(i + 1) === false) return; continue; }
    st = csvStep(st, c, delim);
  }
}
// For text that arrives in pieces cut anywhere (streams): scan [from, end)
// starting in quote state `state` (CSV_FIELD_START at a row start), by the
// same rules. -> { last: the byte after the last row end (-1: none), rows:
// row ends seen, state: the state at `end`, to pass with the next piece }.
export function csvTail(bytes, from, end, delim, state) {
  let last = -1, rows = 0;
  if (state !== 2 && bytes.subarray(from, end).indexOf(QUOTE) === -1) { // no quotes: newlines are row ends
    rows = countByte(bytes, NL, from, end);
    if (rows) last = bytes.subarray(from, end).lastIndexOf(NL) + from + 1;
    if (end > from) state = bytes[end - 1] === NL || bytes[end - 1] === delim ? 0 : 1;
    return { last, rows, state };
  }
  for (let i = from; i < end; i++) {
    const c = bytes[i];
    if (c === NL && ROW_END[state]) { state = 0; rows++; last = i + 1; continue; }
    state = csvStep(state, c, delim);
  }
  return { last, rows, state };
}
// the state at `end`, starting at a field start at `from`
function csvEnd(bytes, from, end, delim) {
  let st = 0;
  for (let i = from; i < end; i++) {
    const c = bytes[i];
    st = c === NL && ROW_END[st] ? 0 : csvStep(st, c, delim);
  }
  return st;
}

// Parse chunk u straight into the output columns.
// A = [bytes, bounds, rowStarts, fieldMap, kinds, check, ...columns]
//   fieldMap[f] = output column for field f (-1 = skip)
//   kinds[o] = 0 number, 1 category (dictionary codes), 2 free text (byte refs)
//   check[o] = 1: a guessed number column; set to 2 where a value isn't a number
//     (text after the sampled rows), so the caller can load it as text instead;
//     a guessed free-text column: set to 2 at a value too long for a byte ref
//     (the caller loads it as a category instead)
// sc = [delimiter, number of fields]
// Text columns get chunk-local codes; emit() reports each chunk's dictionary
// so the caller can renumber them into one shared dictionary.
export function csvParse(u, A, sc, emit) {
  const bytes = A[0], bounds = A[1], rowStarts = A[2], fieldMap = A[3], kinds = A[4], check = A[5];
  const cols = [];
  for (let o = 0; o < kinds.length; o++) cols.push(A[6 + o]);
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
        if (kind === 0) {
          const x = parseNumber(bytes, s, e);
          cols[o][row] = x;
          if (x !== x && check[o] === 1 && !blankOrNaN(bytes, s, e)) check[o] = 2;
        }
        else if (kind === 1) cols[o][row] = dicts[o].code(s, e, escaped);
        else {
          if (e - s > STR_MAX_LEN) {
            if (check[o] !== 1) throw new RangeError(`vectro: a text value in row ${row + 1} is longer than ${STR_MAX_LEN} bytes (load the column as a category)`);
            check[o] = 2;
            cols[o][row] = NaN;
          } else cols[o][row] = s * STR_SHIFT + (e - s) + (escaped ? STR_ESCAPED : 0);
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

// an empty value, or the text NaN (spaces around allowed): fine in a number column
function blankOrNaN(b, s, e) {
  while (s < e && b[s] === SPACE) s++;
  while (e > s && (b[e - 1] === SPACE || b[e - 1] === CR)) e--;
  return s === e || (e - s === 3 && b[s] === 78 && b[s + 1] === 97 && b[s + 2] === 78);
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
