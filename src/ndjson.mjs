// JSON lines (one flat object per line) straight into columns, without making
// a JS object per row:
//   {"ts":1700000000000,"price":12.5,"qty":3,"region":"EU"}
// Only the wanted keys are read. Number columns take numbers (and numeric
// strings); category columns take strings (and numbers, as their text).
// null, missing keys and nested values give NaN (numbers) or "" (categories).
// A line that isn't a JSON object is skipped and counted in `bad`.
import { ByteDict, parseNumber } from './csv-kernel.mjs';
import { encode } from './bytes.mjs';
import { parseDateBytes } from './dates.mjs';

const NL = 10, CR = 13, SPACE = 32, TAB = 9, QUOTE = 34, BACKSLASH = 92, COMMA = 44, COLON = 58;
const LBRACE = 123, RBRACE = 125, LBRACKET = 91, RBRACKET = 93;
const isSpace = (c) => c === SPACE || c === TAB || c === CR || c === NL;
const unescapeJSON = (raw) => JSON.parse(`"${raw}"`);
// The kind a JSON text value asks for: 'number' when Number() reads it ("2", " 1e3 "), 'blank'
// for blank text (missing in either kind), else 'category'. Numeric strings go into number columns.
export const textKind = (s) => (!s.trim() ? 'blank' : Number.isNaN(Number(s)) ? 'category' : 'number');
const RANK = { blank: 0, number: 1, category: 2 };

// names: column names; kinds: 'number' | 'category' | 'date' for each
// (dates: ISO text or ms numbers, as ms).
// Returns { rows, cols: Float64Array[], dicts: (string[] | null)[], bad }.
// report (optional, an object): filled with text (a Uint8Array, 1 for a number column that got
// text that isn't a number) and extra (a Map: each other key with plain values -> 'number',
// 'category' or 'blank' (only blank text), as its values look), so a caller can load the lines again with those kinds.
// nestedText: a nested value in a text column is its JSON text (loading a file; streams leave it missing).
export function parseNDJSON(bytes, start, end, names, kinds, report = null, { nestedText = false } = {}) {
  const k = names.length;
  const keyBytes = names.map((n) => encode(n));
  let cap = 1;
  for (let i = start; i < end; i++) if (bytes[i] === NL) cap++;
  const cols = names.map(() => new Float64Array(cap));
  const dicts = kinds.map((kind) => (kind === 'category' ? new ByteDict(bytes, unescapeJSON) : null));
  const empty = dicts.map((d) => (d ? -1 : NaN)); // code of "" per category column, made on first use
  const isDate = kinds.map((kind) => kind === 'date');
  const seen = new Uint8Array(k);
  let guess = new Int32Array(64).fill(-1); // column of the j-th key on the previous line
  let rows = 0, bad = 0;
  const text = report ? (report.text = new Uint8Array(k)) : null, extra = report ? (report.extra = new Map()) : null;
  const extraKeys = []; // [key bytes, name] of the other keys met so far
  const extraName = (s, e, esc) => {
    if (esc) return unescapeJSON(decodeLatin(bytes, s, e));
    for (const [kb, name] of extraKeys) if (same(kb, s, e)) return name;
    const name = decodeLatin(bytes, s, e);
    extraKeys.push([bytes.slice(s, e), name]);
    return name;
  };
  const noteExtra = (name, kind) => { if (!extra.has(name) || RANK[kind] > RANK[extra.get(name)]) extra.set(name, kind); };

  // Which column does the key in bytes[s, e) name? (-1: not wanted)
  const columnOf = (s, e, j) => {
    const g = j < guess.length ? guess[j] : -1;
    if (g >= 0 && same(keyBytes[g], s, e)) return g;
    for (let c = 0; c < k; c++) if (same(keyBytes[c], s, e)) { if (j < guess.length) guess[j] = c; return c; }
    return -1;
  };
  const same = (kb, s, e) => {
    if (kb.length !== e - s) return false;
    for (let i = 0; i < kb.length; i++) if (kb[i] !== bytes[s + i]) return false;
    return true;
  };
  const emptyCode = (c) => (empty[c] === -1 ? (empty[c] = dicts[c].code(0, 0, false)) : empty[c]);

  let i = start;
  while (i < end) {
    // one line: [i, eol)
    let eol = i;
    while (eol < end && bytes[eol] !== NL) eol++;
    let p = i;
    if (bytes[p] === 0xef && bytes[p + 1] === 0xbb && bytes[p + 2] === 0xbf && p + 2 < eol) p += 3; // (a UTF-8 BOM: at the start of a file, or of one joined onto another)
    while (p < eol && isSpace(bytes[p])) p++;
    if (p === eol) { i = eol + 1; continue; } // blank line
    const row = rows;
    seen.fill(0);
    let ok = bytes[p] === LBRACE;
    p++;
    let j = 0;
    while (ok) {
      while (p < eol && isSpace(bytes[p])) p++;
      if (bytes[p] === RBRACE) { p++; break; }
      if (j > 0) { if (bytes[p] !== COMMA) { ok = false; break; } p++; while (p < eol && isSpace(bytes[p])) p++; }
      if (bytes[p] !== QUOTE) { ok = false; break; }
      // key
      const ks = ++p;
      let kEsc = false;
      while (p < eol && bytes[p] !== QUOTE) { if (bytes[p] === BACKSLASH) { kEsc = true; p++; } p++; }
      if (p >= eol) { ok = false; break; }
      const c = kEsc ? columnOfText(unescapeJSON(decodeLatin(bytes, ks, p)), names) : columnOf(ks, p, j);
      const kEnd = p;
      p++;
      while (p < eol && isSpace(bytes[p])) p++;
      if (bytes[p] !== COLON) { ok = false; break; }
      p++;
      while (p < eol && isSpace(bytes[p])) p++;
      // value
      const b = bytes[p];
      if (b === QUOTE) {
        const vs = ++p;
        let esc = false;
        while (p < eol && bytes[p] !== QUOTE) { if (bytes[p] === BACKSLASH) { esc = true; p++; } p++; }
        if (p >= eol) { ok = false; break; }
        if (c >= 0) {
          if (dicts[c]) cols[c][row] = dicts[c].code(vs, p, esc);
          else if (isDate[c]) cols[c][row] = parseDateBytes(bytes, vs, p);
          else {
            const x = esc ? unescapeJSON(decodeLatin(bytes, vs, p)) : null;
            cols[c][row] = esc ? (x.trim() ? Number(x) : NaN) : parseNumber(bytes, vs, p);
            if (text && p > vs && cols[c][row] !== cols[c][row]) text[c] = 1; // (text that isn't a number)
          }
          seen[c] = 1;
        } else if (extra) noteExtra(extraName(ks, kEnd, kEsc), textKind(esc ? unescapeJSON(decodeLatin(bytes, vs, p)) : decodeLatin(bytes, vs, p)));
        p++;
      } else if (b === LBRACE || b === LBRACKET) { // nested: missing (nestedText: its JSON text in a text column)
        const vs = p;
        p = skipNested(bytes, p, eol);
        if (p < 0) { ok = false; break; }
        if (c >= 0 && dicts[c] && nestedText) { cols[c][row] = dicts[c].code(vs, p, false); seen[c] = 1; }
        else if (c < 0 && extra) noteExtra(extraName(ks, kEnd, kEsc), 'category'); // (a key first seen so, after the first lines)
      } else {
        const vs = p;
        while (p < eol && bytes[p] !== COMMA && bytes[p] !== RBRACE && !isSpace(bytes[p])) p++;
        if (c >= 0) {
          const first = bytes[vs];
          if (first === 110) { /* null: leave empty */ }
          else if (dicts[c]) { cols[c][row] = dicts[c].code(vs, p, false); seen[c] = 1; }
          else { cols[c][row] = first === 116 ? 1 : first === 102 ? 0 : parseNumber(bytes, vs, p); seen[c] = 1; }
        } else if (extra && bytes[vs] !== 110) noteExtra(extraName(ks, kEnd, kEsc), 'number');
      }
      j++;
    }
    if (ok) {
      while (p < eol && isSpace(bytes[p])) p++;
      ok = p === eol;
    }
    if (ok) {
      for (let c = 0; c < k; c++) if (!seen[c]) cols[c][row] = dicts[c] ? emptyCode(c) : NaN;
      rows++;
    } else {
      bad++;
    }
    i = eol + 1;
  }
  return { rows, cols: cols.map((col) => col.subarray(0, rows)), dicts: dicts.map((d) => d?.strings ?? null), bad };
}

function columnOfText(text, names) { return names.indexOf(text); }

function decodeLatin(bytes, s, e) {
  return new TextDecoder().decode(bytes.subarray(s, e));
}

// Index just past the nested object or array starting at p (-1 if unbalanced).
function skipNested(bytes, p, eol) {
  let depth = 0;
  for (; p < eol; p++) {
    const c = bytes[p];
    if (c === QUOTE) { for (p++; p < eol && bytes[p] !== QUOTE; p++) if (bytes[p] === BACKSLASH) p++; }
    else if (c === LBRACE || c === LBRACKET) depth++;
    else if (c === RBRACE || c === RBRACKET) { if (--depth === 0) return p + 1; }
  }
  return -1;
}
