// Parallel CSV loading into a Table.
//
//   1. The file's bytes go straight into engine memory (no JS strings).
//      Reading files is in node.mjs; this module works anywhere.
//   2. Cut it into chunks at row boundaries. With quotes in the file, a
//      parallel pass counts quote characters per chunk so each cut can tell
//      whether it lands inside a quoted field (which may contain newlines).
//   3. Count rows per chunk in parallel, so every chunk knows its first row.
//   4. Parse every chunk in parallel straight into the final columns. Text
//      columns get per-chunk dictionaries that are merged at the end.

import { Table } from './table.mjs';
import { OP } from './kernels.mjs';
import { parseNumber, STR_SHIFT, STR_ESCAPED } from './csv-kernel.mjs';
import { parseDateBytes, looksLikeDate } from './dates.mjs';
import { decode, encode, indexOfByte } from './bytes.mjs';

const KIND = { number: 0, category: 1, string: 2, date: 2 }; // dates are read as text, then parsed (see below)

const NL = 10, CR = 13, QUOTE = 34;
const CHUNK_BYTES = 2 << 20;
const RESERVED = new Set(['Infinity', 'NaN', 'sum', 'mean', 'min', 'max', 'count', 'abs', 'sqrt', 'floor', 'ceil']);

// CSV given as a string or bytes (Uint8Array, Buffer).
export function parseCSV(engine, text, opts = {}) { return engine._drive(parseCSVG(engine, text, opts)); }
export function parseCSVAsync(engine, text, opts = {}) { return engine._driveAsync(parseCSVG(engine, text, opts)); }

function* parseCSVG(engine, text, opts) {
  const src = typeof text === 'string' ? encode(text) : text;
  const bytes = engine.alloc(Uint8Array, src.length);
  bytes.set(src);
  return yield* loadBytesG(engine, bytes, opts);
}

export function loadBytes(engine, bytes, opts) { return engine._drive(loadBytesG(engine, bytes, opts)); }
export function loadBytesAsync(engine, bytes, opts) { return engine._driveAsync(loadBytesG(engine, bytes, opts)); }

// options:
//   delimiter  ',' (one character)
//   header     true: first line has column names; false: col0, col1, ...
//   types      { name: 'number' | 'category' | 'string' | 'text' } to override
//              the guess. category = repeating labels (filterable, groupable);
//              string = free text (kept as bytes; returned, not filtered yet);
//              text = pick category or string from the sample.
//   columns    [names] to load only some columns
//   type       Float64Array (default) or Float32Array for number storage
//   sampleRows rows used to guess column types (default 1000)
//   chunkBytes target bytes per parallel chunk (tests use small chunks)
//   pieces     internal (split tables): byte lengths of consecutive pieces of
//              the data, each starting at a row; they become the chunks, and
//              onPieceRows(rows per piece) is called before parsing
// Parse CSV bytes that are already in engine memory.
// (Plain memory moves when it grows, so views are refreshed after each alloc.)
function* loadBytesG(engine, bytes, { delimiter = ',', header = true, types = {}, columns: only = null, type = Float64Array, sampleRows = 1000, chunkBytes = CHUNK_BYTES, pieces = null, onPieceRows = null } = {}) {
  const temp = [bytes];
  try {
    const info = inspectCSV(bytes, { delimiter, header, types, sampleRows });
    const { delim, names } = info;
    let { start, end } = info;
    const nFields = names.length;
    const want = only ? new Set(only) : null;
    if (want) for (const nm of want) if (!names.includes(nm)) throw new Error(`vectro: no column "${nm}" in the CSV (columns: ${names.join(', ')})`);

    const out = []; // { name, kind }
    let fieldMap = engine.alloc(Float64Array, nFields);
    temp.push(fieldMap);
    bytes = engine.live(bytes);
    names.forEach((nm, f) => {
      if (want && !want.has(nm)) { fieldMap[f] = -1; return; }
      fieldMap[f] = out.length;
      out.push({ name: nm, kind: KIND[info.kinds[f]], date: info.kinds[f] === 'date' });
    });

    // Cut points at row boundaries.
    const T = engine.T;
    const dataLen = Math.max(0, end - start);
    const chunks = pieces ? pieces.length : Math.max(1, Math.min(T * 16, Math.ceil(dataLen / chunkBytes)));
    let bounds = engine.alloc(Float64Array, chunks + 1);
    temp.push(bounds);
    bytes = engine.live(bytes);
    fieldMap = engine.live(fieldMap);
    const hasQuotes = indexOfByte(bytes, QUOTE, start, end) !== -1;
    if (pieces) {
      bounds[0] = start;
      for (let k = 0; k < chunks; k++) bounds[k + 1] = Math.min(end, bounds[k] + pieces[k]); // the end may exclude trailing blank lines
      if (bounds[chunks] < end) throw new Error('vectro: CSV pieces don\'t cover the data');
    } else {
      for (let k = 0; k <= chunks; k++) bounds[k] = start + Math.floor((k * dataLen) / chunks);
    }

    if (pieces) {
      // already cut at rows
    } else if (hasQuotes) {
      const { parts: quotes } = yield* engine._chunksG(OP.CSV_SCAN, chunks, [bytes, bounds], [0, 1]);
      let before = 0;
      const raw = Float64Array.from(bounds);
      for (let k = 1; k < chunks; k++) {
        before += quotes[k - 1];
        let inQuotes = before % 2 === 1;
        let j = raw[k];
        for (; j < end; j++) {
          const c = bytes[j];
          if (c === QUOTE) inQuotes = !inQuotes;
          else if (c === NL && !inQuotes) break;
        }
        bounds[k] = Math.max(bounds[k - 1], Math.min(end, j + 1));
      }
    } else {
      for (let k = 1; k < chunks; k++) {
        const nl = indexOfByte(bytes, NL, bounds[k], end);
        bounds[k] = Math.max(bounds[k - 1], nl === -1 ? end : nl + 1);
      }
    }
    bounds[chunks] = end;

    // Rows per chunk -> first row of each chunk.
    const { parts: rowsPer } = yield* engine._chunksG(OP.CSV_SCAN, chunks, [bytes, bounds], [1, hasQuotes ? 1 : 0]);
    onPieceRows?.(Array.from(rowsPer));
    let rowStarts = engine.alloc(Float64Array, chunks);
    temp.push(rowStarts);
    let total = 0;
    for (let k = 0; k < chunks; k++) { rowStarts[k] = total; total += rowsPer[k]; }

    // Parse.
    let cols = out.map(() => engine.alloc(type, total));
    const kinds = engine.alloc(Float64Array, out.length);
    temp.push(kinds);
    out.forEach((c, o) => { kinds[o] = c.kind; });
    // one dictionary message per chunk per category column
    const expect = chunks * out.filter((c) => c.kind === 1).length;
    const { messages } = yield* engine._chunksG(OP.CSV_PARSE, chunks, [bytes, bounds, rowStarts, fieldMap, kinds, ...cols], [delim, nFields], expect);

    // One dictionary per text column, numbered in order of first appearance.
    [bytes, rowStarts] = [engine.live(bytes), engine.live(rowStarts)];
    cols = cols.map((c) => engine.live(c));
    const dicts = out.map((c) => (c.kind === 1 ? { dict: [], codes: new Map() } : null));
    messages.sort((a, b) => a.unit - b.unit || a.col - b.col);
    for (const m of messages) {
      const { dict, codes } = dicts[m.col];
      const lut = new Float64Array(m.strings.length);
      m.strings.forEach((str, local) => {
        let g = codes.get(str);
        if (g === undefined) { g = dict.length; dict.push(str); codes.set(str, g); }
        lut[local] = g;
      });
      const col = cols[m.col];
      for (let r = rowStarts[m.unit], stop = r + rowsPer[m.unit]; r < stop; r++) col[r] = lut[col[r]];
    }

    // Date columns: each value's text (still in the file's bytes) as ms.
    out.forEach((c, o) => {
      if (!c.date) return;
      const col = cols[o];
      for (let r = 0; r < total; r++) {
        const v = col[r];
        if (v !== v) continue;
        const s = Math.floor(v / STR_SHIFT), len = (v - s * STR_SHIFT) % STR_ESCAPED;
        col[r] = parseDateBytes(bytes, s, s + len);
      }
    });

    // Free-text columns point into the file's bytes, so the table keeps them.
    const keepBytes = out.some((c) => c.kind === 2 && !c.date);
    if (keepBytes) temp.shift(); // temp[0] is the bytes array
    const tableCols = new Map();
    out.forEach((c, o) => tableCols.set(c.name, {
      data: cols[o], dict: dicts[o]?.dict ?? null, codes: dicts[o]?.codes ?? null, strings: c.kind === 2 && !c.date ? bytes : null, date: c.date,
    }));
    return new Table(engine, tableCols, total, { owned: out.map((c) => c.name), ownedBuffers: keepBytes ? [bytes] : [] });
  } finally {
    for (const t of temp) engine.free(t);
  }
}

// Header, data range and column types of CSV bytes, from the header and a
// sample of rows (the rest isn't read). kinds[f] is 'number', 'category' or
// 'string' for field f, after applying the `types` overrides.
export function inspectCSV(bytes, { delimiter = ',', header = true, types = {}, sampleRows = 1000 } = {}) {
  const delim = delimiter.charCodeAt(0);
  if (delimiter.length !== 1 || delim === QUOTE || delim === NL) throw new Error('vectro: delimiter must be one character, not a quote or newline');

  let start = bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf ? 3 : 0; // UTF-8 BOM
  let end = bytes.length;
  while (end > start && (bytes[end - 1] === NL || bytes[end - 1] === CR)) end--;

  const first = splitRow(bytes, start, end, delim);
  const headerStart = start;
  let names;
  if (header) {
    names = cleanNames(first.fields.map((f) => fieldText(bytes, f)));
    start = Math.min(first.next, end);
  } else {
    names = first.fields.map((_, i) => `col${i}`);
  }
  const nFields = names.length;

  const isNumber = names.map(() => true), isDate = names.map(() => true);
  const distinct = names.map(() => new Set());
  const filled = names.map(() => 0);
  for (let i = start, r = 0; i < end && r < sampleRows; r++) {
    const row = splitRow(bytes, i, end, delim);
    row.fields.forEach((f, k) => {
      if (k >= nFields || f.e === f.s) return;
      const text = fieldText(bytes, f);
      filled[k]++;
      distinct[k].add(text);
      if (isNumber[k] && Number.isNaN(parseNumber(bytes, f.s, f.e)) && text.trim() !== 'NaN') isNumber[k] = false;
      if (isDate[k] && !looksLikeDate(text)) isDate[k] = false;
    });
    i = row.next;
  }
  // Text where most sampled values differ (ids, notes, emails) is free text;
  // text that repeats (regions, statuses) is a category.
  const guess = (k) => (isNumber[k] ? 'number' : isDate[k] && filled[k] ? 'date' : filled[k] >= 50 && distinct[k].size > filled[k] / 2 ? 'string' : 'category');
  const kinds = names.map((nm, f) => {
    let kind = types[nm] ?? guess(f);
    if (kind === 'text') kind = guess(f) === 'string' ? 'string' : 'category';
    if (!(kind in KIND)) throw new Error(`vectro: type for "${nm}" must be 'number', 'category', 'string', 'text' or 'date'`);
    return kind;
  });
  return { delim, names, kinds, headerStart, start, end };
}

// Fields of one row (quote-aware): [{ s, e, escaped }], plus where the next row starts.
function splitRow(bytes, i, end, delim) {
  const fields = [];
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
      while (i < end && bytes[i] !== delim && bytes[i] !== NL) i++;
      e = i;
      if (e > s && bytes[e - 1] === CR) e--;
    }
    fields.push({ s, e, escaped });
    if (i < end && bytes[i] === delim) { i++; continue; }
    return { fields, next: i + 1 };
  }
}

function fieldText(bytes, f) {
  const str = decode(bytes, f.s, f.e);
  return f.escaped ? str.replaceAll('""', '"') : str;
}

// Column names must work in expressions: "Order Date" -> Order_Date.
function cleanNames(raw) {
  const seen = new Map();
  return raw.map((r, i) => {
    let nm = r.trim().replace(/[^A-Za-z0-9_]/g, '_') || `col${i}`;
    if (/^\d/.test(nm)) nm = `_${nm}`;
    if (RESERVED.has(nm)) nm = `${nm}_`;
    const k = seen.get(nm) ?? 0;
    seen.set(nm, k + 1);
    return k ? `${nm}_${k + 1}` : nm;
  });
}
