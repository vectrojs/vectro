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
import { OP, MAX_ARGS } from './kernels.mjs';
import { parseNumber, STR_SHIFT, STR_ESCAPED, STR_MAX_LEN, CSV_FIELD_START } from './csv-kernel.mjs';
import { parseDateBytes, looksLikeDate, f32Dates } from './dates.mjs';
import { decode, encode, indexOfByte } from './bytes.mjs';

const KIND = { number: 0, category: 1, string: 2, date: 2 }; // dates are read as text, then parsed (see below)

const NL = 10, CR = 13, QUOTE = 34;
const CHUNK_BYTES = 2 << 20;
const COLS_PER_PASS = MAX_ARGS - 8; // (a parse task's other arrays: bytes, bounds, rowStarts, fieldMap, kinds, check)
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
//   checkTypes names of columns whose type was guessed (default: those not in
//              types); badType 'retype' (default): one with a value that isn't
//              its type (text after the sampled rows) is loaded again as text;
//              'throw': a clear error instead (pieces parsed separately)
//   trimEnd    false: the bytes are a piece of a file, not its end, so blank lines at their end
//              are rows (of missing values), as in the middle of a file
// A file whose lines end in CR alone (old Mac files: no LF anywhere, a CR somewhere): its CRs become
// LFs, in place. It loaded as one header line and no rows, with no error. Returns whether it was one.
export function crOnlyToLF(b) {
  if (b.indexOf(10) !== -1 || b.indexOf(13) === -1) return false;
  for (let i = b.indexOf(13); i !== -1; i = b.indexOf(13, i + 1)) b[i] = 10;
  return true;
}

function* loadBytesG(engine, bytes, opts = {}) {
  crOnlyToLF(engine.live(bytes));
  const { delimiter = ',', header = true, types = {}, columns: only = null, type = Float64Array, sampleRows = 1000, chunkBytes = CHUNK_BYTES, pieces = null, onPieceRows = null, checkTypes = null, badType = 'retype', trimEnd = true } = opts;
  const temp = [bytes];
  try {
    const info = inspectCSV(bytes, { delimiter, header, types, sampleRows, trimEnd });
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
      if (info.kinds[f] === 'date' && type === Float32Array) throw new Error(f32Dates(nm));
      let kind = info.kinds[f];
      // free text is stored as byte offsets, which float32 can't hold exactly: a category instead
      if (kind === 'string' && type === Float32Array) {
        if (Object.hasOwn(types, nm) && types[nm] === 'string') throw new Error(`vectro: "${nm}" is a free-text column, and a Float32Array table can't hold its byte offsets exactly; load it with types: { ${nm}: 'category' }, or use the default Float64Array`);
        kind = 'category';
      }
      out.push({ name: nm, kind: KIND[kind], date: kind === 'date' });
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
      // every chunk scanned from each parser state at once (csv-kernel.mjs csvScan mode 2); then the
      // real state is followed chunk to chunk, and each cut moves to the first row start after it
      let scan = engine.alloc(Float64Array, chunks * 8);
      temp.push(scan);
      yield* engine._chunksG(OP.CSV_SCAN, chunks, [bytes, bounds, scan], [2, 1, delim]);
      scan = engine.live(scan);
      const cut = new Float64Array(chunks + 1).fill(-1);
      let state = CSV_FIELD_START; // (the data starts at a row)
      for (let k = 0; k < chunks; k++) {
        if (k > 0) cut[k] = scan[k * 8 + 2 * state + 1];
        state = scan[k * 8 + 2 * state];
      }
      for (let k = chunks - 1; k >= 1; k--) bounds[k] = cut[k] >= 0 ? cut[k] : k + 1 < chunks ? bounds[k + 1] : end; // (no row end in a chunk: it joins the next)
      for (let k = 1; k < chunks; k++) bounds[k] = Math.max(bounds[k - 1], Math.min(end, bounds[k]));
    } else {
      for (let k = 1; k < chunks; k++) {
        const nl = indexOfByte(bytes, NL, bounds[k], end);
        bounds[k] = Math.max(bounds[k - 1], nl === -1 ? end : nl + 1);
      }
    }
    bounds[chunks] = end;

    // Rows per chunk -> first row of each chunk.
    const { parts: rowsPer } = yield* engine._chunksG(OP.CSV_SCAN, chunks, [bytes, bounds], [1, hasQuotes ? 1 : 0, delim]);
    onPieceRows?.(Array.from(rowsPer));
    let rowStarts = engine.alloc(Float64Array, chunks);
    temp.push(rowStarts);
    let total = 0;
    for (let k = 0; k < chunks; k++) { rowStarts[k] = total; total += rowsPer[k]; }

    // Parse.
    let cols = out.map(() => engine.alloc(type, total));
    const kinds = engine.alloc(Float64Array, out.length), check = engine.alloc(Float64Array, out.length);
    temp.push(kinds, check);
    const guessed = (nm) => (checkTypes ? checkTypes.includes(nm) : !(nm in types));
    out.forEach((c, o) => { kinds[o] = c.kind; check[o] = guessed(c.name) && (c.kind === 0 || c.kind === 2) ? 1 : 0; }); // (2: dates and free text)
    // One dictionary message per chunk per category column. A task takes at most MAX_ARGS arrays,
    // so a very wide file is parsed in batches of columns (the others skipped in each pass).
    const messages = [];
    for (let b0 = 0; b0 < out.length || b0 === 0; b0 += COLS_PER_PASS) {
      const b1 = Math.min(out.length, b0 + COLS_PER_PASS), whole = b0 === 0 && b1 === out.length;
      let fm = fieldMap, kd = kinds, ck = check;
      if (!whole) {
        fm = engine.alloc(Float64Array, nFields); kd = engine.alloc(Float64Array, b1 - b0); ck = engine.alloc(Float64Array, b1 - b0);
        temp.push(fm, kd, ck);
        const fmAll = engine.live(fieldMap), kAll = engine.live(kinds), cAll = engine.live(check);
        for (let f = 0; f < nFields; f++) fm[f] = fmAll[f] >= b0 && fmAll[f] < b1 ? fmAll[f] - b0 : -1;
        for (let o = b0; o < b1; o++) { kd[o - b0] = kAll[o]; ck[o - b0] = cAll[o]; }
      }
      const expect = chunks * out.slice(b0, b1).filter((c) => c.kind === 1).length;
      const r = yield* engine._chunksG(OP.CSV_PARSE, chunks, [bytes, bounds, rowStarts, fm, kd, ck, ...cols.slice(b0, b1)], [delim, nFields], expect);
      for (const m of r.messages) messages.push(whole ? m : { ...m, col: m.col + b0 });
      if (!whole) { const cAll = engine.live(check), cb = engine.live(ck); for (let o = b0; o < b1; o++) cAll[o] = cb[o - b0]; }
      if (whole) break;
    }

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
    const checked = engine.live(check);
    out.forEach((c, o) => {
      if (!c.date) return;
      const col = cols[o];
      for (let r = 0; r < total; r++) {
        const v = col[r];
        if (v !== v) continue;
        const s = Math.floor(v / STR_SHIFT), len = (v - s * STR_SHIFT) % STR_ESCAPED;
        col[r] = parseDateBytes(bytes, s, s + len);
        if (col[r] !== col[r] && len && checked[o] === 1 && decode(bytes, s, s + len).trim()) checked[o] = 2;
      }
    });
    // A guessed number or date column with other text after the sampled rows, or a guessed
    // free-text column with a value too long for a byte ref (see csvParse): loaded again as
    // text (category), or a clear error where pieces are parsed apart (split tables, datasets).
    const bad = out.filter((c, o) => checked[o] === 2).map((c) => c.name);
    if (bad.length) {
      const fix = Object.fromEntries(bad.map((nm) => [nm, 'category']));
      const first = out.find((c) => c.name === bad[0]);
      if (badType === 'throw') throw new Error(first.kind === 2 && !first.date ? `vectro: column "${first.name}" has a text value longer than ${STR_MAX_LEN} bytes; load with types: ${JSON.stringify(fix)}`
        : `vectro: column${bad.length > 1 ? 's' : ''} ${bad.map((nm) => `"${nm}"`).join(', ')} looked like ${first.date ? 'dates' : 'numbers'} in the first ${sampleRows} rows, but later rows have other text; load with types: ${JSON.stringify(fix)}`);
      for (const c of cols) engine.free(c);
      temp.shift(); // (the bytes go on to the next try)
      return yield* loadBytesG(engine, bytes, { ...opts, types: { ...types, ...fix } });
    }

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
export function inspectCSV(bytes, { delimiter = ',', header = true, types = {}, sampleRows = 1000, trimEnd = true } = {}) {
  const delim = delimiter.charCodeAt(0);
  if (delimiter.length !== 1 || delim === QUOTE || delim === NL) throw new Error('vectro: delimiter must be one character, not a quote or newline');

  let start = bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf ? 3 : 0; // UTF-8 BOM
  let end = bytes.length;
  if (trimEnd) while (end > start && (bytes[end - 1] === NL || bytes[end - 1] === CR)) end--; // (blank lines at the end of the file: no rows)

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
    let kind = (Object.hasOwn(types, nm) ? types[nm] : undefined) ?? guess(f); // (not types.constructor: a column named so)
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
export function cleanNames(raw) {
  const base = raw.map((r, i) => {
    let nm = r.trim().replace(/[^\p{L}\p{M}\p{N}_]/gu, '_') || `col${i}`;
    if (/^\d/.test(nm)) nm = `_${nm}`;
    if (RESERVED.has(nm)) nm = `${nm}_`;
    return nm;
  });
  // a name that is already fine keeps it (a-b, a_b -> a_b_2, a_b); a repeated name gets _2, _3...
  // skipping every name the header already has (a, a, a_2 -> a, a_3, a_2)
  const taken = new Set(base), used = new Set(), kept = base.map((nm, i) => nm === raw[i] && !used.has(nm) && used.add(nm));
  return base.map((nm, i) => {
    if (kept[i]) return nm;
    if (!used.has(nm)) { used.add(nm); return nm; }
    let k = 2;
    while (taken.has(`${nm}_${k}`) || used.has(`${nm}_${k}`)) k++;
    used.add(`${nm}_${k}`);
    return `${nm}_${k}`;
  });
}
