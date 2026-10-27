// Copying rows between tables: the rows of several tables (or ranges of
// them) one after another as a new table, with category labels merged and
// free text given its own bytes. Used by datasets (chunks, spills) and by
// set operations (UNION).
import { Table } from './table.mjs';
import { decodeOne } from './pack.mjs';
import { STR_SHIFT, STR_ESCAPED } from './csv-kernel.mjs';

export const textLength = (v) => { const field = v - Math.floor(v / STR_SHIFT) * STR_SHIFT; return field >= STR_ESCAPED ? field - STR_ESCAPED : field; };
// Rows [lo, hi) of column c as plain numbers in out (packed: decoded).
export function plainInto(engine, c, lo, hi, out, f32) {
  const src = engine.live(c.data);
  if (!c.enc) { out.set(src.subarray(lo, hi)); return; }
  for (let i = lo; i < hi; i++) out[i - lo] = decodeOne(c.enc, src[i], f32);
}

// A table's category codes as codes of the dataset's dictionary (new labels added).
export function remapCodes(data, dict, labels) {
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

