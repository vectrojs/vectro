// PostgreSQL types for the wire protocol (pg.mjs): which type each answer
// column is said to have, and its values as text or binary, as PostgreSQL
// writes them; parameters sent by clients, read back into SQL literals.
//
// Vectro's column types (sql's opts.types) map to:
//   int -> int8, float -> float8, bool -> bool, text -> text,
//   date -> timestamp, day -> date
// Catalog answers (pgcatalog.mjs) use the others below too.

export const OID = {
  bool: 16, bytea: 17, char: 18, name: 19, int8: 20, int2: 21, int4: 23, regproc: 24, text: 25, oid: 26,
  float4: 700, float8: 701, unknown: 705, bpchar: 1042, varchar: 1043, date: 1082, time: 1083,
  timestamp: 1114, timestamptz: 1184, interval: 1186, numeric: 1700, regclass: 2205, regtype: 2206,
};
export const TYPE_OF = { int: 'int8', float: 'float8', bool: 'bool', text: 'text', date: 'timestamp', day: 'date' };
// (size for RowDescription; -1: variable)
const LEN = { bool: 1, char: 1, int2: 2, int4: 4, oid: 4, regproc: 4, regclass: 4, regtype: 4, float4: 4, date: 4, int8: 8, float8: 8, timestamp: 8, timestamptz: 8, name: 64 };
export const typeLength = (t) => LEN[t] ?? -1;

const EPOCH_2000 = 946_684_800_000; // ms from 1970 to 2000-01-01, PostgreSQL's zero for dates and times
const p2 = (x) => (x < 10 ? `0${x}` : `${x}`);

function dayText(ms) {
  const d = new Date(ms), y = d.getUTCFullYear();
  const ys = y > 0 ? (y < 1000 ? String(y).padStart(4, '0') : String(y)) : String(1 - y).padStart(4, '0');
  return `${ys}-${p2(d.getUTCMonth() + 1)}-${p2(d.getUTCDate())}${y > 0 ? '' : ' BC'}`;
}
function timestampText(ms) {
  const d = new Date(ms), y = d.getUTCFullYear();
  const day = dayText(ms).replace(' BC', '');
  const f = ((ms % 1000) + 1000) % 1000;
  const frac = f ? `.${String(Math.round(f * 1000)).padStart(6, '0').replace(/0+$/, '')}` : '';
  return `${day} ${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}:${p2(d.getUTCSeconds())}${frac}${y > 0 ? '' : ' BC'}`;
}
// float8 as PostgreSQL writes it (shortest exact form; exponents with two digits)
function floatText(v) {
  if (v === Infinity) return 'Infinity';
  if (v === -Infinity) return '-Infinity';
  const s = String(v);
  return s.includes('e') ? s.replace(/e([+-])(\d)$/, 'e$10$2') : s;
}

// value -> text (null for SQL NULL); v is a number for number types and
// dates (ms), a string for text types
// (keepEmpty: '' is an empty text, not SQL NULL as in Vectro's tables)
export function toPgText(type, v, keepEmpty = false) {
  if (v === null || v === undefined) return null;
  switch (type) {
    case 'text': case 'varchar': case 'name': case 'bpchar': case 'char': case 'regclass': case 'regtype': case 'regproc':
      if (typeof v === 'number') return v === v ? floatText(v) : null;
      return v === '' && type === 'text' && !keepEmpty ? null : String(v);
    case 'bool': return v !== v ? null : typeof v === 'string' ? (v === 't' || v === 'true' ? 't' : 'f') : v ? 't' : 'f';
    case 'int8': case 'int4': case 'int2': case 'oid':
      if (typeof v === 'string') return v;
      return v === v && Number.isFinite(v) ? String(Math.round(v) || 0) : null;
    case 'float8': case 'float4': case 'numeric': return v === v ? floatText(v) : null;
    case 'date': return Number.isFinite(v) ? dayText(v) : null;
    case 'timestamp': case 'timestamptz': return Number.isFinite(v) ? timestampText(v) + (type === 'timestamptz' ? '+00' : '') : null;
    default: return typeof v === 'number' ? (v === v ? floatText(v) : null) : String(v);
  }
}

// value -> binary bytes (null for SQL NULL)
export function toPgBinary(type, v, keepEmpty = false) {
  if (v === null || v === undefined || (typeof v === 'number' && v !== v)) return null;
  switch (type) {
    case 'int8': { const b = Buffer.alloc(8); b.writeBigInt64BE(BigInt(Math.round(v))); return b; }
    case 'int4': case 'oid': case 'regproc': case 'regclass': case 'regtype': { const b = Buffer.alloc(4); if (type === 'int4') b.writeInt32BE(Math.round(v)); else b.writeUInt32BE(Math.round(v) >>> 0); return b; }
    case 'int2': { const b = Buffer.alloc(2); b.writeInt16BE(Math.round(v)); return b; }
    case 'float8': { const b = Buffer.alloc(8); b.writeDoubleBE(v); return b; }
    case 'float4': { const b = Buffer.alloc(4); b.writeFloatBE(v); return b; }
    case 'bool': return Buffer.from([typeof v === 'string' ? (v === 't' || v === 'true' ? 1 : 0) : v ? 1 : 0]);
    case 'char': return Buffer.from(String(v).slice(0, 1), 'utf8');
    case 'date': { if (!Number.isFinite(v)) return null; const b = Buffer.alloc(4); b.writeInt32BE(Math.floor((v - EPOCH_2000) / 86_400_000)); return b; }
    case 'timestamp': case 'timestamptz': { if (!Number.isFinite(v)) return null; const b = Buffer.alloc(8); b.writeBigInt64BE(BigInt(Math.round((v - EPOCH_2000) * 1000))); return b; }
    default: { const t = toPgText(type, v, keepEmpty); return t === null ? null : Buffer.from(t, 'utf8'); }
  }
}

// Which of our encodings can go binary (a client asking binary for others gets text).
export const BINARY = new Set(['int8', 'int4', 'int2', 'oid', 'float8', 'float4', 'bool', 'date', 'timestamp', 'timestamptz', 'text', 'varchar', 'name', 'bpchar', 'char', 'regproc', 'regclass', 'regtype']);

const typeByOid = new Map(Object.entries(OID).map(([k, v]) => [v, k]));
export const typeName = (oid) => typeByOid.get(oid) ?? null;

// A bound parameter (bytes, format 0 text / 1 binary, its type oid or 0) as a SQL literal.
export function paramLiteral(bytes, format, oid) {
  if (bytes === null) return 'NULL';
  const type = typeByOid.get(oid);
  const quote = (s) => `'${String(s).replaceAll("'", "''")}'`;
  if (format === 1) {
    switch (type) {
      case 'int8': return bytes.readBigInt64BE().toString();
      case 'int4': return String(bytes.readInt32BE());
      case 'int2': return String(bytes.readInt16BE());
      case 'oid': return String(bytes.readUInt32BE());
      case 'float8': { const v = bytes.readDoubleBE(); return v === v ? `(${v})` : 'NULL'; }
      case 'float4': { const v = bytes.readFloatBE(); return v === v ? `(${v})` : 'NULL'; }
      case 'bool': return bytes[0] ? 'TRUE' : 'FALSE';
      case 'date': return `DATE ${quote(dayText(EPOCH_2000 + bytes.readInt32BE() * 86_400_000))}`;
      case 'timestamp': case 'timestamptz': return `TIMESTAMP ${quote(timestampText(EPOCH_2000 + Number(bytes.readBigInt64BE()) / 1000))}`;
      case 'numeric': return numericLiteral(bytes);
      default: return quote(bytes.toString('utf8'));
    }
  }
  const s = bytes.toString('utf8');
  switch (type) {
    case 'int8': case 'int4': case 'int2': case 'oid': case 'float8': case 'float4': case 'numeric':
      return /^\s*[+-]?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?\s*$/i.test(s) ? `(${s.trim()})` : /^\s*nan\s*$/i.test(s) ? 'NULL' : quote(s);
    case 'bool': return /^\s*(t|true|y|yes|on|1)\s*$/i.test(s) ? 'TRUE' : 'FALSE';
    case 'date': return `DATE ${quote(s)}`;
    case 'timestamp': case 'timestamptz': return `TIMESTAMP ${quote(s)}`;
    default: return quote(s);
  }
}
// numeric in binary: ndigits, weight, sign, dscale, then base-10000 digits
function numericLiteral(b) {
  const n = b.readInt16BE(0), weight = b.readInt16BE(2), sign = b.readUInt16BE(4);
  if (sign === 0xc000) return 'NULL';
  let v = 0;
  for (let i = 0; i < n; i++) v += b.readInt16BE(8 + 2 * i) * 10_000 ** (weight - i);
  return `(${sign === 0x4000 ? -v : v})`;
}

// Put parameters into a query: $1, $2... outside quotes, comments and names.
export function bindParams(text, literals) {
  if (!literals.length) return text;
  let out = '', i = 0;
  while (i < text.length) {
    const c = text[i];
    if (c === "'" || c === '"') { // a quoted text or name, as it is
      let j = i + 1;
      for (; j < text.length; j++) if (text[j] === c) { if (text[j + 1] === c) j++; else break; }
      out += text.slice(i, j + 1); i = j + 1; continue;
    }
    if (c === '-' && text[i + 1] === '-') { const j = text.indexOf('\n', i); out += text.slice(i, j < 0 ? text.length : j); i = j < 0 ? text.length : j; continue; }
    if (c === '/' && text[i + 1] === '*') { const j = text.indexOf('*/', i + 2); const e = j < 0 ? text.length : j + 2; out += text.slice(i, e); i = e; continue; }
    if (c === '$' && /\d/.test(text[i + 1] ?? '') && !/[\w$]/.test(text[i - 1] ?? '')) {
      let j = i + 1;
      while (/\d/.test(text[j] ?? '')) j++;
      const k = Number(text.slice(i + 1, j));
      if (k < 1 || k > literals.length) throw new PgError('08P01', `there is no parameter $${k}`);
      out += literals[k - 1]; i = j; continue;
    }
    out += c; i++;
  }
  return out;
}

// An error for the client, with its SQLSTATE code.
export class PgError extends Error { constructor(code, message) { super(message); this.code = code; } }

// ---- writing many rows: one growing buffer per batch, no Buffer per value -------------
const dayCache = new Map();
function fastTimestamp(ms) { // timestampText, with each day's text made once
  if (!(ms > -6e13 && ms < 2.5e14)) return timestampText(ms); // (years before 100 or after 9999)
  const day = Math.floor(ms / 86_400_000);
  let d = dayCache.get(day);
  if (d === undefined) { if (dayCache.size > 100_000) dayCache.clear(); d = dayText(day * 86_400_000); dayCache.set(day, d); }
  let t = ms - day * 86_400_000;
  const h = Math.floor(t / 3_600_000); t -= h * 3_600_000;
  const m = Math.floor(t / 60_000); t -= m * 60_000;
  const sec = Math.floor(t / 1000), f = t - sec * 1000;
  const frac = f ? `.${String(Math.round(f * 1000)).padStart(6, '0').replace(/0+$/, '')}` : '';
  return `${d} ${p2(h)}:${p2(m)}:${p2(sec)}${frac}`;
}

// DataRow messages for rows [from, to): cell(i, k) values, column types and formats
export function dataRows(columns, formats, cell, from, to, keepEmpty) {
  const K = columns.length;
  let buf = Buffer.allocUnsafe(Math.max(1024, (to - from) * (7 + K * 12))), at = 0;
  const need = (n) => { if (at + n > buf.length) { const b = Buffer.allocUnsafe(Math.max(buf.length * 2, at + n)); buf.copy(b, 0, 0, at); buf = b; } };
  const kinds = columns.map((c, k) => (formats[k] === 1 ? `b:${c.type}` : c.type === 'timestamp' ? 't:ts' : 't'));
  for (let i = from; i < to; i++) {
    need(7);
    const start = at;
    buf[at] = 0x44; at += 5; // 'D', length later
    buf.writeInt16BE(K, at); at += 2;
    for (let k = 0; k < K; k++) {
      const v = cell(i, k), kind = kinds[k];
      if (v === null || v === undefined || (typeof v === 'number' && v !== v)) { need(4); buf.writeInt32BE(-1, at); at += 4; continue; }
      if (kind === 'b:int8' || kind === 'b:timestamp' || kind === 'b:timestamptz') {
        const x = kind === 'b:int8' ? Math.round(v) : Math.round((v - EPOCH_2000) * 1000);
        need(12); buf.writeInt32BE(8, at); const hi = Math.floor(x / 4294967296); buf.writeInt32BE(hi, at + 4); buf.writeUInt32BE(x - hi * 4294967296, at + 8); at += 12; continue;
      }
      if (kind === 'b:float8') { need(12); buf.writeInt32BE(8, at); buf.writeDoubleBE(v, at + 4); at += 12; continue; }
      if (kind === 'b:date') { need(8); buf.writeInt32BE(4, at); buf.writeInt32BE(Math.floor((v - EPOCH_2000) / 86_400_000), at + 4); at += 8; continue; }
      let bytes = null, text = null;
      if (kind[0] === 'b') bytes = toPgBinary(columns[k].type, v, keepEmpty);
      else text = kind === 't:ts' && typeof v === 'number' ? (Number.isFinite(v) ? fastTimestamp(v) : null) : toPgText(columns[k].type, v, keepEmpty);
      if (bytes === null && text === null) { need(4); buf.writeInt32BE(-1, at); at += 4; continue; }
      if (bytes) { need(4 + bytes.length); buf.writeInt32BE(bytes.length, at); bytes.copy(buf, at + 4); at += 4 + bytes.length; continue; }
      const n = Buffer.byteLength(text);
      need(4 + n); buf.writeInt32BE(n, at); buf.write(text, at + 4, 'utf8'); at += 4 + n;
    }
    buf.writeInt32BE(at - start - 1, start + 1);
  }
  return buf.subarray(0, at);
}
