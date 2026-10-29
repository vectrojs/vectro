// Query results as bytes for HTTP clients: JSON, CSV, JSON lines and Arrow,
// written a slice of rows at a time (a large result never becomes one string).
//   columns: [{ name, type: 'number' | 'text' | 'date' }]
//   data: table.toColumns() (numbers as typed arrays, text as { codes, labels } or strings)
// Each cell is text made once per label (a category's labels are few), or a
// number / date written straight away.
import { writeArrow } from '../arrow.mjs';

const SLICE = 65_536;

export const FORMATS = {
  json: 'application/json; charset=utf-8',
  csv: 'text/csv; charset=utf-8',
  ndjson: 'application/x-ndjson; charset=utf-8',
  arrow: 'application/vnd.apache.arrow.stream',
};

export const columnsOf = (table) => [...table.cols].map(([name, c]) => ({ name, type: c.dict || c.strings ? 'text' : c.date ? 'date' : 'number' }));

// RFC 4180: quoted when it holds a comma, a quote or a line break
const csvText = (v) => (/[",\r\n]/.test(v) ? `"${v.replaceAll('"', '""')}"` : v);

// ISO 8601 in UTC, as Date.toISOString writes it, with each day's part made once
const days = new Map();
const p2 = (x) => (x < 10 ? `0${x}` : `${x}`);
function iso(ms) {
  const day = Math.floor(ms / 86_400_000);
  let d = days.get(day);
  if (d === undefined) { if (days.size > 100_000) days.clear(); const x = new Date(day * 86_400_000).toISOString(); d = x.slice(0, x.indexOf('T') + 1); days.set(day, d); }
  let t = ms - day * 86_400_000;
  const h = Math.floor(t / 3_600_000); t -= h * 3_600_000;
  const m = Math.floor(t / 60_000); t -= m * 60_000;
  const s = Math.floor(t / 1000), f = Math.floor(t - s * 1000);
  return `${d}${p2(h)}:${p2(m)}:${p2(s)}.${f < 10 ? `00${f}` : f < 100 ? `0${f}` : f}Z`;
}
const dateOK = (v) => v === v && v > -8.64e15 && v < 8.64e15;

// cell(i) for one column, as text for the format; null cells as `missing`
function cells(col, d, format) {
  const missing = format === 'csv' ? '' : 'null';
  const quote = format === 'csv' ? csvText : JSON.stringify;
  if (d && d.labels) {
    const text = d.labels.map((s) => (s === '' ? missing : quote(s)));
    const codes = d.codes;
    return (i) => text[codes[i]] ?? missing;
  }
  if (col.type === 'text') return (i) => (d[i] === '' ? missing : quote(d[i]));
  if (col.type === 'date') return format === 'csv' ? (i) => (dateOK(d[i]) ? iso(d[i]) : missing) : (i) => (dateOK(d[i]) ? `"${iso(d[i])}"` : missing);
  return (i) => { const v = d[i]; return v === v && v !== Infinity && v !== -Infinity ? `${v}` : missing; };
}

// The body as chunks (strings or bytes), for res.write one after another.
export function* encode(format, columns, data, n, extra = {}) {
  if (format === 'arrow') {
    yield writeArrow(data, { dates: columns.filter((c) => c.type === 'date').map((c) => c.name) });
    return;
  }
  const cell = columns.map((c) => cells(c, data[c.name], format));
  const K = cell.length;
  const rows = (a, b, start, sep, end, keys) => {
    let s = '';
    for (let i = a; i < b; i++) {
      s += start;
      for (let k = 0; k < K; k++) s += (k ? sep : '') + (keys ? keys[k] : '') + cell[k](i);
      s += end;
    }
    return s;
  };
  if (format === 'csv') {
    yield `${columns.map((c) => csvText(c.name)).join(',')}\r\n`;
    for (let a = 0; a < n; a += SLICE) yield rows(a, Math.min(n, a + SLICE), '', ',', '\r\n', null);
    return;
  }
  if (format === 'ndjson') {
    const keys = columns.map((c) => `${JSON.stringify(c.name)}:`);
    for (let a = 0; a < n; a += SLICE) yield rows(a, Math.min(n, a + SLICE), '{', ',', '}\n', keys);
    return;
  }
  // json: { columns, rows: [[...]...], ... }
  yield `{"columns":${JSON.stringify(columns)},"rows":[`;
  for (let a = 0; a < n; a += SLICE) {
    const s = rows(a, Math.min(n, a + SLICE), '[', ',', '],', null);
    yield a + SLICE >= n ? s.slice(0, -1) : s; // (no comma after the last row)
  }
  yield Object.keys(extra).length ? `],${JSON.stringify(extra).slice(1)}` : ']}';
}
