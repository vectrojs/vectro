// Zone maps: the min and max of each block of rows, per column. A filter
// such as `ts >= start && price > 900` can't match a block whose max ts is
// below `start`, so a query only needs the rows from the first block that
// can match to the last one. On time-ordered data (logs, orders, streams),
// "the last hour" becomes a small slice of the table.
//
// Exact by construction: only `column <op> number` parts of a top-level
// `&&` chain are used (number columns and text codes; never free text), and
// the rows left out provably fail the filter.
import { parse } from './fuse.mjs';

export const ZONE_ROWS = 16_384;
const FLIP = { '<': '>', '<=': '>=', '>': '<', '>=': '<=', '==': '==' };

// Min and max per block, skipping NaN (a block of only NaN has min Infinity).
export function computeZones(data, n, B = ZONE_ROWS) {
  const blocks = Math.ceil(n / B);
  const mins = new Float64Array(blocks).fill(Infinity), maxs = new Float64Array(blocks).fill(-Infinity);
  for (let b = 0; b < blocks; b++) {
    let lo = Infinity, hi = -Infinity;
    for (let i = b * B, end = Math.min(n, i + B); i < end; i++) {
      const v = data[i];
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    }
    mins[b] = lo; maxs[b] = hi;
  }
  return { block: B, shift: 0, mins, maxs };
}

// `column <op> value` conditions every matching row must meet.
export function conditions(pred, cols, params) {
  const out = [];
  const value = (m) => {
    if (m.t === 'num') return m.v;
    if (m.t === 'var' && !cols.has(m.name) && typeof params[m.name] === 'number') return params[m.name];
    if (m.t === 'un' && m.op === '-') { const v = value(m.x); return v === undefined ? undefined : -v; }
    return undefined;
  };
  const column = (m) => (m.t === 'var' && cols.has(m.name) && !cols.get(m.name).strings ? m.name : null);
  (function visit(node) {
    if (node.t === 'bin' && node.op === '&&') { visit(node.l); visit(node.r); return; }
    if (node.t !== 'bin' || !FLIP[node.op]) return;
    let col = column(node.l), v = value(node.r), op = node.op;
    if (!col) { col = column(node.r); v = value(node.l); op = FLIP[op]; }
    if (col && typeof v === 'number' && !Number.isNaN(v)) out.push({ col, op, v });
  })(parse(pred));
  return out;
}

export const mayMatch = (op, lo, hi, v) =>
  op === '>' ? hi > v : op === '>=' ? hi >= v : op === '<' ? lo < v : op === '<=' ? lo <= v : lo <= v && v <= hi;

// The rows [lo, hi) that can match `pred`, or null when zone maps can't
// narrow it usefully. zonesOf(name) gives a column's zones (computed lazily).
// round: how the compiled filter rounds numbers (Math.fround for float32
// tables), so the zone check compares exactly like the filter will.
export function zoneRange(pred, cols, params, n, zonesOf, round = (v) => v) {
  const conds = conditions(pred, cols, params).map((c) => ({ ...c, v: round(c.v) }));
  if (!conds.length) return null;
  let first = Infinity, last = -Infinity, zones = null;
  const maps = conds.map((c) => ({ ...c, z: zonesOf(c.col) }));
  zones = maps[0].z;
  const { block: B, shift } = zones;
  const blocks = zones.mins.length;
  for (let b = 0; b < blocks; b++) {
    let ok = true;
    for (const { op, v, z } of maps) if (!mayMatch(op, z.mins[b], z.maxs[b], v)) { ok = false; break; }
    if (ok) { if (b < first) first = b; last = b; }
  }
  if (first === Infinity) return [0, 0];
  const lo = Math.max(0, first * B - shift), hi = Math.min(n, (last + 1) * B - shift);
  if (hi - lo > 0.9 * n) return null; // not worth a narrower run
  return [lo, Math.max(lo, hi)];
}
