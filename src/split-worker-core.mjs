// Worker side of split tables. Each worker has its own single-thread engine
// and owns slices of tables (no shared memory). The page sends requests
// { rid, op, args } and gets back { rid, ok, value | error }.
import { Engine } from './engine.mjs';
import { Table } from './table.mjs';
import { parse } from './fuse.mjs';
import { createStreams } from './stream-worker.mjs';
import { parseKeys } from './sort.mjs';
import { searchTable } from './vectors.mjs';
import { decodeOne } from './pack.mjs';
import { SKETCH_SIZE, sketchAdd, hashNumber, hashText } from './sketch.mjs';

export function serveSplitWorker(post, listen) {
  let engine;
  const tables = new Map(); // table id -> Table (this worker's slice)
  const streams = createStreams(() => engine); // live streams (see stream-worker.mjs)

  const table = (id) => {
    const t = tables.get(id) ?? streams.view(id);
    if (!t) throw new Error(`vectro: no table ${id} in this worker`);
    return t;
  };
  // A filtered view: the page keeps filters as text and sends them with each query.
  const view = (id, preds = []) => preds.reduce((t, p) => t.filter(p.expr, p.params), table(id));
  const dictsOf = (t) => Object.fromEntries([...t.cols].filter(([, c]) => c.dict).map(([name, c]) => [name, c.dict]));

  // A worker's rows come in segments (blocks of the original table, in this
  // worker's order); the page needs rows per segment to put rows from all
  // workers back in table order. With filters, the compiled count(filter)
  // runs on each segment's rows. With `limit`, counting stops once that many
  // rows matched: later segments can't be among the first `limit` rows.
  function segmentCounts(id, preds, segs, limit = Infinity) {
    if (!preds?.length) return Float64Array.from(segs);
    const v = view(id, preds), t = table(id);
    const used = new Set();
    (function walk(node) {
      if (node.t === 'var') used.add(node.name);
      for (const k of ['x', 'y', 'c', 'l', 'r']) if (node[k]) walk(node[k]);
      if (node.args) node.args.forEach(walk);
    })(parse(v.pred));
    const columns = [...used].filter((name) => t.cols.has(name)).map((name) => [name, t.cols.get(name)]);
    const scalars = Object.fromEntries([...used].filter((name) => !t.cols.has(name)).map((name) => [name, v.params[name]]));
    const src = `count(${v.pred})`;
    const out = [];
    let r = 0, total = 0;
    for (let k = 0; k < segs.length && total < limit; k++) {
      const vars = { ...scalars };
      for (const [name, c] of columns) { // (memory may have grown: live views)
        const part = engine.live(c.data).subarray(r, r + segs[k]);
        vars[name] = c.enc ? { data: part, enc: c.enc, n: segs[k], type: t.type === Float32Array ? 'f32' : 'f64' } : part;
      }
      const c = segs[k] ? engine.eval(src, vars) : 0;
      out.push(c);
      total += c;
      r += segs[k];
    }
    return Float64Array.from(out);
  }

  const handlers = {
    init({ engineOptions }) {
      engine = new Engine(engineOptions);
      return { jit: engine.jit };
    },
    // A slice of columns. Text columns get this worker's own dictionary; the
    // page merges them and sends `remap` so every worker uses the same codes.
    load({ id, columns }) {
      const t = engine.table(columns);
      tables.set(id, t);
      return { n: t.length, dicts: dictsOf(t), names: t.columnNames };
    },
    // Columns already encoded by the page: numbers, plus text as codes into a
    // shared dictionary (so nothing needs renumbering afterwards).
    loadEncoded({ id, order, numbers, text, float32 = false, dates = [] }) {
      const T = float32 ? Float32Array : Float64Array; // one number type per table
      const base = Object.keys(numbers).length ? engine.table(numbers, { type: T }) : null;
      const cols = new Map();
      for (const name of order) {
        if (text[name]) {
          const { codes, dict } = text[name];
          const data = engine.alloc(T, codes.length);
          data.set(codes);
          cols.set(name, { data, dict, codes: new Map(dict.map((v, i) => [v, i])), strings: null });
        } else {
          cols.set(name, base.cols.get(name));
        }
      }
      for (const c of cols.values()) c.data = engine.live(c.data); // later allocations may have moved memory
      for (const name of dates) cols.get(name).date = true;
      const n = base ? base.length : (Object.values(text)[0]?.codes.length ?? 0);
      const t = new Table(engine, cols, n, { owned: [...order] });
      tables.set(id, t);
      return { n: t.length, names: t.columnNames };
    },
    // CSV: a header plus this worker's pieces of the file, back to back.
    // `pieces` are their byte lengths; returns the rows in each piece.
    loadCSV({ id, bytes, opts, pieces }) {
      let segs = null;
      const t = engine.parseCSV(bytes, { ...opts, pieces, onPieceRows: (rows) => { segs = rows; } });
      tables.set(id, t);
      return { n: t.length, segs, dicts: dictsOf(t), names: t.columnNames, strings: [...t.cols].filter(([, c]) => c.strings).map(([n]) => n), dates: [...t.cols].filter(([, c]) => c.date).map(([n]) => n) };
    },
    // Switch text columns to the shared dictionary: code -> lut[code].
    remap({ id, maps }) {
      const t = table(id);
      let repack = false;
      for (const [name, { lut, dict }] of Object.entries(maps)) {
        const c = t.cols.get(name);
        if (c.enc) { // packed codes: unpack, renumber, pack again below
          const plain = engine.alloc(c.plainType, t.length), codes = engine.live(c.data);
          for (let i = 0; i < t.length; i++) plain[i] = decodeOne(c.enc, codes[i], c.plainType === Float32Array);
          engine.free(c.data);
          c.data = plain;
          delete c.enc; delete c.plainType; delete c.rows;
          repack = true;
        }
        const data = engine.live(c.data);
        for (let i = 0; i < t.length; i++) data[i] = lut[data[i]];
        c.dict = dict;
        c.codes = new Map(dict.map((s, i) => [s, i]));
      }
      if (repack) t.compress();
      return true;
    },
    agg({ id, preds, spec, params }) {
      return view(id, preds).agg(spec, params);
    },
    // Partial group results, as columns: one per key (labels for text keys)
    // + one array per aggregate.
    groupBy({ id, preds, keys, spec, params }) {
      const g = view(id, preds).groupBy(keys).agg(spec, params);
      try {
        const keyCols = keys.map((key) => {
          const k = g.cols.get(key);
          const codes = engine.live(k.data);
          return k.dict ? Array.from(codes, (c) => k.dict[c]) : Float64Array.from(codes);
        });
        const cols = {};
        for (const out of Object.keys(spec)) cols[out] = Float64Array.from(engine.live(g.cols.get(out).data));
        return { keyCols, cols };
      } finally {
        g.dispose();
      }
    },
    collect({ id, preds, newId, segs }) {
      const counts = segmentCounts(id, preds, segs);
      const t = view(id, preds).collect();
      tables.set(newId, t);
      return { n: t.length, segs: counts };
    },
    derive({ id, newId, name, expr, params }) {
      const t = table(id).derive(name, expr, params);
      tables.set(newId, t);
      return t.length;
    },
    // This worker's first k rows by `by`, with each expression key's values
    // (text keys are compared by label on the page).
    top({ id, preds, k, by, columns = null }) {
      const v = view(id, preds);
      const t = (columns ? v.select(columns) : v).top(k, by);
      try {
        const keys = parseKeys(by, t.cols);
        const values = keys.map((key) => {
          if (key.column) return null;
          const d = t.derive('__np_k', key.text);
          try { return Float64Array.from(engine.live(d.cols.get('__np_k').data)); } finally { d.dispose(); }
        });
        return { rows: t.toRows(), keys: keys.map((key) => (key.column ? { label: key.column, desc: key.desc } : null)), values };
      } finally {
        t.dispose();
      }
    },
    // Vector search over this worker's rows (see vectors.mjs).
    vectorTop({ id, preds, query, opts }) {
      return searchTable(view(id, preds), query, opts);
    },
    // Distinct (group..., value) pairs of this worker's rows, or with sketch
    // one distinct-count sketch per group (the page merges them).
    distinct({ id, preds, keys, arg, params, sketch }) {
      const v = view(id, preds);
      const plain = /^[A-Za-z_]\w*$/.test(arg.trim()) && v.cols.has(arg.trim()) && !keys.includes(arg.trim()); // (a key: count a copy)
      const t = plain ? v : v.derive('__np_dv', arg, params);
      const name = plain ? arg.trim() : '__np_dv';
      const g = t.groupBy([...keys, name]).agg({ __np_n: 'count()' });
      try {
        const colOf = (k) => { const c = g.cols.get(k), d = engine.live(c.data); return c.dict ? Array.from(d, (x) => c.dict[x]) : Float64Array.from(d); };
        const keyCols = keys.map(colOf), values = colOf(name);
        if (!sketch) return { keyCols, values };
        const index = new Map(), groupCols = keys.map(() => []), regs = [];
        for (let r = 0; r < values.length; r++) {
          const tuple = keyCols.map((c) => c[r]);
          const id = tuple.map((x) => (typeof x === 'string' ? JSON.stringify(x) : String(x))).join(',');
          let at = index.get(id);
          if (at === undefined) { at = index.size; index.set(id, at); tuple.forEach((x, k) => groupCols[k].push(x)); regs.push(new Uint8Array(SKETCH_SIZE)); }
          const x = values[r];
          if (typeof x === 'string') sketchAdd(regs[at], 0, hashText(x));
          else if (x === x) sketchAdd(regs[at], 0, hashNumber(x));
        }
        const sketches = new Uint8Array(regs.length * SKETCH_SIZE);
        regs.forEach((r, i) => sketches.set(r, i * SKETCH_SIZE));
        return { keyCols: groupCols, sketches };
      } finally {
        g.dispose();
        if (!plain) t.dispose();
      }
    },
    columns({ id, preds, segs }) {
      return { columns: view(id, preds).toColumns(), segs: segmentCounts(id, preds, segs) };
    },
    toRows({ id, preds, limit, segs }) {
      return { rows: view(id, preds).toRows(limit), segs: segmentCounts(id, preds, segs, limit ?? Infinity) };
    },
    ...streams.handlers,
    streamSnapshot: (args) => streams.handlers.streamSnapshot({ ...args, tables }),
    dispose({ id }) {
      if (streams.close(id)) return true;
      tables.get(id)?.dispose();
      tables.delete(id);
      return true;
    },
  };

  listen(async ({ rid, op, args }) => {
    try {
      const value = await handlers[op](args);
      post({ rid, ok: true, value }, transferList(value));
    } catch (err) {
      post({ rid, ok: false, error: (globalThis.process?.env?.VECTRO_DEBUG ? err?.stack : err?.message) ?? String(err) });
    }
  });
}

// Typed arrays in a group result move to the page without copying.
function transferList(value) {
  const out = [];
  if (value && typeof value === 'object') {
    if (ArrayBuffer.isView(value.keys)) out.push(value.keys.buffer);
    if (value.keyCols) for (const k of value.keyCols) if (ArrayBuffer.isView(k)) out.push(k.buffer);
    if (ArrayBuffer.isView(value.sketches)) out.push(value.sketches.buffer);
    if (ArrayBuffer.isView(value.values)) out.push(value.values.buffer);
    if (value.cols) for (const c of Object.values(value.cols)) out.push(c.buffer);
    if (ArrayBuffer.isView(value.segs)) out.push(value.segs.buffer);
    if (value.columns) for (const c of Object.values(value.columns)) if (ArrayBuffer.isView(c) || ArrayBuffer.isView(c.codes)) out.push((c.codes ?? c).buffer);
    if (Array.isArray(value.values)) for (const v of value.values) if (ArrayBuffer.isView(v)) out.push(v.buffer);
  }
  return out;
}
