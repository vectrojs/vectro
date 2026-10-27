// Columnar tables on top of the engine. Each column is one array in engine
// memory; text columns are dictionary-encoded (each distinct string gets a
// number code), so `region == 'EU'` becomes a numeric comparison.
//
// Filters are lazy: t.filter(p).sum(e) compiles to one fused pass computing
// sum(p ? e : 0). No filtered copy is made unless you ask with collect().

import { parseKeys, labelRanks, sortIndex } from './sort.mjs';
import { ZONE_ROWS, computeZones, zoneRange } from './zones.mjs';
import { parse, toSource, walk, analyze, isMask } from './fuse.mjs';
import { generateGroupBy, generateCompact, generateHashGroups } from './table-kernels.mjs';
import { decodeString } from './csv-kernel.mjs';
import { maxUnits } from './kernels.mjs';
import { sortPermG, gatherG } from './parallel-sort.mjs';
import { joinTablesG, joinViewG } from './join.mjs';
import { Query } from './plan.mjs';
import { bounds, windowValues } from './window.mjs';
import { copyRows } from './rows.mjs';
import { textmapRows } from './textmap.mjs';
import { writeArrow } from './arrow.mjs';
import { interpretGroup, interpretCompact, interpretFilteredReduce } from './interp.mjs';
import { DATE_FNS, DATE_RESULT, parseDateText, toMillis } from './dates.mjs';
import { chooseEncoding, encodingCandidates, packInto, decodeOne, KINDS } from './pack.mjs';

const IDENT = /^[A-Za-z_]\w*$/;
const AGG_FNS = new Set(['sum', 'mean', 'min', 'max', 'count', 'countDistinct', 'approxDistinct']);
const DISTINCT = new Set(['countDistinct', 'approxDistinct']); // exact on a table (see #distinctCountsG)
const IDENTITY = { sum: 0, count: 0, mean: 0, min: Infinity, max: -Infinity };
const MAX_GROUP_SLOTS = 2 ** 28; // threads * aggregations * groups (2GB of float64)
const PARALLEL_SORT_ROWS = 50_000; // smaller tables sort on the calling thread

export class Table {
  // cols: Map name -> { data, dict, codes, strings }
  //   category text: dict (code -> string) and codes (string -> code)
  //   free text:     strings = bytes the values point into (see csv-kernel)
  constructor(engine, cols, n, { pred = null, params = {}, owned = [], ownedBuffers = [], zones = true } = {}) {
    this.engine = engine;
    this.cols = cols;
    this.n = n;
    this.pred = pred;       // filter, as bound expression source (or null)
    this.params = params;   // scalar values the filter refers to
    this.owned = owned;     // columns this table allocated (freed by dispose)
    this.ownedBuffers = ownedBuffers; // e.g. CSV bytes that free-text columns point into
    this.zones = zones;     // may zone maps narrow filtered queries (see zones.mjs)
  }

  // With a filter, the rows that can match it according to the zone maps
  // (min and max per block, kept on each column and computed on first use),
  // as a table over just those rows; otherwise this table.
  #narrow() {
    if (!this.pred || !this.zones || this.n < 4 * ZONE_ROWS) return this;
    const zonesOf = (name) => {
      const c = this.cols.get(name);
      if (c.zones) return c.zones;
      if (!c.enc) return (c.zones = computeZones(this.engine.live(c.data), this.n));
      const codes = this.engine.live(c.data), plain = new Float64Array(this.n); // packed: zones of the values
      for (let i = 0; i < this.n; i++) plain[i] = decodeOne(c.enc, codes[i], this.type === Float32Array);
      return (c.zones = computeZones(plain, this.n));
    };
    const range = zoneRange(this.pred, this.cols, this.params, this.n, zonesOf, this.type === Float32Array ? Math.fround : undefined);
    if (!range) return this;
    const [lo, hi] = range;
    this.engine._zoneSkips = (this.engine._zoneSkips ?? 0) + (this.n - (hi - lo)); // rows skipped (tests, benchmarks)
    const cols = new Map();
    for (const [name, c] of this.cols) cols.set(name, { ...c, data: this.engine.live(c.data).subarray(lo, hi), zones: undefined });
    return new Table(this.engine, cols, hi - lo, { pred: this.pred, params: this.params, zones: false });
  }

  // ---- building tables --------------------------------------------------------

  // dates: names of columns holding dates as ms or ISO text (arrays of Date
  // objects are dates anyway).
  static fromColumns(engine, columns, { type = Float64Array, dates = [], compress = false } = {}) {
    for (const [name, values] of Object.entries(columns)) {
      if (dates.includes(name) || isDates(values)) columns = { ...columns, [name]: { dates: Float64Array.from(values, toMillis) } };
    }
    const entries = Object.entries(columns);
    if (!entries.length) throw new TypeError('vectro: a table needs at least one column');
    const len = (v) => (isCoded(v) ? v.codes.length : v.dates ? v.dates.length : v.length);
    const n = len(entries[0][1]);
    const cols = new Map();
    for (const [name, values] of entries) {
      checkName(name);
      if (len(values) !== n) throw new RangeError(`vectro: column "${name}" has ${len(values)} rows, expected ${n}`);
      cols.set(name, values.dates ? { data: copyNumbers(engine, type, values.dates), dict: null, codes: null, date: true }
        : isCoded(values) ? codedText(engine, type, values) : isText(values) ? encodeText(engine, type, values) : { data: copyNumbers(engine, type, values), dict: null, codes: null });
    }
    const t = new Table(engine, cols, n, { owned: [...cols.keys()] });
    return compress ? t.compress() : t;
  }

  // Convert an array of objects. Column types come from the first row.
  static fromRows(engine, rows, { type = Float64Array } = {}) {
    if (!rows.length) throw new TypeError('vectro: no rows');
    const n = rows.length;
    const cols = new Map();
    for (const name of Object.keys(rows[0])) {
      checkName(name);
      const data = engine.alloc(type, n);
      if (typeof rows[0][name] === 'string') {
        const codes = new Map(), dict = [];
        for (let i = 0; i < n; i++) {
          const s = rows[i][name];
          let c = codes.get(s);
          if (c === undefined) { c = dict.length; dict.push(s); codes.set(s, c); }
          data[i] = c;
        }
        cols.set(name, { data, dict, codes });
      } else {
        for (let i = 0; i < n; i++) data[i] = rows[i][name];
        cols.set(name, { data, dict: null, codes: null });
      }
    }
    return new Table(engine, cols, n, { owned: [...cols.keys()] });
  }

  // ---- shape ----------------------------------------------------------------------

  get length() { return this.n; }            // physical rows (ignores the filter)
  get columnNames() { return [...this.cols.keys()]; }
  get type() { const c = this.cols.values().next().value; return c.enc ? c.plainType : c.data.constructor; }

  column(name) {
    const c = this.cols.get(name);
    if (!c) throw new Error(`vectro: no column "${name}"`);
    return this.engine.live(c.data);
  }

  // ---- expressions ----------------------------------------------------------------

  // Parse an expression, swap text literals for dictionary codes, and check
  // every name is a column or a param. Returns { ast, src }.
  #bind(src, params) {
    const ast = rewriteText(parse(src), this.cols, src);
    walk(ast, (m) => {
      if (m.t !== 'var') return;
      if (!this.cols.has(m.name) && !(m.name in params)) {
        throw new Error(`vectro: unknown column "${m.name}" in "${src}" (columns: ${this.columnNames.join(', ')})`);
      }
      if (this.cols.get(m.name)?.strings) {
        throw new Error(`vectro: "${m.name}" is a free-text column; it can be returned but not used in expressions yet (load it as types: { ${m.name}: 'category' } to filter or group on it)`);
      }
    });
    return { ast, src: toSource(ast) };
  }

  #vars(params) {
    const vars = { ...params };
    const t = this.type === Float32Array ? 'f32' : 'f64';
    for (const [name, c] of this.cols) vars[name] = c.enc ? { data: c.data, enc: c.enc, n: this.n, type: t } : c.data; // packed: the kernels decode it
    return vars;
  }

  // ---- compressed columns (see pack.mjs) ---------------------------------------------
  // Store each number and category column in 1, 2 or 4 bytes per row where
  // every value decodes back exactly; others stay as they are. In place: the
  // table (and its views) keep working, with less memory. Returns the table.
  compress() { return this.engine._drive(this.#compressG()); }
  compressAsync() { return this.engine._driveAsync(this.#compressG()); }

  *#compressG() {
    const { engine } = this;
    const f32 = this.type === Float32Array;
    for (const c of new Set(this.cols.values())) {
      if (c.enc || c.strings) continue;
      const values = engine.live(c.data);
      const n = values.length;
      const enc = engine.jit ? yield* this.#chooseEncodingG(c.data, n, f32) : chooseEncoding(values, n, f32);
      if (!enc) continue;
      const plainType = values.constructor, old = c.data;
      c.data = engine.jit ? yield* this.#packG(old, n, enc) : packInto(engine, old, n, enc);
      c.enc = enc;
      c.plainType = plainType;
      c.rows = n;
      engine.free(old);
    }
    for (const c of this.cols.values()) c.data = engine.live(c.data);
    return this;
  }

  // On every thread: statistics in one pass, then each candidate checked
  // value by value with the kernels' own arithmetic (count of values that
  // wouldn't come back exactly).
  *#chooseEncodingG(data, n, f32) {
    const { engine } = this;
    if (!n) return null;
    const [min, max, frac, nan, inf, negZero] = yield* engine.evalG([
      'min(v == v ? v : Infinity)', 'max(v == v ? v : -Infinity)', 'count(v == v && v != floor(v))',
      'count(v != v)', 'count(v == Infinity || v == -Infinity)', 'count(v == 0 && 1 / v < 0)'], { v: data });
    const cands = encodingCandidates({ min, max, whole: frac === 0, nan: nan > 0, inf: inf > 0, negZero: negZero > 0 }, f32);
    for (const enc of cands) {
      const back = enc.div ? `floor(v * ${enc.div} + 0.5) / ${enc.div}` : `floor((v - base) / ${enc.mul} + 0.5) * ${enc.mul} + base`;
      const bad = yield* engine.evalG(`count(v == v && ${back} != v)`, { v: data, base: enc.base });
      if (bad === 0) return enc;
    }
    return null;
  }

  // The codes, computed on every thread, in a new packed array.
  *#packG(data, n, enc) {
    const { engine } = this;
    const k = KINDS[enc.kind];
    const codes = engine.alloc(this.type, n);
    try {
      const code = enc.div ? `floor(v * ${enc.div} + 0.5)` : `floor((v - base) / ${enc.mul} + 0.5)`;
      yield* engine.evalG(`v == v ? ${code} : ${k.nan}`, { v: data, base: enc.base }, codes);
      const out = engine.alloc(k.Type, n + 16 / k.bytes);
      out.set(engine.live(codes).subarray(0, n));
      return out;
    } finally { engine.free(codes); }
  }

  // Bytes the columns take now, and without packing.
  memoryBytes() {
    let now = 0, plain = 0;
    const bpe = this.type.BYTES_PER_ELEMENT;
    for (const c of new Set(this.cols.values())) {
      const rows = c.enc ? c.rows : c.data.length;
      now += c.enc ? c.data.BYTES_PER_ELEMENT * rows : c.data.byteLength;
      plain += rows * bpe;
    }
    return { bytes: now, uncompressed: plain };
  }

  #hasPacked() { for (const c of this.cols.values()) if (c.enc) return true; return false; }

  // The same rows with packed columns decoded (new arrays, owned): all of
  // them, or those in `names` (the others stay packed).
  *#decodedG(names = null) {
    const { engine } = this;
    const t = this.type === Float32Array ? 'f32' : 'f64';
    const cols = new Map(), owned = [];
    for (const [name, c] of this.cols) {
      if (!c.enc || (names && !names.has(name))) { cols.set(name, c); continue; }
      const out = engine.alloc(c.plainType, this.n);
      if (engine.jit) yield* engine.evalG('x', { x: { data: c.data, enc: c.enc, n: this.n, type: t } }, out);
      else { const codes = engine.live(c.data), o = engine.live(out); for (let i = 0; i < this.n; i++) o[i] = decodeOne(c.enc, codes[i], t === 'f32'); }
      const { enc, plainType, rows, ...rest } = c;
      cols.set(name, { ...rest, data: out });
      owned.push(name);
    }
    for (const c of cols.values()) c.data = engine.live(c.data);
    return new Table(engine, cols, this.n, { pred: this.pred, params: this.params, owned, zones: this.zones });
  }

  // collect() for a table with packed columns: the filter as 1 / 0 (fused,
  // reads packed columns), the matching row numbers (compacted), then every
  // column gathered (packed columns stay packed).
  *#collectPackedG() {
    const { engine, n } = this;
    const T = this.type, t = T === Float32Array ? 'f32' : 'f64';
    const predAst = parse(this.pred);
    const arrayNames = [];
    walk(predAst, (m) => { if (m.t === 'var' && this.cols.has(m.name) && !arrayNames.includes(m.name)) arrayNames.push(m.name); });
    if (!arrayNames.length) return yield* this.#viaPlainG((x) => x.#collectG()); // (a filter on no column)
    // the matching row numbers, in one fused pass that reads packed columns as they are
    const { parts, units } = yield* engine.compile(`count(${this.pred})`).runPartsG(this.#vars(this.params));
    const offsets = engine.alloc(Float64Array, units);
    let m = 0;
    for (let u = 0; u < units; u++) { offsets[u] = m; m += parts[u]; }
    const scalarNames = [];
    walk(predAst, (x) => { if (x.t === 'var' && !this.cols.has(x.name) && !scalarNames.includes(x.name)) scalarNames.push(x.name); });
    const encs = arrayNames.map((nm) => this.cols.get(nm).enc ?? null);
    const cacheKey = `compactrows|${t}|${this.pred}|${arrayNames}|${scalarNames}|${JSON.stringify(encs)}`;
    engine._tableKernels ??= new Map();
    let v = engine._tableKernels.get(cacheKey);
    if (!v) engine._tableKernels.set(cacheKey, (v = engine._register(generateCompact(predAst, t, arrayNames, [], scalarNames, engine.isShared, encs, true))));
    let perm;
    try {
      perm = engine.alloc(Uint32Array, Math.max(1, m));
      const first = engine.live(this.cols.get(arrayNames[0]).data);
      yield* engine._compactG(v, n, arrayNames.map((nm) => this.cols.get(nm).data), [perm], offsets, [...scalarNames.map((nm) => this.params[nm]), first.byteOffset], units);
      // every column gathered by those rows (packed columns stay packed)
      const out = new Map();
      for (const [name, c] of this.cols) out.set(name, { ...c, data: yield* gatherG(engine, c.data, perm, m, t, c.enc), zones: undefined, ...(c.enc ? { rows: m } : {}) });
      for (const c of out.values()) c.data = engine.live(c.data);
      return new Table(engine, out, m, { owned: [...out.keys()] });
    } finally { engine.free(offsets); if (perm) engine.free(perm); }
  }

  // Run op on the decoded table (packed columns read as plain numbers).
  *#viaPlainG(op, names = null) {
    const t = yield* this.#decodedG(names);
    let out;
    try { out = yield* op(t); return out; } finally { if (out !== t) t.dispose(); }
  }

  #mergeParams(params) {
    params = Object.fromEntries(Object.entries(params).map(([k, v]) => [k, v instanceof Date ? v.getTime() : v])); // dates as ms
    for (const k of Object.keys(params)) {
      if (this.cols.has(k)) throw new Error(`vectro: param "${k}" has the same name as a column`);
      if (k in this.params && this.params[k] !== params[k]) throw new Error(`vectro: param "${k}" is already set to ${this.params[k]}`);
    }
    return { ...this.params, ...params };
  }

  // ---- operations -------------------------------------------------------------------

  // Lazy filter: returns a view of the same columns with a condition attached.
  filter(expr, params = {}) {
    const merged = this.#mergeParams(params);
    const { src } = this.#bind(expr, merged);
    const pred = this.pred ? `(${this.pred}) && (${src})` : src;
    return new Table(this.engine, this.cols, this.n, { pred, params: merged, zones: this.zones });
  }

  // New column computed for every row, in one fused pass.
  derive(name, expr, params = {}) { return this.engine._drive(this.#deriveG(name, expr, params)); }
  deriveAsync(name, expr, params = {}) { return this.engine._driveAsync(this.#deriveG(name, expr, params)); }

  *#deriveG(name, expr, params) {
    checkName(name);
    if (this.cols.has(name)) throw new Error(`vectro: column "${name}" already exists`);
    const merged = this.#mergeParams(params);
    const { src } = this.#bind(expr, merged);
    const out = this.engine.alloc(this.type, this.n);
    yield* this.engine.evalG(src, this.#vars(merged), out);
    const top = parse(expr);
    const date = (top.t === 'call' && DATE_RESULT.has(top.fn)) || (top.t === 'var' && !!this.cols.get(top.name)?.date); // dayStart(ts), monthStart(ts)...: still dates
    const same = top.t === 'var' ? this.cols.get(top.name) : null; // a copy of a text column stays text
    const cols = new Map(this.cols).set(name, { data: out, dict: same?.dict ?? null, codes: same?.dict ? same.codes : null, date });
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params, owned: [name] });
  }

  // Several aggregations in one pass: agg({ revenue: 'sum(price * qty)', n: 'count()' })
  agg(spec, params = {}) { return this.engine._drive(this.#aggG(spec, params)); }
  aggAsync(spec, params = {}) { return this.engine._driveAsync(this.#aggG(spec, params)); }

  *#aggG(spec, params) {
    if (!this.engine.jit && this.#hasPacked()) return yield* this.#viaPlainG((t) => t.#aggG(spec, params)); // (the interpreter reads plain numbers)
    const narrow = this.#narrow();
    if (narrow !== this) return yield* narrow.#aggG(spec, params);
    const merged = this.#mergeParams(params);
    const P = this.pred;
    const all = Object.entries(spec).map(([out, src]) => ({ out, ...this.#aggItem(src, merged) }));
    const distinct = all.filter((it) => DISTINCT.has(it.fn));
    if (distinct.length) { // distinct values of the matching rows
      const t = yield* this.#collectG();
      try {
        for (const it of distinct) it.value = (yield* t.#distinctCountsG(it.arg, merged, null, 1))[0];
      } finally {
        if (t !== this) t.dispose();
      }
    }
    const items = all.filter((it) => !DISTINCT.has(it.fn));
    const done = (values) => Object.fromEntries(all.map((it) => [it.out, it.value !== undefined ? it.value : values[it.out]]));
    if (P && !this.engine.jit) {
      // interpreter: evaluate the filter once and aggregate only matching rows
      const truthy = (ast) => (isMask(ast) ? ast : { t: 'bin', op: '!=', l: ast, r: { t: 'num', v: 0 } });
      const work = items.flatMap((it) => (it.fn === 'mean' ? [{ reduce: 'sum', ast: parse(it.arg) }, { reduce: 'count', ast: truthy(parse(it.present)) }]
        : it.fn === 'min' || it.fn === 'max' ? [{ reduce: it.fn, ast: parse(it.arg) }, { reduce: 'count', ast: truthy(parse(it.present)) }]
        : [{ reduce: it.fn, ast: it.arg === null ? null : it.fn === 'count' ? truthy(parse(it.arg)) : parse(it.arg) }]));
      const t = this.type === Float32Array ? 'f32' : 'f64';
      const values = interpretFilteredReduce(this.engine, t, parse(P), work, this.#vars(merged), merged, this.n);
      let j = 0;
      const out = {};
      for (const it of items) {
        if (it.fn === 'mean') out[it.out] = values[j++] / values[j++];
        else if (it.fn === 'min' || it.fn === 'max') { const v = values[j++]; out[it.out] = values[j++] ? v : NaN; }
        else out[it.out] = values[j++];
      }
      return done(out);
    }
    const reductions = [];
    const slot = (s) => reductions.push(s) - 1;
    for (const it of items) {
      const e = it.arg;
      if (it.fn === 'count' && !P && !e) it.value = this.n; // plain row count: nothing to compute
      else if (it.fn === 'count') it.slot = slot(P ? `count((${P}) && (${e ?? 1}))` : `count(${e ?? 1})`);
      else if (it.fn === 'sum') it.slot = slot(P ? `sum((${P}) ? (${e}) : 0)` : `sum(${e})`);
      else if (it.fn === 'min' || it.fn === 'max') { // NaN if no value is present
        it.slot = slot(P ? `${it.fn}((${P}) ? (${e}) : ${it.fn === 'min' ? '' : '-'}Infinity)` : `${it.fn}(${e})`);
        it.presentSlot = slot(P ? `count((${P}) && (${it.present}))` : `count(${it.present})`);
      }
      else { // mean: over the values present
        it.slot = slot(P ? `sum((${P}) ? (${e}) : 0)` : `sum(${e})`);
        it.countSlot = slot(P ? `count((${P}) && (${it.present}))` : `count(${it.present})`);
      }
    }
    const r = []; // a kernel takes at most 8 reductions: run them in batches
    for (let i = 0; i < reductions.length; i += 8) r.push(...[(yield* this.engine.evalG(reductions.slice(i, i + 8), this.#vars(merged)))].flat());
    const value = (it) => (it.value !== undefined ? it.value : it.presentSlot !== undefined ? (r[it.presentSlot] ? r[it.slot] : NaN) : it.countSlot === undefined ? r[it.slot] : r[it.slot] / r[it.countSlot]);
    return done(Object.fromEntries(items.map((it) => [it.out, value(it)])));
  }

  sum(expr, params) { return this.agg({ v: `sum(${expr})` }, params).v; }
  mean(expr, params) { return this.agg({ v: `mean(${expr})` }, params).v; }
  min(expr, params) { return this.agg({ v: `min(${expr})` }, params).v; }
  max(expr, params) { return this.agg({ v: `max(${expr})` }, params).v; }
  count(expr, params) { return this.agg({ v: expr ? `count(${expr})` : 'count()' }, params).v; }

  async sumAsync(expr, params) { return (await this.aggAsync({ v: `sum(${expr})` }, params)).v; }
  async meanAsync(expr, params) { return (await this.aggAsync({ v: `mean(${expr})` }, params)).v; }
  async minAsync(expr, params) { return (await this.aggAsync({ v: `min(${expr})` }, params)).v; }
  async maxAsync(expr, params) { return (await this.aggAsync({ v: `max(${expr})` }, params)).v; }
  async countAsync(expr, params) { return (await this.aggAsync({ v: expr ? `count(${expr})` : 'count()' }, params)).v; }

  // 'sum(price * qty)' -> { fn: 'sum', arg: '(price * qty)' } with text bound to codes.
  #aggItem(src, params) {
    const ast = parse(src);
    if (ast.t !== 'call' || !AGG_FNS.has(ast.fn) || ast.args.length > 1 || (ast.fn !== 'count' && ast.args.length !== 1)) {
      throw new SyntaxError(`vectro: an aggregation must look like sum(expr), mean(expr), min(expr), max(expr), count([expr]) or countDistinct(expr), got "${src}"`);
    }
    if (!ast.args.length) return { fn: ast.fn, arg: null };
    if (!DISTINCT.has(ast.fn)) return missingRules(ast.fn, this.#bind(toSource(ast.args[0]), params).src);
    if (DISTINCT.has(ast.fn) && ast.args[0].t === 'var' && this.cols.get(ast.args[0].name)?.strings) {
      throw new Error(`vectro: ${ast.fn} can't count a free-text column ("${ast.args[0].name}"); load it as a category`);
    }
    return { fn: ast.fn, arg: this.#bind(toSource(ast.args[0]), params).src };
  }

  // Group by one key or several: groupBy('region'), groupBy(['region', 'day']).
  // Keys can be category columns or any numbers (IDs, prices, NaN groups).
  // Groups come out sorted by key: text in dictionary order, numbers
  // ascending (NaN last), earlier keys first.
  groupBy(key) {
    const keys = Array.isArray(key) ? key : [key];
    if (!keys.length) throw new Error('vectro: groupBy needs at least one key column');
    if (new Set(keys).size !== keys.length) throw new Error(`vectro: groupBy got the same key twice (${keys.join(', ')})`);
    for (const k of keys) {
      const c = this.cols.get(k);
      if (!c) throw new Error(`vectro: no column "${k}"`);
      if (c.strings) throw new Error(`vectro: can't group by free-text column "${k}" (load it as types: { ${k}: 'category' })`);
    }
    return {
      agg: (spec, params = {}) => this.engine._drive(this.#groupAggG(keys, spec, params)),
      aggAsync: (spec, params = {}) => this.engine._driveAsync(this.#groupAggG(keys, spec, params)),
    };
  }

  *#groupAggG(keys, spec, params) {
    if (this.#hasPacked()) {
      const used = new Set(keys);
      for (const src of [...Object.values(spec), this.pred ?? '1']) walk(parse(src), (m) => { if (m.t === 'var' && this.cols.has(m.name)) used.add(m.name); });
      if (!this.engine.jit) { // the interpreter reads plain numbers
        if ([...used].some((n) => this.cols.get(n).enc)) return yield* this.#viaPlainG((t) => t.#groupAggG(keys, spec, params), used);
      } else if (keys.some((k) => this.cols.get(k).enc)) { // the group kernel reads packed columns; the hash path needs plain keys
        const direct = yield* this.#directGroups(keys, Object.keys(spec).length + 1);
        if (direct?.owned) this.engine.free(direct.data);
        if (!direct) return yield* this.#viaPlainG((t) => t.#groupAggG(keys, spec, params), new Set(keys.filter((k) => this.cols.get(k).enc)));
      }
    }
    const narrow = this.#narrow();
    if (narrow !== this) return yield* narrow.#groupAggG(keys, spec, params);
    const merged = this.#mergeParams(params);
    const items = Object.entries(spec).map(([out, src]) => ({ out, ...this.#aggItem(src, merged) }));
    for (const it of items) if (keys.includes(it.out)) throw new Error(`vectro: aggregation "${it.out}" has the same name as a key`);
    if (this.pred && items.some((it) => DISTINCT.has(it.fn))) { // distinct counts need the matching rows themselves
      const t = yield* this.#collectG();
      try { return yield* t.#groupAggG(keys, spec, params); } finally { t.dispose(); }
    }
    const direct = yield* this.#directGroups(keys, items.length + 1);
    if (direct) {
      try { return yield* this.#runGroupsG(keys, direct, items, merged); } finally { if (direct.owned) this.engine.free(direct.data); }
    }
    // Hash path: keep the matching rows, number their distinct keys 0..K-1
    // (in order of first appearance), then group by that number.
    const t = yield* this.#collectG();
    const ids = this.engine.jit && t.n >= 4096 ? yield* t.#hashGroupsParallelG(keys) : t.#hashGroups(keys);
    try {
      return yield* t.#runGroupsG(keys, ids, items, merged);
    } finally {
      this.engine.free(ids.data);
      for (const x of ids.extra ?? []) this.engine.free(x);
      if (t !== this) t.dispose();
    }
  }

  // Keys small enough to be group numbers as they are: a text column's codes
  // or whole numbers 0..2^24, several combined as k1 * size2 + k2..., while
  // the accumulators stay small (or no larger than the rows). Else null.
  *#directGroups(keys, J) {
    const { engine } = this;
    const sizes = [], offsets = [];
    for (const key of keys) {
      const c = this.cols.get(key);
      if (c.dict) { sizes.push(c.dict.length); offsets.push(0); continue; }
      if (!this.n) { sizes.push(0); offsets.push(0); continue; }
      // whole numbers in a range under 2^24 (from their minimum: 202301..202412, IDs from 1e9, negatives)
      const k = c.enc ? { data: c.data, enc: c.enc, n: this.n, type: this.type === Float32Array ? 'f32' : 'f64' } : c.data;
      const [lo, hi, frac] = yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k });
      if (frac > 0 || !(hi - lo < 2 ** 24) || !Number.isFinite(lo)) return null;
      sizes.push(hi - lo + 1);
      offsets.push(lo);
    }
    const K = sizes.reduce((a, b) => a * b, 1);
    const C = engine.jit && K <= 1024 ? 4 : 1;
    if (K > 2 ** 24 || engine.T * J * K * C > (engine._maxGroupSlots ?? MAX_GROUP_SLOTS) || (K > 65_536 && K > 2 * this.n)) return null;
    const below = sizes.map((_, k) => sizes.slice(k + 1).reduce((a, b) => a * b, 1));
    const keyColumn = (k, keep) => { // key k of each kept group (codes for text)
      const out = new Float64Array(keep.length), d = below[k], size = sizes[k], o = offsets[k];
      for (let r = 0; r < keep.length; r++) out[r] = (Math.floor(keep[r] / d) % size) + o;
      return out;
    };
    if (keys.length === 1 && offsets[0] === 0) return { K, data: this.cols.get(keys[0]).data, enc: this.cols.get(keys[0]).enc ?? null, keyColumn, owned: false, sorted: true };
    const term = (k) => (offsets[k] ? `(${keys[k]} - ${offsets[k] < 0 ? `(${offsets[k]})` : offsets[k]})` : keys[k]);
    let expr = term(0);
    for (let k = 1; k < keys.length; k++) expr = `(${expr}) * ${sizes[k]} + ${term(k)}`;
    const data = engine.alloc(this.type, this.n);
    yield* engine.evalG(expr, this.#vars({}), data);
    return { K, data, keyColumn, owned: true, sorted: true };
  }

  // Number each row's distinct key (tuple) with a hash table: { K, data
  // (group number per row, in engine memory), values(g), sorted: false }.
  // Each group's key values are kept side by side (no trips back to its first
  // row), and keys are hashed on their bits (-0 as 0, every NaN alike).
  #hashGroups(keys) {
    const { engine, n } = this;
    if (this.type === Float32Array && n > 2 ** 24) throw new RangeError('vectro: groupBy on a float32 table is limited to 16,777,216 rows on the hash path');
    const nk = keys.length, f32 = this.type === Float32Array, W = f32 ? 1 : 2; // 32-bit words per value
    const data = engine.alloc(this.type, n);
    const cols = keys.map((k) => engine.live(this.cols.get(k).data));
    const words = cols.map((c) => new Uint32Array(c.buffer, c.byteOffset, c.length * W));
    const ids = engine.live(data);
    let cap = 1024, slots = new Int32Array(cap).fill(-1), hashes = new Int32Array(cap);
    let kv = new Float64Array(1024 * nk); // group g's keys: kv[g * nk + k]
    let K = 0;
    for (let i = 0; i < n; i++) {
      let h = 0x811c9dc5;
      for (let k = 0; k < nk; k++) {
        const v = cols[k][i];
        if (v === v && v !== 0) {
          h = Math.imul(h ^ words[k][i * W], 0x01000193);
          if (W === 2) h = Math.imul(h ^ words[k][i * 2 + 1], 0x01000193);
        } else h = Math.imul(h ^ (v === 0 ? 0 : 0x7ff8), 0x01000193); // 0 and -0; NaN
      }
      h ^= h >>> 15; h = Math.imul(h, 0x2c1b3c6d); h ^= h >>> 12;
      const mask = cap - 1;
      let slot = h & mask, g = slots[slot];
      while (g !== -1) {
        if (hashes[slot] === h) {
          let k = 0;
          for (; k < nk; k++) { const a = kv[g * nk + k], b = cols[k][i]; if (a !== b && (a === a || b === b)) break; }
          if (k === nk) break;
        }
        slot = (slot + 1) & mask;
        g = slots[slot];
      }
      if (g === -1) {
        g = K++;
        if ((g + 1) * nk > kv.length) { const bigger = new Float64Array(kv.length * 2); bigger.set(kv); kv = bigger; }
        for (let k = 0; k < nk; k++) { const v = cols[k][i]; kv[g * nk + k] = v === 0 ? 0 : v; }
        slots[slot] = g;
        hashes[slot] = h;
        if (K * 2 > cap) { // grow: rehash every group
          cap *= 2;
          const s2 = new Int32Array(cap).fill(-1), h2 = new Int32Array(cap);
          for (let x = 0; x < slots.length; x++) {
            if (slots[x] === -1) continue;
            let y = hashes[x] & (cap - 1);
            while (s2[y] !== -1) y = (y + 1) & (cap - 1);
            s2[y] = slots[x]; h2[y] = hashes[x];
          }
          slots = s2; hashes = h2;
        }
      }
      ids[i] = g;
    }
    const keyColumn = (k, keep) => { // key k of each kept group (codes for text)
      const out = new Float64Array(keep.length);
      for (let r = 0; r < keep.length; r++) out[r] = kv[keep[r] * nk + k];
      return out;
    };
    return { K, data, keyColumn, owned: true, sorted: false };
  }

  // The same numbering on every thread at once (see generateHashGroups): one
  // shared table, sized from an estimate of the number of groups (Chao1 on a
  // sample of rows); if more groups turn up, again with a bigger table.
  // Group numbers follow first arrival, not row order (any order is fine:
  // the groups are sorted by key afterwards).
  *#hashGroupsParallelG(keys) {
    const { engine, n } = this;
    const nk = keys.length, t = this.type === Float32Array ? 'f32' : 'f64';
    const cols = keys.map((k) => this.cols.get(k).data);
    let limit = Math.min(n, engine._hashLimit ?? this.#estimateGroups(cols)); // (_hashLimit: tests)
    if (t === 'f32') limit = Math.min(limit, 2 ** 24);
    const cacheKey = `hashgroups|${t}|${nk}`;
    engine._tableKernels ??= new Map();
    let v = engine._tableKernels.get(cacheKey);
    if (!v) { v = engine._register(generateHashGroups(t, nk, engine.isShared)); engine._tableKernels.set(cacheKey, v); }
    const slack = 64 * maxUnits(Math.max(1, engine.T)); // group numbers reserved but unused: < 64 per unit
    for (;;) {
      const room = limit + slack;
      let cap = 1024;
      while (cap < room * 2) cap *= 2;
      const slots = engine.alloc(Int32Array, cap), keyBits = engine.alloc(Float64Array, room * nk), counter = engine.alloc(Int32Array, 4);
      const data = engine.alloc(this.type, n);
      engine.live(slots).fill(-1);
      engine.live(counter)[0] = 0;
      let overflow, K;
      try {
        const { parts } = yield* engine._fusedG(v, n, [...cols, data], [engine.live(slots).byteOffset, cap - 1, engine.live(keyBits).byteOffset, room, engine.live(counter).byteOffset], 1);
        K = engine.live(counter)[0];
        overflow = parts.some((x) => x !== 0); // (K may pass limit by numbers reserved but unused)
      } catch (err) { engine.free(data); engine.free(keyBits); throw err; } finally { engine.free(slots); engine.free(counter); }
      if (overflow) {
        engine.free(keyBits); engine.free(data);
        if (limit >= n || (t === 'f32' && limit >= 2 ** 24)) throw new Error('vectro: groupBy hash table overflowed'); // can't happen: at most n groups
        limit = Math.min(n, limit * 4);
        continue;
      }
      const keyColumn = (k, keep) => { // key k of each kept group (codes for text)
        const kv = engine.live(keyBits), out = new Float64Array(keep.length);
        for (let r = 0; r < keep.length; r++) out[r] = kv[keep[r] * nk + k];
        return out;
      };
      return { K, data, keyColumn, owned: true, sorted: false, extra: [keyBits] };
    }
  }

  // Estimated number of distinct keys (tuples), from up to 65,536 sampled
  // rows: Chao1 = seen + once^2 / (2 * twice); plenty of room is cheap.
  #estimateGroups(cols) {
    const live = cols.map((c) => this.engine.live(c));
    const m = Math.min(this.n, 65_536), step = this.n / m;
    const counts = new Map();
    for (let j = 0; j < m; j++) {
      const i = Math.floor(j * step);
      const key = live.length === 1 ? live[0][i] : live.map((c) => c[i]).join(',');
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    let once = 0, twice = 0;
    for (const c of counts.values()) { if (c === 1) once++; else if (c === 2) twice++; }
    const est = counts.size + (twice ? (once * once) / (2 * twice) : once * (this.n / m));
    return Math.ceil(est * 1.5) + 1024;
  }

  // Run the group kernel over group numbers `groups.data` (0..K-1).
  *#runGroupsG(keys, groups, allItems, merged) {
    const { engine } = this;
    const { K } = groups;
    const items = allItems.filter((it) => !DISTINCT.has(it.fn));
    // means need the count of values present: one more kernel item each
    const kernelItems = [...items, ...items.filter((it) => it.present).map((it) => ({ fn: 'count', arg: it.present, of: it }))]; // means, mins, maxes
    const totals = yield* this.#groupTotalsG(groups, kernelItems, merged);
    const presentOf = (it) => totals[1 + kernelItems.findIndex((k) => k.of === it)];
    // Keep groups that have rows, sorted by key (codes for text keys, so
    // dictionary order; NaN last), and build whole columns.
    const counts = totals[0];
    let m = 0;
    for (let g = 0; g < K; g++) if (counts[g] > 0) m++;
    let keep = new Float64Array(m);
    for (let g = 0, r = 0; g < K; g++) if (counts[g] > 0) keep[r++] = g;
    let codes = keys.map((_, k) => groups.keyColumn(k, keep));
    if (!groups.sorted) {
      let idx;
      if (engine.jit && m >= PARALLEL_SORT_ROWS) { // many groups: sort them on every thread
        const arrays = codes.map((a) => { const x = engine.alloc(Float64Array, m); engine.live(x).set(a); return x; });
        try {
          const perm = yield* sortPermG(engine, arrays, m, 'f64');
          idx = engine.live(perm).slice();
          engine.free(perm);
        } finally {
          for (const x of arrays) engine.free(x);
        }
      } else idx = sortIndex(codes, m);
      const order = new Float64Array(m);
      for (let r = 0; r < m; r++) order[r] = keep[idx[r]];
      keep = order;
      codes = codes.map((a) => { const out = new Float64Array(m); for (let r = 0; r < m; r++) out[r] = a[idx[r]]; return out; });
    }
    const columns = {};
    keys.forEach((key, k) => {
      const dict = this.cols.get(key).dict;
      columns[key] = dict ? { codes: new Uint32Array(codes[k]), labels: dict } : codes[k];
    });
    for (const it of allItems) { // distinct counts per group: from (group, value) pairs
      if (!DISTINCT.has(it.fn)) continue;
      let gData = groups.data;
      if (groups.enc) { // a packed key: group numbers as plain numbers
        gData = engine.alloc(this.type, this.n);
        yield* engine.evalG('x', { x: { data: groups.data, enc: groups.enc, n: this.n, type: this.type === Float32Array ? 'f32' : 'f64' } }, gData);
      }
      try { it.perGroup = yield* this.#distinctCountsG(it.arg, merged, gData, K); } finally { if (gData !== groups.data) engine.free(gData); }
    }
    allItems.forEach((it) => {
      const i = items.indexOf(it);
      const v = i >= 0 ? totals[i + 1] : it.perGroup, out = new Float64Array(m);
      if (it.fn === 'mean') { const p = presentOf(it); for (let r = 0; r < m; r++) out[r] = v[keep[r]] / p[keep[r]]; }
      else if (it.fn === 'min' || it.fn === 'max') { const p = presentOf(it); for (let r = 0; r < m; r++) out[r] = p[keep[r]] ? v[keep[r]] : NaN; }
      else for (let r = 0; r < m; r++) out[r] = v[keep[r]];
      columns[it.out] = out;
    });
    return Table.fromColumns(engine, columns, { type: Float64Array, dates: keys.filter((k) => this.cols.get(k).date) });
  }

  // (datasets, see dataset.mjs) Add the rows that match this table's filter
  // into running per-group totals: state.acc holds J * K float64s for the J
  // reductions in `sources` ('count(1)', 'sum(x)', 'min(x)'... at most 8,
  // missing-value rules already applied), for K groups numbered 0..K-1 by
  // the expression `group`. Each thread adds up its own range of groups;
  // packed columns are read as they are.
  _groupInto(group, K, sources, state) { return this.engine._drive(this.#groupIntoG(group, K, sources, state)); }
  _groupIntoAsync(group, K, sources, state) { return this.engine._driveAsync(this.#groupIntoG(group, K, sources, state)); }

  *#groupIntoG(group, K, sources, state) {
    const { engine } = this;
    const merged = this.params;
    const bound = sources.map((src) => this.#bind(src, merged).src);
    const analysis = analyze(bound);
    if (analysis.items.length !== bound.length || bound.length > 8) throw new Error('vectro: internal: up to 8 different reductions per pass');
    const J = bound.length;
    if (!state.acc) {
      state.acc = engine.alloc(Float64Array, J * K);
      analysis.items.forEach((it, j) => engine.live(state.acc).fill(IDENTITY[it.reduce], j * K, (j + 1) * K));
    }
    if (!this.n) return;
    const gData = engine.alloc(this.type, this.n);
    try {
      yield* engine.evalG(this.#bind(group, merged).src, this.#vars(merged), gData);
      const predAst = this.pred ? parse(this.pred) : null;
      const names = [...analysis.vars];
      if (predAst) walk(predAst, (m) => { if (m.t === 'var' && !names.includes(m.name)) names.push(m.name); });
      const arrayNames = names.filter((nm) => this.cols.has(nm)), scalarNames = names.filter((nm) => !this.cols.has(nm));
      const t = this.type === Float32Array ? 'f32' : 'f64';
      const encs = arrayNames.map((nm) => this.cols.get(nm).enc ?? null);
      const cacheKey = `grouprange|${t}|${bound.join(';')}|${this.pred}|${arrayNames}|${scalarNames}|${JSON.stringify([encs, null])}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, 1, engine.isShared, true, encs, null));
        engine._tableKernels.set(cacheKey, v);
      }
      const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), gData];
      yield* engine._groupRangeG(v, this.n, arrays, scalarNames.map((nm) => merged[nm]), engine.live(state.acc).byteOffset, K);
    } finally {
      engine.free(gData);
    }
  }

  // Per group number (0..K-1): [row counts, then one array per item]. A
  // kernel takes at most 8 reductions (the row count is one): more items run
  // in several passes.
  *#groupTotalsG(groups, items, merged) {
    if (items.length > 7) {
      const totals = [];
      for (let i = 0; i < items.length; i += 7) {
        const part = yield* this.#groupTotalsG(groups, items.slice(i, i + 7), merged);
        totals.push(...(i ? part.slice(1) : part));
      }
      return totals;
    }
    const { engine } = this;
    const { K } = groups;
    const sources = ['count(1)', ...items.map((it) => `${it.fn === 'mean' ? 'sum' : it.fn}(${it.arg ?? 1})`)];
    const analysis = analyze(sources);
    const predAst = this.pred ? parse(this.pred) : null;
    const names = [...analysis.vars];
    if (predAst) walk(predAst, (m) => { if (m.t === 'var' && !names.includes(m.name)) names.push(m.name); });
    const arrayNames = names.filter((nm) => this.cols.has(nm));
    const scalarNames = names.filter((nm) => !this.cols.has(nm));
    const t = this.type === Float32Array ? 'f32' : 'f64';

    const J = analysis.items.length;
    const T = engine.T;
    // Few groups: 4 rotating copies per group (see generateGroupBy). The
    // interpreter's precompiled scatter kernels use one slot per group.
    const C = engine.jit && K <= 1024 ? 4 : 1;
    let totals;
    // Per-thread accumulators pay off while they stay small; past that (or
    // past the memory cap) one thread adds up rows that were already
    // filtered (the hash path collects them first).
    const slotsNeeded = T * J * K * C;
    const big = T > 1 && slotsNeeded > (engine._bigGroupSlots ?? 2 ** 23);
    // packed columns (and a packed key) are decoded by the kernel as it reads them
    const encs = arrayNames.map((nm) => this.cols.get(nm).enc ?? null), keyEnc = groups.enc ?? null;
    const encKey = JSON.stringify([encs, keyEnc]);
    if (big && engine.jit && J * K <= (engine._maxGroupSlots ?? MAX_GROUP_SLOTS)) {
      // many groups: each thread adds up its own range of groups, all rows
      const acc = engine.alloc(Float64Array, J * K);
      analysis.items.forEach((it, j) => engine.live(acc).fill(IDENTITY[it.reduce], j * K, (j + 1) * K));
      const cacheKey = `grouprange|${t}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}|${encKey}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, 1, engine.isShared, true, encs, keyEnc));
        engine._tableKernels.set(cacheKey, v);
      }
      const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), groups.data];
      yield* engine._groupRangeG(v, this.n, arrays, scalarNames.map((nm) => merged[nm]), engine.live(acc).byteOffset, K);
      const a = engine.live(acc);
      totals = analysis.items.map((_, j) => a.slice(j * K, (j + 1) * K));
      engine.free(acc);
    } else if (slotsNeeded > (engine._maxGroupSlots ?? MAX_GROUP_SLOTS) || (!this.pred && big)) {
      let keyData = groups.data;
      if (keyEnc) { // (the scatter reads group numbers as plain numbers)
        keyData = engine.alloc(this.type, this.n);
        const codes = engine.live(groups.data), o = engine.live(keyData);
        for (let i = 0; i < this.n; i++) o[i] = decodeOne(keyEnc, codes[i], t === 'f32');
      }
      try { totals = yield* this.#scatterG(keyData, K, analysis.items, sources, merged); } finally { if (keyData !== groups.data) engine.free(keyData); } // too many groups for per-thread accumulators
    } else {
      // Accumulators: one block per thread, padded to 256 bytes so two threads
      // never update the same cache line. Every slot starts at its identity.
      const block = Math.ceil((J * K * C) / 32) * 32; // in float64s
      const acc = engine.alloc(Float64Array, Math.max(1, T * block));
      const slots = (s, j) => [s * block + j * K * C, s * block + (j + 1) * K * C];
      analysis.items.forEach((it, j) => {
        for (let s = 0; s < T; s++) acc.fill(IDENTITY[it.reduce], ...slots(s, j));
      });
      if (engine.jit) {
        const cacheKey = `group|${t}|${C}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}|${encKey}`;
        engine._tableKernels ??= new Map();
        let v = engine._tableKernels.get(cacheKey);
        if (!v) {
          v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, C, engine.isShared, false, encs, keyEnc));
          engine._tableKernels.set(cacheKey, v);
        }
        const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), groups.data];
        yield* engine._groupG(v, this.n, arrays, scalarNames.map((nm) => merged[nm]), acc.byteOffset, block * 8, K);
      } else {
        // the calling thread fills thread 0's block
        const cols = Object.fromEntries(arrayNames.map((nm) => [nm, this.cols.get(nm).data]));
        interpretGroup(engine, t, analysis.items, predAst, cols, merged, groups.data, acc, K, this.n);
      }

      // Combine threads (and copies) into one value per group and aggregation.
      totals = [];
      for (const [j, it] of analysis.items.entries()) {
        const op = it.reduce === 'min' ? Math.min : it.reduce === 'max' ? Math.max : (a, b) => a + b;
        if (C > 1 || T === 1 || !engine.jit) { // a plain loop is enough
          const a = engine.live(acc);
          const out = new Float64Array(K).fill(IDENTITY[it.reduce]);
          for (let s = 0; s < T; s++) {
            const [lo] = slots(s, j);
            for (let g = 0; g < K; g++) for (let c = 0; c < C; c++) out[g] = op(out[g], a[lo + g * C + c]);
          }
          totals.push(out);
          continue;
        }
        // many groups: one fused pass over all thread blocks, written into thread 0's
        const vars = {};
        for (let s = 0; s < T; s++) vars[`t${s}`] = acc.subarray(...slots(s, j));
        const vnames = Object.keys(vars);
        const expr = it.reduce === 'min' || it.reduce === 'max'
          ? vnames.slice(1).reduce((e, nm) => `${it.reduce}(${e}, ${nm})`, vnames[0])
          : vnames.join(' + ');
        yield* engine.evalG(expr, vars, vars.t0);
        totals.push(Float64Array.from(engine.live(vars.t0)));
      }
      engine.free(acc);
    }

    return totals;
  }

  // Distinct values of `arg` (bound source) in each group: groupData holds
  // each row's group number (0..K-1), or null for one group. The table must
  // have no filter. The (group, value) pairs are numbered with the hash
  // table (every thread), and each pair adds one to its group. Missing values
  // (NaN) don't count, as in SQL's COUNT(DISTINCT x).
  *#distinctCountsG(arg, merged, groupData, K) {
    if (this.#hasPacked()) return yield* this.#viaPlainG((t) => t.#distinctCountsG(arg, merged, groupData, K));
    const { engine, n } = this;
    const counts = new Float64Array(K);
    if (!n) return counts;
    const plain = /^[A-Za-z_]\w*$/.test(arg.trim()) ? this.cols.get(arg.trim()) : null;
    let own = null;
    if (!plain) { own = engine.alloc(this.type, n); yield* engine.evalG(arg, this.#vars(merged), own); }
    const cols = new Map();
    if (groupData) cols.set('g', { data: groupData, dict: null, codes: null });
    cols.set('v', { data: plain ? plain.data : own, dict: null, codes: null });
    const tmp = new Table(engine, cols, n, { zones: false });
    const keys = groupData ? ['g', 'v'] : ['v'];
    let pairs = null;
    try {
      // small values: pair numbers as they are; others: the hash table
      pairs = (yield* tmp.#directGroups(keys, 1)) ?? (engine.jit && n >= 4096 ? yield* tmp.#hashGroupsParallelG(keys) : tmp.#hashGroups(keys));
      const [rowsPer] = yield* tmp.#groupTotalsG(pairs, [], merged); // which pair numbers have rows (every thread)
      let m = 0;
      for (let p = 0; p < pairs.K; p++) if (rowsPer[p] > 0) m++;
      const keep = new Float64Array(m);
      for (let p = 0, r = 0; p < pairs.K; p++) if (rowsPer[p] > 0) keep[r++] = p;
      const gs = groupData ? pairs.keyColumn(0, keep) : null, vs = pairs.keyColumn(groupData ? 1 : 0, keep);
      for (let r = 0; r < m; r++) if (vs[r] === vs[r]) counts[gs ? gs[r] : 0]++;
      return counts;
    } finally {
      if (pairs?.owned) engine.free(pairs.data);
      for (const x of pairs?.extra ?? []) engine.free(x);
      if (own) engine.free(own);
    }
  }

  // Many groups (more than per-thread accumulators allow): evaluate each
  // aggregation's argument as a column (on all threads), then add up per
  // group on this thread. `groups` has no filter here (rows were collected).
  *#scatterG(groupData, K, kernelItems, sources, merged) {
    const { engine } = this;
    const totals = [];
    for (const [j, it] of kernelItems.entries()) {
      const out = new Float64Array(K).fill(IDENTITY[it.reduce]);
      const ast = parse(sources[j]);
      const arg = ast.args[0];
      let vals = null;
      if (!(arg.t === 'num' && arg.v === 1 && it.reduce === 'count')) {
        const src = it.reduce === 'count' ? `(${toSource(arg)}) ${isMask(arg) ? '' : '!= 0'} ? 1 : 0` : toSource(arg);
        const col = engine.alloc(this.type, this.n);
        yield* engine.evalG(src, this.#vars(merged), col);
        vals = Float64Array.from(engine.live(col));
        engine.free(col);
      }
      const n = this.n, ids = engine.live(groupData); // (after any allocation above)
      if (!vals) for (let i = 0; i < n; i++) out[ids[i]]++;
      else if (it.reduce === 'min') for (let i = 0; i < n; i++) out[ids[i]] = Math.min(out[ids[i]], vals[i]);
      else if (it.reduce === 'max') for (let i = 0; i < n; i++) out[ids[i]] = Math.max(out[ids[i]], vals[i]);
      else for (let i = 0; i < n; i++) out[ids[i]] += vals[i];
      totals.push(out);
    }
    return totals;
  }

  // Materialize the filter: a new table holding only the matching rows.
  // Pass 1 counts matches per unit of work; pass 2 copies every unit's rows
  // straight to its final offset, all columns at once.
  collect() { return this.engine._drive(this.#collectG()); }
  collectAsync() { return this.engine._driveAsync(this.#collectG()); }

  *#collectG() {
    if (this.#hasPacked() && this.pred) return yield* (this.engine.jit ? this.#collectPackedG() : this.#viaPlainG((t) => t.#collectG())); // (gathering packed codes needs compiled kernels)
    const narrow = this.#narrow();
    if (narrow !== this) return yield* narrow.#collectG();
    if (!this.pred) return this;
    const { engine } = this;
    const vars = this.#vars(this.params);
    if (!engine.jit) {
      const names = this.columnNames;
      const dst = interpretCompact(engine, this.type === Float32Array ? 'f32' : 'f64', parse(this.pred), vars, this.params, names, this.n);
      const out = new Map(names.map((nm, k) => [nm, { ...this.cols.get(nm), data: dst[k] }]));
      return new Table(engine, out, dst[0].length, { owned: names });
    }
    const { parts, units } = yield* engine.compile(`count(${this.pred})`).runPartsG(vars);
    const offsets = engine.alloc(Float64Array, units);
    let total = 0;
    for (let u = 0; u < units; u++) { offsets[u] = total; total += parts[u]; }

    const predAst = parse(this.pred);
    const predNames = [];
    walk(predAst, (m) => { if (m.t === 'var' && !predNames.includes(m.name)) predNames.push(m.name); });
    const scalarNames = predNames.filter((nm) => !this.cols.has(nm));
    const scalars = scalarNames.map((nm) => this.params[nm]);
    const t = this.type === Float32Array ? 'f32' : 'f64';

    const out = new Map();
    const names = this.columnNames;
    // A kernel takes at most 32 arrays, so wide tables are copied in batches.
    for (let i = 0; i < names.length; i += 12) {
      const batch = names.slice(i, i + 12);
      const srcNames = [...new Set([...predNames.filter((nm) => this.cols.has(nm)), ...batch])];
      const copyFrom = batch.map((nm) => srcNames.indexOf(nm));
      const cacheKey = `compact|${t}|${this.pred}|${srcNames}|${copyFrom}|${scalarNames}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateCompact(predAst, t, srcNames, copyFrom, scalarNames, engine.isShared));
        engine._tableKernels.set(cacheKey, v);
      }
      const dst = batch.map(() => engine.alloc(this.type, total));
      yield* engine._compactG(v, this.n, srcNames.map((nm) => this.cols.get(nm).data), dst, offsets, scalars, units);
      batch.forEach((nm, k) => out.set(nm, { ...this.cols.get(nm), data: dst[k], zones: undefined })); // (new rows: no zone maps yet)
    }
    engine.free(offsets);
    return new Table(engine, out, total, { owned: names });
  }

  // A view with only some columns (no copy). Columns the filter reads stay
  // available to it.
  select(...names) {
    const keep = new Set(names.flat());
    for (const nm of keep) if (!this.cols.has(nm)) throw new Error(`vectro: no column "${nm}"`);
    if (this.pred) walk(parse(this.pred), (m) => { if (m.t === 'var' && this.cols.has(m.name)) keep.add(m.name); });
    const cols = new Map([...keep].map((nm) => [nm, this.cols.get(nm)])); // in the order asked (then the filter's columns)
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params, zones: this.zones });
  }

  // ---- sorting ---------------------------------------------------------------
  // sort(by): a new table in order of `by` (a column, an expression, or a list;
  // "-" first means large to small; text sorts by label). Stable; NaN last.
  // The order is computed on the calling thread (a radix sort); key
  // expressions are computed on all threads.
  sort(by) { return this.engine._drive(this.#sortG(by)); }
  sortAsync(by) { return this.engine._driveAsync(this.#sortG(by)); }

  // (SQL window functions, see window.mjs) The rows, sorted by the partition
  // keys and then the order keys, with one new column per item:
  //   spec = { partition: [names], order: [{ name, desc }], items: [{ name,
  //     fn, arg (a column name or null), frame, offset, def, k, p }] }
  // A view with columns renamed (map: old name -> new name); a column the new
  // names replace is left out. The view owns nothing (see plan.mjs deriveAllG).
  _renamed(map) {
    const taken = new Set(map.values()), cols = new Map();
    for (const [name, c] of this.cols) {
      if (map.has(name)) cols.set(map.get(name), c);
      else if (!taken.has(name)) cols.set(name, c);
    }
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params, zones: false });
  }

  // (SQL text functions) a new column `name` = fn(values of the input
  // columns), run once per distinct combination of them (see textmap.mjs):
  // text answers make a category column with state's labels ({ dict, codes },
  // shared by every call with that state), else numbers (date: true marks dates).
  _textmap(name, spec) { return this.engine._drive(this.#textmapG(name, spec)); }
  _textmapAsync(name, spec) { return this.engine._driveAsync(this.#textmapG(name, spec)); }

  *#textmapG(name, { inputs = [], fn, text = true, date = false, state = { dict: [], codes: new Map() } }) {
    checkName(name);
    const { engine } = this;
    const t = this.pred ? yield* this.#collectG() : this;
    if (t.cols.has(name)) throw new Error(`vectro: column "${name}" already exists`);
    const ins = inputs.map((k) => t.cols.get(k) ?? (() => { throw new Error(`vectro: no column "${k}"`); })());
    const ranges = [], made = []; // number columns' [min, max, values not whole] (every thread)
    for (let k = 0; k < ins.length; k++) {
      const c = ins[k];
      if (c.dict || c.strings || c.enc || !t.n) { ranges.push(null); continue; } // (packed: read value by value)
      const r = yield* engine.evalG(['min(k)', 'max(k)', 'count(k == k && k != floor(k))'], { k: c.data });
      if (r && !r[2] && r[1] - r[0] < 2 ** 22) { ranges.push(r); continue; }
      // others: numbered by the hash group kernel (every thread), each number's value once
      const one = new Table(engine, new Map([['v', c]]), t.n, { zones: false });
      const ids = engine.jit && t.n >= 4096 ? yield* one.#hashGroupsParallelG(['v']) : one.#hashGroups(['v']);
      for (const x of ids.extra ?? []) engine.free(x);
      made.push(ids.data);
      const vals = ids.keyColumn(0, Array.from({ length: ids.K }, (_, g) => g));
      ins[k] = { data: ids.data, dict: null, numbered: (g) => vals[g] };
      ranges.push([0, Math.max(0, ids.K - 1), 0]);
    }
    const data = engine.alloc(t.type, t.n);
    try { textmapRows(engine, ins, t.n, t.type === Float32Array, fn, text, state, engine.live(data), ranges); } finally { for (const x of made) engine.free(x); }
    const cols = new Map(t.cols), owned = t === this ? [] : [...t.owned];
    if (t !== this) t.owned = [];
    cols.set(name, text ? { data, dict: state.dict, codes: state.codes } : { data, dict: null, codes: null, date: date || undefined });
    owned.push(name);
    for (const c of cols.values()) c.data = engine.live(c.data);
    return new Table(engine, cols, t.n, { owned, ownedBuffers: t === this ? [] : t.ownedBuffers.splice(0), zones: false });
  }

  _window(spec) { return this.engine._drive(this.#windowG(spec)); }
  _windowAsync(spec) { return this.engine._driveAsync(this.#windowG(spec)); }

  *#windowG({ partition = [], order = [], items }) {
    const { engine } = this;
    if (items.length && order.length === 1 && !order[0].desc && items.every((it) => (it.fn === 'percentile_cont' || it.fn === 'percentile_disc') && it.arg === order[0].name) && !this.cols.get(order[0].name)?.dict) return yield* this.#percentilesG(partition, items); // (text: by its sorted labels, below)
    const by = [...partition, ...order.map((o) => `${o.desc ? '-' : ''}${o.name}`)];
    const t = by.length ? yield* this.#sortG(by) : yield* this.#collectG();
    const n = t.n, f32 = t.type === Float32Array;
    const values = (name) => { // a column's values (codes for text), plain
      const c = t.cols.get(name);
      if (!c) throw new Error(`vectro: no column "${name}"`);
      if (c.strings) throw new Error(`vectro: window functions can't read the free-text column "${name}"`);
      const d = engine.live(c.data), out = new Float64Array(n);
      if (c.enc) for (let i = 0; i < n; i++) out[i] = decodeOne(c.enc, d[i], f32); else out.set(d.subarray(0, n));
      return out;
    };
    const b = bounds(n, partition.map(values), order.map((o) => values(o.name)));
    const orderKey = order.length === 1 ? values(order[0].name) : null;
    const cols = new Map(t.cols), owned = t === this ? [] : [...t.owned];
    if (t !== this) t.owned = [];
    const KEEPS_KIND = new Set(['lag', 'lead', 'first_value', 'last_value', 'nth_value', 'min', 'max', 'percentile_disc']);
    for (const it of items) {
      if (cols.has(it.name)) throw new Error(`vectro: column "${it.name}" already exists`);
      const vals = windowValues(n, b, { ...it, arg: it.arg ? values(it.arg) : null }, order.length > 0, orderKey, order[0]?.desc);
      const data = engine.alloc(t.type, n);
      engine.live(data).set(vals);
      const src = it.arg && KEEPS_KIND.has(it.fn) ? t.cols.get(it.arg) : null; // (text stays text, dates stay dates)
      cols.set(it.name, { data, dict: src?.dict ?? null, codes: src?.codes ?? null, date: src?.date || (it.fn === 'percentile_cont' && t.cols.get(it.arg)?.date) || undefined });
      owned.push(it.name);
    }
    for (const c of cols.values()) c.data = engine.live(c.data);
    return new Table(engine, cols, n, { owned, zones: false });
  }

  // Percentiles of each partition, for every row, without sorting the table:
  // rows numbered by partition (as for groupBy), each partition's values
  // gathered (a counting sort by partition number), the ranks needed picked
  // with quickselect. Rows stay in their order.
  *#percentilesG(partition, items) {
    const { engine } = this;
    const t = yield* this.#collectG();
    const n = t.n, f32 = t.type === Float32Array;
    const plainOf = (c, data = c.data) => { const d = engine.live(data), out = new Float64Array(n); if (c.enc) for (let i = 0; i < n; i++) out[i] = decodeOne(c.enc, d[i], f32); else out.set(d.subarray(0, n)); return out; };
    let K = 1, g = null;
    if (partition.length && n) {
      for (const k of partition) if (!t.cols.has(k)) throw new Error(`vectro: no column "${k}"`);
      const ids = (yield* t.#directGroups(partition, 1)) ?? (engine.jit && n >= 4096 ? yield* t.#hashGroupsParallelG(partition) : t.#hashGroups(partition));
      try { K = ids.K; g = ids.enc ? Int32Array.from(plainOf({ enc: ids.enc }, ids.data)) : toInt32(engine.live(ids.data), n); } finally { if (ids.owned) engine.free(ids.data); for (const x of ids.extra ?? []) engine.free(x); }
    }
    const cols = new Map(t.cols), owned = t === this ? [] : [...t.owned];
    if (t !== this) t.owned = [];
    const valuesOf = (arg) => {
      const c = t.cols.get(arg);
      if (!c) throw new Error(`vectro: no column "${arg}"`);
      if (c.strings) throw new Error(`vectro: window functions can't read the free-text column "${arg}"`);
      return !c.enc && !f32 ? engine.live(c.data).subarray(0, n) : plainOf(c); // (read only)
    };
    // the ranks each item needs in each partition, then picked all at once per column
    const byArg = new Map();
    for (const it of items) {
      if (cols.has(it.name)) throw new Error(`vectro: column "${it.name}" already exists`);
      if (!byArg.has(it.arg)) byArg.set(it.arg, []);
      byArg.get(it.arg).push(it);
    }
    const per = new Map();
    for (const [arg, its] of byArg) {
      const got = percentiles(valuesOf(arg), g, K, its);
      its.forEach((it, j) => per.set(it, got[j]));
    }
    for (const it of items) {
      const per1 = per.get(it);
      const data = engine.alloc(t.type, n), d = engine.live(data);
      if (g) spread(d, per1, g, n); else d.fill(per1[0] ?? NaN, 0, n);
      const src = it.fn === 'percentile_disc' ? t.cols.get(it.arg) : null; // (text stays text, dates stay dates)
      cols.set(it.name, { data, dict: src?.dict ?? null, codes: src?.codes ?? null, date: t.cols.get(it.arg)?.date || undefined });
      owned.push(it.name);
    }
    for (const c of cols.values()) c.data = engine.live(c.data);
    return new Table(engine, cols, n, { owned, zones: false });
  }

  // (SQL UNION ALL) this table's rows, then each other's, as a new table;
  // columns by position, named as this table's.
  _concat(others) {
    const names = this.columnNames;
    const parts = [this, ...others].map((t) => {
      const tn = t.columnNames;
      if (tn.length !== names.length) throw new Error(`vectro: UNION needs the same number of columns on each side (${names.length} and ${tn.length})`);
      const kind = (c) => (c.strings ? 'free text' : c.dict ? 'text' : 'number');
      const cols = new Map(names.map((nm, k) => {
        const c = t.cols.get(tn[k]), want = this.cols.get(nm);
        if (kind(c) !== kind(want)) throw new Error(`vectro: UNION column ${k + 1} is ${kind(want)} on one side and ${kind(c)} on another`);
        return [nm, c];
      }));
      const v = new Table(t.engine, cols, t.n, { pred: t.pred, params: t.params });
      return v.pred ? v.collect() : v;
    });
    try { return copyRows(this.engine, parts.map((t) => [t, 0, t.n])); } finally { for (const p of parts) if (p.owned.length) p.dispose(); }
  }
  async _concatAsync(others) { return this._concat(others); }

  // top(k, by): the first k rows of sort(by), e.g. top(10, '-revenue') for
  // the 10 largest, without sorting everything: rows are kept only if their
  // first key is at most a cutoff estimated from a sample (counted and
  // collected on all threads), and only those are sorted.
  top(k, by) { return this.engine._drive(this.#topG(k, by)); }
  topAsync(k, by) { return this.engine._driveAsync(this.#topG(k, by)); }

  // Sort key values for every row of this (unfiltered) table, as JS arrays.
  *#keyArraysG(keys) {
    const { engine } = this;
    const out = [];
    for (const key of keys) {
      if (key.column) {
        const c = this.cols.get(key.column), rank = labelRanks(c.dict, key.desc), codes = engine.live(c.data);
        const out1 = new Float64Array(this.n);
        for (let i = 0; i < this.n; i++) out1[i] = rank[c.enc ? decodeOne(c.enc, codes[i]) : codes[i]];
        out.push(out1);
        continue;
      }
      const d = yield* this.#deriveG('__np_sortkey', key.text, {});
      try { out.push(Float64Array.from(engine.live(d.cols.get('__np_sortkey').data))); } finally { d.dispose(); }
    }
    return out;
  }

  *#sortG(by) {
    const t = yield* this.#collectG();
    try {
      const keys = parseKeys(by, t.cols);
      if (this.engine.jit && t.n >= PARALLEL_SORT_ROWS) return yield* t.#sortParallelG(keys);
      const perm = sortIndex(yield* t.#keyArraysG(keys), t.n);
      return t.#gather(perm, perm.length);
    } finally {
      if (t !== this) t.dispose();
    }
  }

  // Sort on every thread (see parallel-sort.mjs): keys as engine arrays of
  // the table's type (a plain number column is used as it is), then every
  // column gathered in the new order on every thread.
  *#sortParallelG(keys) {
    const { engine, n } = this;
    const t = this.type === Float32Array ? 'f32' : 'f64';
    const made = [], arrays = [];
    try {
      for (const key of keys) {
        const c = key.column ? null : this.cols.get(key.text.trim());
        if (c && !c.dict && !c.strings && !c.enc) { arrays.push(c.data); continue; } // a number column: as it is (packed: decoded below)
        const a = engine.alloc(this.type, n);
        made.push(a);
        arrays.push(a);
        if (key.column) { // text: rank of each label, A to Z (or Z to A)
          const col = this.cols.get(key.column), rank = labelRanks(col.dict, key.desc);
          const codes = engine.live(col.data), out = engine.live(a);
          for (let i = 0; i < n; i++) out[i] = rank[col.enc ? decodeOne(col.enc, codes[i]) : codes[i]];
        } else yield* engine.evalG(this.#bind(key.text, {}).src, this.#vars({}), a);
      }
      const perm = yield* sortPermG(engine, arrays, n, t);
      try {
        const out = new Map();
        for (const [name, c] of this.cols) out.set(name, { ...c, data: yield* gatherG(engine, c.data, perm, n, t, c.enc), zones: undefined, ...(c.enc ? { rows: n } : {}) }); // (packed stays packed)
        for (const c of out.values()) c.data = engine.live(c.data); // later allocations may have moved memory
        return new Table(engine, out, n, { owned: [...out.keys()] });
      } finally {
        engine.free(perm);
      }
    } finally {
      for (const a of made) engine.free(a);
    }
  }

  // A new table with rows perm[0..count) of this one (columns in `omit` left out).
  #gather(perm, count, omit = null) {
    const { engine } = this;
    const out = new Map();
    for (const [name, c] of this.cols) {
      if (omit?.has(name)) continue;
      const data = engine.alloc(c.data.constructor, count + (c.enc ? 16 / c.data.BYTES_PER_ELEMENT : 0)); // (packed: codes copied as they are, room after the end)
      const src = engine.live(c.data);
      for (let i = 0; i < count; i++) data[i] = src[perm[i]];
      out.set(name, { ...c, data, zones: undefined, ...(c.enc ? { rows: count } : {}) }); // (new rows: no zone maps yet)
    }
    for (const c of out.values()) c.data = engine.live(c.data); // later allocations may have moved memory
    return new Table(engine, out, count, { owned: [...out.keys()] });
  }

  *#topG(k, by) {
    const narrow = this.#narrow(); // sample and count only rows that can match the filter
    if (narrow !== this) return yield* narrow.#topG(k, by);
    const { engine } = this;
    k = Math.max(0, Math.floor(k));
    const keys = parseKeys(by, this.cols);
    const n = this.n;
    const sortAll = function* (t) {
      const sorted = yield* t.#sortG(by);
      if (k >= sorted.n) return sorted;
      const head = sorted.#gather(Uint32Array.from({ length: k }, (_, i) => i), k);
      sorted.dispose();
      return head;
    };
    // small tables, k close to n, or a text first key: just sort
    if (keys[0].column || n <= 50_000 || k * 8 >= n) return yield* sortAll(this);

    // A cutoff from an evenly spaced sample: the first key is computed for the
    // sampled rows only. The full passes (count, then collect) are fused
    // filters "first key <= cutoff" on all threads; nothing else is stored.
    const S = Math.min(20_000, Math.max(1_024, Math.ceil(n / 64))), stride = Math.max(1, Math.floor(n / S));
    const picked = Uint32Array.from({ length: Math.ceil(n / stride) }, (_, i) => i * stride);
    // copy only the columns the first key reads (a table can be very wide)
    const used = new Set();
    walk(parse(keys[0].text), (m) => { if (m.t === 'var' && this.cols.has(m.name)) used.add(m.name); });
    const sampleTable = this.#gather(picked, picked.length, new Set(this.columnNames.filter((nm) => !used.has(nm))));
    let sample;
    try {
      const d = yield* sampleTable.#deriveG('__np_topkey', keys[0].text, {});
      try {
        sample = Float64Array.from(engine.live(d.cols.get('__np_topkey').data)).sort(); // native numeric sort; NaN goes last
        let m = sample.length;
        while (m > 0 && sample[m - 1] !== sample[m - 1]) m--;
        sample = sample.subarray(0, m);
      } finally { d.dispose(); }
    } finally {
      sampleTable.dispose();
    }
    const below = (cutoff) => this.filter(`(${keys[0].text}) <= __np_cutoff`, { __np_cutoff: cutoff });
    let q = Math.min(1, (3 * k) / n), cutoff = Infinity, count = 0;
    for (let tries = 0; tries < 8; tries++) {
      const at = Math.min(sample.length - 1, Math.ceil(q * sample.length) + 8);
      cutoff = q >= 1 || at < 0 ? Infinity : sample[at];
      ({ c: count } = yield* below(cutoff).#aggG({ c: 'count()' }, {}));
      if (count >= k || cutoff === Infinity) break;
      q = Math.min(1, q * 4);
    }
    if (count < k) return yield* sortAll(this); // fewer than k rows with a value: NaN rows are needed
    // every row that can be among the first k has a first key <= cutoff
    const candidates = yield* below(cutoff).#collectG();
    try {
      const perm = sortIndex(yield* candidates.#keyArraysG(keys), candidates.n);
      return candidates.#gather(perm, Math.min(k, perm.length));
    } finally {
      candidates.dispose();
    }
  }

  // Plain JS objects (decodes text columns). Collects the filter first.
  // The rows (matching the filter) as plain columns: Float64Array (Float32Array
  // for float32 tables) per number column, { codes, labels } per category
  // column, string[] per free-text column. Copies, safe to keep.
  toColumns() { return this.engine._drive(this.#toColumnsG()); }
  toColumnsAsync() { return this.engine._driveAsync(this.#toColumnsG()); }

  *#toColumnsG() {
    if (this.#hasPacked()) return yield* this.#viaPlainG((t) => t.#toColumnsG());
    const t = yield* this.#collectG();
    const out = {};
    for (const [name, c] of t.cols) {
      const data = this.engine.live(c.data).subarray(0, t.n);
      if (c.dict) { const codes = new Uint32Array(t.n); codes.set(data); out[name] = { codes, labels: c.dict.slice() }; }
      else if (c.strings) { const sbuf = this.engine.live(c.strings); out[name] = Array.from(data, (v) => decodeString(sbuf, v)); }
      else out[name] = data.slice();
    }
    if (t !== this) t.dispose();
    return out;
  }

  // The rows (matching the filter) as Arrow IPC bytes: the stream format, or
  // with file: true the file format (.arrow / .feather for pandas, pyarrow,
  // Polars, DuckDB). Categories become dictionary-encoded strings.
  toArrow({ file = false } = {}) { return writeArrow(this.toColumns(), { file, dates: this.#dateNames() }); }
  async toArrowAsync({ file = false } = {}) { return writeArrow(await this.toColumnsAsync(), { file, dates: this.#dateNames() }); }
  #dateNames() { return [...this.cols].filter(([, c]) => c.date).map(([name]) => name); }

  toRows(limit = Infinity) { return this.engine._drive(this.#toRowsG(limit)); }
  toRowsAsync(limit = Infinity) { return this.engine._driveAsync(this.#toRowsG(limit)); }

  *#toRowsG(limit) {
    const t = yield* this.#collectG();
    const rows = [];
    const { engine } = this;
    const cols = [...t.cols].map(([name, c]) => [name, c, engine.live(c.data), c.strings ? engine.live(c.strings) : null]);
    const f32 = t.type === Float32Array;
    for (let i = 0; i < Math.min(t.n, limit); i++) {
      const row = {};
      for (const [name, c, data, sbuf] of cols) {
        const x = c.enc ? decodeOne(c.enc, data[i], f32) : data[i]; // (packed: decoded here, row by row)
        row[name] = c.dict ? c.dict[x] : sbuf ? decodeString(sbuf, x) : c.date ? (x === x ? new Date(x) : null) : x;
      }
      rows.push(row);
    }
    if (t !== this) t.dispose();
    return rows;
  }

  // A lazy query over this table (see plan.mjs): steps are recorded,
  // optimized together and run by collect() / toRows() / agg() / count().
  lazy() { return new Query({ op: 'scan', table: this }); }

  // The first n rows (matching the filter) as a new table.
  head(n) { return this.engine._drive(this.#headG(n)); }
  headAsync(n) { return this.engine._driveAsync(this.#headG(n)); }
  *#headG(n) {
    const t = yield* this.#collectG();
    if (n >= t.n) return t;
    const out = t.#gather(Uint32Array.from({ length: Math.max(0, n) }, (_, i) => i), Math.max(0, n));
    if (t !== this) t.dispose();
    return out;
  }

  // Rows [start, end) (matching the filter) as a new table.
  slice(start, end = Infinity) { return this.engine._drive(this.#sliceG(start, end)); }
  sliceAsync(start, end = Infinity) { return this.engine._driveAsync(this.#sliceG(start, end)); }
  *#sliceG(start, end) {
    const t = yield* this.#collectG();
    const a = Math.max(0, Math.min(t.n, Math.floor(start))), b = Math.max(a, Math.min(t.n, Math.floor(end)));
    const out = t.#gather(Uint32Array.from({ length: b - a }, (_, i) => a + i), b - a);
    if (t !== this) t.dispose();
    return out;
  }

  // Hash join: orders.join(customers, { on: 'customer_id', how: 'inner' | 'left' }).
  // on: a column both tables have, several (['region', 'day']), or
  // { left: 'cust', right: 'id' }. Put the smaller table on the right (it is
  // the one hashed). Rows come in left-table order, each with its matches in
  // right-table order; right columns named like a left column get `suffix`
  // (default '_right'). See join.mjs.
  join(other, opts) { return this.engine._drive(this.#joinG(other, opts)); }
  joinAsync(other, opts) { return this.engine._driveAsync(this.#joinG(other, opts)); }

  *#joinG(other, opts) {
    if (opts?.view && other instanceof Table && !other.pred && !other.#hasPacked()) { // (datasets: unique right keys as a view, see joinViewG)
      let collected = null, view = null;
      try {
        view = yield* joinViewG(this, other, opts, function* () { collected = yield* this.#collectG(); return collected; }.bind(this), (cols, n, pred, left, made) => {
          const owned = [...made];
          const ownedBuffers = [];
          if (left !== this) { owned.push(...left.owned); ownedBuffers.push(...left.ownedBuffers); left.owned = []; left.ownedBuffers = []; } // (the view keeps the collected rows)
          return new Table(this.engine, cols, n, { pred, owned, ownedBuffers, zones: false });
        });
      } finally {
        if (!view && collected && collected !== this) collected.dispose();
      }
      if (view) return view;
    }
    if (this.#hasPacked()) return yield* this.#viaPlainG((t) => t.#joinG(other, opts));
    if (other instanceof Table && other.#hasPacked()) { const o = yield* other.#decodedG(); try { return yield* this.#joinG(o, opts); } finally { o.dispose(); } }
    if (!(other instanceof Table)) throw new TypeError('vectro: join needs another table of the same engine');
    const l = yield* this.#collectG(), r = yield* other.#collectG();
    try {
      return yield* joinTablesG(l, r, opts, (cols, n) => new Table(this.engine, cols, n, { owned: [...cols.keys()] }));
    } finally {
      if (l !== this) l.dispose();
      if (r !== other) r.dispose();
    }
  }

  // Free the columns this table allocated (shared columns stay alive).
  dispose() {
    for (const name of this.owned) {
      const data = this.cols.get(name)?.data;
      if (data) this.engine.free(data);
    }
    for (const b of this.ownedBuffers) this.engine.free(b);
    this.owned = [];
    this.ownedBuffers = [];
  }
}

// ---- helpers ---------------------------------------------------------------------

const RESERVED = new Set(['Infinity', 'NaN']); // (function names are fine: sum(...) is a call, sum a column)
function checkName(name) {
  if (!IDENT.test(name) || RESERVED.has(name)) throw new Error(`vectro: "${name}" can't be a column name (use letters, digits and _)`);
}

// Missing values (NaN) in aggregations, as in SQL: sum and mean skip them
// (mean divides by the values present), min and max skip them, count(x)
// counts the values present (count(condition) still counts where it holds).
// arg is the bound source; returns the aggregation item.
function missingRules(fn, arg) {
  const present = `(${arg}) == (${arg})`;
  if (fn === 'count') return { fn, arg: isMask(parse(arg)) ? arg : present };
  if (fn === 'min') return { fn, arg: `(${present} ? (${arg}) : Infinity)`, present }; // (present: NaN when nothing is)
  if (fn === 'max') return { fn, arg: `(${present} ? (${arg}) : -Infinity)`, present };
  return fn === 'mean' ? { fn, arg: `(${present} ? (${arg}) : 0)`, present } : { fn, arg: `(${present} ? (${arg}) : 0)` }; // sum, mean
}

// a category column given as codes into a labels array: { codes, labels }
export const isCoded = (values) => values != null && !ArrayBuffer.isView(values) && !Array.isArray(values) && values.codes != null && Array.isArray(values.labels);

// Codes + labels as a category column. Repeated labels share one code.
function codedText(engine, type, { codes: src, labels }) {
  const data = engine.alloc(type, src.length);
  const codes = new Map(), dict = [], lut = new Float64Array(labels.length);
  labels.forEach((s, j) => {
    let c = codes.get(s);
    if (c === undefined) { c = dict.length; dict.push(s); codes.set(s, c); }
    lut[j] = c;
  });
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    if (!(c >= 0 && c < labels.length)) throw new RangeError(`vectro: code ${c} at row ${i} has no label (${labels.length} labels)`);
    data[i] = lut[c];
  }
  return { data, dict, codes };
}

const isDates = (values) => Array.isArray(values) && values.find((v) => v != null) instanceof Date;
const isText = (values) => !ArrayBuffer.isView(values) && values.length > 0 && typeof values[0] === 'string';

function copyNumbers(engine, type, values) {
  const data = engine.alloc(type, values.length);
  data.set(values);
  return data;
}

function encodeText(engine, type, values) {
  const data = engine.alloc(type, values.length);
  const codes = new Map(), dict = [];
  for (let i = 0; i < values.length; i++) {
    const s = values[i];
    let c = codes.get(s);
    if (c === undefined) { c = dict.length; dict.push(s); codes.set(s, c); }
    data[i] = c;
  }
  return { data, dict, codes };
}

// Text functions, run once per distinct label of a category column, not per
// row: the matching codes become a few range checks the fused loop can run.
const TEXT_FNS = {
  startsWith: (label, [p]) => label.startsWith(p),
  endsWith: (label, [p]) => label.endsWith(p),
  contains: (label, [p]) => label.includes(p),
  in: (label, list) => list.includes(label),
  like: (label, [pattern], re) => re.test(label),
  ilike: (label, [pattern], re) => re.test(label), // (the pattern ignoring case)
};
const MAX_RANGES = 256;

// startsWith(region, 'E') -> (region >= 0 && region <= 1) || region == 5
function textCall(n, cols, src) {
  const [colNode, ...rest] = n.args;
  const c = colNode?.t === 'var' ? cols.get(colNode.name) : null;
  if (c?.strings) throw new Error(`vectro: "${colNode.name}" is a free-text column; ${n.fn}() needs a category column (load it as types: { ${colNode.name}: 'category' })`);
  if (!c?.dict) throw new SyntaxError(`vectro: ${n.fn}() needs a text column first, then text in quotes, in "${src}"`);
  if (!rest.length || rest.some((a) => a.t !== 'str')) throw new SyntaxError(`vectro: ${n.fn}(${colNode.name}, …) needs text in quotes after the column, in "${src}"`);
  if (n.fn !== 'in' && rest.length !== 1) throw new SyntaxError(`vectro: ${n.fn}() takes a column and one text, in "${src}"`);
  const args = rest.map((a) => a.v);
  const re = n.fn === 'like' || n.fn === 'ilike' ? new RegExp(`^${args[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '[\\s\\S]*').replace(/_/g, '[\\s\\S]')}$`, n.fn === 'ilike' ? 'iu' : '') : null;
  const match = c.dict.map((label) => TEXT_FNS[n.fn](label, args, re));
  // runs of consecutive codes that match (or, if shorter, that don't)
  const runs = (want) => {
    const out = [];
    for (let i = 0; i < match.length; i++) {
      if (match[i] !== want) continue;
      let j = i;
      while (j + 1 < match.length && match[j + 1] === want) j++;
      out.push([i, j]);
      i = j;
    }
    return out;
  };
  const yes = runs(true), no = runs(false);
  const col = { t: 'var', name: colNode.name };
  const num = (v) => ({ t: 'num', v });
  const anyOf = (list) => list.map(([a, b]) => (a === b ? { t: 'bin', op: '==', l: col, r: num(a) } : { t: 'bin', op: '&&', l: { t: 'bin', op: '>=', l: col, r: num(a) }, r: { t: 'bin', op: '<=', l: col, r: num(b) } }))
    .reduce((x, y) => ({ t: 'bin', op: '||', l: x, r: y }));
  if (!yes.length) return { t: 'bin', op: '==', l: col, r: num(-1) }; // nothing matches
  if (!no.length) return { t: 'bin', op: '==', l: col, r: col };      // everything matches
  if (Math.min(yes.length, no.length) > MAX_RANGES) throw new Error(`vectro: ${n.fn}() matches ${match.filter(Boolean).length} labels of "${colNode.name}" spread over too many runs; group by it first or filter differently`);
  return yes.length <= no.length ? anyOf(yes) : { t: 'un', op: '!', x: anyOf(no) };
}

// `region == 'EU'` -> `region == 3` (the code for 'EU', or -1 if absent).
export function rewriteText(ast, cols, src) {
  const textCol = (n) => n.t === 'var' && cols.get(n.name)?.dict;
  const dateCol = (n) => n.t === 'var' && cols.get(n.name)?.date;
  const one = (n) => { if (n.args.length !== 1) throw new SyntaxError(`vectro: ${n.fn}() takes one value, in "${src}"`); return fix(n.args[0]); };
  const fix = (n) => {
    if (n.t === 'call' && TEXT_FNS[n.fn]) return textCall(n, cols, src);
    if (n.t === 'call' && n.fn === 'date') { // date('2024-03-01'): the time in ms
      const v = n.args.length === 1 && n.args[0].t === 'str' ? parseDateText(n.args[0].v) : NaN;
      if (Number.isNaN(v)) throw new SyntaxError(`vectro: date() needs ISO text such as date('2024-03-01') or date('2024-03-01T12:00:00Z'), in "${src}"`);
      return { t: 'num', v };
    }
    if (n.t === 'call' && DATE_FNS[n.fn]) return DATE_FNS[n.fn](one(n));
    if (n.t === 'call' && n.fn === 'isMissing') { const x = one(n); return { t: 'bin', op: '!=', l: x, r: x }; }
    if (n.t === 'call' && n.fn === 'coalesce') { // first value that isn't missing
      if (n.args.length < 2) throw new SyntaxError(`vectro: coalesce() needs two values or more, in "${src}"`);
      return n.args.map(fix).reduceRight((rest, x) => ({ t: 'cond', c: { t: 'bin', op: '==', l: x, r: x }, x, y: rest }));
    }
    if (n.t === 'bin' && (n.l.t === 'str' || n.r.t === 'str') && (dateCol(n.l) || dateCol(n.r))) { // ts >= '2024-03-01'
      const [colNode, strNode] = n.l.t === 'str' ? [n.r, n.l] : [n.l, n.r];
      const v = parseDateText(strNode.v);
      if (Number.isNaN(v)) throw new SyntaxError(`vectro: ${JSON.stringify(strNode.v)} isn't a date (use ISO text such as '2024-03-01' or '2024-03-01T12:00:00Z'), in "${src}"`);
      const lit = { t: 'num', v };
      return n.l.t === 'str' ? { ...n, l: lit, r: colNode } : { ...n, l: colNode, r: lit };
    }
    if (n.t === 'bin') {
      if ((n.op === '==' || n.op === '!=') && (n.l.t === 'str' || n.r.t === 'str')) {
        const [colNode, strNode] = n.l.t === 'str' ? [n.r, n.l] : [n.l, n.r];
        if (colNode.t === 'var' && cols.get(colNode.name)?.strings) {
          throw new Error(`vectro: "${colNode.name}" is a free-text column; it can be returned but not used in expressions yet (load it as types: { ${colNode.name}: 'category' } to filter or group on it)`);
        }
        if (!textCol(colNode)) throw new SyntaxError(`vectro: ${JSON.stringify(strNode.v)} can only be compared with a text column, in "${src}"`);
        const code = cols.get(colNode.name).codes.get(strNode.v) ?? -1;
        return { ...n, l: colNode, r: { t: 'num', v: code } };
      }
      return { ...n, l: fix(n.l), r: fix(n.r) };
    }
    if (n.t === 'un') return { ...n, x: fix(n.x) };
    if (n.t === 'call') return { ...n, args: n.args.map(fix) };
    if (n.t === 'cond') return { ...n, c: fix(n.c), x: fix(n.x), y: fix(n.y) };
    return n;
  };
  return fix(ast);
}

// Percentiles of each partition (items: { fn: 'percentile_cont' |
// 'percentile_disc', p }): x the values (NaN skipped), g each row's partition
// 0..K-1 (null: one). Returns one Float64Array(K) per item. With few
// partitions, a radix pick: each value's top 16 bits in sortable order are
// counted, then only the values in the buckets holding the wanted ranks are
// gathered and selected among (two passes over x, in order). With many
// (small) partitions, each one's values are gathered and selected among.
function percentiles(x, g, K, items) {
  const n = x.length, radix = K <= 64 && n >= 4096, B = 65536;
  const sizes = new Int32Array(K);
  let hist = null, hi = null;
  if (radix) {
    hist = new Int32Array(K * B);
    hi = new Int32Array(x.buffer, x.byteOffset, n * 2); // (signed: stays a small integer in V8)
    if (g) { for (let i = 0; i < n; i++) { const v = x[i]; if (v === v) { const h = hi[2 * i + 1]; hist[g[i] * B + (h < 0 ? ~h >>> 16 : (h >>> 16) | 0x8000)]++; } } } // (negatives reversed)
    else for (let i = 0; i < n; i++) { const v = x[i]; if (v === v) { const h = hi[2 * i + 1]; hist[h < 0 ? ~h >>> 16 : (h >>> 16) | 0x8000]++; } }
    for (let k = 0; k < K; k++) { let m = 0; for (let b = k * B, e = b + B; b < e; b++) m += hist[b]; sizes[k] = m; }
  } else if (g) { for (let i = 0; i < n; i++) if (x[i] === x[i]) sizes[g[i]]++; } else for (let i = 0; i < n; i++) if (x[i] === x[i]) sizes[0]++;
  // the ranks wanted (0 = the smallest value present)
  const wants = [];
  const plan = items.map((it) => Array.from(sizes, (m, k) => {
    if (!m) return null;
    if (it.fn === 'percentile_disc') { wants.push([k, Math.max(0, Math.ceil(it.p * m) - 1)]); return { at: wants.length - 1 }; }
    const r = it.p * (m - 1), lo = Math.floor(r);
    wants.push([k, lo]);
    if (lo + 1 < m && r > lo) { wants.push([k, lo + 1]); return { at: wants.length - 2, frac: r - lo }; }
    return { at: wants.length - 1 };
  }));
  const got = new Float64Array(wants.length);
  if (!radix) {
    const start = new Int32Array(K + 1);
    for (let k = 0; k < K; k++) start[k + 1] = start[k] + sizes[k];
    const at = start.slice(0, K), buf = new Float64Array(start[K]);
    for (let i = 0; i < n; i++) { const v = x[i]; if (v === v) buf[at[g ? g[i] : 0]++] = v; }
    wants.forEach(([k, r], j) => { got[j] = select(buf, start[k], start[k + 1], start[k] + r); });
  } else if (wants.length) {
    // each wanted rank's bucket, and its rank within the bucket
    const slotOf = new Map(), place = [];
    for (const [k, r] of wants) {
      let b = 0, below = 0;
      const h0 = k * B;
      while (below + hist[h0 + b] <= r) below += hist[h0 + b++];
      if (!slotOf.has(h0 + b)) slotOf.set(h0 + b, slotOf.size);
      place.push([slotOf.get(h0 + b), r - below]);
    }
    const slot = new Int32Array(K * B).fill(-1);
    const bufs = [...slotOf.keys()].map((key, j) => { slot[key] = j; return new Float64Array(hist[key]); }), fill = new Int32Array(bufs.length);
    for (let i = 0; i < n; i++) {
      const v = x[i];
      if (v !== v) continue;
      const h = hi[2 * i + 1], j = slot[(g ? g[i] * B : 0) + (h < 0 ? ~h >>> 16 : (h >>> 16) | 0x8000)];
      if (j >= 0) bufs[j][fill[j]++] = v;
    }
    place.forEach(([j, r], w) => { got[w] = select(bufs[j], 0, bufs[j].length, r); });
  }
  return plan.map((ws) => Float64Array.from(ws, (w) => (!w ? NaN : w.frac ? got[w.at] + w.frac * (got[w.at + 1] - got[w.at]) : got[w.at])));
}
function toInt32(a, n) { const out = new Int32Array(n); for (let i = 0; i < n; i++) out[i] = a[i]; return out; }
function spread(d, per, g, n) { for (let i = 0; i < n; i++) d[i] = per[g[i]]; }

// The k-th smallest of a[lo..hi) (quickselect, in place: afterwards a[k] is
// it, smaller values before it, larger after).
function select(a, lo, hi, k) {
  let l = lo, r = hi - 1;
  while (r > l) {
    if (r - l < 16) { for (let i = l + 1; i <= r; i++) { const v = a[i]; let j = i - 1; while (j >= l && a[j] > v) { a[j + 1] = a[j]; j--; } a[j + 1] = v; } break; }
    const m = (l + r) >> 1; // median of three as the pivot
    if (a[m] < a[l]) { const x = a[m]; a[m] = a[l]; a[l] = x; }
    if (a[r] < a[l]) { const x = a[r]; a[r] = a[l]; a[l] = x; }
    if (a[r] < a[m]) { const x = a[r]; a[r] = a[m]; a[m] = x; }
    const pivot = a[m];
    let i = l, j = r;
    while (i <= j) {
      while (a[i] < pivot) i++;
      while (a[j] > pivot) j--;
      if (i <= j) { const x = a[i]; a[i] = a[j]; a[j] = x; i++; j--; }
    }
    if (k <= j) r = j; else if (k >= i) l = i; else break;
  }
  return a[k];
}
