// Columnar tables on top of the engine. Each column is one array in engine
// memory; text columns are dictionary-encoded (each distinct string gets a
// number code), so `region == 'EU'` becomes a numeric comparison.
//
// Filters are lazy: t.filter(p).sum(e) compiles to one fused pass computing
// sum(p ? e : 0). No filtered copy is made unless you ask with collect().

import { parseKeys, labelRanks, sortIndex } from './sort.mjs';
import { ZONE_ROWS, computeZones, zoneRange } from './zones.mjs';
import { parse, toSource, walk, analyze, isMask, fold, checkSpec } from './fuse.mjs';
import { generateGroupBy, generateCompact, generateHashGroups, generateMarkPairs, generateStridedKeys } from './table-kernels.mjs';
import { decodeString } from './csv-kernel.mjs';
import { maxUnits } from './kernels.mjs';
import { topModule, markModule } from './rank-kernels.mjs';
import { sortPermG, gatherG } from './parallel-sort.mjs';
import { partitionedGroupsG, estimateGroups, PARTITION_ROWS, PARTITION_GROUPS, DIRECT_SLOTS } from './partition.mjs';
import { joinTablesG, joinViewG, lookupGroupsG } from './join.mjs';
import { Query } from './plan.mjs';
import { bounds, windowValues, readsPeers } from './window.mjs';
import { copyRows } from './rows.mjs';
import { textmapRows } from './textmap.mjs';
import { writeArrow } from './arrow.mjs';
import { interpretGroup, interpretCompact, interpretFilteredReduce } from './interp.mjs';
import { DATE_FNS, DATE_RESULT, parseDateText, toMillis, f32Dates } from './dates.mjs';
import { chooseEncoding, encodingCandidates, packInto, decodeOne, KINDS } from './pack.mjs';

const IDENT = /^[A-Za-z_]\w*$/;
const AGG_FNS = new Set(['sum', 'mean', 'min', 'max', 'count', 'countDistinct', 'approxDistinct']);
const DISTINCT = new Set(['countDistinct', 'approxDistinct']); // exact on a table (see #distinctCountsG)
const IDENTITY = { sum: 0, count: 0, mean: 0, min: Infinity, max: -Infinity };
const MAX_GROUP_SLOTS = 2 ** 28; // threads * aggregations * groups (2GB of float64)
const PARALLEL_SORT_ROWS = 50_000; // smaller tables sort on the calling thread
const TIME_STEPS = [86_400_000, 3_600_000, 60_000, 1_000]; // a day, an hour, a minute, a second (ms): see #directGroups
// Column facts worked out once and remembered with the column's array
// (arrays are never written once in a table):
const MISSING = new WeakMap(); // { n: rows, missing: how many missing values }
const KEY_RANGE = new WeakMap(); // { n: rows, range: [min, max, how many aren't whole] } (see #directGroups)

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
      if (dates.includes(name) || isDates(values)) {
        if (type === Float32Array) throw new Error(f32Dates(name));
        columns = { ...columns, [name]: { dates: Float64Array.from(values, toMillis) } };
      }
    }
    const entries = Object.entries(columns);
    if (!entries.length) throw new TypeError('vectro: a table needs at least one column');
    const len = (v) => (isCoded(v) ? v.codes.length : v.dates ? v.dates.length : v.length);
    const n = len(entries[0][1]);
    for (const [name, values] of entries) { // (checked before anything is copied)
      checkName(name);
      if (len(values) !== n) throw new RangeError(`vectro: column "${name}" has ${len(values)} rows, expected ${n}`);
    }
    const cols = new Map();
    try {
      for (const [name, values] of entries) {
        cols.set(name, values.dates ? { data: copyNumbers(engine, type, values.dates), dict: null, codes: null, date: true }
          : isCoded(values) ? codedText(engine, type, values) : isText(values) ? encodeText(engine, type, values) : { data: copyNumbers(engine, type, values), dict: null, codes: null });
      }
    } catch (err) { // (a bad code, out of memory: the columns copied so far go back)
      for (const c of cols.values()) engine.free(c.data);
      throw err;
    }
    const t = new Table(engine, cols, n, { owned: [...cols.keys()] });
    return compress ? t.compress() : t;
  }

  // Convert an array of objects. Column types come from the first row.
  static fromRows(engine, rows, { type = Float64Array } = {}) {
    if (!rows.length) throw new TypeError('vectro: no rows');
    const n = rows.length;
    const cols = new Map();
    for (const name of Object.keys(rows[0])) checkName(name); // (checked before anything is copied)
    try {
      for (const name of Object.keys(rows[0])) {
        const data = engine.alloc(type, n);
        const first = firstPresent(rows.map((r) => r[name]));
        if (typeof first === 'string') {
          const codes = new Map(), dict = [];
          for (let i = 0; i < n; i++) {
            const s = rows[i][name] ?? ''; // (null / undefined: missing text)
            let c = codes.get(s);
            if (c === undefined) { c = dict.length; dict.push(s); codes.set(s, c); }
            data[i] = c;
          }
          cols.set(name, { data, dict, codes });
        } else {
          const date = first instanceof Date;
          if (date && type === Float32Array) { engine.free(data); throw new Error(f32Dates(name)); }
          for (let i = 0; i < n; i++) { const v = rows[i][name]; data[i] = v === null || v === undefined ? NaN : date ? toMillis(v) : v; } // (null: missing)
          cols.set(name, { data, dict: null, codes: null, ...(date ? { date: true } : {}) });
        }
      }
    } catch (err) { for (const c of cols.values()) engine.free(c.data); throw err; } // (out of memory: the columns copied so far go back)
    return new Table(engine, cols, n, { owned: [...cols.keys()] });
  }

  // ---- shape ----------------------------------------------------------------------

  get length() { return this.n; }            // physical rows (ignores the filter)
  get columnNames() { return [...this.cols.keys()].filter((nm) => !this.cols.get(nm).hidden); } // (hidden: a text filter's 0 / 1 per row)
  get type() { const c = this.cols.values().next().value; return c.enc ? c.plainType : c.data.constructor; }

  column(name) {
    const c = this.cols.get(name);
    if (!c) throw new Error(`vectro: no column "${name}"`);
    return this.engine.live(c.data);
  }

  // ---- expressions ----------------------------------------------------------------

  // Parse an expression, swap text literals for dictionary codes, and check
  // every name is a column or a param. Returns { ast, src }. codes: the expression may do
  // arithmetic on text columns' codes (internal group numbers and keys), else that is an error.
  #bind(src, params, codes = false) {
    const parsed = parse(src);
    if (!codes) checkTextUse(parsed, this.cols, src);
    const ast = rewriteText(parsed, this.cols, src);
    // a text function matching labels scattered over many codes: one 0 / 1 per row, made from its
    // per-label answers (once per column's data), added as a hidden column the kernels read
    // (and the same for a text column compared with another: its codes in the other's dictionary)
    walk(ast, (m) => {
      if (m.t !== 'var' || !m.lut) return;
      const c = this.cols.get(m.lut.column);
      if (!MASKS.has(c.data)) MASKS.set(c.data, new Map());
      const masks = MASKS.get(c.data);
      let mask = masks.get(m.name);
      if (!mask || mask.length < this.n) {
        mask = this.engine.alloc(this.type === Float32Array ? Float32Array : Float64Array, this.n);
        const codes = this.engine.live(c.data), { match, values } = m.lut, f32 = this.type === Float32Array;
        if (values) for (let i = 0; i < this.n; i++) mask[i] = values[c.enc ? decodeOne(c.enc, codes[i], f32) : codes[i]]; // (a text column's codes in another one's dictionary)
        else for (let i = 0; i < this.n; i++) mask[i] = match[c.enc ? decodeOne(c.enc, codes[i], f32) : codes[i]] ? 1 : 0;
        masks.set(m.name, mask);
      }
      if (this.cols.get(m.name)?.data !== mask) this.cols.set(m.name, { data: mask, dict: null, codes: null, hidden: true }); // (views sharing these columns have the same rows)
    });
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
      if (c.enc || c.strings || c.hidden) continue; // (hidden: a text filter's 0 / 1, cached in MASKS, or a column of another table's, see select)
      const values = engine.live(c.data);
      const n = values.length;
      const enc = engine.jit ? yield* this.#chooseEncodingG(c.data, n, f32) : chooseEncoding(values, n, f32);
      if (!enc) continue;
      const plainType = values.constructor, old = c.data;
      c.data = engine.jit ? yield* this.#packG(old, n, enc) : packInto(engine, old, n, enc);
      c.enc = enc;
      c.plainType = plainType;
      c.rows = n;
      if (MASKS.has(old)) { MASKS.set(c.data, MASKS.get(old)); MASKS.delete(old); } // (a text filter's 0 / 1 per row: still right, freed with the packed column)
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
      for (const [name, c] of this.cols) if (!c.hidden) out.set(name, { ...c, data: yield* gatherG(engine, c.data, perm, m, t, c.enc), zones: undefined, ...(c.enc ? { rows: m } : {}) }); // (hidden: the filter's, done with)
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
    for (const [k, v] of Object.entries(params)) {
      if (typeof v !== 'number' && !(v instanceof Date)) throw new TypeError(`vectro: param "${k}" must be a number or a Date, got ${typeof v === 'string' ? `text "${v}"` : typeof v} (text goes in the expression in quotes, e.g. region == 'EU')`);
    }
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
    let { ast, src } = this.#bind(expr, merged);
    if (ast.t === 'var' && this.cols.get(ast.name)?.dict) textAsNumber(ast.name, expr); // filter('city'): its codes as conditions
    walk(ast, (m) => { if (m.t === 'str') throw new SyntaxError(`vectro: text like ${JSON.stringify(m.v)} can only be compared with == or != to a text column, in "${expr}" (to compare dates, load the column as dates: { dates: ['name'] })`); });
    const k = fold(withParams(ast, merged, this.cols));
    if (k.t === 'num') { // no column in it (1 == 1, on == 1 with a param): every row, or none (a kernel needs a column to read)
      if (k.v !== 0) return new Table(this.engine, this.cols, this.n, { pred: this.pred, params: merged, zones: this.zones });
      const c = [...this.cols.keys()].find((x) => !this.cols.get(x).strings) ?? this.cols.keys().next().value;
      src = `${c} != ${c} && ${c} == ${c}`;
    }
    const pred = this.pred ? `(${this.pred}) && (${src})` : src;
    return new Table(this.engine, this.cols, this.n, { pred, params: merged, zones: this.zones });
  }

  // New column computed for every row, in one fused pass.
  // (internal columns, named __...: keys made from text codes on purpose, see #bind)
  derive(name, expr, params = {}) { return this.engine._drive(this.#deriveG(name, expr, params, name.startsWith('__'))); }
  deriveAsync(name, expr, params = {}) { return this.engine._driveAsync(this.#deriveG(name, expr, params, name.startsWith('__'))); }

  *#deriveG(name, expr, params, codes = false) {
    checkName(name);
    if (this.cols.has(name)) throw new Error(`vectro: column "${name}" already exists`);
    const merged = this.#mergeParams(params);
    const { src } = this.#bind(expr, merged, codes);
    const out = this.engine.alloc(this.type, this.n);
    yield* this.engine.evalG(src, this.#vars(merged), out);
    const top = parse(expr);
    const date = (top.t === 'call' && DATE_RESULT.has(top.fn)) || (top.t === 'var' && !!this.cols.get(top.name)?.date); // dayStart(ts), monthStart(ts)...: still dates
    const kinds = valueKinds(top, this.cols), dict = kinds.size === 1 ? kinds.values().next().value : 'number';
    const same = dict === 'number' ? null : [...this.cols.values()].find((c) => c.dict === dict); // a copy of a text column stays text (through ? : and coalesce too)
    const cols = new Map(this.cols).set(name, { data: out, dict: same?.dict ?? null, codes: same?.dict ? same.codes : null, date });
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params, owned: [name] });
  }

  // Several aggregations in one pass: agg({ revenue: 'sum(price * qty)', n: 'count()' })
  agg(spec, params = {}) { return this.engine._drive(this.#aggG(spec, params)); }
  aggAsync(spec, params = {}) { return this.engine._driveAsync(this.#aggG(spec, params)); }

  *#aggG(spec, params) {
    checkSpec(spec);
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
    // reductions that read no column (sum(1), count(p) with params) are worked out here (a
    // kernel needs a column to read); the others run in batches (a kernel takes at most 8)
    const r = reductions.map((src) => this.#constReduction(src, merged));
    const run = reductions.flatMap((src, j) => (r[j] === undefined ? [[src, j]] : []));
    for (let i = 0; i < run.length; i += 8) {
      const batch = run.slice(i, i + 8), got = [(yield* this.engine.evalG(batch.map(([src]) => src), this.#vars(merged)))].flat();
      batch.forEach(([, j], q) => { r[j] = got[q]; });
    }
    const value = (it) => (it.value !== undefined ? it.value : it.presentSlot !== undefined ? (r[it.presentSlot] ? r[it.slot] : NaN) : it.countSlot === undefined ? r[it.slot] : r[it.slot] / r[it.countSlot]);
    return done(Object.fromEntries(items.map((it) => [it.out, value(it)])));
  }

  // The value of a reduction such as 'sum((1) == (1) ? (1) : 0)' that reads no column (params
  // written in), as the kernels would add it up over every row; undefined if it reads one.
  #constReduction(src, params) {
    const ast = parse(src);
    let reads = false;
    walk(ast, (m) => { if (m.t === 'var' && this.cols.has(m.name)) reads = true; });
    const k = reads ? null : fold(withParams(ast.args[0], params, this.cols));
    if (k?.t !== 'num') return undefined;
    const v = this.type === Float32Array ? Math.fround(k.v) : k.v, n = this.n; // (float32 kernels round it)
    if (ast.fn === 'count') return k.v !== 0 ? n : 0; // (as the kernels count: NaN != 0)
    if (ast.fn === 'sum') return n ? v * n : 0;
    if (ast.fn === 'min' || ast.fn === 'max') return n ? v : IDENTITY[ast.fn];
    return v; // mean
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
    // a category column: its missing values are '' (count / countDistinct skip them, as SQL skips NULL)
    const a0 = ast.args[0], text = a0.t === 'var' && this.cols.get(a0.name)?.dict;
    if (text && ast.fn === 'count') return missingRules('count', this.#bind(`${a0.name} != ''`, params).src);
    if (text && !DISTINCT.has(ast.fn)) textAsNumber(a0.name, src); // sum(city), min(city): of its codes
    if (text && DISTINCT.has(ast.fn)) return { fn: ast.fn, arg: this.#bind(`${a0.name} != '' ? ${a0.name} : 0 / 0`, params, true).src };
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

  // (plan.mjs) GROUP BY right-table columns of an inner join on unique right
  // keys, aggregating this (left) table's columns: each matching row's join
  // key is looked up straight to its group number inside the group kernel
  // (filter, lookup and sums in one pass over the left rows, packed columns
  // read as they are), so no joined rows are made. Null when it doesn't
  // apply (the plan then joins as usual). on: { left, right } key names;
  // rightKeys[k]: the right column behind output key keys[k].
  _lookupGroupAgg(R, on, keys, rightKeys, spec) { return this.engine._drive(this.#lookupGroupAggG(R, on, keys, rightKeys, spec)); }
  _lookupGroupAggAsync(R, on, keys, rightKeys, spec) { return this.engine._driveAsync(this.#lookupGroupAggG(R, on, keys, rightKeys, spec)); }

  *#lookupGroupAggG(R, on, keys, rightKeys, spec) {
    const { engine } = this;
    if (!engine.jit || !(R instanceof Table) || R.engine !== engine || R.pred || R.#hasPacked() || R.type !== this.type) return null;
    const c = this.cols.get(on.left);
    if (!c || c.strings) return null;
    const merged = this.#mergeParams({});
    const items = Object.entries(spec).map(([out, src]) => ({ out, ...this.#aggItem(src, merged) }));
    if (items.some((it) => DISTINCT.has(it.fn) || keys.includes(it.out))) return null;
    const look = yield* lookupGroupsG(this, R, on, rightKeys);
    if (!look) return null;
    const groups = {
      K: look.G, data: c.data, enc: c.enc ?? null, lookup: look, sorted: true, owned: false, keyMeta: look.meta,
      keyColumn: (k, keep) => { const v = look.values[k], out = new Float64Array(keep.length); for (let i = 0; i < keep.length; i++) out[i] = v[keep[i]]; return out; },
    };
    return yield* this.#runGroupsG(keys, groups, items, merged);
  }

  *#groupAggG(keys, spec, params) {
    if (this.#hasPacked()) {
      const used = new Set(keys);
      for (const src of [...Object.values(spec), this.pred ?? '1']) walk(parse(src), (m) => { if (m.t === 'var' && this.cols.has(m.name)) used.add(m.name); });
      if (!this.engine.jit) { // the interpreter reads plain numbers
        if ([...used].some((n) => this.cols.get(n).enc)) return yield* this.#viaPlainG((t) => t.#groupAggG(keys, spec, params), used);
      } else if (keys.some((k) => this.cols.get(k).enc)) { // the group kernel reads packed columns; the other paths need plain keys
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
    const { engine } = this;
    const totals = 1 + items.length + items.filter((it) => it.present).length, capped = {};
    // (packed keys: numbered as they are read, no copy to make; the other paths would decode them first)
    const packedKey = keys.some((k) => this.cols.get(k).enc);
    const direct = yield* this.#directGroups(keys, items.length + 1, packedKey || items.some((it) => DISTINCT.has(it.fn)) ? 2 ** 24 : this.#directMax(totals), capped);
    if (direct) {
      try { return yield* this.#runGroupsG(keys, direct, items, merged); } finally { if (direct.owned) this.engine.free(direct.data); }
    }
    // Otherwise keep the matching rows. Big tables: partitioned by key range
    // (see partition.mjs). Else (and for distinct counts, which need each
    // row's group) the hash path: number the distinct keys 0..K-1 (in order
    // of first appearance), then group by that number.
    const t = yield* this.#collectG();
    let ids = null;
    try {
      let est; // (groups expected)
      if (engine.jit && t.n >= (engine._partitionRows ?? PARTITION_ROWS) && !items.some((it) => DISTINCT.has(it.fn))) {
        est = estimateGroups(t, keys);
        const out = est >= (engine._partitionGroups ?? PARTITION_GROUPS) ? yield* t.#partitionedGroupsG(keys, items, merged, est) : null;
        if (out) return out;
        // not partitioned after all: keys the direct path turned down only to leave them to it,
        // numbered directly when most of their range is in use (else the hash path is faster)
        if (capped.K && est * 8 >= capped.K) {
          ids = yield* t.#directGroups(keys, items.length + 1);
          if (ids) return yield* t.#runGroupsG(keys, ids, items, merged);
        }
      }
      ids = engine.jit && t.n >= 4096 ? yield* t.#hashGroupsParallelG(keys, est) : t.#hashGroups(keys);
      return yield* t.#runGroupsG(keys, ids, items, merged);
    } finally {
      if (ids && ids.owned !== false) { // (direct numbers can be a key column itself)
        this.engine.free(ids.data);
        for (const x of ids.extra ?? []) this.engine.free(x);
      }
      if (t !== this) t.dispose();
    }
  }

  // The result table from the partitioned path (see partition.mjs), or null
  // when it doesn't suit these keys and aggregations. Totals as #runGroupsG
  // has them: the row count first, then the items, then each mean's, min's
  // and max's count of values present.
  *#partitionedGroupsG(keys, items, merged, est) {
    const { engine } = this;
    const kernelItems = [...items, ...items.filter((it) => it.present).map((it) => ({ fn: 'count', arg: it.present, of: it }))];
    const sources = ['count(1)', ...kernelItems.map((it) => `${it.fn === 'mean' ? 'sum' : it.fn}(${it.arg ?? 1})`)];
    const r = yield* partitionedGroupsG(this, keys, sources, merged, est);
    if (!r) return null;
    const { G, keyCols, accCols, map } = r;
    const pending = new Set([...keyCols, ...accCols]); // freed unless they become result columns
    try {
      const cols = new Map(), used = new Set();
      keys.forEach((key, k) => {
        const src = this.cols.get(key);
        cols.set(key, { data: keyCols[k], dict: src.dict ?? null, codes: src.dict ? src.codes : null, date: src.date || undefined });
      });
      for (const [i, it] of items.entries()) {
        let data = accCols[map[1 + i]];
        if (it.present || used.has(data)) {
          const out = engine.alloc(Float64Array, G);
          pending.add(out);
          if (!it.present) engine.live(out).set(engine.live(data)); // (a total two items share)
          else if (G) { // mean: over the values present; min, max: NaN where there is none
            const p = accCols[map[1 + kernelItems.findIndex((k) => k.of === it)]];
            yield* engine.evalG(it.fn === 'mean' ? 'v / p' : '(p != 0) ? v : NaN', { v: data, p }, out);
          }
          data = out;
        }
        used.add(data);
        cols.set(it.out, { data, dict: null, codes: null });
      }
      for (const c of cols.values()) { pending.delete(c.data); c.data = engine.live(c.data); }
      return new Table(engine, cols, G, { owned: [...cols.keys()] });
    } finally {
      for (const x of pending) engine.free(x);
    }
  }

  // The most groups to number directly with `totals` totals per group: past
  // threads x totals x groups = DIRECT_SLOTS, the partitioned path is faster
  // (on tables it takes).
  #directMax(totals) {
    const { engine } = this;
    if (!engine.jit || this.n < (engine._partitionRows ?? PARTITION_ROWS)) return 2 ** 24;
    return Math.floor((engine._directSlots ?? DIRECT_SLOTS) / (engine.T * totals));
  }

  // Is src a number column of this table with no missing values? Counted
  // once per column (one pass on every thread) and remembered with its array.
  *#noMissingG(src) {
    const c = src && IDENT.test(src) ? this.cols.get(src) : null;
    if (!c || c.dict || c.strings) return false;
    let known = MISSING.get(c.data);
    if (known?.n !== this.n) {
      const k = c.enc ? { data: c.data, enc: c.enc, n: this.n, type: this.type === Float32Array ? 'f32' : 'f64' } : c.data;
      MISSING.set(c.data, (known = { n: this.n, missing: (yield* this.engine.evalG(['count(k != k)'], { k }))[0] }));
    }
    return known.missing === 0;
  }

  // Keys small enough to be group numbers as they are: a text column's codes
  // or whole numbers 0..2^24, several combined as k1 * size2 + k2..., while
  // the accumulators stay small (or no larger than the rows). Else null.
  *#directGroups(keys, J, maxK = 2 ** 24, info = null) { // (info.K: the groups when only maxK stood in the way)
    const { engine } = this;
    const sizes = [], offsets = [], steps = [];
    for (const key of keys) {
      const c = this.cols.get(key);
      if (c.dict) { sizes.push(c.dict.length); offsets.push(0); steps.push(1); continue; }
      if (!this.n) { sizes.push(0); offsets.push(0); steps.push(1); continue; }
      // whole numbers in a range under 2^24 (from their minimum: 202301..202412, IDs from 1e9, negatives)
      const k = c.enc ? { data: c.data, enc: c.enc, n: this.n, type: this.type === Float32Array ? 'f32' : 'f64' } : c.data;
      let known = KEY_RANGE.get(c.data);
      if (known?.n !== this.n) KEY_RANGE.set(c.data, (known = { n: this.n, range: yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k }) }));
      const [lo, hi, frac] = known.range;
      if (frac > 0 || !Number.isFinite(lo) || !Number.isFinite(hi)) return null;
      // or times a whole number of days (hours, minutes, seconds) apart: numbered by that unit
      let step = 1;
      if (!(hi - lo < 2 ** 24)) {
        const units = TIME_STEPS.filter((s) => (hi - lo) / s < 2 ** 24);
        const off = units.length ? yield* engine.evalG(units.map((s) => `count((k - lo) / ${s} != floor((k - lo) / ${s}))`), { k, lo }) : [];
        step = units.find((_, j) => [off].flat()[j] === 0);
        if (!step) return null;
      }
      sizes.push((hi - lo) / step + 1);
      offsets.push(lo);
      steps.push(step);
    }
    const fits = (K) => !(K > 2 ** 24 || engine.T * J * K * (engine.jit && K <= 1024 ? 4 : 1) > (engine._maxGroupSlots ?? MAX_GROUP_SLOTS) || (K > 65_536 && K > 8 * this.n));
    // One key from a small positive number (IDs from 1, years from 1990):
    // numbered from 0 with a few groups left empty, so the key column itself
    // is the group number and needs no copy.
    if (keys.length === 1 && steps[0] === 1 && offsets[0] > 0 && offsets[0] <= Math.max(1024, sizes[0]) && offsets[0] + sizes[0] <= maxK && fits(offsets[0] + sizes[0])) {
      sizes[0] += offsets[0];
      offsets[0] = 0;
    }
    const K = sizes.reduce((a, b) => a * b, 1);
    if (!fits(K)) return null;
    if (K > maxK) { if (info) info.K = K; return null; }
    const below = sizes.map((_, k) => sizes.slice(k + 1).reduce((a, b) => a * b, 1));
    const keyColumn = (k, keep) => directKeys(keep, below[k], sizes[k], steps[k], offsets[k]); // key k of each kept group (codes for text)
    if (keys.length === 1 && offsets[0] === 0 && steps[0] === 1) return { K, data: this.cols.get(keys[0]).data, enc: this.cols.get(keys[0]).enc ?? null, keyColumn, owned: false, sorted: true };
    const term = (k) => {
      const t = offsets[k] ? `(${keys[k]} - ${offsets[k] < 0 ? `(${offsets[k]})` : offsets[k]})` : keys[k];
      return steps[k] === 1 ? t : `(${t} / ${steps[k]})`;
    };
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
    const keyColumn = (k, keep) => stridedKeys(kv, keep, nk, k); // key k of each kept group (codes for text)
    return { K, data, keyColumn, owned: true, sorted: false };
  }

  // The same numbering on every thread at once (see generateHashGroups): one
  // shared table, sized from an estimate of the number of groups (Chao1 on a
  // sample of rows); if more groups turn up, again with a bigger table.
  // Group numbers follow first arrival, not row order (any order is fine:
  // the groups are sorted by key afterwards).
  *#hashGroupsParallelG(keys, est) { // (est: groups expected, if known)
    const { engine, n } = this;
    const nk = keys.length, t = this.type === Float32Array ? 'f32' : 'f64';
    const cols = keys.map((k) => this.cols.get(k).data);
    let limit = Math.min(n, engine._hashLimit ?? (est === undefined ? this.#estimateGroups(cols) : Math.ceil(est * 1.5) + 1024)); // (_hashLimit: tests)
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
      const keyColumn = (k, keep) => stridedKeys(engine.live(keyBits), keep, nk, k); // key k of each kept group (codes for text)
      const keyArrayG = function* (k, keep, m) { // the same into a new engine array, on every thread (keep: an engine Uint32Array)
        engine._tableKernels ??= new Map();
        let s = engine._tableKernels.get('stridedkeys');
        if (!s) { s = engine._register(generateStridedKeys(engine.isShared)); engine._tableKernels.set('stridedkeys', s); }
        const dst = engine.alloc(Float64Array, m);
        if (m) yield* engine._unitsG(s, m, [engine.live(keyBits).byteOffset, engine.live(keep).byteOffset, engine.live(dst).byteOffset, nk, k]);
        return dst;
      };
      return { K, data, keyColumn, keyArrayG, owned: true, sorted: false, extra: [keyBits] };
    }
  }

  // Estimated number of distinct keys (tuples), from up to 65,536 sampled
  // rows: Chao1 = seen + once^2 / (2 * twice); plenty of room is cheap.
  #estimateGroups(cols) {
    const live = cols.map((c) => this.engine.live(c));
    const m = Math.min(this.n, 65_536), step = this.n / m;
    const counts = sampleCounts(live, this.type === Float32Array, m, step);
    let once = 0, twice = 0;
    for (const c of counts.values()) { if (c === 1) once++; else if (c === 2) twice++; }
    const est = counts.size + (twice ? (once * once) / (2 * twice) : once * (this.n / m));
    return Math.ceil(est * 1.5) + 1024;
  }

  // Run the group kernel over group numbers `groups.data` (0..K-1).
  *#runGroupsG(keys, groups, allItems, merged) {
    const { engine } = this;
    const { K } = groups;
    // Over a column with no missing values, count(x) is the row count (always
    // counted) and sum / mean / min / max need no check for missing values:
    // SQL's SUM(x) is sum(x) and count(x), so this takes a whole accumulator off.
    const full = new Set();
    for (const it of allItems) if (!DISTINCT.has(it.fn) && (yield* this.#noMissingG(it.raw))) full.add(it);
    const rowCount = (it) => it.fn === 'count' && (!it.arg || full.has(it));
    const items = allItems.filter((it) => !DISTINCT.has(it.fn) && !rowCount(it));
    // means need the count of values present: one more kernel item each
    const kernelItems = [...items.map((it) => (full.has(it) ? { fn: it.fn, arg: it.raw } : it)),
      ...items.filter((it) => it.present && !full.has(it)).map((it) => ({ fn: 'count', arg: it.present, of: it }))]; // means, mins, maxes
    const totals = yield* this.#groupTotalsG(groups, kernelItems, merged);
    if (!totals) return null; // (lookup groups the kernels can't take)
    const totalOf = (it) => (rowCount(it) ? totals[0] : totals[1 + items.indexOf(it)]);
    const presentOf = (it) => (full.has(it) ? totals[0] : totals[1 + kernelItems.findIndex((k) => k.of === it)]);
    // Keep groups that have rows, sorted by key (codes for text keys, so
    // dictionary order; NaN last), and build whole columns.
    let keep = keptGroups(totals[0], K);
    const m = keep.length;
    for (const it of allItems) { // distinct counts per group: from (group, value) pairs
      if (!DISTINCT.has(it.fn)) continue;
      let gData = groups.data;
      if (groups.enc) { // a packed key: group numbers as plain numbers
        gData = engine.alloc(this.type, this.n);
        yield* engine.evalG('x', { x: { data: groups.data, enc: groups.enc, n: this.n, type: this.type === Float32Array ? 'f32' : 'f64' } }, gData);
      }
      try { it.perGroup = yield* this.#distinctCountsG(it.arg, merged, gData, K); } finally { if (gData !== groups.data) engine.free(gData); }
    }
    // each item's value for every group number
    const perGroup = allItems.map((it) => {
      const v = DISTINCT.has(it.fn) ? it.perGroup : totalOf(it);
      return it.fn === 'mean' ? ratioOf(v, presentOf(it)) : it.fn === 'min' || it.fn === 'max' ? presentOnly(v, presentOf(it)) : v;
    });
    if (engine.jit && m >= PARALLEL_SORT_ROWS) return yield* this.#groupTableG(keys, groups, allItems, perGroup, keep);
    let codes = keys.map((_, k) => groups.keyColumn(k, keep));
    if (!groups.sorted) {
      const idx = sortIndex(codes, m);
      keep = gatherU32(keep, idx);
      codes = codes.map((a) => gatherF64(a, idx));
    }
    // (text keys keep their codes as numbers: a code with no label, NaN, stays NaN)
    const cols = new Map(), made = [];
    const toEngine = (a) => { const x = engine.alloc(Float64Array, m); engine.live(x).set(a); made.push(x); return x; };
    try {
      keys.forEach((key, k) => {
        const src = groups.keyMeta?.[k] ?? this.cols.get(key);
        cols.set(key, { data: toEngine(codes[k]), dict: src.dict ?? null, codes: src.dict ? src.codes : null, date: src.date || undefined });
      });
      allItems.forEach((it, j) => cols.set(it.out, { data: toEngine(gatherF64(perGroup[j], keep)), dict: null, codes: null }));
      for (const c of cols.values()) c.data = engine.live(c.data);
      made.length = 0;
      return new Table(engine, cols, m, { owned: [...cols.keys()] });
    } finally {
      for (const x of made) engine.free(x);
    }
  }

  // (see #runGroupsG) Many groups: the result columns made in engine memory
  // on every thread. keep: the group numbers kept, ascending; their keys are
  // sorted (unless they are in order already), then every column gathered
  // in that order.
  *#groupTableG(keys, groups, allItems, perGroup, keep) {
    const { engine } = this;
    const m = keep.length, made = [];
    const toEngine = (a, Type = Float64Array) => { const x = engine.alloc(Type, a.length); engine.live(x).set(a); made.push(x); return x; };
    const owned = (x) => { made.splice(made.indexOf(x), 1); return x; };
    try {
      const keepE = toEngine(keep, Uint32Array), keyArrs = [];
      for (let k = 0; k < keys.length; k++) {
        if (groups.keyArrayG) { const a = yield* groups.keyArrayG(k, keepE, m); made.push(a); keyArrs.push(a); } else keyArrs.push(toEngine(groups.keyColumn(k, keep)));
      }
      const order = groups.sorted ? null : yield* groupPermG(engine, keyArrs, m); // (row r of the result: kept group order[r])
      if (order) made.push(order);
      const at = order ? yield* gatherG(engine, keepE, order, m, 'f32') : keepE; // (group numbers in result order; 4-byte copies)
      if (order) made.push(at);
      const cols = new Map();
      keys.forEach((key, k) => {
        const src = groups.keyMeta?.[k] ?? this.cols.get(key);
        cols.set(key, { data: null, dict: src.dict ?? null, codes: src.dict ? src.codes : null, date: src.date || undefined, k });
      });
      for (const c of cols.values()) { c.data = order ? yield* gatherG(engine, keyArrs[c.k], order, m, 'f64') : owned(keyArrs[c.k]); delete c.k; }
      for (const [j, it] of allItems.entries()) cols.set(it.out, { data: yield* gatherG(engine, toEngine(perGroup[j]), at, m, 'f64'), dict: null, codes: null });
      for (const c of cols.values()) c.data = engine.live(c.data);
      return new Table(engine, cols, m, { owned: [...cols.keys()] });
    } finally {
      for (const x of made) engine.free(x);
    }
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
      yield* engine.evalG(this.#bind(group, merged, true).src, this.#vars(merged), gData);
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
        if (!part) return null;
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
    const look = groups.lookup ?? null; // (join keys looked up as group numbers, see _lookupGroupAgg)
    if (look && big) return null; // (many groups: every thread would scan every row; the usual join path)
    const encKey = JSON.stringify([encs, keyEnc, !!look]);
    const lookArgs = look ? [engine.live(look.lut).byteOffset, look.min, look.size] : [];
    if (big && engine.jit && J * K <= (engine._maxGroupSlots ?? MAX_GROUP_SLOTS)) {
      // many groups: each thread adds up its own range of groups, all rows
      const acc = engine.alloc(Float64Array, J * K);
      analysis.items.forEach((it, j) => engine.live(acc).fill(IDENTITY[it.reduce], j * K, (j + 1) * K));
      const cacheKey = `grouprange|${t}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}|${encKey}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, 1, engine.isShared, true, encs, keyEnc, !!look));
        engine._tableKernels.set(cacheKey, v);
      }
      const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), groups.data];
      yield* engine._groupRangeG(v, this.n, arrays, [...scalarNames.map((nm) => merged[nm]), ...lookArgs], engine.live(acc).byteOffset, K);
      const a = engine.live(acc);
      totals = analysis.items.map((_, j) => a.slice(j * K, (j + 1) * K));
      engine.free(acc);
    } else if (slotsNeeded > (engine._maxGroupSlots ?? MAX_GROUP_SLOTS) || (!this.pred && big)) {
      if (look) return null; // (the scatter reads group numbers: the caller joins the usual way)
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
          v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, C, engine.isShared, false, encs, keyEnc, !!look));
          engine._tableKernels.set(cacheKey, v);
        }
        const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), groups.data];
        yield* engine._groupG(v, this.n, arrays, [...scalarNames.map((nm) => merged[nm]), ...lookArgs], acc.byteOffset, block * 8, K);
      } else {
        if (look) { engine.free(acc); return null; } // (the interpreter has no lookup)
        // the calling thread fills thread 0's block
        const cols = Object.fromEntries(arrayNames.map((nm) => [nm, this.cols.get(nm).data]));
        interpretGroup(engine, t, analysis.items, predAst, cols, merged, groups.data, acc, K, this.n);
      }

      // Combine threads (and copies) into one value per group and aggregation:
      // the thread blocks by fused passes added into thread 0's, 15 blocks a
      // pass (kernels, so fast from the first query; small ones run on this
      // thread), then the C copies of each group by a plain loop.
      totals = [];
      for (const [j, it] of analysis.items.entries()) {
        let from = T;
        if (T > 1 && engine.jit) {
          for (let s0 = 1; s0 < T; s0 += 15) {
            const vars = { t0: acc.subarray(...slots(0, j)) };
            for (let s = s0; s < Math.min(T, s0 + 15); s++) vars[`t${s}`] = acc.subarray(...slots(s, j));
            const vnames = Object.keys(vars);
            const expr = it.reduce === 'min' || it.reduce === 'max'
              ? vnames.slice(1).reduce((e, nm) => `${it.reduce}(${e}, ${nm})`, vnames[0])
              : vnames.join(' + ');
            yield* engine.evalG(expr, vars, vars.t0);
          }
          from = 1;
        }
        const a = engine.live(acc);
        if (C === 1 && from === 1) { const [lo, hi] = slots(0, j); totals.push(a.slice(lo, hi)); continue; }
        const op = it.reduce === 'min' ? Math.min : it.reduce === 'max' ? Math.max : (x, y) => x + y;
        const out = new Float64Array(K).fill(IDENTITY[it.reduce]);
        for (let s = 0; s < from; s++) {
          const [lo] = slots(s, j);
          for (let g = 0; g < K; g++) for (let c = 0; c < C; c++) out[g] = op(out[g], a[lo + g * C + c]);
        }
        totals.push(out);
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
    if (engine.jit) { // whole numbers in a small range: a byte per (group, value) instead of the hash table
      let dense = null;
      try {
        dense = yield* this.#denseDistinctG(plain ? plain.data : own, groupData, K);
        if (!dense && n >= (engine._partitionRows ?? PARTITION_ROWS) && !engine._noPartitionedDistinct) dense = yield* this.#partitionedDistinctG(plain ? plain.data : own, groupData, K);
        if (!dense && n >= 4096) dense = yield* this.#numberedDistinctG(plain ? plain.data : own, groupData, K);
      } catch (err) { if (own) engine.free(own); throw err; }
      if (dense) { if (own) engine.free(own); return dense; }
    }
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

  // Distinct counts per group (see #distinctCountsG) when the values are whole
  // numbers spanning a range small enough for one byte per (group, value):
  // every thread marks its rows' pairs in one byte map (generateMarkPairs),
  // then each group's marks are counted. Else null.
  *#denseDistinctG(values, groupData, K) {
    const { engine, n } = this;
    const [lo, hi, frac] = yield* engine.evalG(['min(v == v ? v : 1 / 0)', 'max(v == v ? v : -1 / 0)', 'count(v == v && v != floor(v))'], { v: values }); // (missing values skipped)
    if (!(hi >= lo) || frac > 0) return hi >= lo ? null : new Float64Array(K); // (no values: no distinct ones)
    const fits = (span) => Math.ceil((span + 1) / 8) * 8 * K <= Math.min(2 ** 26, Math.max(2 ** 20, 16 * n));
    // too wide, but maybe every value is lo plus a multiple of one step (IDs 1000, 1010, 1020...):
    // the step from a sample (it divides the real one), checked on every row
    let step = 1;
    if (!fits(hi - lo) && hi - lo < 2 ** 53 && !engine._noStride) {
      step = sampleStep(engine.live(values), n, lo);
      if (!(step > 1 && fits((hi - lo) / step))) return null;
    }
    const width = Math.ceil(((hi - lo) / step + 1) / 8) * 8; // (each group's bytes start on an 8-byte boundary)
    if (!(width * K <= Math.min(2 ** 26, Math.max(2 ** 20, 16 * n)))) return null;
    const t = this.type === Float32Array ? 'f32' : 'f64';
    engine._tableKernels ??= new Map();
    const name = `markpairs|${t}|${step > 1}`;
    let v = engine._tableKernels.get(name);
    if (!v) { v = engine._register(generateMarkPairs(t, engine.isShared, step > 1)); engine._tableKernels.set(name, v); }
    const map = engine.alloc(Uint8Array, width * K);
    try {
      engine.live(map).fill(0);
      const { parts } = yield* engine._fusedG(v, n, [groupData ?? values, values], [engine.live(map).byteOffset, width, lo, groupData ? 1 : 0, step], 1);
      if (step > 1 && parts.some((x) => x > 0)) return null; // (some value off the step: checked in f64 as it marks, so float32 tables too)
      return countMarks(engine.live(map), K, width);
    } finally {
      engine.free(map);
    }
  }

  // Distinct counts of any values with many of them (see partition.mjs): the
  // distinct (group, value) pairs are the groups of a partitioned group-by
  // with nothing to add up. One group: only how many there are (no sort, no
  // result columns). Missing values make one pair per group, taken off.
  // Null when few distinct pairs are expected (the hash path is cheaper) or
  // the partitioned path turns them down.
  *#partitionedDistinctG(values, groupData, K) {
    const { engine, n } = this;
    const cols = new Map();
    if (groupData) cols.set('g', { data: groupData, dict: null, codes: null });
    cols.set('v', { data: values, dict: null, codes: null });
    const tmp = new Table(engine, cols, n, { zones: false });
    const keys = groupData ? ['g', 'v'] : ['v'];
    const est = estimateGroups(tmp, keys);
    if (est < (engine._partitionGroups ?? PARTITION_GROUPS)) return null;
    if (!groupData) {
      const r = yield* partitionedGroupsG(tmp, keys, [], {}, est, { countOnly: true });
      if (!r) return null;
      const [missing] = yield* engine.evalG(['count(v != v)'], { v: values });
      return Float64Array.of(r.G - (missing > 0 ? 1 : 0));
    }
    const r = yield* partitionedGroupsG(tmp, keys, [], {}, est);
    if (!r) return null;
    try { // the pairs come sorted by group, then value (NaN last): each group's run, found by binary search
      const counts = new Float64Array(K), gs = engine.live(r.keyCols[0]), vs = engine.live(r.keyCols[1]);
      // galloping from the last run's end: O(K log(G / K)), not O(K log G) (1M groups: 90 ms -> 16 ms)
      const after = (g, s) => {
        let a = s, b = s, step = 1;
        while (b < r.G && gs[b] <= g) { a = b + 1; b = s + step; step *= 2; }
        if (b > r.G) b = r.G;
        while (a < b) { const m = (a + b) >>> 1; if (gs[m] <= g) a = m + 1; else b = m; }
        return a;
      };
      for (let g = 0, s = 0; g < K; g++) {
        const e = after(g, s);
        counts[g] = e - s - (e > s && vs[e - 1] !== vs[e - 1] ? 1 : 0);
        s = e;
      }
      return counts;
    } finally {
      for (const x of [...r.keyCols, ...r.accCols]) engine.free(x);
    }
  }

  // Distinct counts of any values: each distinct value numbered by the hash
  // table (every thread; missing values left out), then the numbers counted
  // as in #denseDistinctG. Null when that byte map would be too big.
  *#numberedDistinctG(values, groupData, K) {
    const { engine, n } = this;
    const one = new Table(engine, new Map([['v', { data: values, dict: null, codes: null }]]), n, { zones: false });
    const ids = yield* one.#hashGroupsParallelG(['v']);
    for (const x of ids.extra ?? []) engine.free(x);
    try {
      if (K * Math.ceil(ids.K / 8) * 8 > Math.min(2 ** 26, Math.max(2 ** 20, 16 * n))) return null;
      yield* engine.evalG('v == v ? id : 0 / 0', { v: values, id: ids.data }, ids.data); // (missing values: no number)
      return yield* this.#denseDistinctG(ids.data, groupData, K);
    } finally {
      engine.free(ids.data);
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
    for (const nm of keep) if (!this.cols.has(nm) || this.cols.get(nm).hidden) throw new Error(`vectro: no column "${nm}"`);
    const cols = new Map([...keep].map((nm) => [nm, this.cols.get(nm)])); // in the order asked
    if (!this.pred) return new Table(this.engine, cols, this.n, { params: this.params, zones: this.zones });
    // the filter's other columns: kept for it, hidden, under internal names (the user's names stay free)
    const ast = parse(this.pred), renamed = new Map();
    let k = 0;
    walk(ast, (m) => {
      if (m.t !== 'var' || !this.cols.has(m.name) || keep.has(m.name)) return;
      const c = this.cols.get(m.name);
      if (c.hidden) { cols.set(m.name, c); return; } // (already internal: a text filter's 0 / 1, an earlier select's)
      if (!renamed.has(m.name)) {
        let nm;
        do nm = `__vectro_kept_${k++}`; while (this.cols.has(nm) || cols.has(nm));
        renamed.set(m.name, nm);
        cols.set(nm, hiddenView(c));
      }
      m.name = renamed.get(m.name);
    });
    return new Table(this.engine, cols, this.n, { pred: renamed.size ? toSource(ast) : this.pred, params: this.params, zones: this.zones });
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

  *#textmapG(name, { inputs = [], fn, text = true, date = false, labels = null, state = { dict: [], codes: new Map() } }) {
    checkName(name);
    const { engine } = this;
    const t = this.pred ? yield* this.#collectG() : this;
    if (t.cols.has(name)) throw new Error(`vectro: column "${name}" already exists`);
    const ins = inputs.map((k) => t.cols.get(k) ?? (() => { throw new Error(`vectro: no column "${k}"`); })());
    if (labels) { // codes already: copied (on every thread), with the labels
      const data = engine.alloc(t.type, t.n);
      if (t.n) yield* engine.evalG('x', { x: ins[0].enc ? { data: ins[0].data, enc: ins[0].enc, n: t.n, type: t.type === Float32Array ? 'f32' : 'f64' } : ins[0].data }, data);
      const cols = new Map(t.cols), owned = t === this ? [] : [...t.owned];
      if (t !== this) t.owned = [];
      cols.set(name, { data, dict: state.dict, codes: state.codes });
      owned.push(name);
      for (const c of cols.values()) c.data = engine.live(c.data);
      return new Table(engine, cols, t.n, { owned, ownedBuffers: t === this ? [] : t.ownedBuffers.splice(0), zones: false });
    }
    const ranges = [], made = []; // number columns' [min, max, values not whole] (every thread)
    for (let k = 0; k < ins.length; k++) {
      const c = ins[k];
      if (c.dict || c.strings || c.enc || !t.n) { ranges.push(null); continue; } // (packed: read value by value)
      const r = yield* engine.evalG(['min(k)', 'max(k)', 'count(k == k && k != floor(k))'], { k: c.data });
      if (r && !r[2] && r[1] - r[0] < 2 ** 22) { ranges.push(r); continue; }
      // others: numbered by the hash group kernel (every thread), each number's value once
      const one = new Table(engine, new Map([['v', c]]), t.n, { zones: false });
      const ids = engine.jit && t.n >= 4096 ? yield* one.#hashGroupsParallelG(['v']) : one.#hashGroups(['v']);
      made.push(ids.data);
      const vals = ids.keyColumn(0, Array.from({ length: ids.K }, (_, g) => g));
      for (const x of ids.extra ?? []) engine.free(x); // (the key values: read just above)
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

  *#windowG({ partition = [], order = [], items, firstRanks = null }) {
    const { engine } = this;
    if (items.length && order.length === 1 && !order[0].desc && items.every((it) => (it.fn === 'percentile_cont' || it.fn === 'percentile_disc') && it.arg === order[0].name) && !this.cols.get(order[0].name)?.dict) return yield* this.#percentilesG(partition, items); // (text: by its sorted labels, below)
    if (firstRanks && order.length) { // only rows that can rank in the first k (see plan.mjs firstRanks)
      const few = yield* this.#firstRowsG(partition, order[0], firstRanks);
      if (few) { try { return yield* few.#windowG({ partition, order, items }); } finally { few.dispose(); } }
    }
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
    const b = bounds(n, partition.map(values), order.map((o) => values(o.name)), readsPeers(items));
    const orderKey = order.length === 1 ? values(order[0].name) : null;
    const cols = new Map(t.cols), owned = t === this ? [] : [...t.owned];
    if (t !== this) t.owned = [];
    const KEEPS_KIND = new Set(['lag', 'lead', 'first_value', 'last_value', 'nth_value', 'min', 'max', 'percentile_disc']);
    for (const it of items) {
      if (cols.has(it.name)) throw new Error(`vectro: column "${it.name}" already exists`);
      const src = it.arg && KEEPS_KIND.has(it.fn) ? t.cols.get(it.arg) : null; // (text stays text, dates stay dates)
      let arg = it.arg ? values(it.arg) : null, codeOf = null;
      if ((it.fn === 'min' || it.fn === 'max') && src?.dict) { // text: by its labels' order (missing text skipped), then back to codes
        const rank = labelRanks(src.dict);
        codeOf = new Float64Array(src.dict.length);
        rank.forEach((r, code) => { if (r === r) codeOf[r] = code; });
        arg = arg.map((code) => (code === code ? rank[code] : NaN));
      }
      let vals = windowValues(n, b, { ...it, arg }, order.length > 0, orderKey, order[0]?.desc);
      if (codeOf) vals = vals.map((r) => (r === r ? codeOf[r] : NaN));
      let dict = src?.dict ?? null, codes = src?.codes ?? null;
      if (dict && vals.includes(NaN)) { // no row (LAG past the start, an empty frame): missing text, ''
        let e = codes.get('');
        if (e === undefined) { dict = [...dict, '']; codes = new Map(codes).set('', dict.length - 1); e = dict.length - 1; }
        for (let i = 0; i < n; i++) if (vals[i] !== vals[i]) vals[i] = e;
      }
      const data = engine.alloc(t.type, n);
      engine.live(data).set(vals);
      cols.set(it.name, { data, dict, codes, date: src?.date || (it.fn === 'percentile_cont' && t.cols.get(it.arg)?.date) || undefined });
      owned.push(it.name);
    }
    for (const c of cols.values()) c.data = engine.live(c.data);
    return new Table(engine, cols, n, { owned, zones: false });
  }

  // The rows that can be among the first k of their partition in the window's
  // order: those whose first order key is at most the partition's k-th
  // smallest (all its ties too), as a new table; null when that isn't fewer
  // rows. Every row ordered before a kept row is kept, so row numbers and
  // ranks computed on these rows alone are the same.
  *#firstRowsG(partition, first, k) {
    const { engine } = this;
    const t = yield* this.#collectG();
    const made = []; // engine arrays to free
    try {
      const n = t.n, f32 = t.type === Float32Array, c = t.cols.get(first.name);
      if (n < 4096 || !c || c.strings || partition.some((p) => !t.cols.has(p))) return null;
      const parallel = engine.jit && !f32;
      // each row's partition number 0..K-1 (as float64 for the kernels)
      let K = 1, g = null;
      if (partition.length) {
        const ids = (yield* t.#directGroups(partition, 1)) ?? (engine.jit ? yield* t.#hashGroupsParallelG(partition) : t.#hashGroups(partition));
        for (const x of ids.extra ?? []) engine.free(x);
        K = ids.K;
        if (ids.owned) made.push(ids.data);
        if (ids.enc || !parallel) {
          const d = engine.live(ids.data), plain = parallel ? engine.alloc(Float64Array, n) : null, out = plain ? engine.live(plain) : new Int32Array(n);
          if (ids.enc) for (let i = 0; i < n; i++) out[i] = decodeOne(ids.enc, d[i], f32); else for (let i = 0; i < n; i++) out[i] = d[i];
          g = plain ?? out;
          if (plain) made.push(plain);
        } else g = ids.data;
      }
      if (K * k > 2 ** 24 || K * k * 2 > n) return null;
      let keep;
      if (parallel) {
        if (!g) { g = engine.alloc(Float64Array, n); engine.live(g).fill(0); made.push(g); }
        const plainKey = !c.dict && !c.enc;
        const key = plainKey ? c.data : engine.alloc(Float64Array, n);
        if (!plainKey) { engine.live(key).set(firstKey(engine, c, first.desc, n, false)); made.push(key); }
        keep = yield* firstRowsParallelG(engine, key, plainKey && first.desc, g, n, K, k);
      } else keep = firstRows(firstKey(engine, c, first.desc, n, f32), g, n, K, k);
      return keep.length * 2 > n ? null : t.#gather(keep, keep.length);
    } finally {
      for (const x of made) engine.free(x);
      if (t !== this) t.dispose();
    }
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
    let K = 1, g = null, gData = null; // each row's partition: JS int32, and in engine memory while needed
    const made = [];
    if (partition.length && n) {
      for (const k of partition) if (!t.cols.has(k)) throw new Error(`vectro: no column "${k}"`);
      const ids = (yield* t.#directGroups(partition, 1)) ?? (engine.jit && n >= 4096 ? yield* t.#hashGroupsParallelG(partition) : t.#hashGroups(partition));
      for (const x of ids.extra ?? []) engine.free(x);
      if (ids.owned) made.push(ids.data);
      K = ids.K;
      g = ids.enc ? Int32Array.from(plainOf({ enc: ids.enc }, ids.data)) : toInt32(engine.live(ids.data), n);
      if (!ids.enc && !f32) gData = ids.data;
    }
    try {
      return yield* t.#percentileColumnsG(items, K, g, gData, t !== this);
    } finally {
      for (const x of made) engine.free(x);
    }
  }

  // (see #percentilesG) The new columns: each item's value for its row's
  // partition. temp: this table is a copy, its columns can move to the result.
  *#percentileColumnsG(items, K, g, gData, temp) {
    const { engine } = this;
    const t = this, n = t.n, f32 = t.type === Float32Array;
    const plainOf = (c, data = c.data) => { const d = engine.live(data), out = new Float64Array(n); if (c.enc) for (let i = 0; i < n; i++) out[i] = decodeOne(c.enc, d[i], f32); else out.set(d.subarray(0, n)); return out; };
    const cols = new Map(t.cols), owned = temp ? [...t.owned] : [];
    if (temp) t.owned = [];
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
      const c = t.cols.get(arg);
      let got = null;
      if (engine.jit && !f32 && n >= 65_536 && K <= 64 && c && !c.enc && !c.strings && (gData || K === 1)) got = yield* t.#percentilesParallelG(c.data, gData, K, its);
      got ??= percentiles(valuesOf(arg), g, K, its);
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

  // percentiles (below) for few partitions on every thread: each value's
  // bucket (4096 per partition, evenly over the values' range; missing
  // values in one more) is counted with the group kernel, the buckets
  // holding the wanted ranks found from the counts, and only their values
  // collected and selected among. values, gData: float64 engine arrays (gData
  // null: one partition). null when the values aren't all finite.
  *#percentilesParallelG(values, gData, K, items) {
    const { engine, n } = this;
    const [lo, hi] = yield* engine.evalG(['min(v == v ? v : 1 / 0)', 'max(v == v ? v : -1 / 0)'], { v: values });
    if (!(hi >= lo)) return items.map(() => new Float64Array(K).fill(NaN));
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return null;
    const B = 4096, W = B + 1, vars = { v: values, lo, sc: hi > lo ? B / (hi - lo) : 0 };
    if (gData) vars.g = gData;
    const bucket = `(v == v ? (floor((v - lo) * sc) >= ${B} ? ${B - 1} : floor((v - lo) * sc)) : ${B})`;
    const keys = engine.alloc(Float64Array, n);
    try {
      yield* engine.evalG(gData ? `g * ${W} + ${bucket}` : bucket, vars, keys);
      const [counts] = yield* this.#groupTotalsG({ K: K * W, data: keys }, [], {});
      const sizes = new Int32Array(K);
      for (let k = 0; k < K; k++) { let m = 0; for (let b = k * W, e = b + B; b < e; b++) m += counts[b]; sizes[k] = m; }
      const { plan, wants } = percentileRanks(sizes, items);
      // each wanted rank's bucket and its rank there
      const chosen = new Map(), place = wants.map(([k, r]) => {
        let b = k * W, below = 0;
        while (below + counts[b] <= r) below += counts[b++];
        if (!chosen.has(b)) chosen.set(b, chosen.size);
        return [chosen.get(b), r - below];
      });
      const got = new Float64Array(wants.length);
      if (wants.length) {
        const cols = new Map([['k', { data: keys, dict: null, codes: null }], ['v', { data: values, dict: null, codes: null }]]);
        const pred = [...chosen.keys()].map((b) => `k == ${b}`).join(' || ');
        const few = yield* new Table(engine, cols, n, { pred, zones: false }).#collectG();
        try {
          const fk = engine.live(few.cols.get('k').data), fv = engine.live(few.cols.get('v').data);
          const bufs = [...chosen.keys()].map((b) => new Float64Array(counts[b])), fill = new Int32Array(bufs.length);
          for (let i = 0; i < few.n; i++) { const j = chosen.get(fk[i]); bufs[j][fill[j]++] = fv[i]; }
          place.forEach(([j, r], w) => { got[w] = select(bufs[j], 0, bufs[j].length, r); });
        } finally {
          few.dispose();
        }
      }
      return percentileValues(plan, got);
    } finally {
      engine.free(keys);
    }
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
    // rows in one part only (an outer join's unmatched rows are often none): that part as it is
    const full = parts.filter((p) => p.n > 0);
    const keep = full.length === 1 ? full[0] : null;
    try { return keep ?? copyRows(this.engine, parts.map((t) => [t, 0, t.n])); } finally { for (const p of parts) if (p !== keep && p.owned.length) p.dispose(); }
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
        for (const [name, c] of this.cols) if (!c.hidden) out.set(name, { ...c, data: yield* gatherG(engine, c.data, perm, n, t, c.enc), zones: undefined, ...(c.enc ? { rows: n } : {}) }); // (packed stays packed)
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
      if (omit?.has(name) || c.hidden) continue;
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
    if (typeof k !== 'number' || k !== k) throw new TypeError(`vectro: top(k, by) needs k, the number of rows to keep, got ${typeof k === 'number' ? k : JSON.stringify(k) ?? k}`);
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
    this.#bind(keys[0].text, {}); // (an unknown column: reported with this table's columns, not the sample's)

    // A cutoff from an evenly spaced sample: the first key is computed for the
    // sampled rows only. The full passes (count, then collect) are fused
    // filters "first key <= cutoff" on all threads; nothing else is stored.
    const S = Math.min(20_000, Math.max(1_024, Math.ceil(n / 64))), stride = Math.max(1, Math.floor(n / S));
    const picked = Uint32Array.from({ length: Math.ceil(n / stride) }, (_, i) => i * stride);
    // copy only the columns the first key reads (a table can be very wide)
    const used = new Set();
    walk(parse(keys[0].text), (m) => { if (m.t === 'var' && this.cols.has(m.name)) used.add(m.name); });
    // a first key that reads no column is the same on every row: the other keys decide (none: table order)
    if (!used.size) return yield* (keys.length > 1 ? this.#topG(k, [by].flat().slice(1)) : this.#headG(k));
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
      if (c.hidden) continue;
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
    const cols = [...t.cols].filter(([, c]) => !c.hidden).map(([name, c]) => [name, c, engine.live(c.data), c.strings ? engine.live(c.strings) : null]);
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
  join(other, opts) { return this.engine._drive(this.#joinOuterG(other, opts)); }
  joinAsync(other, opts) { return this.engine._driveAsync(this.#joinOuterG(other, opts)); }

  // (SQL RIGHT / FULL JOIN) opts.unmatched: an object the join puts the right
  // rows no left row matched in, as a table (unmatched.table), from the
  // probe's own matches.
  *#joinOuterG(other, opts) {
    const u = opts?.unmatched;
    if (u) u.result = null;
    const out = yield* this.#joinG(other, opts);
    if (u) {
      if (!u.result) { out?.dispose?.(); throw new Error('vectro: join: no unmatched rows were made'); }
      const { cols, n } = u.result;
      u.result = null;
      u.table = new Table(this.engine, cols, n, { owned: [...cols.keys()] });
    }
    return out;
  }

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
    if (this.#hasPacked()) { // (decoded first; a view join keeps the decoded columns it shows)
      const t = yield* this.#decodedG();
      let out;
      try {
        out = yield* t.#joinG(other, opts);
        const at = (c) => c?.data && this.engine.live(c.data).byteOffset;
        const kept = t.owned.filter((nm) => at(out.cols.get(nm)) === at(t.cols.get(nm)));
        if (kept.length) { out.owned.push(...kept); t.owned = t.owned.filter((nm) => !kept.includes(nm)); }
        return out;
      } finally { if (out !== t) t.dispose(); }
    }
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
      const c = this.cols.get(name);
      if (c?.data) this.engine.free(c.data);
      for (const m of MASKS.get(c?.data)?.values() ?? []) this.engine.free(m);
      MASKS.delete(c?.data);
    }
    for (const b of this.ownedBuffers) this.engine.free(b);
    this.owned = [];
    this.ownedBuffers = [];
  }
}

// ---- helpers ---------------------------------------------------------------------

// A column as a hidden one (select's: a column only the filter reads). It reads and writes the
// column itself, so it follows compress() of the table it came from (packed, the old array freed).
const COLUMN_FIELDS = ['data', 'dict', 'codes', 'date', 'strings', 'enc', 'plainType', 'rows', 'zones'];
function hiddenView(c) {
  const v = { hidden: true };
  for (const k of COLUMN_FIELDS) Object.defineProperty(v, k, { get: () => c[k], set: (x) => { c[k] = x; }, enumerable: true, configurable: true });
  return v;
}

const RESERVED = new Set(['Infinity', 'NaN']); // (function names are fine: sum(...) is a call, sum a column)
function checkName(name) {
  if (!IDENT.test(name) || RESERVED.has(name)) throw new Error(`vectro: "${name}" can't be a column name (use letters, digits and _)`);
}

// A category column used as a number (city > 0, price + city, -city) would use its codes, which
// mean nothing as numbers: an error. Checked before rewriteText, which compares codes itself.
// Allowed: == / != (to text in quotes), text functions, isMissing, coalesce, and passing the
// value through (a copy of the column, a branch of ? :).
function textAsNumber(name, src) {
  throw new TypeError(`vectro: ${name} is a text column; compare it with == or != to text in quotes (${name} == 'Paris') or use a text function such as startsWith(${name}, 'P'), but it can't be used as a number, in "${src}"`);
}
// Also an error: text compared with a number (city == 1, city == p), and ? : or coalesce() mixing a
// text column with numbers or with another text column (each has its own codes).
function checkTextUse(ast, cols, src) {
  const PASS = new Set([...Object.keys(TEXT_FNS), 'isMissing', 'coalesce', 'sum', 'count', 'mean', 'min', 'max']); // (reductions: from datasets, see #groupIntoG)
  const textVar = (n) => n.t === 'var' && cols.get(n.name)?.dict;
  const asNumber = (n) => { if (textVar(n)) textAsNumber(n.name, src); visit(n); };
  const mixed = (n) => {
    const kinds = valueKinds(n, cols);
    if (kinds.size > 1 && [...kinds].some((k) => k !== 'number')) {
      throw new TypeError(`vectro: ${n.t === 'cond' ? '? :' : 'coalesce()'} mixes ${kinds.has('number') ? 'a text column and numbers' : 'two text columns'}, in "${src}"; its values must all come from one text column, or all be numbers`);
    }
  };
  const visit = (n) => {
    if (n.t === 'bin' && (n.op === '==' || n.op === '!=')) {
      for (const [a, b] of [[n.l, n.r], [n.r, n.l]]) {
        if (b.t !== 'str' && !textVar(b) && [...valueKinds(a, cols)].some((k) => k !== 'number')) {
          const name = textVar(a) ? a.name : 'text';
          throw new TypeError(`vectro: ${name} is text, compared with ${b.t === 'num' ? `the number ${toSource(b)}` : b.t === 'var' ? `"${b.name}", a number` : 'a number'}, in "${src}" (compare it with text in quotes, ${textVar(a) ? a.name : 'city'} == 'Paris', or with another text column)`);
        }
      }
      visit(n.l); visit(n.r);
    } else if (n.t === 'bin') { const f = n.l.t === 'str' || n.r.t === 'str' ? visit : asNumber; f(n.l); f(n.r); } // (ts >= '2024-06-01': its own error, about dates)
    else if (n.t === 'un') asNumber(n.x);
    else if (n.t === 'cond') { mixed(n); asNumber(n.c); visit(n.x); visit(n.y); }
    else if (n.t === 'call') { if (n.fn === 'coalesce') mixed(n); n.args.forEach(PASS.has(n.fn) && (n.args.length === 1 || !(n.fn === 'min' || n.fn === 'max')) ? visit : asNumber); }
  };
  visit(ast);
}

// What an expression's values are, as ? : and coalesce() pass them through: a text column's
// dictionary, or 'number'.
function valueKinds(n, cols, out = new Set()) {
  if (n.t === 'var' && cols.get(n.name)?.dict) out.add(cols.get(n.name).dict);
  else if (n.t === 'cond') { valueKinds(n.x, cols, out); valueKinds(n.y, cols, out); }
  else if (n.t === 'call' && n.fn === 'coalesce') for (const a of n.args) valueKinds(a, cols, out);
  else out.add('number');
  return out;
}

// One text column's codes in another's dictionary (-1: a label it doesn't have), for ship == bill.
const DICT_IDS = new WeakMap();
let dictIds = 0;
const dictId = (dict) => { if (!DICT_IDS.has(dict)) DICT_IDS.set(dict, dictIds++); return DICT_IDS.get(dict); };
function codesIn(colNode, into, cols) {
  const from = cols.get(colNode.name), to = cols.get(into.name);
  const key = `${colNode.name}|in|${into.name}|${dictId(to.dict)}`;
  if (!LUT_NAMES.has(key)) LUT_NAMES.set(key, `__vectro_text_codes_${LUT_NAMES.size}`);
  const values = Float64Array.from(from.dict, (label) => to.codes.get(label) ?? -1);
  return { t: 'var', name: LUT_NAMES.get(key), lut: { column: colNode.name, values } };
}

// The expression with params written in as numbers (for folding: on == 1 with { on: 1 } reads no column).
function withParams(n, params, cols) {
  switch (n.t) {
    case 'var': return !cols.has(n.name) && typeof params[n.name] === 'number' ? { t: 'num', v: params[n.name] } : n;
    case 'un': return { ...n, x: withParams(n.x, params, cols) };
    case 'bin': return { ...n, l: withParams(n.l, params, cols), r: withParams(n.r, params, cols) };
    case 'call': return { ...n, args: n.args.map((a) => withParams(a, params, cols)) };
    case 'cond': return { ...n, c: withParams(n.c, params, cols), x: withParams(n.x, params, cols), y: withParams(n.y, params, cols) };
  }
  return n;
}

// Missing values (NaN) in aggregations, as in SQL: sum and mean skip them
// (mean divides by the values present), min and max skip them, count(x)
// counts the values present (count(condition) still counts where it holds).
// arg is the bound source; returns the aggregation item.
// (raw: the argument as given; where it is a column with no missing values,
// the group-by uses it as it is and takes counts from the row count)
function missingRules(fn, arg) {
  const present = `(${arg}) == (${arg})`;
  if (fn === 'count') return isMask(parse(arg)) ? { fn, arg } : { fn, arg: present, raw: arg };
  if (fn === 'min') return { fn, arg: `(${present} ? (${arg}) : Infinity)`, present, raw: arg }; // (present: NaN when nothing is)
  if (fn === 'max') return { fn, arg: `(${present} ? (${arg}) : -Infinity)`, present, raw: arg };
  return fn === 'mean' ? { fn, arg: `(${present} ? (${arg}) : 0)`, present, raw: arg } : { fn, arg: `(${present} ? (${arg}) : 0)`, raw: arg }; // sum, mean
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
    if (!(c >= 0 && c < labels.length)) { engine.free(data); throw new RangeError(`vectro: code ${c} at row ${i} has no label (${labels.length} labels)`); }
    data[i] = lut[c];
  }
  return { data, dict, codes };
}

const isDates = (values) => Array.isArray(values) && values.find((v) => v != null) instanceof Date;
// the first value present decides (a column may start with null / undefined: missing)
const firstPresent = (values) => { for (const v of values) if (v !== null && v !== undefined) return v; return undefined; };
const isText = (values) => !ArrayBuffer.isView(values) && values.length > 0 && typeof firstPresent(values) === 'string';

function copyNumbers(engine, type, values) {
  const data = engine.alloc(type, values.length);
  if (Array.isArray(values)) for (let i = 0; i < values.length; i++) { const v = values[i]; data[i] = v === null || v === undefined ? NaN : v; } // (null: missing, as in JSON)
  else data.set(values);
  return data;
}

function encodeText(engine, type, values) {
  const data = engine.alloc(type, values.length);
  const codes = new Map(), dict = [];
  for (let i = 0; i < values.length; i++) {
    const s = values[i] ?? ''; // (null / undefined: missing text, as in JSON)
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
const LUT_NAMES = new Map(); // column|function|args -> the name of its per-row 0 / 1 array
const MASKS = new WeakMap(); // a column's data array -> those arrays made for it (freed with it)

// startsWith(region, 'E') -> (region >= 0 && region <= 1) || region == 5
function textCall(n, cols, src) {
  const [colNode, ...rest] = n.args;
  const c = colNode?.t === 'var' ? cols.get(colNode.name) : null;
  if (c?.strings) throw new Error(`vectro: "${colNode.name}" is a free-text column; ${n.fn}() needs a category column (load it as types: { ${colNode.name}: 'category' })`);
  if (!c?.dict) throw new SyntaxError(`vectro: ${n.fn}() needs a text column first, then text in quotes, in "${src}"`);
  if (!rest.length || rest.some((a) => a.t !== 'str')) throw new SyntaxError(`vectro: ${n.fn}(${colNode.name}, …) needs text in quotes after the column, in "${src}"`);
  if (n.fn !== 'in' && rest.length !== 1) throw new SyntaxError(`vectro: ${n.fn}() takes a column and one text, in "${src}"`);
  const args = rest.map((a) => a.v);
  const re = n.fn === 'like' || n.fn === 'ilike' ? new RegExp(`^${args[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '[\\s\\S]*').replace(/_/g, '[\\s\\S]')}$`, n.fn === 'ilike' ? 'iu' : 'u') : null; // (u: _ is one character, an emoji too)
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
  if (Math.min(yes.length, no.length) > MAX_RANGES) { // too many ranges for one expression: a 0 / 1 per row (see Table #bind)
    const key = `${colNode.name}|${n.fn}|${JSON.stringify(args)}`;
    if (!LUT_NAMES.has(key)) LUT_NAMES.set(key, `__vectro_text_match_${LUT_NAMES.size}`);
    return { t: 'bin', op: '!=', l: { t: 'var', name: LUT_NAMES.get(key), lut: { column: colNode.name, match } }, r: num(0) };
  }
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
    if (n.t === 'call' && n.fn === 'isMissing') {
      if (n.args.length === 1 && textCol(n.args[0])) { // missing text is '': its code (none: never missing)
        const code = cols.get(n.args[0].name).codes.get('');
        return { t: 'bin', op: '==', l: n.args[0], r: { t: 'num', v: code ?? -1 } };
      }
      const x = one(n);
      return { t: 'bin', op: '!=', l: x, r: x };
    }
    if (n.t === 'call' && n.fn === 'coalesce') { // first value that isn't missing
      if (n.args.length < 2) throw new SyntaxError(`vectro: coalesce() needs two values or more, in "${src}"`);
      const present = (x) => (textCol(x) ? { t: 'bin', op: '!=', l: x, r: { t: 'num', v: cols.get(x.name).codes.get('') ?? -1 } } : { t: 'bin', op: '==', l: x, r: x }); // (missing text is '')
      return n.args.map(fix).reduceRight((rest, x) => ({ t: 'cond', c: present(x), x, y: rest }));
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
      if ((n.op === '==' || n.op === '!=') && textCol(n.l) && textCol(n.r) && cols.get(n.l.name).dict !== cols.get(n.r.name).dict) { // ship == bill: by label
        return { ...n, l: codesIn(n.l, n.r, cols), r: n.r };
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
  const { plan, wants } = percentileRanks(sizes, items);
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
  return percentileValues(plan, got);
}

// The ranks each item needs in each partition of sizes[k] values present
// (0 = the smallest): wants = [[partition, rank]...], plan[item][partition] =
// { at (index into wants), frac (between it and the next) } or null.
function percentileRanks(sizes, items) {
  const wants = [];
  const plan = items.map((it) => Array.from(sizes, (m, k) => {
    if (!m) return null;
    if (it.fn === 'percentile_disc') { wants.push([k, Math.max(0, Math.ceil(it.p * m) - 1)]); return { at: wants.length - 1 }; }
    const r = it.p * (m - 1), lo = Math.floor(r);
    wants.push([k, lo]);
    if (lo + 1 < m && r > lo) { wants.push([k, lo + 1]); return { at: wants.length - 2, frac: r - lo }; }
    return { at: wants.length - 1 };
  }));
  return { plan, wants };
}
// Each item's value per partition, from the values found for the wanted ranks.
function percentileValues(plan, got) {
  return plan.map((ws) => Float64Array.from(ws, (w) => (!w ? NaN : w.frac ? got[w.at] + w.frac * (got[w.at + 1] - got[w.at]) : got[w.at])));
}
// Key k of each kept group: from keys stored side by side (kv[g * nk + k]),
// or from a direct group number (g / below % size steps from the offset).
function stridedKeys(kv, keep, nk, k) { const n = keep.length, out = new Float64Array(n); for (let r = 0; r < n; r++) out[r] = kv[keep[r] * nk + k]; return out; }
function directKeys(keep, below, size, step, offset) { const n = keep.length, out = new Float64Array(n); for (let r = 0; r < n; r++) out[r] = (Math.floor(keep[r] / below) % size) * step + offset; return out; }
// Group numbers 0..K-1 with rows (counts[g] > 0), in order.
function keptGroups(counts, K) {
  let m = 0;
  for (let g = 0; g < K; g++) if (counts[g] > 0) m++;
  const keep = new Uint32Array(m);
  for (let g = 0, r = 0; g < K; g++) if (counts[g] > 0) keep[r++] = g;
  return keep;
}
function gatherF64(src, idx) { const n = idx.length, out = new Float64Array(n); for (let r = 0; r < n; r++) out[r] = src[idx[r]]; return out; }
function gatherU32(src, idx) { const n = idx.length, out = new Uint32Array(n); for (let r = 0; r < n; r++) out[r] = src[idx[r]]; return out; }

// The order of m groups by their keys (engine arrays, one per key, first
// most important; NaN last) on every thread, as an engine Uint32Array:
// whole-number keys (codes, IDs) combined into one key when their ranges
// allow (fewer passes).
function* groupPermG(engine, keyArrs, m) {
  let sortKeys = keyArrs, one = null;
  if (keyArrs.length > 1) {
    const vars = Object.fromEntries(keyArrs.map((a, k) => [`k${k}`, a]));
    let span = 1, expr = null;
    for (let k = 0; k < keyArrs.length && span <= 2 ** 53; k++) {
      const [lo, hi, frac] = yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k: keyArrs[k] });
      if (frac > 0 || !Number.isFinite(lo) || !Number.isFinite(hi)) { span = Infinity; break; }
      vars[`lo${k}`] = lo;
      span *= hi - lo + 1;
      expr = expr === null ? `(k${k} - lo${k})` : `(${expr}) * ${hi - lo + 1} + (k${k} - lo${k})`;
    }
    if (span <= 2 ** 53) { // (exact: every combination below 2^53)
      one = engine.alloc(Float64Array, m);
      yield* engine.evalG(expr, vars, one);
      sortKeys = [one];
    }
  }
  try { return yield* sortPermG(engine, sortKeys, m, 'f64'); } finally { if (one) engine.free(one); }
}
function ratioOf(v, p) { const n = v.length, out = new Float64Array(n); for (let g = 0; g < n; g++) out[g] = v[g] / p[g]; return out; } // (means)
function presentOnly(v, p) { const n = v.length, out = new Float64Array(n); for (let g = 0; g < n; g++) out[g] = p[g] ? v[g] : NaN; return out; } // (mins, maxes: NaN where no value)

// How often each key (tuple) shows up among m rows taken every `step` rows:
// keys as one 32-bit hash of their bits (-0 as 0); for an estimate only.
function sampleCounts(cols, f32, m, step) {
  const W = f32 ? 1 : 2, words = cols.map((c) => new Int32Array(c.buffer, c.byteOffset, c.length * W)), counts = new Map();
  for (let j = 0; j < m; j++) {
    const i = Math.floor(j * step);
    let h = 0x811c9dc5;
    for (let k = 0; k < cols.length; k++) {
      if (cols[k][i] === 0) { h = Math.imul(h, 0x01000193); continue; }
      h = Math.imul(h ^ words[k][i * W], 0x01000193);
      if (W === 2) h = Math.imul(h ^ words[k][i * W + 1], 0x01000193);
    }
    counts.set(h, (counts.get(h) ?? 0) + 1);
  }
  return counts;
}

// The greatest common divisor of (v - lo) over up to 4096 values spread
// over the column (NaN skipped): the real step divides it. 1: no step.
function sampleStep(d, n, lo) {
  const m = Math.min(n, 4096), at = n / m;
  let g = 0;
  for (let j = 0; j < m && g !== 1; j++) {
    const v = d[Math.floor(j * at)];
    if (v !== v) continue;
    let a = v - lo, b = g;
    while (b) { const r = a % b; a = b; b = r; }
    g = a;
  }
  return g > 1 ? g : 1;
}

// The marks (0 or 1 bytes) in each group's `width` bytes, 4 at a time.
function countMarks(map, K, width) {
  const w = new Uint32Array(map.buffer, map.byteOffset, (K * width) >> 2), per = width >> 2, out = new Float64Array(K);
  for (let g = 0, i = 0; g < K; g++) {
    let c = 0;
    for (const e = i + per; i < e; i++) c += Math.imul(w[i], 0x01010101) >>> 24;
    out[g] = c;
  }
  return out;
}

// A column's values as numbers sorting ascending in its order (NaN last):
// text by its labels' order, descending keys negated.
function firstKey(engine, c, desc, n, f32) {
  const d = engine.live(c.data), key = new Float64Array(n);
  if (c.dict) { const rank = labelRanks(c.dict, desc); for (let i = 0; i < n; i++) key[i] = rank[c.enc ? decodeOne(c.enc, d[i]) : d[i]]; }
  else if (c.enc) for (let i = 0; i < n; i++) key[i] = (desc ? -1 : 1) * decodeOne(c.enc, d[i], f32);
  else if (desc) for (let i = 0; i < n; i++) key[i] = -d[i]; else key.set(d.subarray(0, n));
  return key;
}

// firstRows (below) on every thread (see rank-kernels.mjs): key and g are
// float64 engine arrays; neg: keys are negated as read.
function* firstRowsParallelG(engine, key, neg, g, n, K, k) {
  engine._tableKernels ??= new Map();
  const kernel = (name, make) => {
    let v = engine._tableKernels.get(name);
    if (!v) { v = engine._register(make(engine.isShared)); engine._tableKernels.set(name, v); }
    return v;
  };
  const top = kernel('firstrows-top', topModule), mark = kernel('firstrows-mark', markModule);
  const slots = engine.alloc(Float64Array, K * (k + 1)), mask = engine.alloc(Uint8Array, n);
  try {
    engine.live(slots).fill(0);
    yield* engine._groupRangeG(top, n, [key, g], [k, neg ? 1 : 0], engine.live(slots).byteOffset, K);
    yield* engine._groupRangeG(mark, n, [key, g, mask], [k, neg ? 1 : 0], engine.live(slots).byteOffset, K);
    return maskedRows(engine.live(mask), n);
  } finally {
    engine.free(slots);
    engine.free(mask);
  }
}
function maskedRows(m, n) {
  let c = 0;
  for (let i = 0; i < n; i++) c += m[i];
  const out = new Uint32Array(c);
  for (let i = 0, j = 0; i < n; i++) if (m[i]) out[j++] = i;
  return out;
}

// Row numbers (in order) whose key is at most the k-th smallest key of their
// partition g[i] (0..K-1; g null: one partition), NaN sorting last; every row
// of a partition with fewer than k rows. Each partition keeps its k smallest
// keys seen so far, sorted, so most rows cost one comparison.
function firstRows(key, g, n, K, k) {
  const top = new Float64Array(K * k), cnt = new Int32Array(K);
  for (let i = 0; i < n; i++) {
    const v = key[i], p = g === null ? 0 : g[i], c = cnt[p], base = p * k;
    let j;
    if (c < k) { j = base + c; cnt[p] = c + 1; } else {
      const last = top[base + k - 1];
      if (last !== last ? v !== v : v !== v || v >= last) continue; // not smaller than the k-th
      j = base + k - 1;
    }
    for (; j > base; j--) { const a = top[j - 1]; if (a !== a ? v !== v : v !== v || v >= a) break; top[j] = a; } // insert, keeping order
    top[j] = v;
  }
  let m = 0;
  for (let pass = 0, out = null; pass < 2; pass++) {
    for (let i = 0; i < n; i++) {
      const v = key[i], p = g === null ? 0 : g[i], last = top[p * k + k - 1];
      if (cnt[p] < k || last !== last || (v === v && v <= last)) { if (out) out[m] = i; m++; }
    }
    if (!pass) { out = new Uint32Array(m); m = 0; } else return out;
  }
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
