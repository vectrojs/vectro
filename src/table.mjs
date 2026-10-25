// Columnar tables on top of the engine. Each column is one array in engine
// memory; text columns are dictionary-encoded (each distinct string gets a
// number code), so `region == 'EU'` becomes a numeric comparison.
//
// Filters are lazy: t.filter(p).sum(e) compiles to one fused pass computing
// sum(p ? e : 0). No filtered copy is made unless you ask with collect().

import { parseKeys, labelRanks, sortIndex } from './sort.mjs';
import { ZONE_ROWS, computeZones, zoneRange } from './zones.mjs';
import { parse, toSource, walk, analyze, isMask } from './fuse.mjs';
import { generateGroupBy, generateCompact } from './table-kernels.mjs';
import { decodeString } from './csv-kernel.mjs';
import { writeArrow } from './arrow.mjs';
import { interpretGroup, interpretCompact, interpretFilteredReduce } from './interp.mjs';

const IDENT = /^[A-Za-z_]\w*$/;
const AGG_FNS = new Set(['sum', 'mean', 'min', 'max', 'count']);
const IDENTITY = { sum: 0, count: 0, mean: 0, min: Infinity, max: -Infinity };
const MAX_GROUP_SLOTS = 2 ** 28; // threads * aggregations * groups (2GB of float64)

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
      return (c.zones ??= computeZones(this.engine.live(c.data), this.n));
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

  static fromColumns(engine, columns, { type = Float64Array } = {}) {
    const entries = Object.entries(columns);
    if (!entries.length) throw new TypeError('vectro: a table needs at least one column');
    const len = (v) => (isCoded(v) ? v.codes.length : v.length);
    const n = len(entries[0][1]);
    const cols = new Map();
    for (const [name, values] of entries) {
      checkName(name);
      if (len(values) !== n) throw new RangeError(`vectro: column "${name}" has ${len(values)} rows, expected ${n}`);
      cols.set(name, isCoded(values) ? codedText(engine, type, values) : isText(values) ? encodeText(engine, type, values) : { data: copyNumbers(engine, type, values), dict: null, codes: null });
    }
    return new Table(engine, cols, n, { owned: [...cols.keys()] });
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
  get type() { return this.cols.values().next().value.data.constructor; }

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
    for (const [name, c] of this.cols) vars[name] = c.data;
    return vars;
  }

  #mergeParams(params) {
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
    return new Table(this.engine, this.cols, this.n, { pred, params: merged });
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
    const cols = new Map(this.cols).set(name, { data: out, dict: null, codes: null });
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params, owned: [name] });
  }

  // Several aggregations in one pass: agg({ revenue: 'sum(price * qty)', n: 'count()' })
  agg(spec, params = {}) { return this.engine._drive(this.#aggG(spec, params)); }
  aggAsync(spec, params = {}) { return this.engine._driveAsync(this.#aggG(spec, params)); }

  *#aggG(spec, params) {
    const narrow = this.#narrow();
    if (narrow !== this) return yield* narrow.#aggG(spec, params);
    const merged = this.#mergeParams(params);
    const P = this.pred;
    const items = Object.entries(spec).map(([out, src]) => ({ out, ...this.#aggItem(src, merged) }));
    if (P && !this.engine.jit) {
      // interpreter: evaluate the filter once and aggregate only matching rows
      const truthy = (ast) => (isMask(ast) ? ast : { t: 'bin', op: '!=', l: ast, r: { t: 'num', v: 0 } });
      const work = items.map((it) => ({
        reduce: it.fn,
        ast: it.arg === null ? null : it.fn === 'count' ? truthy(parse(it.arg)) : parse(it.arg),
      }));
      const t = this.type === Float32Array ? 'f32' : 'f64';
      const values = interpretFilteredReduce(this.engine, t, parse(P), work, this.#vars(merged), merged, this.n);
      return Object.fromEntries(items.map((it, j) => [it.out, values[j]]));
    }
    const reductions = [];
    const slot = (s) => reductions.push(s) - 1;
    for (const it of items) {
      const e = it.arg;
      if (it.fn === 'count' && !P && !e) it.value = this.n; // plain row count: nothing to compute
      else if (it.fn === 'count') it.slot = slot(P ? `count((${P}) && (${e ?? 1}))` : `count(${e ?? 1})`);
      else if (it.fn === 'sum') it.slot = slot(P ? `sum((${P}) ? (${e}) : 0)` : `sum(${e})`);
      else if (it.fn === 'min') it.slot = slot(P ? `min((${P}) ? (${e}) : Infinity)` : `min(${e})`);
      else if (it.fn === 'max') it.slot = slot(P ? `max((${P}) ? (${e}) : -Infinity)` : `max(${e})`);
      else if (P) { it.slot = slot(`sum((${P}) ? (${e}) : 0)`); it.countSlot = slot(`count(${P})`); }
      else it.slot = slot(`mean(${e})`);
    }
    const r = reductions.length ? yield* this.engine.evalG(reductions, this.#vars(merged)) : [];
    const value = (it) => (it.value !== undefined ? it.value : it.countSlot === undefined ? r[it.slot] : r[it.slot] / r[it.countSlot]);
    return Object.fromEntries(items.map((it) => [it.out, value(it)]));
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
      throw new SyntaxError(`vectro: an aggregation must look like sum(expr), mean(expr), min(expr), max(expr) or count([expr]), got "${src}"`);
    }
    if (!ast.args.length) return { fn: ast.fn, arg: null };
    return { fn: ast.fn, arg: this.#bind(toSource(ast.args[0]), params).src };
  }

  groupBy(key) {
    const c = this.cols.get(key);
    if (!c) throw new Error(`vectro: no column "${key}"`);
    if (c.strings) throw new Error(`vectro: can't group by free-text column "${key}" (load it as types: { ${key}: 'category' })`);
    return {
      agg: (spec, params = {}) => this.engine._drive(this.#groupAggG(key, spec, params)),
      aggAsync: (spec, params = {}) => this.engine._driveAsync(this.#groupAggG(key, spec, params)),
    };
  }

  *#groupAggG(key, spec, params) {
    const narrow = this.#narrow();
    if (narrow !== this) return yield* narrow.#groupAggG(key, spec, params);
    const { engine } = this;
    const merged = this.#mergeParams(params);
    const keyCol = this.cols.get(key);

    // Number of groups: the dictionary size, or max+1 for integer keys.
    let K, label;
    if (keyCol.dict) {
      K = keyCol.dict.length;
      label = (g) => keyCol.dict[g];
    } else {
      const [lo, hi, frac] = yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k: keyCol.data });
      if (this.n && (lo < 0 || frac > 0 || !(hi < 2 ** 24))) {
        throw new Error(`vectro: groupBy("${key}") needs a text column or whole numbers from 0 to 16,777,215`);
      }
      K = this.n ? hi + 1 : 0;
      label = (g) => g;
    }

    // Kernel items: a hidden row count, then each aggregation (mean = sum / count).
    const items = Object.entries(spec).map(([out, src]) => ({ out, ...this.#aggItem(src, merged) }));
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
    if (T * J * K * C > MAX_GROUP_SLOTS) throw new RangeError(`vectro: too many groups (${K.toLocaleString()}) for groupBy("${key}")`);

    // Accumulators: one block per thread, padded to 256 bytes so two threads
    // never update the same cache line. Every slot starts at its identity.
    const block = Math.ceil((J * K * C) / 32) * 32; // in float64s
    const acc = engine.alloc(Float64Array, Math.max(1, T * block));
    const slots = (s, j) => [s * block + j * K * C, s * block + (j + 1) * K * C];
    analysis.items.forEach((it, j) => {
      for (let s = 0; s < T; s++) acc.fill(IDENTITY[it.reduce], ...slots(s, j));
    });
    if (engine.jit) {
      const cacheKey = `group|${t}|${C}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, C, engine.isShared));
        engine._tableKernels.set(cacheKey, v);
      }
      const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), keyCol.data];
      yield* engine._groupG(v, this.n, arrays, scalarNames.map((nm) => merged[nm]), acc.byteOffset, block * 8, K);
    } else {
      // the calling thread fills thread 0's block
      const cols = Object.fromEntries(arrayNames.map((nm) => [nm, this.cols.get(nm).data]));
      interpretGroup(engine, t, analysis.items, predAst, cols, merged, keyCol.data, acc, K, this.n);
    }

    // Combine threads (and copies) into one value per group and aggregation.
    const totals = [];
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
      const names = Object.keys(vars);
      const expr = it.reduce === 'min' || it.reduce === 'max'
        ? names.slice(1).reduce((e, nm) => `${it.reduce}(${e}, ${nm})`, names[0])
        : names.join(' + ');
      yield* engine.evalG(expr, vars, vars.t0);
      totals.push(vars.t0);
    }

    // Keep groups that have rows.
    const counts = totals[0];
    const keep = [];
    for (let g = 0; g < K; g++) if (counts[g] > 0) keep.push(g);
    const columns = { [key]: keep.map(label) };
    items.forEach((it, i) => {
      const vals = totals[i + 1];
      columns[it.out] = it.fn === 'mean' ? keep.map((g) => vals[g] / counts[g]) : keep.map((g) => vals[g]);
    });
    engine.free(acc);
    return Table.fromColumns(engine, columns, { type: Float64Array });
  }

  // Materialize the filter: a new table holding only the matching rows.
  // Pass 1 counts matches per unit of work; pass 2 copies every unit's rows
  // straight to its final offset, all columns at once.
  collect() { return this.engine._drive(this.#collectG()); }
  collectAsync() { return this.engine._driveAsync(this.#collectG()); }

  *#collectG() {
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
      batch.forEach((nm, k) => out.set(nm, { ...this.cols.get(nm), data: dst[k] }));
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
    const cols = new Map([...this.cols].filter(([nm]) => keep.has(nm)));
    return new Table(this.engine, cols, this.n, { pred: this.pred, params: this.params });
  }

  // ---- sorting ---------------------------------------------------------------
  // sort(by): a new table in order of `by` (a column, an expression, or a list;
  // "-" first means large to small; text sorts by label). Stable; NaN last.
  // The order is computed on the calling thread (a radix sort); key
  // expressions are computed on all threads.
  sort(by) { return this.engine._drive(this.#sortG(by)); }
  sortAsync(by) { return this.engine._driveAsync(this.#sortG(by)); }

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
        out.push(Float64Array.from(codes, (code) => rank[code]));
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
      const perm = sortIndex(yield* t.#keyArraysG(keys), t.n);
      return t.#gather(perm, perm.length);
    } finally {
      if (t !== this) t.dispose();
    }
  }

  // A new table with rows perm[0..count) of this one (columns in `omit` left out).
  #gather(perm, count, omit = null) {
    const { engine } = this;
    const out = new Map();
    for (const [name, c] of this.cols) {
      if (omit?.has(name)) continue;
      const data = engine.alloc(c.data.constructor, count);
      const src = engine.live(c.data);
      for (let i = 0; i < count; i++) data[i] = src[perm[i]];
      out.set(name, { ...c, data });
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
  toArrow({ file = false } = {}) { return writeArrow(this.toColumns(), { file }); }
  async toArrowAsync({ file = false } = {}) { return writeArrow(await this.toColumnsAsync(), { file }); }

  toRows(limit = Infinity) { return this.engine._drive(this.#toRowsG(limit)); }
  toRowsAsync(limit = Infinity) { return this.engine._driveAsync(this.#toRowsG(limit)); }

  *#toRowsG(limit) {
    const t = yield* this.#collectG();
    const rows = [];
    const { engine } = this;
    const cols = [...t.cols].map(([name, c]) => [name, c, engine.live(c.data), c.strings ? engine.live(c.strings) : null]);
    for (let i = 0; i < Math.min(t.n, limit); i++) {
      const row = {};
      for (const [name, c, data, sbuf] of cols) row[name] = c.dict ? c.dict[data[i]] : sbuf ? decodeString(sbuf, data[i]) : data[i];
      rows.push(row);
    }
    if (t !== this) t.dispose();
    return rows;
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

const RESERVED = new Set(['Infinity', 'NaN', 'sum', 'mean', 'min', 'max', 'count', 'abs', 'sqrt', 'floor', 'ceil']);
function checkName(name) {
  if (!IDENT.test(name) || RESERVED.has(name)) throw new Error(`vectro: "${name}" can't be a column name (use letters, digits and _, not a function name)`);
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
  const re = n.fn === 'like' ? new RegExp(`^${args[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '[\\s\\S]*').replace(/_/g, '[\\s\\S]')}$`) : null;
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
function rewriteText(ast, cols, src) {
  const textCol = (n) => n.t === 'var' && cols.get(n.name)?.dict;
  const fix = (n) => {
    if (n.t === 'call' && TEXT_FNS[n.fn]) return textCall(n, cols, src);
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
