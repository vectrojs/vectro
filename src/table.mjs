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
import { joinTablesG } from './join.mjs';
import { Query } from './plan.mjs';
import { writeArrow } from './arrow.mjs';
import { interpretGroup, interpretCompact, interpretFilteredReduce } from './interp.mjs';
import { DATE_FNS, DATE_RESULT, parseDateText, toMillis } from './dates.mjs';

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

  // dates: names of columns holding dates as ms or ISO text (arrays of Date
  // objects are dates anyway).
  static fromColumns(engine, columns, { type = Float64Array, dates = [] } = {}) {
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
    const top = parse(expr);
    const date = (top.t === 'call' && DATE_RESULT.has(top.fn)) || (top.t === 'var' && !!this.cols.get(top.name)?.date); // dayStart(ts), monthStart(ts)...: still dates
    const cols = new Map(this.cols).set(name, { data: out, dict: null, codes: null, date });
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
      const [lo, hi, frac] = yield* engine.evalG(['min(k)', 'max(k)', 'count(k != floor(k))'], { k: c.data });
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
    if (keys.length === 1 && offsets[0] === 0) return { K, data: this.cols.get(keys[0]).data, keyColumn, owned: false, sorted: true };
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
      if (DISTINCT.has(it.fn)) it.perGroup = yield* this.#distinctCountsG(it.arg, merged, groups.data, K);
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
    if (big && engine.jit && J * K <= (engine._maxGroupSlots ?? MAX_GROUP_SLOTS)) {
      // many groups: each thread adds up its own range of groups, all rows
      const acc = engine.alloc(Float64Array, J * K);
      analysis.items.forEach((it, j) => engine.live(acc).fill(IDENTITY[it.reduce], j * K, (j + 1) * K));
      const cacheKey = `grouprange|${t}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}`;
      engine._tableKernels ??= new Map();
      let v = engine._tableKernels.get(cacheKey);
      if (!v) {
        v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, 1, engine.isShared, true));
        engine._tableKernels.set(cacheKey, v);
      }
      const arrays = [...arrayNames.map((nm) => this.cols.get(nm).data), groups.data];
      yield* engine._groupRangeG(v, this.n, arrays, scalarNames.map((nm) => merged[nm]), engine.live(acc).byteOffset, K);
      const a = engine.live(acc);
      totals = analysis.items.map((_, j) => a.slice(j * K, (j + 1) * K));
      engine.free(acc);
    } else if (slotsNeeded > (engine._maxGroupSlots ?? MAX_GROUP_SLOTS) || (!this.pred && big)) {
      totals = yield* this.#scatterG(groups.data, K, analysis.items, sources, merged); // too many groups for per-thread accumulators
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
        const cacheKey = `group|${t}|${C}|${sources.join(';')}|${this.pred}|${arrayNames}|${scalarNames}`;
        engine._tableKernels ??= new Map();
        let v = engine._tableKernels.get(cacheKey);
        if (!v) {
          v = engine._register(generateGroupBy(analysis.items, predAst, t, arrayNames, scalarNames, C, engine.isShared));
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
        if (c && !c.dict && !c.strings) { arrays.push(c.data); continue; } // a number column: as it is
        const a = engine.alloc(this.type, n);
        made.push(a);
        arrays.push(a);
        if (key.column) { // text: rank of each label, A to Z (or Z to A)
          const col = this.cols.get(key.column), rank = labelRanks(col.dict, key.desc);
          const codes = engine.live(col.data), out = engine.live(a);
          for (let i = 0; i < n; i++) out[i] = rank[codes[i]];
        } else yield* engine.evalG(this.#bind(key.text, {}).src, this.#vars({}), a);
      }
      const perm = yield* sortPermG(engine, arrays, n, t);
      try {
        const out = new Map();
        for (const [name, c] of this.cols) out.set(name, { ...c, data: yield* gatherG(engine, c.data, perm, n, t), zones: undefined });
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
      const data = engine.alloc(c.data.constructor, count);
      const src = engine.live(c.data);
      for (let i = 0; i < count; i++) data[i] = src[perm[i]];
      out.set(name, { ...c, data, zones: undefined }); // (new rows: no zone maps yet)
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
    for (let i = 0; i < Math.min(t.n, limit); i++) {
      const row = {};
      for (const [name, c, data, sbuf] of cols) row[name] = c.dict ? c.dict[data[i]] : sbuf ? decodeString(sbuf, data[i]) : c.date ? (data[i] === data[i] ? new Date(data[i]) : null) : data[i];
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
