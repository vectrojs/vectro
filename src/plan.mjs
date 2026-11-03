// Lazy queries: table.lazy() records a chain, optimizes it as a whole, then
// runs it with the table methods. Nothing runs until collect(), toRows(),
// agg(), count() (or their Async versions).
//
//   orders.lazy()
//     .derive('revenue', 'price * qty')
//     .filter("region == 'EU' && revenue > 100")
//     .groupBy('customer').agg({ spend: 'sum(revenue)', orders: 'count()' })
//     .sort('-spend').limit(10)
//     .collect();
//
// What the optimizer does:
//   - derived columns are inlined: filters, aggregations and sort keys use
//     the expression itself, so `revenue` above is never built as a column;
//     a derived column that is needed as a column (output, group key, join
//     key) is built after the filters, on the matching rows only;
//   - filters move down: below derives (by inlining), selects and sorts,
//     below groupBy when they only use its keys, and into each side of a join
//     when they only use that side's columns;
//   - only the columns used later are carried (joins, sorts and collects copy
//     nothing else), and derives nobody uses are dropped;
//   - sort + limit becomes top(k), which doesn't sort everything;
//   - parameters become constants, and constant parts of expressions are
//     computed once (in the kernel compiler, which also computes repeated
//     parts of an expression once per row).
// explain() prints the optimized plan.

import { parse, toSource, walk, isMask, checkSpec } from './fuse.mjs';
import { DATE_RESULT } from './dates.mjs';
import { trackAsyncMethods } from './engine.mjs';

export const vars = (ast) => { const out = new Set(); walk(ast, (n) => { if (n.t === 'var') out.add(n.name); }); return out; };
const and = (a, b) => (a ? { t: 'bin', op: '&&', l: a, r: b } : b);
function conjuncts(ast) { return ast.t === 'bin' && ast.op === '&&' ? [...conjuncts(ast.l), ...conjuncts(ast.r)] : [ast]; }

// Parameters as constants (dates as ms); text stays text.
function bindParams(ast, params) {
  if (!params || !Object.keys(params).length) return ast;
  const sub = (n) => {
    if (n.t === 'var' && n.name in params) {
      const v = params[n.name] instanceof Date ? params[n.name].getTime() : params[n.name];
      return typeof v === 'string' ? { t: 'str', v } : { t: 'num', v };
    }
    if (n.t === 'bin') return { ...n, l: sub(n.l), r: sub(n.r) };
    if (n.t === 'un') return { ...n, x: sub(n.x) };
    if (n.t === 'call') return { ...n, args: n.args.map(sub) };
    if (n.t === 'cond') return { ...n, c: sub(n.c), x: sub(n.x), y: sub(n.y) };
    return n;
  };
  return sub(ast);
}

// Replace derived names by their expressions. A text literal compared with a
// derived date (monthStart(ts) >= '2024-01-01') becomes date('...').
function inline(ast, derived) {
  if (!derived.size) return ast;
  const isDerivedDate = (n) => n.t === 'var' && derived.get(n.name)?.date;
  const sub = (n) => {
    if (n.t === 'var' && derived.has(n.name)) return derived.get(n.name).ast;
    if (n.t === 'bin') {
      let { l, r } = n;
      if (l.t === 'str' && isDerivedDate(r)) l = { t: 'call', fn: 'date', args: [l] };
      if (r.t === 'str' && isDerivedDate(l)) r = { t: 'call', fn: 'date', args: [r] };
      return { ...n, l: sub(l), r: sub(r) };
    }
    if (n.t === 'un') return { ...n, x: sub(n.x) };
    if (n.t === 'call') {
      const args = n.args.map(sub);
      // count(big) counts rows where big has a value; with big = price > 100 inlined, count(price > 100)
      // would count where the condition holds: as a value (1 / 0, as derive stores it) it stays the same
      if (n.fn === 'count' && args.length === 1 && n.args[0].t === 'var' && derived.has(n.args[0].name) && isMask(args[0])) args[0] = { t: 'bin', op: '+', l: args[0], r: { t: 'num', v: 0 } };
      return { ...n, args };
    }
    if (n.t === 'cond') return { ...n, c: sub(n.c), x: sub(n.x), y: sub(n.y) };
    return n;
  };
  return sub(ast);
}

// Sort keys: '-name' or an expression.
const IDENT = /^\s*(-?)\s*([A-Za-z_]\w*)\s*$/;
const keyList = (by) => (Array.isArray(by) ? by : [by]);

export class Query {
  constructor(node) { this.node = node; }

  filter(expr, params = {}) { return new Query({ op: 'filter', input: this.node, ast: bindParams(parse(expr), params) }); }
  derive(name, expr, params = {}) { return new Query({ op: 'derive', input: this.node, name, ast: bindParams(parse(expr), params) }); }
  select(...names) { return new Query({ op: 'select', input: this.node, names: names.flat() }); }
  sort(by) { return new Query({ op: 'sort', input: this.node, by: keyList(by) }); }
  limit(n, offset = 0) { return new Query({ op: 'limit', input: this.node, n: Math.max(0, Math.floor(n)), offset: Math.max(0, Math.floor(offset)) }); }
  top(k, by) { return this.sort(by).limit(k); }
  join(other, opts) {
    const right = other instanceof Query ? other.node : { op: 'scan', table: other };
    return new Query({ op: 'join', left: this.node, right, opts: opts ?? {} });
  }
  // (SQL) a column computed by a JavaScript function of whole values, once
  // per distinct combination (text functions): see Table._textmap. The spec's
  // state keeps the labels, so it is made here once for this step. labels:
  // the one input holds codes 0..L-1 of these labels already (no fn).
  textmap(name, spec) { return new Query({ op: 'textmap', input: this.node, name, inputs: spec.inputs ?? [], fn: spec.fn, text: spec.text ?? true, date: spec.date ?? false, label: spec.label ?? 'fn', labels: spec.labels ?? null, state: spec.state ?? (spec.labels ? { dict: spec.labels.slice(), codes: new Map(spec.labels.map((l, i) => [l, i])) } : { dict: [], codes: new Map() }) }); }
  // (SQL) window functions: see Table._window for the spec
  window(spec) { return new Query({ op: 'window', input: this.node, partition: spec.partition ?? [], order: spec.order ?? [], items: spec.items }); }
  // (SQL) the rows of several queries one after another, columns by position
  static union(queries) { return new Query({ op: 'union', inputs: queries.map((q) => q.node) }); }
  // (SQL RIGHT / FULL JOIN) the right rows that a join with opts.unmatched =
  // state matched to no left row, columns `names` (the join's right side);
  // only after that join ran (an earlier input of the same union).
  static unmatched(state, names) { return new Query({ op: 'unmatched', state, names }); }
  groupBy(keys) {
    return { agg: (spec, params = {}) => new Query({ op: 'aggregate', input: this.node, keys: keyList(keys), spec: Object.fromEntries(Object.entries(spec).map(([k, v]) => [k, bindParams(parse(v), params)])) }) };
  }

  // ---- running --------------------------------------------------------------------
  collect() { return runSync(this.#final()); }
  collectAsync() { return runAsync(this.#final()); }
  toRows(limit = Infinity) { const t = this.#limited(limit).collect(); try { return t.toRows(limit); } finally { t.dispose(); } }
  async toRowsAsync(limit = Infinity) { const t = await this.#limited(limit).collectAsync(); try { return await t.toRowsAsync(limit); } finally { t.dispose(); } }
  agg(spec, params = {}) { return runSync(this.#aggNode(spec, params)); }
  aggAsync(spec, params = {}) { return runAsync(this.#aggNode(spec, params)); }
  count() { return this.agg({ n: 'count()' }).n; }
  async countAsync() { return (await this.aggAsync({ n: 'count()' })).n; }

  // The optimized plan, one step per line (innermost first).
  explain() { return describe(optimize(this.#final().node)); }

  #final() { return new Query({ op: 'output', input: this.node }); }
  // (on a dataset, only the rows asked for are read)
  #limited(limit) { return Number.isFinite(limit) && findDataset(this.node) ? this.limit(limit) : this; }
  #aggNode(spec, params) {
    checkSpec(spec);
    return new Query({ op: 'total', input: this.node, spec: Object.fromEntries(Object.entries(spec).map(([k, v]) => [k, bindParams(parse(v), params)])) });
  }
}

// ---- optimizing: move filters down --------------------------------------------------

// Column names a node produces (null: unknown, e.g. a table we haven't seen).
export function namesOf(node) {
  switch (node.op) {
    case 'scan': return node.table.columnNames;
    case 'filter': case 'sort': case 'limit': case 'output': return namesOf(node.input);
    case 'derive': return [...new Set([...namesOf(node.input), node.name])];
    case 'select': return node.names;
    case 'aggregate': return [...node.keys, ...Object.keys(node.spec)];
    case 'join': return joinNames(node).all;
    case 'window': return [...namesOf(node.input).filter((n) => !node.items.some((it) => it.name === n)), ...node.items.map((it) => it.name)];
    case 'textmap': return [...namesOf(node.input).filter((n) => n !== node.name), node.name];
    case 'union': return namesOf(node.inputs[0]);
    case 'unmatched': return node.names;
  }
  return [];
}

// Output names of a join, and where each comes from.
export function joinNames(node) {
  const left = namesOf(node.left), right = namesOf(node.right);
  const { on, suffix = '_right' } = node.opts;
  const [lk, rk] = typeof on === 'string' || Array.isArray(on) ? [[on].flat(), [on].flat()] : [[on?.left].flat(), [on?.right].flat()];
  const same = new Set(rk.filter((n, k) => lk[k] === n));
  const fromRight = new Map(); // output name -> right column
  const all = [...left];
  for (const n of right) {
    if (same.has(n)) continue;
    let out = n;
    while (all.includes(out)) out += suffix;
    all.push(out);
    fromRight.set(out, n);
  }
  return { all, left: new Set(left), fromRight };
}

const rename = (ast, map) => {
  const sub = (n) => {
    if (n.t === 'var' && map.has(n.name)) return { ...n, name: map.get(n.name) };
    if (n.t === 'bin') return { ...n, l: sub(n.l), r: sub(n.r) };
    if (n.t === 'un') return { ...n, x: sub(n.x) };
    if (n.t === 'call') return { ...n, args: n.args.map(sub) };
    if (n.t === 'cond') return { ...n, c: sub(n.c), x: sub(n.x), y: sub(n.y) };
    return n;
  };
  return sub(ast);
};

// Place one condition as deep as it can go below `node`.
function pushFilter(node, cond) {
  const used = vars(cond);
  switch (node.op) {
    case 'filter': return { ...node, input: pushFilter(node.input, cond) };
    case 'sort': case 'select': return { ...node, input: pushFilter(node.input, cond) };
    case 'derive': // below the derive, with its expression in place of its name
      return { ...node, input: pushFilter(node.input, used.has(node.name) ? inline(cond, new Map([[node.name, { ast: node.ast, date: isDateExpr(node) }]])) : cond) };
    case 'aggregate':
      if ([...used].every((n) => node.keys.includes(n))) return { ...node, input: pushFilter(node.input, cond), pushed: (node.pushed ?? 0) + 1 };
      break;
    case 'textmap':
      if (!used.has(node.name)) return { ...node, input: pushFilter(node.input, cond) };
      break;
    case 'window': { // (only a filter on the partition keys keeps every window the same)
      if (node.partition.length && [...used].every((n) => node.partition.includes(n))) return { ...node, input: pushFilter(node.input, cond) };
      const k = firstRanks(cond, node);
      if (k !== null) return { op: 'filter', input: { ...node, firstRanks: Math.min(k, node.firstRanks ?? Infinity) }, ast: cond };
      break;
    }
    case 'join': {
      const { left, fromRight } = joinNames(node);
      if ([...used].every((n) => left.has(n))) return { ...node, left: pushFilter(node.left, cond) };
      if (node.opts.how !== 'left' && [...used].every((n) => fromRight.has(n))) return { ...node, right: pushFilter(node.right, rename(cond, fromRight)) };
      break;
    }
  }
  return { op: 'filter', input: node, ast: cond };
}
const isDateExpr = (node) => node.ast.t === 'call' && DATE_RESULT.has(node.ast.fn);

// A filter keeping only the first k rows (rank) of each partition, as
// `rn <= k`, `rn < k + 1`, `rn = k` or `k >= rn` on a ROW_NUMBER or RANK of
// this window: k, else null. The window then only needs the rows that can
// rank that high (see Table._window); every function of the node must give
// the same values on those rows (it reads only rows ordered before each row).
const PREFIX_FNS = new Set(['row_number', 'rank', 'dense_rank', 'lag']);
function firstRanks(cond, node) {
  if (cond.t !== 'bin' || !node.order.length || !node.items.every((it) => PREFIX_FNS.has(it.fn))) return null;
  const flip = { '>=': '<=', '>': '<', '==': '==' };
  const [v, op, lim] = cond.l.t === 'var' && cond.r.t === 'num' ? [cond.l, cond.op, cond.r.v] : cond.r.t === 'var' && cond.l.t === 'num' ? [cond.r, flip[cond.op], cond.l.v] : [];
  const item = v && node.items.find((it) => it.name === v.name);
  if (!item || (item.fn !== 'row_number' && item.fn !== 'rank')) return null;
  const k = op === '<=' || op === '==' ? Math.floor(lim) : op === '<' ? Math.ceil(lim) - 1 : NaN;
  return k >= 1 && k <= 10_000 ? k : null;
}

export function optimize(node) {
  switch (node.op) {
    case 'scan': case 'unmatched': return node;
    case 'filter': {
      let input = optimize(node.input);
      for (const c of conjuncts(node.ast)) input = pushFilter(input, c);
      return input;
    }
    case 'join': return { ...node, left: optimize(node.left), right: optimize(node.right) };
    case 'union': return { ...node, inputs: node.inputs.map(optimize) };
    case 'aggregate': {
      const input = optimize(node.input);
      return { ...node, input };
    }
    default: return { ...node, input: optimize(node.input) };
  }
}

// ---- running -------------------------------------------------------------------------
// The executor is a generator of table calls { table, method, args } (sync
// or async drivers run them), building on pipeline states:
//   { table (a Table, maybe filtered), derived: name -> { ast, date }, visible }

function* execG(node, needed, temps) {
  switch (node.op) {
    case 'scan': return { table: node.table, derived: new Map(), visible: node.table.columnNames };
    case 'unmatched': { // (the join that made it ran first, in this run)
      const t = runState(temps).get(node.state);
      if (!t) throw new Error('vectro: internal: unmatched rows before their join ran');
      return { table: t, derived: new Map(), visible: node.names };
    }
    case 'filter': {
      const s = yield* execG(node.input, union(needed, vars(node.ast)), temps);
      return { ...s, table: s.table.filter(toSource(inline(node.ast, s.derived))) };
    }
    case 'derive': {
      if (needed && !needed.has(node.name)) return yield* execG(node.input, needed, temps); // nobody uses it
      const s = yield* execG(node.input, union(minus(needed, node.name), vars(node.ast)), temps);
      const derived = new Map(s.derived).set(node.name, { ast: inline(node.ast, s.derived), date: isDateExpr(node) || (node.ast.t === 'var' && !!s.table.cols.get(node.ast.name)?.date) });
      return { ...s, derived, visible: [...s.visible.filter((n) => n !== node.name), node.name] };
    }
    case 'select': {
      const have = namesOf(node.input); // (only the columns used: a dataset's chunks hold just those)
      for (const n of node.names) if ((!needed || needed.has(n)) && !have.includes(n)) throw new Error(`vectro: no column "${n}"`);
      const s = yield* execG(node.input, needed ? new Set(node.names.filter((n) => needed.has(n))) : new Set(node.names), temps);
      return { ...s, visible: node.names };
    }
    case 'aggregate': {
      const fused = yield* lookupAggregateG(node, temps);
      if (fused) return { table: fused, derived: new Map(), visible: fused.columnNames };
      const specAsts = Object.values(node.spec);
      const want = new Set([...node.keys, ...specAsts.flatMap((a) => [...vars(a)])]);
      const s = yield* execG(node.input, want, temps);
      const t = yield* materializeG(s, new Set(node.keys), temps, specAsts);
      const spec = Object.fromEntries(Object.entries(node.spec).map(([k, a]) => [k, toSource(inline(a, s.derived))]));
      const g = yield { table: t, method: node.keys.length ? 'groupAgg' : 'oneRow', args: [node.keys, spec] };
      return { table: g, derived: new Map(), visible: g.columnNames };
    }
    case 'limit': case 'sort': {
      const sortNode = node.op === 'sort' ? node : node.input.op === 'sort' ? node.input : null;
      const k = node.op === 'limit' ? node.n + (node.offset ?? 0) : null;
      const below = sortNode ? sortNode.input : node.input;
      const keyAsts = (sortNode?.by ?? []).map((b) => parse(String(b).trim()));
      const s = yield* execG(below, union(needed, new Set(keyAsts.flatMap((a) => [...vars(a)]))), temps);
      const keep = needed ? s.visible.filter((n) => needed.has(n)) : s.visible;
      // the rows to output, then derived output columns on those rows only
      const later = keep.filter((n) => s.derived.has(n)).map((n) => s.derived.get(n).ast); // derived outputs: built on the rows kept
      const base = yield* materializeG(s, new Set(keep.filter((n) => !s.derived.has(n))), temps, [...keyAsts, ...later], true);
      let r;
      if (sortNode) {
        const by = sortNode.by.map((b) => {
          const m = IDENT.exec(String(b));
          if (m && !s.derived.has(m[2])) return String(b).trim(); // a column ('-region' stays a label sort)
          const same = m ? inline(parse(m[2]), s.derived) : null;
          if (same?.t === 'var') return `${m[1]}${same.name}`; // a copy of a column: sorted as that column (text by label)
          return toSource(inline(parse(String(b)), s.derived));
        });
        r = yield { table: base, method: k === null ? 'sort' : 'top', args: k === null ? [by] : [k, by] };
      } else r = yield { table: base, method: 'head', args: [k] };
      if (node.offset) r = yield { table: r, method: 'slice', args: [node.offset] };
      r = yield* deriveAllG(r, keep.filter((n) => s.derived.has(n)), s.derived);
      return { table: r, derived: new Map(), visible: keep };
    }
    case 'window': {
      const made = new Set(node.items.map((it) => it.name));
      const inputs = [...node.partition, ...node.order.map((o) => o.name), ...node.items.map((it) => it.arg).filter(Boolean)];
      const s = yield* execG(node.input, needed ? new Set([...[...needed].filter((n) => !made.has(n)), ...inputs]) : null, temps);
      const keep = needed ? s.visible.filter((n) => needed.has(n) || inputs.includes(n)) : s.visible;
      const t = yield* materializeG(s, new Set([...keep, ...inputs]), temps, [], true);
      const w = yield { table: t.select(...new Set([...keep, ...inputs])), method: '_window', args: [{ partition: node.partition, order: node.order, items: node.items, firstRanks: node.firstRanks }] };
      return { table: w, derived: new Map(), visible: [...keep, ...node.items.map((it) => it.name)] };
    }
    case 'textmap': {
      const s = yield* execG(node.input, needed ? new Set([...[...needed].filter((n) => n !== node.name), ...node.inputs]) : null, temps);
      const keep = (needed ? s.visible.filter((n) => needed.has(n) || node.inputs.includes(n)) : s.visible).filter((n) => n !== node.name);
      const cols = new Set([...keep, ...node.inputs]);
      const t = yield* materializeG(s, cols, temps, [], true);
      const out = yield { table: cols.size ? t.select(...cols) : t, method: '_textmap', args: [node.name, node] };
      return { table: out, derived: new Map(), visible: [...keep, node.name] };
    }
    case 'union': {
      // only the columns used later (by position: each input names them its own way)
      const names0 = namesOf(node.inputs[0]);
      let at = names0.map((_, k) => k);
      if (needed) { at = at.filter((k) => needed.has(names0[k])); if (!at.length) at = [0]; } // (the row count needs one)
      const parts = [];
      for (const input of node.inputs) {
        const names = namesOf(input);
        parts.push(yield* execG({ op: 'output', input: at.length === names.length ? input : { op: 'select', input, names: at.map((k) => names[k]) } }, null, temps));
      }
      const u = yield { table: parts[0], method: '_concat', args: [parts.slice(1)] };
      return { table: u, derived: new Map(), visible: u.columnNames };
    }
    case 'join': {
      const { left: leftNames, fromRight } = joinNames(node);
      const { on } = node.opts;
      const [lk, rk] = typeof on === 'string' || Array.isArray(on) ? [[on].flat(), [on].flat()] : [[on?.left].flat(), [on?.right].flat()];
      const needL = new Set(lk), needR = new Set(rk);
      for (const n of needed ?? namesOf(node)) { if (leftNames.has(n)) needL.add(n); else if (fromRight.has(n)) needR.add(fromRight.get(n)); else if (rk.includes(n)) needR.add(n); }
      // (a right column renamed for a name it shares: those names come along, so the join names it the same)
      const suffix = node.opts.suffix ?? '_right';
      for (const [out, n] of fromRight) {
        if (out === n || !needR.has(n)) continue;
        for (let x = n; x !== out; x += suffix) { if (leftNames.has(x)) needL.add(x); else if (fromRight.has(x)) needR.add(fromRight.get(x)); }
      }
      const sl = yield* execG(node.left, needL, temps), sr = yield* execG(node.right, needR, temps);
      const lt = yield* materializeG(sl, needL, temps, [], true), rt = yield* materializeG(sr, needR, temps, [], true);
      // (RIGHT / FULL JOIN) the right rows it matched to none, kept for this run's 'unmatched' step
      const holder = node.opts.unmatched ? {} : null;
      const j = yield { table: lt, method: 'join', args: [rt, holder ? { ...node.opts, unmatched: holder } : node.opts] };
      if (holder) { temps.push(holder.table); runState(temps).set(node.opts.unmatched, holder.table); }
      return { table: j, derived: new Map(), visible: j.columnNames.filter((n) => n !== '__vectro_found') }; // (a view join's match flag)
    }
    case 'output': {
      const s = yield* execG(node.input, null, temps);
      const t = yield* materializeG(s, new Set(s.visible), temps, [], true);
      const out = yield { table: t.select(...s.visible), method: 'collect', args: [] };
      return out.columnNames.length === s.visible.length ? out : out.select(...s.visible); // (a filter's columns came along: drop them)
    }
    case 'groupInto': { // (datasets) add one chunk's matching rows into running group totals
      const asts = [node.group, ...node.sources];
      const s = yield* execG(node.input, new Set(asts.flatMap((a) => [...vars(a)])), temps);
      const src = (a) => toSource(inline(a, s.derived));
      return yield { table: s.table, method: '_groupInto', args: [src(node.group), node.K, node.sources.map(src), node.state] };
    }
    case 'total': {
      const asts = Object.values(node.spec);
      const s = yield* execG(node.input, new Set(asts.flatMap((a) => [...vars(a)])), temps);
      const spec = Object.fromEntries(Object.entries(node.spec).map(([k, a]) => [k, toSource(inline(a, s.derived))]));
      return yield { table: s.table, method: 'agg', args: [spec] };
    }
  }
  throw new Error(`vectro: unknown plan step ${node.op}`);
}

// GROUP BY right columns of an inner join on one key, aggregating left
// columns only: the left rows (still filtered, packed columns as they are)
// go straight into groups found by looking up their key (see
// Table._lookupGroupAgg), no joined rows made. Null when that doesn't apply.
function* lookupAggregateG(node, temps) {
  const J = node.input;
  if (J.op !== 'join' || (J.opts.how ?? 'inner') !== 'inner' || !node.keys.length) return null;
  const lazyOnly = (n) => n.op === 'scan' || ((n.op === 'filter' || n.op === 'derive' || n.op === 'select') && lazyOnly(n.input)); // (views: cheap to make again if this path declines)
  if (!lazyOnly(J.left) || !lazyOnly(J.right)) return null;
  const { on } = J.opts;
  const [lk, rk] = typeof on === 'string' || Array.isArray(on) ? [[on].flat(), [on].flat()] : [[on?.left].flat(), [on?.right].flat()];
  if (lk.length !== 1 || rk.length !== 1) return null;
  const { left: leftNames, fromRight } = joinNames(J);
  if (!node.keys.every((k) => fromRight.has(k))) return null;
  const specAsts = Object.values(node.spec);
  const used = new Set(specAsts.flatMap((a) => [...vars(a)]));
  if (![...used].every((n) => leftNames.has(n))) return null;
  const rightKeys = node.keys.map((k) => fromRight.get(k));
  const sl = yield* execG(J.left, new Set([...used, lk[0]]), temps);
  if (sl.derived.has(lk[0])) return null;
  const sr = yield* execG(J.right, new Set([...rightKeys, rk[0]]), temps);
  if (sr.derived.has(rk[0]) || rightKeys.some((k) => sr.derived.has(k))) return null;
  const spec = Object.fromEntries(Object.entries(node.spec).map(([k, a]) => [k, toSource(inline(a, sl.derived))]));
  const base = new Set([lk[0]]);
  for (const a of Object.values(spec)) for (const v of vars(parse(a))) if (sl.table.cols.has(v)) base.add(v);
  const lt = sl.table.select(...base), rt = sr.table.select(...new Set([rk[0], ...rightKeys]));
  return yield { table: lt, method: '_lookupGroupAgg', args: [rt, { left: lk[0], right: rk[0] }, node.keys, rightKeys, spec] };
}

// A table holding `names` (base or derived) of state s, still filtered.
// Derived columns are built on the matching rows only (collect first).
// exprs: expressions that will be evaluated on it (their columns are kept).
function* materializeG(s, names, temps, exprs = [], keepVisibleOrder = false) {
  const derivedNames = [...names].filter((n) => s.derived.has(n));
  const base = new Set([...names].filter((n) => !s.derived.has(n)));
  for (const n of derivedNames) for (const v of vars(s.derived.get(n).ast)) base.add(v);
  for (const e of exprs) for (const v of vars(inline(e, s.derived))) if (s.table.cols.has(v)) base.add(v);
  for (const n of base) if (!s.table.cols.has(n)) throw new Error(`vectro: no column "${n}"`);
  let t = base.size ? s.table.select(...base) : s.table;
  if (!derivedNames.length) return t;
  if (t.pred) t = yield { table: t, method: 'collect', args: [] };
  return yield* deriveAllG(t, derivedNames, s.derived);
}

// The derived columns `names` on table t. One named like a column t still
// has (shadowed: SELECT price AS cust, cust AS price) is made under a fresh
// name, then the names are swapped in a view; a derived name for that very
// column is the column itself.
function* deriveAllG(t, names, derived) {
  const swap = new Map();
  for (const n of names) {
    const d = derived.get(n);
    if (d.ast.t === 'var' && d.ast.name === n && t.cols.has(n)) continue;
    const at = t.cols.has(n) ? `__vectro_d${swap.size}_${n}` : n;
    if (at !== n) swap.set(at, n);
    t = yield { table: t, method: 'derive', args: [at, toSource(d.ast)], date: d.date };
  }
  if (swap.size) t = t._renamed(swap);
  return t;
}

const union = (a, b) => (a ? new Set([...a, ...b]) : null);
// what one run of a plan keeps between steps (its temps list stands for the run)
const runStates = new WeakMap();
const runState = (temps) => { let m = runStates.get(temps); if (!m) runStates.set(temps, (m = new Map())); return m; };
const minus = (a, name) => (a ? new Set([...a].filter((n) => n !== name)) : null);

// Free the intermediate tables, except columns the result still uses.
function release(result, temps) {
  const used = new Set();
  if (result?.cols) for (const c of result.cols.values()) used.add(c.data);
  for (const t of temps) {
    if (t === result) continue;
    for (const name of [...t.owned]) {
      const c = t.cols.get(name);
      if (c && used.has(c.data)) { // the result shares it: the result owns it now
        t.owned = t.owned.filter((n) => n !== name);
        const outName = [...result.cols].find(([, rc]) => rc.data === c.data)?.[0];
        if (outName && !result.owned.includes(outName)) result.owned.push(outName);
      }
    }
    t.dispose();
  }
}

function call(req, sync) {
  const { table, method, args } = req;
  const suffix = sync ? '' : 'Async';
  if (method === 'groupAgg') return table.groupBy(args[0])[`agg${suffix}`](args[1]);
  if (method === 'oneRow') { // totals without keys, as a one-row table
    const asTable = (o) => table.engine.table(Object.fromEntries(Object.entries(o).map(([k, v]) => [k, [v]])));
    return sync ? asTable(table.agg(args[1])) : table.aggAsync(args[1]).then(asTable);
  }
  if (method === 'derive' && req.date) { // keep the date flag of dayStart(ts) and friends
    const mark = (t) => { t.cols.get(args[0]).date = true; return t; };
    return sync ? mark(table.derive(...args)) : table.deriveAsync(...args).then(mark);
  }
  return table[`${method}${suffix}`](...args);
}

// Tables a call made (not the one it was called on) are intermediate: freed
// at the end unless the result uses their columns.
const record = (temps, req, out) => { if (out && out !== req.table && out.cols && !temps.includes(out)) temps.push(out); return out; };

// A dataset (tables on disk, see dataset.mjs) in the plan: it runs the plan
// chunk by chunk.
export function findDataset(node) {
  if (node.op === 'scan') return node.table.isDataset ? node.table : null;
  if (node.op === 'unmatched') return null;
  if (node.op === 'union') { for (const i of node.inputs) { const d = findDataset(i); if (d) return d; } return null; }
  return node.op === 'join' ? findDataset(node.left) ?? findDataset(node.right) : findDataset(node.input);
}

function runSync(q) {
  const plan = optimize(q.node);
  const ds = findDataset(plan);
  return ds ? ds.runPlan(plan, true) : execute(plan, true);
}

function runAsync(q) {
  const plan = optimize(q.node);
  const ds = findDataset(plan);
  return ds ? ds.runPlan(plan, false) : execute(plan, false);
}

// Run an optimized plan over tables in memory (sync: the result; else a promise).
export function execute(plan, sync) { return sync ? executeSync(plan) : executeAsync(plan); }

function executeSync(plan) {
  const temps = [];
  const g = execG(plan, null, temps);
  let result;
  try {
    let step = g.next();
    while (!step.done) step = g.next(record(temps, step.value, call(step.value, true)));
    result = step.value;
    return result;
  } finally { release(result, temps); }
}

async function executeAsync(plan) {
  const temps = [];
  const g = execG(plan, null, temps);
  let result;
  try {
    let step = g.next();
    while (!step.done) step = g.next(record(temps, step.value, await call(step.value, false)));
    result = step.value;
    return result;
  } finally { release(result, temps); }
}

// ---- explain -----------------------------------------------------------------------------
function describe(node, depth = 0) {
  const pad = '  '.repeat(depth);
  switch (node.op) {
    case 'scan': return node.table.isDataset ? `${pad}scan dataset ${node.table.path} (${node.table.chunkCount} chunks, read one at a time; chunks the filters rule out are skipped) [${node.table.columnNames.join(', ')}]` : `${pad}scan [${node.table.columnNames.join(', ')}]`;
    case 'filter': return `${describe(node.input, depth)}\n${pad}filter ${toSource(node.ast)}`;
    case 'derive': return `${describe(node.input, depth)}\n${pad}derive ${node.name} = ${toSource(node.ast)} (inlined where used)`;
    case 'select': return `${describe(node.input, depth)}\n${pad}select ${node.names.join(', ')}`;
    case 'aggregate': return `${describe(node.input, depth)}\n${pad}groupBy ${node.keys.join(', ')}: ${Object.entries(node.spec).map(([k, a]) => `${k} = ${toSource(a)}`).join(', ')}${node.pushed ? ` (${node.pushed} filter(s) on keys moved below)` : ''}`;
    case 'sort': return `${describe(node.input, depth)}\n${pad}sort ${node.by.join(', ')}`;
    case 'limit': return node.input.op === 'sort'
      ? `${describe(node.input.input, depth)}\n${pad}top ${node.n + (node.offset ?? 0)} by ${node.input.by.join(', ')} (sort + limit)${node.offset ? `, then skip ${node.offset}` : ''}`
      : `${describe(node.input, depth)}\n${pad}limit ${node.n}${node.offset ? ` offset ${node.offset}` : ''}`;
    case 'join': return `${pad}join ${JSON.stringify(node.opts.on)} ${node.opts.how ?? 'inner'}\n${pad}  left:\n${describe(node.left, depth + 2)}\n${pad}  right:\n${describe(node.right, depth + 2)}`;
    case 'output': return describe(node.input, depth);
    case 'textmap': return `${describe(node.input, depth)}\n${pad}${node.text ? 'text' : 'value'} ${node.name} = ${node.label}(${node.inputs.join(', ')}) (once per distinct value)`;
    case 'window': return `${describe(node.input, depth)}\n${pad}window ${node.items.map((it) => `${it.name} = ${it.fn}(${it.arg ?? ''})`).join(', ')}${node.partition.length ? ` partition by ${node.partition.join(', ')}` : ''}${node.order.length ? ` order by ${node.order.map((o) => (o.desc ? `-${o.name}` : o.name)).join(', ')}` : ''}${node.firstRanks ? ` (rows that can rank in the first ${node.firstRanks} only)` : ''}`;
    case 'union': return `${pad}union all\n${node.inputs.map((i) => describe(i, depth + 1)).join(`\n${pad}  and\n`)}`;
    case 'unmatched': return `${pad}right rows the join above matched to none (from its probe) [${node.names.join(', ')}]`;
    case 'total': return `${describe(node.input, depth)}\n${pad}totals: ${Object.entries(node.spec).map(([k, a]) => `${k} = ${toSource(a)}`).join(', ')}`;
  }
  return `${pad}${node.op}`;
}

// (a query's engine: the one its scanned table belongs to)
trackAsyncMethods(Query, (q) => engineOfNode(q.node));
function engineOfNode(n) { // (any scan's: UNION / INTERSECT / EXCEPT branches and both sides of a join too)
  if (!n) return undefined;
  if (n.op === 'scan') return n.table?.engine;
  for (const c of [n.input, n.left, n.right, ...(n.inputs ?? [])]) { const e = engineOfNode(c); if (e) return e; }
  return undefined;
}
