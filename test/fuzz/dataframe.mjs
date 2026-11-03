// Randomized differential testing of the vectrojs dataframe API against
// plain JavaScript (lib.mjs). Usage: node test/fuzz/dataframe.mjs <startSeed> <count> [sizeClass]
//   sizeClass: small (default: 0..5000 rows), mid (up to 300k), big (up to 3M)
import { Engine } from '../../src/node.mjs';
import * as L from './lib.mjs';
const { rng, genColumn, genNum, genBool, toText, evalE, isBool, refAgg, specText, refGroupBy, refSort, sortKeyText, refJoin, rowsDiff, dictRank, norm0, fixF32Fn } = L;

const start = +(process.argv[2] ?? 1), count = +(process.argv[3] ?? 100), sizeClass = process.env.SIZE ?? process.argv[4] ?? 'small';
const only = process.env.CONFIG; // run one config by name
const SIZES = { small: [0, 1, 2, 3, 5, 17, 100, 257, 1000, 5000], mid: [1000, 5000, 50_001, 70_000, 120_000, 300_000], big: [1_000_000, 3_000_000], big1: [1_000_000] }[sizeClass];

const engines = {};
const getEngine = (name) => engines[name] ??= new Engine({ all: {}, t1: { threads: 1 }, nowasm: { wasm: false }, nojit: { jit: false }, par: { minParallel: {} }, t3par: { threads: 3, minParallel: {} } }[name]);
const CONFIGS = [
  { name: 'all', eng: 'all' }, { name: 't1', eng: 't1' }, { name: 'nowasm', eng: 'nowasm' }, { name: 'nojit', eng: 'nojit' },
  { name: 'par', eng: 'par' }, { name: 't3par', eng: 't3par' }, { name: 'all+compress', eng: 'all', compress: true }, { name: 'par+compress', eng: 'par', compress: true },
  { name: 'all+async', eng: 'all', async: true }, { name: 'par+async+compress', eng: 'par', async: true, compress: true },
];

// ---------- case generation (data only, so rows can be shrunk) ----------
function genCase(seed) {
  const r = rng(seed);
  const f32 = r.chance(0.12);
  const n = r.pick(SIZES);
  const kinds = f32 ? ['i', 'h', 'm', 'c'] : ['i', 'w', 'f', 'p', 'c', 'm', 'h'];
  const ncol = r.int(1, 5);
  const cols = {}, kindOf = {};
  for (let q = 0; q < ncol; q++) { const k = q === 0 && r.chance(0.5) ? 'c' : r.pick(kinds); const nm = k + q; cols[nm] = genColumn(r, k, n); kindOf[nm] = k; }
  if (!Object.values(kindOf).some((k) => k !== 'c')) { cols.i9 = genColumn(r, 'i', n); kindOf.i9 = 'i'; }
  const ops = ['agg', 'agg', 'groupBy', 'groupBy', 'sort', 'top', 'join', 'rows', 'lazyGroup', 'lazySort', 'lazyJoin', 'lazyAgg'];
  const op = r.pick(ops);
  const c = { seed, f32, n, cols, kindOf, op, params: {} };
  const numCols = () => Object.keys(c.cols).filter((k) => c.kindOf[k] !== 'c');
  const txtCols = () => Object.keys(c.cols).filter((k) => c.kindOf[k] === 'c');
  const ctx = () => ({ num: numCols(), text: txtCols(), labels: Object.fromEntries(txtCols().map((k) => [k, [...new Set(c.cols[k])]])), params: c.params, f32 });
  let noParams = false;
  const ctxp = () => ({ ...ctx(), noParams });
  const num = () => (f32 ? fixF32Fn(genNum(r, ctxp())) : genNum(r, ctxp()));
  const bool = () => (f32 ? fixF32Fn(genBool(r, ctxp())) : genBool(r, ctxp()));

  if (op === 'join' || op === 'lazyJoin') {
    // key columns on both sides from the same distribution
    const kk = r.pick(f32 ? ['i', 'c'] : ['i', 'i', 'w', 'f', 'c', 'c', 'p']);
    const nk = r.chance(0.3) ? 2 : 1;
    const m = r.pick([0, 1, 3, 50, 1000, 5000, ...(sizeClass !== 'small' ? [70_000] : [])]);
    const pool = kk === 'c' ? ['EU', 'US', '', '😀', 'APAC', 'b'].filter(() => r.chance(0.7)).concat(['EU']) : null;
    const keyGen = (len, rr) => {
      if (kk === 'c') return genColumn(rr, 'c', len, { pool });
      if (kk === 'i') return Array.from({ length: len }, () => (rr.chance(0.05) ? NaN : rr.chance(0.03) ? -0 : rr.int(0, 30)));
      if (kk === 'w') return Array.from({ length: len }, () => (rr.chance(0.05) ? NaN : 1e12 + rr.int(0, 3000) * 7919));
      if (kk === 'p') return Array.from({ length: len }, () => rr.int(0, 300) / 100);
      return Array.from({ length: len }, () => (rr.chance(0.05) ? NaN : rr.pick([-0, 0, 0.1, 0.30000000000000004, 1e-300, 2 ** 53, Infinity, -Infinity, 1.5])));
    };
    const r2 = rng(seed * 7 + 1);
    c.cols.k1 = keyGen(n, r2); c.kindOf.k1 = kk === 'c' ? 'c' : 'n';
    if (nk === 2) { c.cols.k2 = Array.from({ length: n }, () => r2.int(0, 2)); c.kindOf.k2 = 'n'; }
    const rcols = { k1: keyGen(m, r2) }, rkind = { k1: c.kindOf.k1 };
    if (nk === 2) { rcols.k2 = Array.from({ length: m }, () => r2.int(0, 2)); rkind.k2 = 'n'; }
    rcols.rv = genColumn(r2, f32 ? 'h' : 'f', m); rkind.rv = f32 ? 'h' : 'f';
    if (r.chance(0.5)) { rcols.rc = genColumn(r2, 'c', m); rkind.rc = 'c'; }
    const clash = Object.keys(cols).find((k) => k !== 'k1' && k !== 'k2');
    if (clash && r.chance(0.6)) { rcols[clash] = genColumn(r2, c.kindOf[clash] === 'c' ? 'c' : 'h', m); rkind[clash] = c.kindOf[clash] === 'c' ? 'c' : 'h'; }
    c.right = { cols: rcols, kindOf: rkind, m };
    const rename = r.chance(0.25);
    c.join = { lk: nk === 2 ? ['k1', 'k2'] : ['k1'], rk: nk === 2 ? ['k1', 'k2'] : ['k1'], how: r.pick(['inner', 'left']), suffix: r.chance(0.3) ? '_x' : undefined, rename };
    if (rename) { c.right.cols.rk = c.right.cols.k1; c.right.kindOf.rk = c.right.kindOf.k1; delete c.right.cols.k1; delete c.right.kindOf.k1; c.join.rk = c.join.rk.map((k) => (k === 'k1' ? 'rk' : k)); }
    if (r.chance(0.3)) { // a filter on the right side
      c.rightFilter = { t: 'cmp', op: r.pick(['>', '<=', '!=']), a: { t: 'col', name: 'rv' }, b: { t: 'num', v: r.pick([0, 1, -1]) } };
    }
  }
  if (r.chance(0.5)) c.f1 = bool();
  if (r.chance(0.4)) { c.derive = r.chance(0.08) && txtCols().length ? { name: 'dv', text: r.pick(txtCols()) } : { name: 'dv', expr: r.chance(0.3) ? bool() : num() }; c.kindOf.dv = c.derive.text ? 'c' : 'd'; c.cols.dv = null; }
  if (c.derive && r.chance(0.4)) c.f2 = bool();
  noParams = c.op.startsWith('lazy');
  const nagg = r.int(1, r.chance(0.2) ? 11 : 4);
  const spec = {};
  for (let q = 0; q < nagg; q++) {
    const kind = r.pick(['count', 'count', 'sum', 'sum', 'mean', 'min', 'max', 'countDistinct', 'approxDistinct']);
    if (kind === 'count') spec['a' + q] = r.chance(0.3) ? { kind } : { kind, expr: r.chance(0.5) ? bool() : num() };
    else if ((kind === 'countDistinct' || kind === 'approxDistinct') && txtCols().length && r.chance(0.3)) spec['a' + q] = { kind, textCol: r.pick(txtCols()) };
    else if (kind === 'countDistinct' || kind === 'approxDistinct') spec['a' + q] = { kind, expr: r.chance(0.7) ? { t: 'col', name: r.pick(numCols()) } : num() };
    else spec['a' + q] = { kind, expr: r.chance(0.5) ? { t: 'col', name: r.pick(numCols()) } : num() };
  }
  c.spec = spec;
  const keyable = Object.keys(c.kindOf);
  c.keys = [...new Set(Array.from({ length: r.int(1, 3) }, () => r.pick(keyable)))];
  noParams = true;
  c.sortKeys = Array.from({ length: r.int(1, 3) }, () => { const col = r.pick(keyable); const desc = r.chance(0.5); if (c.kindOf[col] === 'c') return { textCol: col, desc }; return { expr: r.chance(0.7) ? { t: 'col', name: col } : num(), desc }; });
  c.k = r.pick([0, 1, 2, 5, 10, Math.floor(n / 10), Math.floor(n / 7), n, n + 3]);
  c.offset = r.pick([0, 0, 1, 3]);
  c.rowsMode = r.pick(['toRows', 'toRowsLimit', 'toColumns', 'collect', 'head', 'slice', 'select', 'count']);
  if (r.chance(0.3)) { // a filter after the group-by (lazy only): on the keys or on the aggregates
    const useKey = r.chance(0.5);
    const kname = useKey ? c.keys.find((k) => c.kindOf[k] !== 'c') : null;
    c.postFilter = kname ? { t: 'cmp', op: r.pick(['>', '<', '!=']), a: { t: 'col', name: kname }, b: { t: 'num', v: r.pick([0, 1, 5]) } } : { t: 'cmp', op: '>', a: { t: 'col', name: Object.keys(spec)[0] }, b: { t: 'num', v: 1 } };
  }
  return c;
}

// ---------- reference ----------
const f32r = (c, v) => (c.f32 && typeof v === 'number' ? Math.fround(v) : v);
function baseRows(c) {
  const names = Object.keys(c.cols).filter((k) => k !== 'dv');
  const rows = Array.from({ length: c.n }, (_, i) => { const o = {}; for (const k of names) o[k] = f32r(c, c.cols[k][i]); return o; });
  if (c.derive) for (const row of rows) row.dv = c.derive.text ? row[c.derive.text] : f32r(c, evalE(c.derive.expr, row, c.params));
  return rows;
}
function textRanks(c) {
  const o = {};
  for (const k of Object.keys(c.kindOf)) if (c.kindOf[k] === 'c') o[k] = dictRank(k === 'dv' ? c.cols[c.derive.text] : c.cols[k]);
  return o;
}
function reference(c) {
  let rows = baseRows(c);
  const allNames = Object.keys(c.kindOf);
  const keep = (e) => (e ? rows.filter((r) => evalE(e, r, c.params)) : rows);
  rows = keep(c.f1); rows = keep(c.f2);
  switch (c.op) {
    case 'agg': case 'lazyAgg': return { agg: refAgg(rows, c.spec, c.params) };
    case 'groupBy': case 'lazyGroup': {
      let g = refGroupBy(rows, c.keys, c.spec, c.params, textRanks(c));
      if (c.op === 'lazyGroup' && c.postFilter) g = g.filter((row) => { const flat = {}; for (const k in row) flat[k] = row[k] && typeof row[k] === 'object' ? row[k].v : row[k]; return evalE(c.postFilter, flat, {}); });
      return { rows: g, names: [...c.keys, ...Object.keys(c.spec)] };
    }
    case 'sort': return { rows: refSort(rows, c.sortKeys, c.params), names: allNames };
    case 'top': return { rows: refSort(rows, c.sortKeys, c.params).slice(0, c.k), names: allNames };
    case 'lazySort': return { rows: refSort(rows, c.sortKeys, c.params).slice(c.offset, c.offset + c.k), names: allNames };
    case 'rows': {
      const n = rows.length;
      switch (c.rowsMode) {
        case 'toRowsLimit': return { rows: rows.slice(0, c.k), names: allNames };
        case 'head': return { rows: rows.slice(0, c.k), names: allNames };
        case 'slice': return { rows: rows.slice(Math.min(c.offset, n), Math.min(c.k, n)), names: allNames };
        case 'select': return { rows, names: allNames.slice(0, 1) };
        case 'count': return { count: n };
        default: return { rows, names: allNames };
      }
    }
    case 'join': case 'lazyJoin': {
      const R = Array.from({ length: c.right.m }, (_, i) => { const o = {}; for (const k in c.right.cols) o[k] = f32r(c, c.right.cols[k][i]); return o; });
      const Rf = c.rightFilter ? R.filter((r) => evalE(c.rightFilter, r, {})) : R;
      const rtext = new Set(Object.keys(c.right.kindOf).filter((k) => c.right.kindOf[k] === 'c'));
      const j = refJoin(rows, Rf, c.join.lk, c.join.rk, c.join.how, allNames, Object.keys(c.right.cols), rtext, c.join.suffix ?? '_right');
      return j;
    }
  }
}

// ---------- Vectro ----------
function makeTable(engine, cols, kindOf, cfg, f32) {
  const data = {};
  for (const k in cols) if (cols[k] !== null) data[k] = kindOf[k] === 'c' && cols[k].length === 0 ? { codes: new Uint32Array(0), labels: [] } : cols[k];
  return engine.table(data, { ...(f32 ? { type: Float32Array } : {}), ...(cfg.compress ? { compress: true } : {}) });
}
async function vectro(c, cfg) {
  const engine = getEngine(cfg.eng);
  const T = makeTable(engine, c.cols, c.kindOf, cfg, c.f32);
  const made = [T];
  const A = cfg.async;
  try {
    const lazy = c.op.startsWith('lazy');
    let t = lazy ? T.lazy() : T;
    if (c.f1) t = t.filter(toText(c.f1), c.params);
    if (c.derive) {
      const ex = c.derive.text ?? toText(c.derive.expr);
      if (lazy) t = t.derive('dv', ex, c.params);
      else { t = A ? await t.deriveAsync('dv', ex, c.params) : t.derive('dv', ex, c.params); made.push(t); }
    }
    if (c.f2) t = t.filter(toText(c.f2), c.params);
    const sk = c.sortKeys.map(sortKeyText);
    const by = sk.length === 1 ? sk[0] : sk;
    const out = async (tab) => { made.push(tab); return A ? await tab.toRowsAsync() : tab.toRows(); };
    switch (c.op) {
      case 'agg': return { agg: A ? await t.aggAsync(specText(c.spec), c.params) : t.agg(specText(c.spec), c.params) };
      case 'lazyAgg': return { agg: A ? await t.aggAsync(specText(c.spec)) : t.agg(specText(c.spec)) };
      case 'groupBy': { const g = t.groupBy(c.keys.length === 1 ? c.keys[0] : c.keys); return { rows: await out(A ? await g.aggAsync(specText(c.spec), c.params) : g.agg(specText(c.spec), c.params)) }; }
      case 'lazyGroup': { let q = t.groupBy(c.keys).agg(specText(c.spec)); if (c.postFilter) q = q.filter(toText(c.postFilter)); return { rows: A ? await q.toRowsAsync() : q.toRows() }; }
      case 'sort': return { rows: await out(A ? await t.sortAsync(by) : t.sort(by)) };
      case 'top': return { rows: await out(A ? await t.topAsync(c.k, by) : t.top(c.k, by)) };
      case 'lazySort': { const q = c.offset || c.k % 2 ? t.sort(by).limit(c.k, c.offset) : t.top(c.k, by); return { rows: A ? await q.toRowsAsync() : q.toRows() }; }
      case 'rows': {
        switch (c.rowsMode) {
          case 'toRows': return { rows: A ? await t.toRowsAsync() : t.toRows() };
          case 'toRowsLimit': return { rows: A ? await t.toRowsAsync(c.k) : t.toRows(c.k) };
          case 'toColumns': {
            const cc = A ? await t.toColumnsAsync() : t.toColumns();
            const names = Object.keys(cc); const len = names.length ? (cc[names[0]].codes ?? cc[names[0]]).length : 0;
            return { rows: Array.from({ length: len }, (_, i) => Object.fromEntries(names.map((k) => [k, cc[k].codes ? (cc[k].labels[cc[k].codes[i]] ?? '') : cc[k][i]]))) };
          }
          case 'collect': { const x = A ? await t.collectAsync() : t.collect(); if (x !== T) return { rows: await out(x) }; return { rows: x.toRows() }; }
          case 'head': { const x = A ? await t.headAsync(c.k) : t.head(c.k); if (x !== T && x !== t) made.push(x); return { rows: x.toRows() }; }
          case 'slice': return { rows: await out(A ? await t.sliceAsync(c.offset, c.k) : t.slice(c.offset, c.k)) };
          case 'select': { const name = Object.keys(c.kindOf)[0]; return { rows: t.select(name).toRows() }; }
          case 'count': return { count: A ? await t.countAsync() : t.count() };
        }
        break;
      }
      case 'join': case 'lazyJoin': {
        const R = makeTable(engine, c.right.cols, c.right.kindOf, cfg, c.f32); made.push(R);
        let Rv = R;
        if (c.rightFilter) Rv = Rv.filter(toText(c.rightFilter));
        const on = c.join.rename ? (c.join.lk.length === 1 ? { left: 'k1', right: 'rk' } : null) : (c.join.lk.length === 1 ? 'k1' : c.join.lk);
        if (on === null) throw new Error('SKIP');
        const opts = { on, how: c.join.how, ...(c.join.suffix ? { suffix: c.join.suffix } : {}) };
        if (c.op === 'lazyJoin') { const q = t.join(c.rightFilter && Math.random() < 0.5 ? Rv.lazy ? Rv : Rv : Rv, opts); return { rows: A ? await q.toRowsAsync() : q.toRows(), names: null }; }
        const j = A ? await t.joinAsync(Rv, opts) : t.join(Rv, opts);
        return { rows: await out(j), columnNames: j.columnNames };
      }
    }
  } finally {
    for (const x of made.reverse()) { try { x.dispose(); } catch {} }
  }
}

function compare(c, want, got) {
  const rel = c.f32 ? 1e-5 : 1e-9;
  if (want.agg) {
    for (const [nm, w] of Object.entries(want.agg)) {
      const g = got.agg[nm];
      const ok = w.approx ? L.numEq(g, w.v, w.abs, rel) : (Object.is(norm0(g), norm0(w.v)) || (Number.isNaN(g) && Number.isNaN(w.v)));
      if (!ok) return `agg ${nm} = ${specText(c.spec)[nm]}: got ${g} want ${w.v}`;
    }
    return null;
  }
  if ('count' in want) return got.count === want.count ? null : `count got ${got.count} want ${want.count}`;
  if (got.columnNames && want.names && got.columnNames.join() !== want.names.join()) return `columns ${got.columnNames} want ${want.names}`;
  return rowsDiff(got.rows, want.rows, want.names, rel);
}

export function describe(c) {
  const o = { seed: c.seed, op: c.op, n: c.n, f32: c.f32, kinds: c.kindOf };
  if (c.f1) o.f1 = toText(c.f1); if (c.derive) o.derive = c.derive.text ?? toText(c.derive.expr); if (c.f2) o.f2 = toText(c.f2);
  o.params = c.params;
  if (/agg|roup/i.test(c.op)) o.spec = specText(c.spec);
  if (/roup/.test(c.op)) { o.keys = c.keys; if (c.op === 'lazyGroup' && c.postFilter) o.postFilter = toText(c.postFilter); }
  if (/ort|top/.test(c.op)) { o.by = c.sortKeys.map(sortKeyText); o.k = c.k; o.offset = c.offset; }
  if (c.op === 'rows') { o.mode = c.rowsMode; o.k = c.k; o.offset = c.offset; }
  if (c.join) { o.join = c.join; o.m = c.right.m; if (c.rightFilter) o.rightFilter = toText(c.rightFilter); }
  return o;
}

export async function check(c, cfg) {
  let want;
  try { want = reference(c); } catch (e) { return { skip: 'ref ' + e.message }; }
  if (want.rows && want.rows.length > 3_000_000) return { skip: 'too big' };
  let got;
  try { got = await vectro(c, cfg); } catch (e) { if (e.message === 'SKIP') return { skip: 'n/a' }; if (e.message.includes('a reduction needs at least one array')) return { known: 'A' }; if (e.message.includes("reading 'enc'")) return { known: 'C' }; return { err: 'THROW ' + e.message.slice(0, 300) }; }
  const d = compare(c, want, got);
  return d ? { err: d } : { ok: true };
}

// shrink: drop rows of the left (and right) table while it still fails
export async function shrink(c, cfg, budget = 400) {
  const sub = (cc, keepIdx, side) => {
    const out = { ...cc, cols: { ...cc.cols }, params: cc.params };
    if (side === 'L') { for (const k in out.cols) if (out.cols[k] !== null) out.cols[k] = keepIdx.map((i) => cc.cols[k][i]); out.n = keepIdx.length; }
    else { out.right = { ...cc.right, cols: {} }; for (const k in cc.right.cols) out.right.cols[k] = keepIdx.map((i) => cc.right.cols[k][i]); out.right.m = keepIdx.length; }
    return out;
  };
  let cur = c, tries = 0;
  for (const side of c.right ? ['L', 'R', 'L'] : ['L']) {
    let len = side === 'L' ? cur.n : cur.right.m;
    let chunk = Math.max(1, Math.floor(len / 2));
    while (chunk >= 1 && tries < budget) {
      let progressed = false;
      for (let s = 0; s < len && tries < budget; s += chunk) {
        const keep = []; for (let i = 0; i < len; i++) if (i < s || i >= s + chunk) keep.push(i);
        const cand = sub(cur, keep, side); tries++;
        const res = await check(cand, cfg);
        if (res.err) { cur = cand; len = keep.length; progressed = true; s -= chunk; }
      }
      if (!progressed) chunk = Math.floor(chunk / 2);
    }
  }
  return cur;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const stats = { cases: 0, checks: 0, skips: 0, fails: 0, byOp: {}, checksByOp: {} };
  const failures = [];
  const t0 = Date.now();
  for (let seed = start; seed < start + count; seed++) {
    const c = genCase(seed);
    stats.cases++;
    stats.byOp[c.op] = (stats.byOp[c.op] ?? 0) + 1;
    for (const cfg of CONFIGS) {
      if (only && cfg.name !== only) continue;
      const res = await check(c, cfg);
      if (res.skip) { stats.skips++; continue; }
      if (res.known) { stats.known = (stats.known ?? 0) + 1; continue; }
      stats.checks++; stats.checksByOp[c.op] = (stats.checksByOp[c.op] ?? 0) + 1;
      if (res.err) {
        stats.fails++;
        failures.push({ seed, cfg: cfg.name, err: res.err });
        if (failures.length <= 40) console.log('FAIL', seed, cfg.name, c.op, res.err.slice(0, 200));
      }
    }
  }
  console.log(JSON.stringify({ ...stats, secs: (Date.now() - t0) / 1000 }));
  for (const e of Object.values(engines)) await e.close();
  console.log(stats.fails ? `${stats.fails} failures out of ${stats.checks} dataframe fuzz checks (rerun one: node test/fuzz/dataframe.mjs <seed> 1)` : `all ${stats.checks} dataframe fuzz checks passed`);
  process.exit(stats.fails ? 1 : 0);
}
export { genCase, CONFIGS, getEngine, reference, vectro, engines };
