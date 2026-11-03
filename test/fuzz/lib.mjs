// Shared pieces for the dataframe fuzzer: a seeded RNG, random tables,
// random expressions (AST -> Vectro text + plain-JS evaluator), and plain-JS
// reference implementations of agg / groupBy / sort / join.
export function rng(seed) {
  let a = seed >>> 0;
  const r = () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  r.int = (lo, hi) => lo + Math.floor(r() * (hi - lo + 1));
  r.pick = (xs) => xs[Math.floor(r() * xs.length)];
  r.chance = (p) => r() < p;
  return r;
}

export const LABELS = ['EU', 'US', 'APAC', '', 'é', 'É', '日本', '😀', 'Eu', 'eu-north', 'EU-West', "O'Brien", 'a b', 'b', 'x'.repeat(300) + 'end', 'LATAM'];
const SPECIAL = [NaN, Infinity, -Infinity, -0, 0, 1e-300, -1e-300, 5e-324, 1e300, -1e300, 2 ** 53, 2 ** 53 - 1, -(2 ** 53), 0.1, 0.2, 0.30000000000000004];

// Column kinds: i small ints, w wide ints (big ids), f floats with specials,
// p cents (compressible), c text, m all missing, h halves (f32 exact)
export function genColumn(r, kind, n, opts = {}) {
  const miss = opts.miss ?? (r.chance(0.5) ? 0 : r.pick([0.01, 0.1, 0.5]));
  const out = new Array(n);
  if (kind === 'i') { const k = r.int(1, r.chance(0.2) ? 70000 : 40); const off = r.chance(0.3) ? r.int(-50, 50) : 0; for (let j = 0; j < n; j++) out[j] = r.chance(miss) ? NaN : off + r.int(0, k); }
  else if (kind === 'w') { const step = r.pick([1, 7919, 13, 1e6]); const base = r.pick([0, 1e9, 1e12, 2 ** 53 - 1e7 * 7919, -1e12]); const k = r.int(1, r.chance(0.3) ? 2_000_000 : 500); for (let j = 0; j < n; j++) out[j] = r.chance(miss) ? NaN : base + r.int(0, k) * step; }
  else if (kind === 'f') { const sp = r.chance(0.5) ? 0 : r.pick([0.01, 0.1]); const scale = r.pick([1, 1e-6, 1e6, 1e15]); for (let j = 0; j < n; j++) out[j] = r.chance(miss) ? NaN : r.chance(sp) ? r.pick(SPECIAL) : (r() - 0.3) * scale; }
  else if (kind === 'p') { const k = r.pick([100, 10000, 1e6]); for (let j = 0; j < n; j++) out[j] = r.chance(miss) ? NaN : r.int(0, k) / 100; }
  else if (kind === 'h') { for (let j = 0; j < n; j++) out[j] = r.chance(miss) ? NaN : r.int(-200, 200) / 2; }
  else if (kind === 'm') { for (let j = 0; j < n; j++) out[j] = NaN; }
  else if (kind === 'c') { const pool = opts.pool ?? LABELS.filter(() => r.chance(0.6)); if (!pool.length) pool.push('EU'); const many = !opts.pool && r.chance(0.15); for (let j = 0; j < n; j++) out[j] = many ? 'k' + r.int(0, 3000) : r.pick(pool); }
  return out;
}

// ---------- expressions ----------
const fmtNum = (v) => (v < 0 || Object.is(v, -0) ? `(-${fmtNum(-v)})` : String(v));
export function toText(e) {
  switch (e.t) {
    case 'col': return e.name;
    case 'num': return fmtNum(e.v);
    case 'par': return e.name;
    case 'bin': return `(${toText(e.a)} ${e.op} ${toText(e.b)})`;
    case 'neg': return `(-${toText(e.a)})`;
    case 'fn': return `${e.f}(${e.args.map(toText).join(', ')})`;
    case 'tern': return `(${toText(e.c)} ? ${toText(e.a)} : ${toText(e.b)})`;
    case 'cmp': return `(${toText(e.a)} ${e.op} ${toText(e.b)})`;
    case 'and': return `(${toText(e.a)} && ${toText(e.b)})`;
    case 'or': return `(${toText(e.a)} || ${toText(e.b)})`;
    case 'not': return `!${toText(e.a)}`;
    case 'txt': {
      const q = (s) => (s.includes("'") ? `"${s}"` : `'${s}'`);
      if (e.kind === '==' || e.kind === '!=') return `(${e.col} ${e.kind} ${q(e.vals[0])})`;
      return `${e.kind}(${e.col}, ${e.vals.map(q).join(', ')})`;
    }
  }
  throw new Error('bad node ' + e.t);
}
const likeRe = (p, ci) => new RegExp('^' + [...p].map((ch) => (ch === '%' ? '.*' : ch === '_' ? '.' : ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))).join('') + '$', ci ? 'sui' : 'su'); // like and ilike: _ is one code point
export function evalE(e, row, params) {
  switch (e.t) {
    case 'col': return row[e.name];
    case 'num': return e.v;
    case 'par': return params[e.name];
    case 'neg': return -evalE(e.a, row, params);
    case 'bin': { const a = evalE(e.a, row, params), b = evalE(e.b, row, params); return e.op === '+' ? a + b : e.op === '-' ? a - b : e.op === '*' ? a * b : a / b; }
    case 'fn': {
      const v = e.args.map((x) => evalE(x, row, params));
      switch (e.f) {
        case 'abs': return Math.abs(v[0]); case 'sqrt': return Math.sqrt(v[0]); case 'floor': return Math.floor(v[0]); case 'ceil': return Math.ceil(v[0]);
        case 'min': return Math.min(v[0], v[1]); case 'max': return Math.max(v[0], v[1]);
        case 'coalesce': { for (const x of v) if (!Number.isNaN(x)) return x; return NaN; }
        case 'isMissing': return Number.isNaN(v[0]) ? 1 : 0;
      }
      throw new Error('fn ' + e.f);
    }
    case 'tern': return evalE(e.c, row, params) ? evalE(e.a, row, params) : evalE(e.b, row, params);
    case 'cmp': { const a = evalE(e.a, row, params), b = evalE(e.b, row, params); const o = e.op; return (o === '<' ? a < b : o === '>' ? a > b : o === '<=' ? a <= b : o === '>=' ? a >= b : o === '==' ? a == b : a != b) ? 1 : 0; }
    case 'and': return evalE(e.a, row, params) && evalE(e.b, row, params) ? 1 : 0;
    case 'or': return evalE(e.a, row, params) || evalE(e.b, row, params) ? 1 : 0;
    case 'not': return evalE(e.a, row, params) ? 0 : 1;
    case 'txt': {
      const s = row[e.col];
      switch (e.kind) {
        case '==': return s === e.vals[0] ? 1 : 0; case '!=': return s !== e.vals[0] ? 1 : 0;
        case 'in': return e.vals.includes(s) ? 1 : 0;
        case 'startsWith': return s.startsWith(e.vals[0]) ? 1 : 0; case 'endsWith': return s.endsWith(e.vals[0]) ? 1 : 0;
        case 'contains': return s.includes(e.vals[0]) ? 1 : 0;
        case 'like': return likeRe(e.vals[0], false).test(s) ? 1 : 0;
        case 'ilike': return likeRe(e.vals[0], true).test(s) ? 1 : 0;
      }
    }
  }
  throw new Error('eval ' + e.t);
}
export const isBool = (e) => ['cmp', 'and', 'or', 'not', 'txt'].includes(e.t) || (e.t === 'fn' && e.f === 'isMissing');
export function colsUsed(e, acc = new Set()) {
  if (!e || typeof e !== 'object') return acc;
  if (e.t === 'col') acc.add(e.name);
  if (e.t === 'txt') acc.add(e.col);
  for (const k of ['a', 'b', 'c']) if (e[k]) colsUsed(e[k], acc);
  if (e.args) e.args.forEach((x) => colsUsed(x, acc));
  return acc;
}

// ctx: { num: [colnames], text: [colnames], labels: {col: [labels]}, params: {} , f32 }
const CONSTS = [0, 1, 2, 3, 0.5, 0.1, 100, 1e6, 1e-3, 2.5, 7, 1e300];
const CONSTS32 = [0, 1, 2, 3, 0.5, 4, 10, 100];
export function genNum(r, ctx, d = 0) {
  const leaf = () => {
    const x = r();
    if (x < 0.6 && ctx.num.length) return { t: 'col', name: r.pick(ctx.num) };
    if (x < 0.75 && !ctx.noParams) { const name = 'prm' + Object.keys(ctx.params).length; ctx.params[name] = r.pick(ctx.f32 ? CONSTS32 : CONSTS) * (r.chance(0.3) ? -1 : 1); return { t: 'par', name }; }
    return { t: 'num', v: r.pick(ctx.f32 ? CONSTS32 : CONSTS) * (r.chance(0.2) ? -1 : 1) };
  };
  if (d >= 3 || r.chance(0.35)) return leaf();
  const x = r();
  if (ctx.f32) { // keep float32 arithmetic exact: + - * on halves and small ints
    if (x < 0.8) return { t: 'bin', op: r.pick(['+', '-', '*']), a: genNum(r, ctx, d + 1), b: leaf() };
    const f = r.pick(['abs', 'floor', 'ceil', 'min', 'max']);
    return { t: 'fn', f, args: f === 'min' || f === 'max' ? [genNum(r, ctx, d + 1), leaf()] : [genNum(r, ctx, d + 1)] };
  }
  if (x < 0.45) return { t: 'bin', op: r.pick(['+', '-', '*', '/']), a: genNum(r, ctx, d + 1), b: genNum(r, ctx, d + 1) };
  if (x < 0.5) return { t: 'neg', a: genNum(r, ctx, d + 1) };
  if (x < 0.7) { const f = r.pick(['abs', 'sqrt', 'floor', 'ceil', 'min', 'max', 'coalesce']); const k = f === 'min' || f === 'max' || f === 'coalesce' ? 2 : 1; return { t: 'fn', f, args: Array.from({ length: k }, () => genNum(r, ctx, d + 1)) }; }
  if (x < 0.85) return { t: 'tern', c: genBool(r, ctx, d + 1), a: genNum(r, ctx, d + 1), b: genNum(r, ctx, d + 1) };
  return genBool(r, ctx, d + 1);
}
export function fixF32Fn(e) { if (e.t === 'fn' && (e.f === 'min' || e.f === 'max') && e.args.length === 1) e.args.push({ t: 'num', v: 1 }); for (const k of ['a', 'b', 'c']) if (e[k]) fixF32Fn(e[k]); if (e.args) e.args.forEach(fixF32Fn); return e; }
export function genBool(r, ctx, d = 0) {
  const x = r();
  if (d < 3 && x < 0.15) return { t: r.pick(['and', 'or']), a: genBool(r, ctx, d + 1), b: genBool(r, ctx, d + 1) };
  if (d < 3 && x < 0.2) return { t: 'not', a: genBool(r, ctx, d + 1) };
  if (x < 0.4 && ctx.text.length) {
    const col = r.pick(ctx.text); const labs = ctx.labels[col];
    const kind = r.pick(['==', '!=', 'in', 'startsWith', 'endsWith', 'contains', 'like', 'ilike']);
    const lab = () => (r.chance(0.85) && labs.length ? r.pick(labs) : r.pick(['zz', 'MISSING', 'E']));
    let vals;
    if (kind === 'in') vals = Array.from({ length: r.int(1, 3) }, lab);
    else if (kind === 'like' || kind === 'ilike') { const s = lab(), cp = [...s]; vals = [r.pick([s, cp.slice(0, 1).join('') + '%', '%' + cp.slice(-1).join(''), '_' + cp.slice(1).join(''), '%', s.toLowerCase(), '_'])]; }
    else if (kind === 'startsWith' || kind === 'endsWith' || kind === 'contains') { const s = lab(), cp = [...s]; vals = [r.pick([s, cp.slice(0, 2).join(''), cp.slice(-2).join(''), cp.slice(1, 3).join('')])]; }
    else vals = [lab()];
    if (vals.some((v) => v.includes("'") && v.includes('"'))) vals = ['EU'];
    return { t: 'txt', kind, col, vals };
  }
  if (x < 0.47 && ctx.num.length) return { t: 'fn', f: 'isMissing', args: [{ t: 'col', name: r.pick(ctx.num) }] };
  return { t: 'cmp', op: r.pick(['<', '>', '<=', '>=', '==', '!=']), a: genNum(r, ctx, d + 1), b: genNum(r, ctx, d + 2) };
}

// ---------- references ----------
export const norm0 = (v) => (Object.is(v, -0) ? 0 : v);
export function refAgg(rows, spec, params) {
  // spec: { name: { kind, expr|null, textCol } } -> { name: { v, absSum } }
  const out = {};
  for (const [name, a] of Object.entries(spec)) {
    if (a.kind === 'count' && !a.expr) { out[name] = { v: rows.length }; continue; }
    if (a.kind === 'countDistinct' || a.kind === 'approxDistinct') {
      const s = new Set();
      for (const r of rows) {
        if (a.textCol) { const v = r[a.textCol]; if (v !== '') s.add(v); }
        else { const v = evalE(a.expr, r, params); if (!Number.isNaN(v)) s.add(norm0(v)); }
      }
      out[name] = { v: s.size }; continue;
    }
    let sum = 0, abs = 0, cnt = 0, mn = NaN, mx = NaN;
    for (const r of rows) {
      const v = evalE(a.expr, r, params);
      if (a.kind === 'count') { if (isBool(a.expr) ? v : !Number.isNaN(v)) cnt++; continue; }
      if (Number.isNaN(v)) continue;
      cnt++; sum += v; abs += Math.abs(v);
      if (!(v >= mn)) mn = v; if (!(v <= mx)) mx = v;
    }
    if (a.kind === 'count') out[name] = { v: cnt };
    else if (a.kind === 'sum') out[name] = { v: sum, abs, approx: true };
    else if (a.kind === 'mean') out[name] = { v: cnt ? sum / cnt : NaN, abs: cnt ? abs / cnt : 0, approx: true };
    else if (a.kind === 'min') out[name] = { v: cnt ? mn : NaN };
    else if (a.kind === 'max') out[name] = { v: cnt ? mx : NaN };
  }
  return out;
}
export function specText(spec) {
  const o = {};
  for (const [name, a] of Object.entries(spec)) o[name] = a.kind === 'count' && !a.expr ? 'count()' : `${a.kind}(${a.textCol ?? toText(a.expr)})`;
  return o;
}
// numbers: approx with tolerance relative to the sum of |terms|
export function numEq(got, want, abs = 0, rel = 1e-9) {
  if (Number.isNaN(want)) return Number.isNaN(got);
  if (got === want) return true;
  if (!Number.isFinite(want) || !Number.isFinite(got)) {
    // a sum of finite terms can overflow in one order and not another: only if huge
    return false;
  }
  return Math.abs(got - want) <= rel * Math.max(abs, Math.abs(want)) + 1e-300;
}

// text ranks: dictionary order of first appearance
export function dictRank(labels) { const m = new Map(); for (const s of labels) if (!m.has(s)) m.set(s, m.size); return m; }
export function refGroupBy(rows, keys, spec, params, textRank) {
  const groups = new Map();
  for (const r of rows) {
    const tuple = keys.map((k) => norm0(r[k]));
    const id = tuple.map((v) => (typeof v === 'number' ? (Number.isNaN(v) ? 'NaN' : String(v)) : 'S' + v)).join('\u0001');
    let g = groups.get(id);
    if (!g) groups.set(id, (g = { tuple, rows: [] }));
    g.rows.push(r);
  }
  const cmp = (x, y, k) => {
    if (textRank[k]) { x = textRank[k].get(x); y = textRank[k].get(y); }
    if (x === y) return 0;
    if (Number.isNaN(x)) return Number.isNaN(y) ? 0 : 1;
    if (Number.isNaN(y)) return -1;
    return x < y ? -1 : 1;
  };
  const list = [...groups.values()].sort((a, b) => { for (let i = 0; i < keys.length; i++) { const c = cmp(a.tuple[i], b.tuple[i], keys[i]); if (c) return c; } return 0; });
  return list.map((g) => { const o = {}; keys.forEach((k, i) => { o[k] = g.tuple[i]; }); const ag = refAgg(g.rows, spec, params); for (const n in ag) o[n] = ag[n]; return o; });
}
// sort keys: [{ expr|textCol, desc }]
export function refSort(rows, keys, params) {
  const vals = rows.map((r) => keys.map((k) => (k.textCol ? r[k.textCol] : evalE(k.expr, r, params))));
  const idx = rows.map((_, i) => i);
  idx.sort((i, j) => {
    for (let q = 0; q < keys.length; q++) {
      const k = keys[q]; let a = vals[i][q], b = vals[j][q];
      if (k.textCol) {
        const am = a === '', bm = b === '';
        if (am || bm) { if (am && bm) continue; return am ? 1 : -1; }
        if (a === b) continue;
        const c = a < b ? -1 : 1; return k.desc ? -c : c;
      }
      const an = Number.isNaN(a), bn = Number.isNaN(b);
      if (an || bn) { if (an && bn) continue; return an ? 1 : -1; }
      if (a == b) continue;
      const c = a < b ? -1 : 1; return k.desc ? -c : c;
    }
    return i - j;
  });
  return idx.map((i) => rows[i]);
}
export function sortKeyText(k) { return (k.desc ? '-' : '') + (k.textCol ?? (k.expr.t === 'col' ? k.expr.name : `(${toText(k.expr)})`)); }

// join: left/right rows, lk/rk key names (arrays), textKeys set, how, right columns info
export function refJoin(L, R, lk, rk, how, lcols, rcols, rtext, suffix = '_right') {
  const keyOf = (r, ks) => { const parts = []; for (const k of ks) { const v = r[k]; if (typeof v === 'number') { if (Number.isNaN(v)) return null; parts.push('n' + String(norm0(v))); } else { if (v === '') return null; parts.push('s' + v); } } return parts.join('\u0001'); };
  const idx = new Map();
  R.forEach((r, i) => { const k = keyOf(r, rk); if (k === null) return; if (!idx.has(k)) idx.set(k, []); idx.get(k).push(i); });
  // output column names
  const names = [...lcols]; const map = [];
  for (const c of rcols) {
    const ki = rk.indexOf(c);
    if (ki >= 0 && lk[ki] === c) continue;
    let nm = c; while (names.includes(nm)) nm += suffix;
    names.push(nm); map.push([c, nm]);
  }
  let total = 0;
  for (const l of L) { const k = keyOf(l, lk); const ms = k === null ? undefined : idx.get(k); total += ms ? ms.length : how === 'left' ? 1 : 0; }
  if (total > 1_500_000) throw new Error('too big');
  const out = [];
  for (const l of L) {
    const k = keyOf(l, lk);
    const ms = k === null ? undefined : idx.get(k);
    if (ms) for (const i of ms) { const o = {}; for (const c of lcols) o[c] = l[c]; for (const [c, nm] of map) o[nm] = R[i][c]; out.push(o); }
    else if (how === 'left') { const o = {}; for (const c of lcols) o[c] = l[c]; for (const [c, nm] of map) o[nm] = rtext.has(c) ? '' : NaN; out.push(o); }
  }
  return { rows: out, names };
}

// row compare: exact for keys / plain values (-0 == 0), approx for {v, abs, approx}
export function rowsDiff(got, want, names, rel = 1e-9) {
  if (got.length !== want.length) return `row count ${got.length} != ${want.length}`;
  for (let i = 0; i < want.length; i++) {
    for (const nm of names) {
      let w = want[i][nm], g = got[i][nm];
      if (w && typeof w === 'object' && 'v' in w) { if (w.approx ? !numEq(g, w.v, w.abs, rel) : !(Object.is(norm0(g), norm0(w.v)) || (Number.isNaN(g) && Number.isNaN(w.v)))) return `row ${i} col ${nm}: got ${g} want ${w.v}`; continue; }
      if (typeof w === 'number') { if (!(Object.is(norm0(g), norm0(w)) || (Number.isNaN(g) && Number.isNaN(w)))) return `row ${i} col ${nm}: got ${g} want ${w}`; }
      else if (g !== w) return `row ${i} col ${nm}: got ${JSON.stringify(g)} want ${JSON.stringify(w)}`;
    }
  }
  return null;
}
