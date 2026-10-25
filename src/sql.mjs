// SQL on Vectro tables: sql(text, { orders, customers }) returns a lazy query
// (see plan.mjs) — call .toRows(), .collect(), .count() or .explain().
//
//   SELECT [DISTINCT] items FROM t [AS a]
//     [[INNER | LEFT [OUTER]] JOIN u [AS b] ON a.x = b.y [AND ...] | USING (x, ...)]...
//     [WHERE cond] [GROUP BY exprs] [HAVING cond]
//     [ORDER BY expr [ASC | DESC], ...] [LIMIT n [OFFSET m]]
//
// Expressions: numbers, 'text', DATE '2024-01-01', TIMESTAMP '...', INTERVAL
// '7 days', NULL, TRUE / FALSE; + - * / %; = <> != < <= > >=; AND OR NOT;
// BETWEEN, IN (...), LIKE, IS [NOT] NULL, CASE WHEN ... THEN ... ELSE ... END;
// ABS SQRT FLOOR CEIL ROUND LEAST GREATEST COALESCE IFNULL NULLIF CAST
// YEAR MONTH DAY HOUR MINUTE SECOND EXTRACT(part FROM x) DATE_TRUNC('month', x)
// and the aggregates COUNT(*) COUNT(x) COUNT(DISTINCT x) SUM AVG MIN MAX
// APPROX_COUNT_DISTINCT, anywhere in SELECT / HAVING / ORDER BY.
// Text values can be compared, matched and grouped, not computed.

import { Query } from './plan.mjs';

const KEYWORDS = new Set(['SELECT', 'DISTINCT', 'FROM', 'AS', 'JOIN', 'INNER', 'LEFT', 'OUTER', 'ON', 'USING', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'AND', 'OR', 'NOT', 'BETWEEN', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULLS', 'FIRST', 'LAST', 'CAST', 'DATE', 'TIMESTAMP', 'INTERVAL', 'EXTRACT', 'ALL']);

class SqlError extends SyntaxError {}
const fail = (msg) => { throw new SqlError(`vectro SQL: ${msg}`); };

// ---- tokens ---------------------------------------------------------------------------
function tokenize(src) {
  const toks = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (/\s/.test(c)) { i++; continue; }
    if (src.startsWith('--', i)) { while (i < src.length && src[i] !== '\n') i++; continue; }
    if (src.startsWith('/*', i)) { const e = src.indexOf('*/', i + 2); i = e < 0 ? src.length : e + 2; continue; }
    if (c === "'") { // 'text' ('' is a quote)
      let v = '';
      i++;
      for (;;) {
        if (i >= src.length) fail('a text in quotes is not closed');
        if (src[i] === "'") { if (src[i + 1] === "'") { v += "'"; i += 2; continue; } i++; break; }
        v += src[i++];
      }
      toks.push({ t: 'str', v });
      continue;
    }
    if (c === '"' || c === '`') { // "quoted name"
      const e = src.indexOf(c, i + 1);
      if (e < 0) fail('a quoted name is not closed');
      toks.push({ t: 'id', v: src.slice(i + 1, e) });
      i = e + 1;
      continue;
    }
    const num = /^(\d+\.?\d*(e[+-]?\d+)?|\.\d+(e[+-]?\d+)?)/i.exec(src.slice(i));
    if (num) { toks.push({ t: 'num', v: Number(num[0]) }); i += num[0].length; continue; }
    const id = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
    if (id) {
      const up = id[0].toUpperCase();
      toks.push(KEYWORDS.has(up) ? { t: 'kw', v: up } : { t: 'id', v: id[0] });
      i += id[0].length;
      continue;
    }
    const op = ['<>', '!=', '<=', '>=', '||', '::'].find((o) => src.startsWith(o, i)) ?? c;
    if (op.length === 1 && !'+-*/%=<>(),.;'.includes(op)) fail(`unexpected "${c}"`);
    toks.push({ t: 'op', v: op });
    i += op.length;
  }
  return toks;
}

// ---- parser ---------------------------------------------------------------------------------
function parseSql(src) {
  const toks = tokenize(src);
  let p = 0;
  const peek = (o = 0) => toks[p + o];
  const isKw = (v, o = 0) => peek(o)?.t === 'kw' && peek(o).v === v;
  const isOp = (v, o = 0) => peek(o)?.t === 'op' && peek(o).v === v;
  const eatKw = (v) => { if (isKw(v)) { p++; return true; } return false; };
  const eatOp = (v) => { if (isOp(v)) { p++; return true; } return false; };
  const needKw = (v) => { if (!eatKw(v)) fail(`expected ${v} ${near()}`); };
  const needOp = (v) => { if (!eatOp(v)) fail(`expected "${v}" ${near()}`); };
  const near = () => (peek() ? `near "${peek().v}"` : 'at the end');
  const ident = () => { const t = peek(); if (t?.t !== 'id') fail(`expected a name ${near()}`); p++; return t.v; };

  function expr() { return or(); }
  function or() { let l = and(); while (eatKw('OR')) l = { k: 'bin', op: 'OR', l, r: and() }; return l; }
  function and() { let l = not(); while (eatKw('AND')) l = { k: 'bin', op: 'AND', l, r: not() }; return l; }
  function not() { if (eatKw('NOT')) return { k: 'not', x: not() }; return cmp(); }
  function cmp() {
    const l = add();
    const negate = (x) => ({ k: 'not', x });
    if (isKw('IS')) {
      p++;
      const neg = eatKw('NOT');
      needKw('NULL');
      return neg ? negate({ k: 'isnull', x: l }) : { k: 'isnull', x: l };
    }
    const neg = isKw('NOT') && (isKw('BETWEEN', 1) || isKw('IN', 1) || isKw('LIKE', 1)) ? (p++, true) : false;
    if (eatKw('BETWEEN')) { const lo = add(); needKw('AND'); const hi = add(); const e = { k: 'between', x: l, lo, hi }; return neg ? negate(e) : e; }
    if (eatKw('IN')) {
      needOp('(');
      const list = [expr()];
      while (eatOp(',')) list.push(expr());
      needOp(')');
      const e = { k: 'in', x: l, list };
      return neg ? negate(e) : e;
    }
    if (eatKw('LIKE')) { const e = { k: 'like', x: l, pattern: add() }; return neg ? negate(e) : e; }
    const op = ['=', '<>', '!=', '<', '<=', '>', '>='].find((o) => isOp(o));
    if (op) { p++; return { k: 'bin', op, l, r: add() }; }
    return l;
  }
  function add() {
    let l = mul();
    for (;;) {
      if (eatOp('+')) l = { k: 'bin', op: '+', l, r: mul() };
      else if (eatOp('-')) l = { k: 'bin', op: '-', l, r: mul() };
      else if (isOp('||')) fail('text can be compared and grouped, not joined with ||');
      else return l;
    }
  }
  function mul() {
    let l = unary();
    for (;;) {
      const op = ['*', '/', '%'].find((o) => isOp(o));
      if (!op) return l;
      p++;
      l = { k: 'bin', op, l, r: unary() };
    }
  }
  function unary() {
    if (eatOp('-')) return { k: 'neg', x: unary() };
    if (eatOp('+')) return unary();
    let e = primary();
    while (eatOp('::')) e = { k: 'cast', x: e, type: typeName() };
    return e;
  }
  function typeName() {
    const t = peek();
    if (!t || (t.t !== 'id' && t.t !== 'kw')) fail(`expected a type ${near()}`);
    p++;
    if (eatOp('(')) { while (!eatOp(')')) p++; } // DECIMAL(10, 2)
    return t.v.toUpperCase();
  }
  function primary() {
    const t = peek();
    if (!t) fail('the query ends too early');
    if (t.t === 'num') { p++; return { k: 'num', v: t.v }; }
    if (t.t === 'str') { p++; return { k: 'str', v: t.v }; }
    if (eatOp('(')) { const e = expr(); needOp(')'); return e; }
    if (t.t === 'kw') {
      if (eatKw('NULL')) return { k: 'null' };
      if (eatKw('TRUE')) return { k: 'num', v: 1 };
      if (eatKw('FALSE')) return { k: 'num', v: 0 };
      if (isKw('DATE') || isKw('TIMESTAMP')) {
        if (peek(1)?.t === 'str') { p++; return { k: 'date', v: toks[p++].v }; }
      }
      if (eatKw('INTERVAL')) { const s = peek(); if (s?.t !== 'str') fail("INTERVAL needs text, e.g. INTERVAL '7 days'"); p++; return { k: 'interval', v: s.v, unit: peek()?.t === 'id' ? ident() : null }; }
      if (eatKw('CASE')) {
        const base = isKw('WHEN') ? null : expr();
        const whens = [];
        while (eatKw('WHEN')) { const c = expr(); needKw('THEN'); whens.push({ c, v: expr() }); }
        if (!whens.length) fail('CASE needs WHEN ... THEN ...');
        const otherwise = eatKw('ELSE') ? expr() : { k: 'null' };
        needKw('END');
        return { k: 'case', base, whens, otherwise };
      }
      if (eatKw('CAST')) { needOp('('); const x = expr(); needKw('AS'); const type = typeName(); needOp(')'); return { k: 'cast', x, type }; }
      if (eatKw('EXTRACT')) {
        needOp('(');
        const part = peek(); p++;
        needKw('FROM');
        const x = expr();
        needOp(')');
        return { k: 'call', fn: String(part.v).toUpperCase(), args: [x] };
      }
      fail(`unexpected ${t.v} ${near()}`);
    }
    if (t.t === 'id') {
      p++;
      if (eatOp('(')) { // a function
        const fn = t.v.toUpperCase();
        if (eatOp('*')) { needOp(')'); return { k: 'call', fn, args: [], star: true }; }
        const distinct = eatKw('DISTINCT');
        eatKw('ALL');
        const args = [];
        if (!isOp(')')) { args.push(expr()); while (eatOp(',')) args.push(expr()); }
        needOp(')');
        return { k: 'call', fn, args, distinct };
      }
      if (eatOp('.')) {
        if (eatOp('*')) return { k: 'star', table: t.v };
        return { k: 'col', table: t.v, name: ident() };
      }
      return { k: 'col', name: t.v };
    }
    if (eatOp('*')) return { k: 'star' };
    fail(`unexpected "${t.v}"`);
  }

  // SELECT ...
  needKw('SELECT');
  const distinct = eatKw('DISTINCT');
  eatKw('ALL');
  const items = [];
  do {
    const e = expr();
    let alias = null;
    if (eatKw('AS')) alias = ident();
    else if (peek()?.t === 'id') alias = ident();
    items.push({ e, alias });
  } while (eatOp(','));
  needKw('FROM');
  const tableRef = () => { const name = ident(); let alias = name; if (eatKw('AS')) alias = ident(); else if (peek()?.t === 'id') alias = ident(); return { name, alias }; };
  const from = tableRef();
  const joins = [];
  for (;;) {
    let how = null;
    if (eatKw('JOIN')) how = 'inner';
    else if (isKw('INNER')) { p++; needKw('JOIN'); how = 'inner'; }
    else if (isKw('LEFT')) { p++; eatKw('OUTER'); needKw('JOIN'); how = 'left'; }
    else if (isOp(',')) fail('use JOIN ... ON instead of commas between tables');
    if (!how) break;
    const ref = tableRef();
    if (eatKw('USING')) { needOp('('); const cols = [ident()]; while (eatOp(',')) cols.push(ident()); needOp(')'); joins.push({ ...ref, how, using: cols }); }
    else { needKw('ON'); joins.push({ ...ref, how, on: expr() }); }
  }
  const where = eatKw('WHERE') ? expr() : null;
  let groupBy = [];
  if (eatKw('GROUP')) { needKw('BY'); do groupBy.push(expr()); while (eatOp(',')); }
  const having = eatKw('HAVING') ? expr() : null;
  const orderBy = [];
  if (eatKw('ORDER')) {
    needKw('BY');
    do {
      const e = expr();
      let desc = false;
      if (eatKw('DESC')) desc = true; else eatKw('ASC');
      if (eatKw('NULLS')) { if (eatKw('FIRST')) fail('NULLS FIRST is not supported (missing values sort last)'); needKw('LAST'); }
      orderBy.push({ e, desc });
    } while (eatOp(','));
  }
  let limit = null, offset = 0;
  if (eatKw('LIMIT')) { const t = peek(); if (t?.t !== 'num') fail('LIMIT needs a number'); p++; limit = t.v; }
  if (eatKw('OFFSET')) { const t = peek(); if (t?.t !== 'num') fail('OFFSET needs a number'); p++; offset = t.v; }
  eatOp(';');
  if (p < toks.length) fail(`unexpected "${peek().v}" (one SELECT at a time)`);
  return { distinct, items, from, joins, where, groupBy, having, orderBy, limit, offset };
}

// ---- translation ------------------------------------------------------------------------------
const AGG = { COUNT: 'count', SUM: 'sum', AVG: 'mean', MEAN: 'mean', MIN: 'min', MAX: 'max', APPROX_COUNT_DISTINCT: 'approxDistinct' };
const DATE_PART = { YEAR: 'year', MONTH: 'month', DAY: 'day', HOUR: 'hour', MINUTE: 'minute', SECOND: 'second', ISODOW: 'weekday', DAYOFMONTH: 'day' };
const TRUNC = { year: 'yearStart', month: 'monthStart', week: 'weekStart', day: 'dayStart', hour: 'hourStart' };
const INTERVAL_MS = { millisecond: 1, second: 1000, minute: 60_000, hour: 3_600_000, day: 86_400_000, week: 604_800_000 };

const num = (v) => ({ t: 'num', v });
const v = (name) => ({ t: 'var', name });
const bin = (op, l, r) => ({ t: 'bin', op, l, r });
const call = (fn, ...args) => ({ t: 'call', fn, args });
const cond = (c, x, y) => ({ t: 'cond', c, x, y });
const isAgg = (e) => e.k === 'call' && (AGG[e.fn] || e.fn === 'COUNT');
const key = (e) => JSON.stringify(e);
function hasAgg(e) {
  let found = false;
  const visit = (x) => {
    if (!x || typeof x !== 'object') return;
    if (isAgg(x)) { found = true; return; }
    for (const val of Object.values(x)) if (Array.isArray(val)) val.forEach(visit); else if (val && typeof val === 'object') visit(val);
  };
  visit(e);
  return found;
}

// sql(text, tables) -> Query
export function sql(text, tables = {}) {
  const ast = parseSql(String(text));
  const lookup = (name) => {
    const t = tables[name] ?? Object.entries(tables).find(([k]) => k.toLowerCase() === name.toLowerCase())?.[1];
    if (!t) fail(`no table "${name}" (tables: ${Object.keys(tables).join(', ') || 'none given'})`);
    return t;
  };

  // FROM and JOINs: the query's columns, and how each table's columns are named in it
  const first = lookup(ast.from.name);
  let q = first.lazy();
  const scope = [{ alias: ast.from.alias, table: first, names: new Map(first.columnNames.map((n) => [n, n])) }];
  let outNames = [...first.columnNames];
  const kindOfTable = (t, col) => { const c = t.cols?.get(col); return c?.dict || c?.strings ? 'text' : c?.date ? 'date' : 'number'; };
  const kinds = new Map(first.columnNames.map((n) => [n, kindOfTable(first, n)]));

  const resolve = (ref) => { // { table?, name } -> output column name
    if (ref.table) {
      const s = scope.find((x) => x.alias.toLowerCase() === ref.table.toLowerCase());
      if (!s) fail(`no table or alias "${ref.table}"`);
      const out = s.names.get(ref.name) ?? [...s.names].find(([n]) => n.toLowerCase() === ref.name.toLowerCase())?.[1];
      if (!out) fail(`table "${ref.table}" has no column "${ref.name}"`);
      return out;
    }
    const hits = scope.filter((s) => s.names.has(ref.name));
    if (!hits.length) {
      const ci = scope.flatMap((s) => [...s.names].filter(([n]) => n.toLowerCase() === ref.name.toLowerCase()).map(([, o]) => o));
      if (ci.length === 1) return ci[0];
      fail(`no column "${ref.name}"`);
    }
    const outs = new Set(hits.map((s) => s.names.get(ref.name)));
    if (outs.size > 1) fail(`column "${ref.name}" is ambiguous: write ${hits.map((s) => `${s.alias}.${ref.name}`).join(' or ')}`);
    return [...outs][0];
  };

  for (const j of ast.joins) {
    const right = lookup(j.name);
    const rs = { alias: j.alias, table: right, names: new Map() };
    let on;
    const extra = []; // ON conditions that aren't key equalities (inner joins only)
    if (j.using) on = { left: j.using.map((c) => resolve({ name: c })), right: j.using };
    else {
      const lefts = [], rights = [];
      const conds = [];
      const flat = (e) => (e.k === 'bin' && e.op === 'AND' ? [...flat(e.l), ...flat(e.r)] : [e]);
      for (const c of flat(j.on)) {
        const side = (ref) => (ref.k !== 'col' ? null : (ref.table ? ref.table.toLowerCase() === j.alias.toLowerCase() : right.cols?.has(ref.name) && !scope.some((s) => s.names.has(ref.name))) ? 'right' : 'left');
        if (c.k === 'bin' && c.op === '=' && side(c.l) && side(c.r) && side(c.l) !== side(c.r)) {
          const [l, r] = side(c.l) === 'left' ? [c.l, c.r] : [c.r, c.l];
          if (!right.cols?.has(r.name)) fail(`table "${j.name}" has no column "${r.name}"`);
          lefts.push(resolve(l)); rights.push(r.name);
        } else conds.push(c);
      }
      if (!lefts.length) fail(`JOIN ${j.name} needs ON with an equality between the two sides, e.g. ON a.id = b.id`);
      if (conds.length && j.how === 'left') fail('LEFT JOIN ... ON takes key equalities only (move other conditions to WHERE)');
      on = { left: lefts, right: rights };
      extra.push(...conds);
    }
    // output names, as the join makes them: right key columns named like the left key go away
    const sameKey = new Set(on.right.filter((n, k) => on.left[k] === n));
    for (const n of right.columnNames) {
      if (sameKey.has(n)) { rs.names.set(n, n); continue; }
      let out = n;
      while (outNames.includes(out)) out += '_right';
      outNames.push(out);
      rs.names.set(n, out);
      kinds.set(out, kindOfTable(right, n));
    }
    q = q.join(right, { on: on.left.every((n, k) => n === on.right[k]) ? on.left : on, how: j.how });
    scope.push(rs);
    for (const c of extra) q = q.filter(src(translate(c, plain)));
  }

  // translating expressions; `ctx.col(ref)` turns a column into an AST
  const plain = { col: (ref) => v(resolve(ref)), agg: () => fail('aggregates (SUM, COUNT...) belong in SELECT, HAVING or ORDER BY, not here') };
  const kindOf = (e, ctx) => (e.k === 'col' ? (ctx.kindOfCol?.(e) ?? kinds.get(resolve(e))) : e.k === 'date' ? 'date' : e.k === 'str' ? 'text' : 'number');
  function translate(e, ctx) {
    const T = (x) => translate(x, ctx);
    switch (e.k) {
      case 'num': return num(e.v);
      case 'null': return num(NaN);
      case 'str': return { t: 'str', v: e.v };
      case 'date': return call('date', { t: 'str', v: e.v.trim().replace(' ', 'T') });
      case 'interval': {
        const m = /^\s*(-?\d+(?:\.\d+)?)\s*([a-z]+?)s?\s*$/i.exec(e.unit ? `${e.v} ${e.unit}` : e.v);
        const unit = m && INTERVAL_MS[m[2].toLowerCase()];
        if (!unit) fail(`INTERVAL '${e.v}': use a number of milliseconds, seconds, minutes, hours, days or weeks (months and years vary in length)`);
        return num(Number(m[1]) * unit);
      }
      case 'col': return ctx.col(e);
      case 'star': fail('* only works as a whole SELECT item (or COUNT(*))');
      case 'neg': return { t: 'un', op: '-', x: T(e.x) };
      case 'not': return { t: 'un', op: '!', x: T(e.x) };
      case 'bin': {
        if (e.op === 'AND') return bin('&&', T(e.l), T(e.r));
        if (e.op === 'OR') return bin('||', T(e.l), T(e.r));
        const op = { '=': '==', '<>': '!=' }[e.op] ?? e.op;
        if (op === '%') { // SQL: the sign of the dividend (truncate the quotient toward 0)
          const a = T(e.l), b = T(e.r), qt = bin('/', a, b);
          return bin('-', a, bin('*', cond(bin('>=', qt, num(0)), call('floor', qt), call('ceil', qt)), b));
        }
        return bin(op, T(e.l), T(e.r));
      }
      case 'between': { const x = T(e.x); return bin('&&', bin('>=', x, T(e.lo)), bin('<=', x, T(e.hi))); }
      case 'in': {
        const x = T(e.x);
        if (kindOf(e.x, ctx) === 'text') {
          if (e.list.some((i) => i.k !== 'str')) fail('IN on a text column takes text values in quotes');
          return call('in', x, ...e.list.map((i) => ({ t: 'str', v: i.v })));
        }
        return e.list.map((i) => bin('==', x, T(i))).reduce((a, b) => bin('||', a, b));
      }
      case 'like': {
        if (e.pattern.k !== 'str') fail('LIKE needs a pattern in quotes');
        if (kindOf(e.x, ctx) !== 'text') fail('LIKE works on text columns');
        return call('like', T(e.x), { t: 'str', v: e.pattern.v });
      }
      case 'isnull': return kindOf(e.x, ctx) === 'text' ? bin('==', T(e.x), { t: 'str', v: '' }) : call('isMissing', T(e.x));
      case 'case': {
        let out = e.otherwise.k === 'str' || e.whens.some((w) => w.v.k === 'str') ? fail('CASE can give numbers (or dates), not text') : T(e.otherwise);
        for (let i = e.whens.length - 1; i >= 0; i--) {
          const c = e.base ? T({ k: 'bin', op: '=', l: e.base, r: e.whens[i].c }) : T(e.whens[i].c);
          out = cond(c, T(e.whens[i].v), out);
        }
        return out;
      }
      case 'cast': {
        const x = T(e.x);
        if (['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'].includes(e.type)) return cond(bin('>=', x, num(0)), call('floor', x), call('ceil', x));
        if (['DOUBLE', 'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC'].includes(e.type)) return x;
        if (e.type === 'DATE' || e.type === 'TIMESTAMP') return e.x.k === 'str' ? translate({ k: 'date', v: e.x.v }, ctx) : e.type === 'DATE' ? call('dayStart', x) : x;
        fail(`CAST to ${e.type} is not supported`);
      }
      case 'call': {
        if (isAgg(e)) return ctx.agg(e);
        const a = e.args.map(T), need = (n) => { if (a.length !== n) fail(`${e.fn}() takes ${n} value(s)`); };
        switch (e.fn) {
          case 'ABS': case 'SQRT': case 'FLOOR': need(1); return call(e.fn.toLowerCase(), a[0]);
          case 'CEIL': case 'CEILING': need(1); return call('ceil', a[0]);
          case 'ROUND': {
            const f = a.length > 1 ? 10 ** (e.args[1].k === 'num' ? e.args[1].v : fail('ROUND(x, digits) needs a number of digits')) : 1;
            const s = bin('*', a[0], num(f));
            return bin('/', cond(bin('>=', s, num(0)), call('floor', bin('+', s, num(0.5))), call('ceil', bin('-', s, num(0.5)))), num(f));
          }
          case 'LEAST': case 'GREATEST': if (a.length < 2) fail(`${e.fn}() needs two values or more`); return a.reduce((x, y) => call(e.fn === 'LEAST' ? 'min' : 'max', x, y));
          case 'COALESCE': case 'IFNULL': case 'NVL': if (a.length < 2) fail(`${e.fn}() needs two values or more`); return call('coalesce', ...a);
          case 'NULLIF': need(2); return cond(bin('==', a[0], a[1]), num(NaN), a[0]);
          case 'DATE_TRUNC': {
            need(2);
            const part = e.args[0].k === 'str' ? e.args[0].v.toLowerCase() : null;
            if (!TRUNC[part]) fail(`DATE_TRUNC('${part}', ...) : use 'year', 'month', 'week', 'day' or 'hour'`);
            return call(TRUNC[part], a[1]);
          }
          case 'DOW': case 'DAYOFWEEK': need(1); { const w = call('weekday', a[0]); return bin('-', w, bin('*', call('floor', bin('/', w, num(7))), num(7))); } // 0 = Sunday
          case 'WEEKDAY': case 'ISODOW': need(1); return call('weekday', a[0]);
          case 'NOW': case 'CURRENT_TIMESTAMP': return num(Date.now());
          default:
            if (DATE_PART[e.fn]) { need(1); return call(DATE_PART[e.fn], a[0]); }
            fail(`unknown function ${e.fn}()`);
        }
      }
    }
    fail(`can't use ${e.k} here`);
  }
  const src = (a) => toSourceAst(a);

  if (ast.where) {
    if (hasAgg(ast.where)) fail('aggregates (SUM, COUNT...) can\'t be used in WHERE: use HAVING');
    q = q.filter(src(translate(ast.where, plain)));
  }

  // SELECT items (* expanded)
  const items = [];
  ast.items.forEach(({ e, alias }) => {
    if (e.k === 'star') {
      const from = e.table ? scope.filter((s) => s.alias.toLowerCase() === e.table.toLowerCase()) : scope;
      if (!from.length) fail(`no table or alias "${e.table}"`);
      const seen = new Set(items.map((i) => i.name));
      for (const s of from) for (const out of new Set(s.names.values())) if (!seen.has(out)) { seen.add(out); items.push({ e: { k: 'col', name: out, resolved: true }, name: out }); }
      return;
    }
    items.push({ e, alias, name: alias ?? null });
  });
  const used = new Set();
  items.forEach((it, i) => { // output names: alias, column, sum_price, col3...
    if (!it.name) {
      if (it.e.k === 'col') it.name = it.e.name;
      else if (isAgg(it.e)) it.name = it.e.star ? 'count' : `${AGG[it.e.fn] === 'mean' ? 'avg' : it.e.distinct ? 'count_distinct' : it.e.fn.toLowerCase()}${it.e.args[0]?.k === 'col' ? `_${it.e.args[0].name}` : ''}`;
      else it.name = `col${i + 1}`;
    }
    let n = it.name, k = 2;
    while (used.has(n)) n = `${it.name}_${k++}`;
    if (!/^[A-Za-z_]\w*$/.test(n)) fail(`"${n}" can't be a column name: use AS name`);
    it.name = n;
    used.add(n);
  });
  const colOf = (e) => (e.resolved ? v(e.name) : v(resolve(e)));

  const grouped = ast.groupBy.length > 0 || items.some((it) => hasAgg(it.e)) || (ast.having && hasAgg(ast.having));
  const sortKeys = [];
  let hidden = 0;
  if (grouped) {
    // group keys: ordinal, select alias or expression
    const keys = []; // { name, sqlKey, kind }
    for (const g of ast.groupBy) {
      let e = g;
      if (g.k === 'num') e = items[g.v - 1]?.e ?? fail(`GROUP BY ${g.v}: there is no select item ${g.v}`);
      else if (g.k === 'col' && !g.table && items.some((it) => it.alias === g.name) && !scope.some((s) => s.names.has(g.name))) e = items.find((it) => it.alias === g.name).e;
      if (hasAgg(e)) fail('GROUP BY can\'t use aggregates');
      const t = e.k === 'col' ? colOf(e) : translate(e, plain);
      let name;
      if (t.t === 'var') name = t.name;
      else { name = `__g${keys.length}`; q = q.derive(name, src(t)); }
      keys.push({ name, sqlKey: key(e), colName: t.t === 'var' ? t.name : null, kind: e.k === 'col' ? kinds.get(t.name) : 'number' });
    }
    // aggregates, each once
    const aggs = new Map(); // sqlKey -> name
    const spec = {};
    const aggOf = (e) => {
      const k = key(e);
      if (aggs.has(k)) return v(aggs.get(k));
      const name = `__a${aggs.size}`;
      aggs.set(k, name);
      if (e.args.some(hasAgg)) fail('aggregates can\'t contain aggregates');
      const fn = e.fn === 'COUNT' && e.star ? 'count' : e.fn === 'COUNT' && e.distinct ? 'countDistinct' : AGG[e.fn];
      if (e.distinct && e.fn !== 'COUNT') fail(`${e.fn}(DISTINCT ...) is not supported (COUNT(DISTINCT x) is)`);
      if (!e.star && e.args.length !== 1) fail(`${e.fn}() takes one value`);
      spec[name] = e.star ? 'count()' : `${fn}(${src(translate(e.args[0], plain))})`;
      return v(name);
    };
    // after grouping: aggregates and keys by name; other columns must be keys
    const post = {
      agg: aggOf,
      col: (ref) => {
        const k = keys.find((x) => x.sqlKey === key(ref) || (x.colName && x.colName === (ref.resolved ? ref.name : resolve(ref))));
        if (!k) fail(`column "${ref.table ? `${ref.table}.` : ''}${ref.name}" must be in GROUP BY or inside an aggregate`);
        return v(k.name);
      },
      kindOfCol: (ref) => keys.find((x) => x.colName && x.colName === resolve(ref))?.kind,
    };
    const translatePost = (e) => {
      const k = keys.find((x) => x.sqlKey === key(e));
      if (k && e.k !== 'col') return v(k.name);
      return translate(e, post);
    };
    const outs = items.map((it) => ({ ...it, ast: translatePost(it.e) }));
    const havingAst = ast.having ? translatePost(ast.having) : null;
    const orderAsts = ast.orderBy.map((o) => ({ ...o, ast: orderTarget(o.e, outs, translatePost) }));
    q = q.groupBy(keys.map((k) => k.name)).agg(Object.keys(spec).length ? spec : { __a_rows: 'count()' });
    for (const o of outs) if (!(o.ast.t === 'var' && o.ast.name === o.name)) q = q.derive(o.name, src(o.ast));
    if (havingAst) q = q.filter(src(havingAst));
    for (const o of orderAsts) sortKeys.push(sortKey(o));
    q = finish(q, outs.map((o) => o.name));
  } else {
    const outs = items.map((it) => ({ ...it, ast: it.e.k === 'col' ? colOf(it.e) : translate(it.e, plain) }));
    const orderAsts = ast.orderBy.map((o) => ({ ...o, ast: orderTarget(o.e, outs, (e) => (e.k === 'col' ? colOf(e) : translate(e, plain))) }));
    for (const o of outs) if (!(o.ast.t === 'var' && o.ast.name === o.name)) q = q.derive(o.name, src(o.ast));
    for (const o of orderAsts) sortKeys.push(sortKey(o));
    if (ast.distinct) q = q.groupBy(outs.map((o) => o.name)).agg({ __distinct_rows: 'count()' });
    q = finish(q, outs.map((o) => o.name));
  }
  return q;

  // ORDER BY item -> an output name or an expression
  function orderTarget(e, outs, tr) {
    if (e.k === 'num') { const o = outs[e.v - 1]; if (!o) fail(`ORDER BY ${e.v}: there is no select item ${e.v}`); return v(o.name); }
    if (e.k === 'col' && !e.table) { const o = outs.find((x) => x.name === e.name); if (o) return v(o.name); }
    const t = tr(e), same = outs.find((o) => toSourceAst(o.ast) === toSourceAst(t));
    return same ? v(same.name) : t;
  }
  function sortKey(o) {
    let k;
    if (o.ast.t === 'var') k = o.ast.name;
    else { k = `__o${hidden++}`; q = q.derive(k, src(o.ast)); }
    return o.desc ? `-${k}` : k;
  }
  function finish(query, names) {
    let out = query;
    if (sortKeys.length) out = out.sort(sortKeys);
    if (ast.limit !== null) out = out.limit(ast.limit, ast.offset);
    else if (ast.offset) fail('OFFSET needs LIMIT');
    return out.select(...names);
  }
}

// AST -> expression text (as fuse.mjs toSource, with text quoted for the parser)
function toSourceAst(n) {
  switch (n.t) {
    case 'num': return Number.isNaN(n.v) ? 'NaN' : n.v === Infinity ? 'Infinity' : n.v === -Infinity ? '(-Infinity)' : n.v < 0 ? `(${n.v})` : String(n.v);
    case 'str': return JSON.stringify(n.v);
    case 'var': return n.name;
    case 'un': return `${n.op}(${toSourceAst(n.x)})`;
    case 'bin': return `(${toSourceAst(n.l)} ${n.op} ${toSourceAst(n.r)})`;
    case 'call': return `${n.fn}(${n.args.map(toSourceAst).join(', ')})`;
    case 'cond': return `(${toSourceAst(n.c)} ? ${toSourceAst(n.x)} : ${toSourceAst(n.y)})`;
  }
  throw new Error(`bad node ${n.t}`);
}

export { Query };
