// SQL on Vectro tables and datasets: sql(text, { orders, customers }) returns
// a lazy query (see plan.mjs): call .toRows(), .collect(), .count() or
// .explain() (or their Async versions).
//
//   [WITH name [(cols)] AS (query), ...]
//   SELECT [DISTINCT] items [FROM t [AS a] | (query) [AS] a
//     [[INNER | LEFT | RIGHT | FULL [OUTER] | CROSS] JOIN u [AS b] | (query) b [ON a.x = b.y [AND ...] | USING (x, ...)]]...]
//     [WHERE cond] [GROUP BY exprs] [HAVING cond]
//   [UNION [ALL] | INTERSECT | EXCEPT SELECT ...]...
//   [ORDER BY expr [ASC | DESC] [NULLS FIRST | LAST], ...] [LIMIT n | FETCH FIRST n ROWS ONLY] [OFFSET m]
//
// Expressions: numbers, 'text', DATE '2024-01-01', TIMESTAMP '...', INTERVAL
// '7 days', NULL, TRUE / FALSE; + - * / %; = <> != < <= > >=; AND OR NOT;
// BETWEEN, IN (...), IN (SELECT ...), EXISTS (SELECT ...), (SELECT one value),
// LIKE, IS [NOT] NULL, CASE WHEN ... THEN ... ELSE ... END;
// ABS SQRT FLOOR CEIL ROUND LEAST GREATEST COALESCE IFNULL NULLIF CAST
// YEAR MONTH DAY HOUR MINUTE SECOND EXTRACT(part FROM x) DATE_TRUNC('month', x);
// aggregates COUNT(*) COUNT(x) COUNT(DISTINCT x) SUM AVG MIN MAX
// APPROX_COUNT_DISTINCT MEDIAN PERCENTILE_CONT(p) / PERCENTILE_DISC(p) WITHIN
// GROUP (ORDER BY x) QUANTILE_CONT(x, p), anywhere in SELECT / HAVING / ORDER BY;
// window functions f(...) OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS | RANGE
// BETWEEN ... AND ...]): ROW_NUMBER RANK DENSE_RANK PERCENT_RANK CUME_DIST NTILE
// LAG LEAD FIRST_VALUE LAST_VALUE NTH_VALUE and SUM AVG MIN MAX COUNT;
// text functions (see sqltext.mjs: UPPER, SUBSTRING, ||, REGEXP_REPLACE,
// STRFTIME...), ILIKE, text CASE, CAST(x AS VARCHAR), run once per distinct
// value (Table._textmap).
//
// Subqueries become joins, so the whole query stays one plan: IN / EXISTS
// join the subquery's distinct keys (correlated ones on their equalities:
// WHERE b.k = a.k), a single-value subquery joins its one row (correlated
// ones: grouped by the correlation keys).

import { Query, namesOf, findDataset } from './plan.mjs';
import { quoteText } from './fuse.mjs';
import { TEXT_FUNCTIONS, toText, isNull, textToNumber, textToDate } from './sqltext.mjs';

const KEYWORDS = new Set(['SELECT', 'DISTINCT', 'FROM', 'AS', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'OUTER', 'ON', 'USING', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'AND', 'OR', 'NOT', 'BETWEEN', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULLS', 'FIRST', 'LAST', 'CAST', 'DATE', 'TIMESTAMP', 'INTERVAL', 'EXTRACT', 'ALL', 'EXISTS', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH']);

class SqlError extends SyntaxError {}
const fail = (msg) => { throw new SqlError(`vectro SQL: ${msg}`); };

// ---- tokens ---------------------------------------------------------------------------
// fold: unquoted names in lower case, as PostgreSQL reads them
function tokenize(src, fold = false) {
  const toks = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (/\s/.test(c)) { i++; continue; }
    if ((c === 'E' || c === 'e') && src[i + 1] === "'") { // E'text' with backslash escapes
      let v = '';
      i += 2;
      const ESC = { n: '\n', t: '\t', r: '\r', b: '\b', f: '\f' };
      for (;;) {
        if (i >= src.length) fail('a text in quotes is not closed');
        const ch = src[i];
        if (ch === "'") { if (src[i + 1] === "'") { v += "'"; i += 2; continue; } i++; break; }
        if (ch === '\\' && i + 1 < src.length) {
          const nx = src[i + 1];
          if (ESC[nx]) { v += ESC[nx]; i += 2; continue; }
          const hex = nx === 'x' ? /^[0-9a-fA-F]{1,2}/.exec(src.slice(i + 2)) : nx === 'u' ? /^[0-9a-fA-F]{4}/.exec(src.slice(i + 2)) : nx === 'U' ? /^[0-9a-fA-F]{8}/.exec(src.slice(i + 2)) : null;
          if (hex) { v += String.fromCodePoint(parseInt(hex[0], 16)); i += 2 + hex[0].length; continue; }
          v += nx; i += 2; continue;
        }
        v += ch; i++;
      }
      toks.push({ t: 'str', v });
      continue;
    }
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
      toks.push({ t: 'id', v: src.slice(i + 1, e), quoted: true });
      i = e + 1;
      continue;
    }
    const num = /^(\d+\.?\d*(e[+-]?\d+)?|\.\d+(e[+-]?\d+)?)/i.exec(src.slice(i));
    if (num) { toks.push({ t: 'num', v: Number(num[0]) }); i += num[0].length; continue; }
    const id = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
    if (id) {
      const up = id[0].toUpperCase();
      const raw = fold ? id[0].toLowerCase() : id[0];
      toks.push(KEYWORDS.has(up) ? { t: 'kw', v: up, raw } : { t: 'id', v: raw });
      i += id[0].length;
      continue;
    }
    const op = ['!~*', '<>', '!=', '<=', '>=', '||', '::', '!~', '~*'].find((o) => src.startsWith(o, i)) ?? c;
    if (op.length === 1 && !'+-*/%=<>(),.;~'.includes(op)) fail(`unexpected "${c}"`);
    toks.push({ t: 'op', v: op });
    i += op.length;
  }
  return toks;
}

// ---- parser ---------------------------------------------------------------------------------
// Words that are keywords only where they're expected (columns may be named so).
const SOFT = new Set(['OVER', 'PARTITION', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW', 'WITHIN', 'FETCH', 'NEXT', 'ONLY', 'FILTER']);

function parseStatement(src, fold = false) {
  const toks = tokenize(src, fold);
  let p = 0;
  const peek = (o = 0) => toks[p + o];
  const isKw = (v, o = 0) => peek(o)?.t === 'kw' && peek(o).v === v;
  const isWord = (v, o = 0) => isKw(v, o) || (peek(o)?.t === 'id' && !peek(o).quoted && peek(o).v.toUpperCase() === v);
  const isOp = (v, o = 0) => peek(o)?.t === 'op' && peek(o).v === v;
  const eatKw = (v) => { if (isKw(v)) { p++; return true; } return false; };
  const eatWord = (v) => { if (isWord(v)) { p++; return true; } return false; };
  const eatOp = (v) => { if (isOp(v)) { p++; return true; } return false; };
  const needKw = (v) => { if (!eatKw(v)) fail(`expected ${v} ${near()}`); };
  const needWord = (v) => { if (!eatWord(v)) fail(`expected ${v} ${near()}`); };
  const needOp = (v) => { if (!eatOp(v)) fail(`expected "${v}" ${near()}`); };
  const near = () => (peek() ? `near "${peek().v}"` : 'at the end');
  const ident = () => { const t = peek(); if (t?.t !== 'id') fail(`expected a name ${near()}`); p++; return t.v; };
  // after AS any word is a name (AS first, AS end)
  const aliasName = () => { const t = peek(); if (t?.t === 'kw') { p++; return t.raw; } return ident(); };
  const startsQuery = (o = 0) => isKw('SELECT', o) || isKw('WITH', o);
  const aliasNext = () => peek()?.t === 'id' && !(SOFT.has(peek().v.toUpperCase()) && !peek().quoted && ['OVER', 'WITHIN', 'FETCH', 'FILTER'].includes(peek().v.toUpperCase()));

  function expr() { return or(); }
  function or() { let l = and(); while (eatKw('OR')) l = { k: 'bin', op: 'OR', l, r: and() }; return l; }
  function and() { let l = not(); while (eatKw('AND')) l = { k: 'bin', op: 'AND', l, r: not() }; return l; }
  function not() { if (eatKw('NOT')) return { k: 'not', x: not() }; return cmp(); }
  function cmp() {
    if (isKw('EXISTS')) { p++; needOp('('); const q = query(); needOp(')'); return { k: 'exists', q }; }
    const l = add();
    const negate = (x) => ({ k: 'not', x });
    if (isKw('IS')) {
      p++;
      const neg = eatKw('NOT');
      let e;
      if (eatKw('TRUE')) e = { k: 'bin', op: 'IS', l, r: { k: 'num', v: 1, bool: true } };
      else if (eatKw('FALSE')) e = { k: 'bin', op: 'IS', l, r: { k: 'num', v: 0, bool: true } };
      else if (eatKw('DISTINCT')) { needKw('FROM'); e = negate({ k: 'bin', op: 'IS', l, r: add() }); } // (IS DISTINCT FROM: not the same, missing counting as a value)
      else { needKw('NULL'); e = { k: 'isnull', x: l }; }
      return neg ? negate(e) : e;
    }
    const neg = isKw('NOT') && (isKw('BETWEEN', 1) || isKw('IN', 1) || isKw('LIKE', 1) || isWord('ILIKE', 1)) ? (p++, true) : false;
    if (eatKw('BETWEEN')) { const lo = add(); needKw('AND'); const hi = add(); const e = { k: 'between', x: l, lo, hi }; return neg ? negate(e) : e; }
    if (eatKw('IN')) {
      needOp('(');
      if (startsQuery()) { const q = query(); needOp(')'); const e = { k: 'insub', x: l, q }; return neg ? negate(e) : e; }
      const list = [expr()];
      while (eatOp(',')) list.push(expr());
      needOp(')');
      const e = { k: 'in', x: l, list };
      return neg ? negate(e) : e;
    }
    if (eatKw('LIKE')) { const e = { k: 'like', x: l, pattern: add() }; return neg ? negate(e) : e; }
    if (eatWord('ILIKE')) { const e = { k: 'like', x: l, pattern: add(), ci: true }; return neg ? negate(e) : e; }
    const op = ['=', '<>', '!=', '<', '<=', '>', '>=', '~', '~*', '!~', '!~*'].find((o) => isOp(o));
    if (op) { p++; return { k: 'bin', op, l, r: add() }; }
    return l;
  }
  function add() {
    let l = mul();
    for (;;) {
      if (eatOp('+')) l = { k: 'bin', op: '+', l, r: mul() };
      else if (eatOp('-')) l = { k: 'bin', op: '-', l, r: mul() };
      else if (eatOp('||')) l = { k: 'call', fn: '||', args: l.k === 'call' && l.fn === '||' ? [...l.args, mul()] : [l, mul()] };
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
    let t = peek();
    if (!t || (t.t !== 'id' && t.t !== 'kw')) fail(`expected a type ${near()}`);
    p++;
    while (isOp('.') && (peek(1)?.t === 'id' || peek(1)?.t === 'kw')) { p++; t = peek(); p++; } // pg_catalog.text
    let name = String(t.v).toUpperCase();
    if (name === 'DOUBLE' && eatWord('PRECISION')) name = 'DOUBLE PRECISION';
    if ((name === 'CHARACTER' || name === 'CHAR') && eatWord('VARYING')) name = 'VARCHAR';
    if (eatOp('(')) { while (!eatOp(')')) p++; } // DECIMAL(10, 2), VARCHAR(20), TIMESTAMP(3)
    if (name === 'TIMESTAMP' || name === 'TIME') {
      if (isKw('WITH') && isWord('TIME', 1)) { p++; needWord('TIME'); needWord('ZONE'); name += 'TZ'; }
      else if (eatWord('WITHOUT')) { needWord('TIME'); needWord('ZONE'); }
    }
    if (isOp('[')) fail('arrays are not supported');
    return name;
  }
  function orderItems() {
    const out = [];
    do {
      const e = expr();
      let desc = false, nullsFirst = false;
      if (eatKw('DESC')) desc = true; else eatKw('ASC');
      if (eatKw('NULLS')) { if (eatKw('FIRST')) nullsFirst = true; else needKw('LAST'); }
      out.push({ e, desc, nullsFirst });
    } while (eatOp(','));
    return out;
  }
  function frameBound() {
    if (eatWord('UNBOUNDED')) { if (eatWord('PRECEDING')) return { kind: 'unbounded' }; needWord('FOLLOWING'); return { kind: 'unbounded' }; }
    if (eatWord('CURRENT')) { needWord('ROW'); return { kind: 'current' }; }
    const n = add();
    if (eatWord('PRECEDING')) return { kind: 'preceding', x: n };
    needWord('FOLLOWING');
    return { kind: 'following', x: n };
  }
  function windowSpec() {
    needOp('(');
    const partition = [];
    if (eatWord('PARTITION')) { needKw('BY'); do partition.push(expr()); while (eatOp(',')); }
    const order = eatKw('ORDER') ? (needKw('BY'), orderItems()) : [];
    let frame = null;
    if (isWord('ROWS') || isWord('RANGE')) {
      const unit = peek().v.toUpperCase() === 'ROWS' ? 'rows' : 'range';
      p++;
      if (eatKw('BETWEEN')) { const start = frameBound(); needKw('AND'); frame = { unit, start, end: frameBound() }; }
      else frame = { unit, start: frameBound(), end: { kind: 'current' } };
    }
    needOp(')');
    return { partition, order, frame };
  }
  function primary() {
    const t = peek();
    if (!t) fail('the query ends too early');
    if (t.t === 'num') { p++; return { k: 'num', v: t.v }; }
    if (t.t === 'str') { p++; return { k: 'str', v: t.v }; }
    if (eatOp('(')) {
      if (startsQuery()) { const q = query(); needOp(')'); return { k: 'subq', q }; }
      const e = expr();
      needOp(')');
      return e;
    }
    if (t.t === 'kw') {
      if (eatKw('NULL')) return { k: 'null' };
      if (eatKw('TRUE')) return { k: 'num', v: 1, bool: true };
      if (eatKw('FALSE')) return { k: 'num', v: 0, bool: true };
      if (isKw('DATE') || isKw('TIMESTAMP')) {
        if (peek(1)?.t === 'str') { p++; return { k: 'date', v: toks[p++].v }; }
      }
      if (eatKw('INTERVAL')) { const s = peek(); if (s?.t !== 'str') fail("INTERVAL needs text, e.g. INTERVAL '7 days'"); p++; return { k: 'interval', v: s.v, unit: peek()?.t === 'id' && !SOFT.has(peek().v.toUpperCase()) ? ident() : null }; }
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
      if ((isKw('LEFT') || isKw('RIGHT')) && isOp('(', 1)) { const fn = peek().v; p++; return call(fn); } // LEFT(text, n)
      fail(`unexpected ${t.v} ${near()}`);
    }
    if (t.t === 'id') {
      p++;
      if (isOp('(')) return call(t.v.toUpperCase());
      if (eatOp('.')) {
        if (eatOp('*')) return { k: 'star', table: t.v };
        const second = ident();
        if (isOp('(')) return call(second.toUpperCase()); // (schema.function(...): the function)
        if (eatOp('.')) { if (eatOp('*')) return { k: 'star', table: second }; return { k: 'col', table: second, name: ident() }; } // (schema.table.column)
        return { k: 'col', table: t.v, name: second };
      }
      return { k: 'col', name: t.v };
    }
    if (eatOp('*')) return { k: 'star' };
    fail(`unexpected "${t.v}"`);
  }
  function call(fn) { // after the name: (args) [WITHIN GROUP (ORDER BY x)] [OVER (...)]
    needOp('(');
    let e;
    // standard SQL's own forms: TRIM([BOTH | LEADING | TRAILING] [chars] FROM s), SUBSTRING(s FROM a [FOR n]), POSITION(sub IN s)
    if (fn === 'TRIM' && (isWord('BOTH') || isWord('LEADING') || isWord('TRAILING') || isKw('FROM') || !isOp(')'))) {
      const mode = eatWord('LEADING') ? 'LTRIM' : eatWord('TRAILING') ? 'RTRIM' : (eatWord('BOTH'), 'TRIM');
      if (eatKw('FROM')) { const x = expr(); needOp(')'); return { k: 'call', fn: mode, args: [x] }; }
      const first = expr();
      if (eatKw('FROM')) { const x = expr(); needOp(')'); return { k: 'call', fn: mode, args: [x, first] }; }
      const args = [first];
      while (eatOp(',')) args.push(expr());
      needOp(')');
      return { k: 'call', fn: mode, args };
    }
    if ((fn === 'SUBSTRING' || fn === 'SUBSTR') && !isOp(')')) {
      const x = expr();
      if (eatKw('FROM')) { const args = [x, expr()]; if (eatWord('FOR')) args.push(expr()); needOp(')'); return { k: 'call', fn: 'SUBSTRING', args }; }
      if (eatWord('FOR')) { const args = [x, { k: 'num', v: 1 }, expr()]; needOp(')'); return { k: 'call', fn: 'SUBSTRING', args }; }
      const args = [x];
      while (eatOp(',')) args.push(expr());
      needOp(')');
      return { k: 'call', fn: 'SUBSTRING', args };
    }
    if (fn === 'POSITION' && !isOp(')')) {
      const sub = add();
      if (eatKw('IN')) { const x = expr(); needOp(')'); return { k: 'call', fn: 'STRPOS', args: [x, sub] }; }
      const args = [sub];
      while (eatOp(',')) args.push(expr());
      needOp(')');
      return { k: 'call', fn: 'POSITION', args };
    }
    if (eatOp('*')) { needOp(')'); e = { k: 'call', fn, args: [], star: true }; }
    else {
      const distinct = eatKw('DISTINCT');
      eatKw('ALL');
      const args = [];
      if (!isOp(')')) { args.push(expr()); while (eatOp(',')) args.push(expr()); }
      needOp(')');
      e = { k: 'call', fn, args, distinct };
    }
    if (eatWord('WITHIN')) { needKw('GROUP'); needOp('('); needKw('ORDER'); needKw('BY'); const [o] = orderItems(); needOp(')'); e.within = o; }
    if (eatWord('OVER')) return { k: 'window', call: e, ...windowSpec() };
    return e;
  }

  function fromItem() {
    if (eatOp('(')) {
      if (!startsQuery()) fail(`expected a subquery (SELECT ...) ${near()}`);
      const q = query();
      needOp(')');
      eatKw('AS');
      if (peek()?.t !== 'id') fail('a subquery in FROM needs a name: (SELECT ...) AS name');
      return { q, alias: ident() };
    }
    let name = ident(), schema = null;
    while (eatOp('.')) { schema = name; name = ident(); } // schema.table (db.schema.table: the last two)
    let alias = name;
    if (eatKw('AS')) alias = ident(); else if (aliasNext()) alias = ident();
    return { name, schema, alias };
  }
  function selectCore() {
    needKw('SELECT');
    const distinct = eatKw('DISTINCT');
    eatKw('ALL');
    const items = [];
    do {
      const e = expr();
      let alias = null;
      if (eatKw('AS')) alias = aliasName();
      else if (aliasNext()) alias = ident();
      items.push({ e, alias });
    } while (eatOp(','));
    let from = null;
    const joins = [];
    if (eatKw('FROM')) {
      from = fromItem();
      for (;;) {
        let how = null;
        if (eatKw('JOIN')) how = 'inner';
        else if (isKw('INNER')) { p++; needKw('JOIN'); how = 'inner'; }
        else if (isKw('LEFT')) { p++; eatKw('OUTER'); needKw('JOIN'); how = 'left'; }
        else if (isKw('RIGHT') || isKw('FULL')) { const w = peek().v; p++; eatKw('OUTER'); needKw('JOIN'); how = w.toLowerCase(); }
        else if (isKw('CROSS')) { p++; needKw('JOIN'); how = 'cross'; }
        else if (isOp(',')) fail('use JOIN ... ON instead of commas between tables');
        if (!how) break;
        const ref = fromItem();
        if (how === 'cross') { joins.push({ ...ref, how }); continue; }
        if (eatKw('USING')) { needOp('('); const cols = [ident()]; while (eatOp(',')) cols.push(ident()); needOp(')'); joins.push({ ...ref, how, using: cols }); }
        else { needKw('ON'); joins.push({ ...ref, how, on: expr() }); }
      }
    }
    const where = eatKw('WHERE') ? expr() : null;
    const groupBy = [];
    if (eatKw('GROUP')) { needKw('BY'); do groupBy.push(expr()); while (eatOp(',')); }
    const having = eatKw('HAVING') ? expr() : null;
    return { k: 'select', distinct, items, from, joins, where, groupBy, having };
  }
  function setTerm() {
    if (isOp('(') && startsQuery(1)) { p++; const q = query(); needOp(')'); return q; }
    return selectCore();
  }
  // query: WITH, set operations, then ORDER BY / LIMIT for the whole
  function query() {
    const ctes = [];
    if (eatKw('WITH')) {
      do {
        const name = ident();
        let cols = null;
        if (eatOp('(')) { cols = [ident()]; while (eatOp(',')) cols.push(ident()); needOp(')'); }
        needKw('AS');
        needOp('(');
        const q = query();
        needOp(')');
        ctes.push({ name, cols, q });
      } while (eatOp(','));
    }
    // INTERSECT binds tighter than UNION and EXCEPT (standard SQL): a UNION b INTERSECT c is a UNION (b INTERSECT c)
    const intersectTerm = () => {
      let t = setTerm();
      while (eatKw('INTERSECT')) { if (eatKw('ALL')) fail('INTERSECT ALL is not supported (INTERSECT is)'); eatKw('DISTINCT'); t = { k: 'setop', op: 'intersect', left: t, right: setTerm() }; }
      return t;
    };
    let body = intersectTerm();
    for (;;) {
      let op = null;
      if (eatKw('UNION')) op = eatKw('ALL') ? 'union all' : (eatKw('DISTINCT'), 'union');
      else if (eatKw('EXCEPT')) { if (eatKw('ALL')) fail('EXCEPT ALL is not supported (EXCEPT is)'); eatKw('DISTINCT'); op = 'except'; }
      if (!op) break;
      body = { k: 'setop', op, left: body, right: intersectTerm() };
    }
    const orderBy = eatKw('ORDER') ? (needKw('BY'), orderItems()) : [];
    let limit = null, offset = 0;
    if (eatKw('LIMIT')) {
      if (eatKw('ALL')) limit = null;
      else { const t = peek(); if (t?.t !== 'num') fail('LIMIT needs a number'); p++; limit = t.v; }
    }
    if (eatKw('OFFSET')) { const t = peek(); if (t?.t !== 'num') fail('OFFSET needs a number'); p++; offset = t.v; eatWord('ROWS') || eatWord('ROW'); }
    if (eatWord('FETCH')) {
      if (!eatKw('FIRST')) needWord('NEXT');
      const t = peek(); if (t?.t !== 'num') fail('FETCH FIRST needs a number'); p++; limit = t.v;
      if (!eatWord('ROWS')) needWord('ROW');
      needWord('ONLY');
    }
    return { k: 'query', ctes, body, orderBy, limit, offset };
  }

  const q = query();
  eatOp(';');
  if (p < toks.length) fail(`unexpected "${peek().v}" (one statement at a time)`);
  return q;
}

// ---- translation ------------------------------------------------------------------------------
const AGG = { COUNT: 'count', SUM: 'sum', AVG: 'mean', MEAN: 'mean', MIN: 'min', MAX: 'max', APPROX_COUNT_DISTINCT: 'approxDistinct' };
const PERCENTILE = new Set(['MEDIAN', 'PERCENTILE_CONT', 'PERCENTILE_DISC', 'QUANTILE_CONT', 'QUANTILE_DISC', 'QUANTILE']);
const WINDOW_FNS = { ROW_NUMBER: 'row_number', RANK: 'rank', DENSE_RANK: 'dense_rank', PERCENT_RANK: 'percent_rank', CUME_DIST: 'cume_dist', NTILE: 'ntile', LAG: 'lag', LEAD: 'lead', FIRST_VALUE: 'first_value', LAST_VALUE: 'last_value', NTH_VALUE: 'nth_value', SUM: 'sum', AVG: 'avg', MEAN: 'avg', MIN: 'min', MAX: 'max', COUNT: 'count' };
const KEEPS_KIND = new Set(['lag', 'lead', 'first_value', 'last_value', 'nth_value', 'min', 'max']);
const DATE_PART = { YEAR: 'year', MONTH: 'month', DAY: 'day', HOUR: 'hour', MINUTE: 'minute', SECOND: 'second', ISODOW: 'weekday', DAYOFMONTH: 'day' };
const TRUNC = { year: 'yearStart', month: 'monthStart', week: 'weekStart', day: 'dayStart', hour: 'hourStart' };
const INTERVAL_MS = { millisecond: 1, second: 1000, minute: 60_000, hour: 3_600_000, day: 86_400_000, week: 604_800_000 };

const num = (v) => ({ t: 'num', v });
const v = (name) => ({ t: 'var', name });
const bin = (op, l, r) => ({ t: 'bin', op, l, r });
const call = (fn, ...args) => ({ t: 'call', fn, args });
const cond = (c, x, y) => ({ t: 'cond', c, x, y });
const TEXT_TYPES = new Set(['VARCHAR', 'TEXT', 'STRING', 'CHAR', 'BPCHAR', 'NVARCHAR', 'NCHAR', 'CHARACTER', 'NAME', 'CITEXT', 'UUID', 'REGCLASS', 'REGTYPE', 'REGPROC', 'REGNAMESPACE', 'REGROLE']);
const INT_TYPES = new Set(['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'HUGEINT', 'INT2', 'INT4', 'INT8', 'OID', 'XID', 'CID']);
const FLOAT_TYPES = new Set(['DOUBLE', 'DOUBLE PRECISION', 'FLOAT', 'FLOAT4', 'FLOAT8', 'REAL', 'DECIMAL', 'NUMERIC']);
const BOOL_TYPES = new Set(['BOOL', 'BOOLEAN']);
const DATE_TYPES = new Set(['DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME']);
// session values a bare name reads (current_user), as PostgreSQL has them
const SESSION_NAMES = new Set(['current_user', 'session_user', 'user', 'current_role', 'current_catalog', 'current_schema']);
function astVars(t, out = []) { if (t.t === 'var') out.push(t.name); for (const x of [t.x, t.l, t.r, t.c, t.y, ...(t.args ?? [])]) if (x && typeof x === 'object') astVars(x, out); return out; }
// Column names that aren't plain names ("sum:price", "Order Date") live under
// a plain one: __q_ then letters and digits as they are, others as _<hex>_.
const PLAIN = /^[A-Za-z_]\w*$/;
export const encodeName = (name) => (PLAIN.test(name) ? name : `__q_${[...name].map((ch) => (/[A-Za-z0-9]/.test(ch) ? ch : `_${ch.codePointAt(0).toString(16)}_`)).join('')}`);
export const displayName = (name) => (name.startsWith('__q_') ? name.slice(4).replace(/_([0-9a-f]+)_/g, (m, h) => String.fromCodePoint(parseInt(h, 16))) : name);
const constNum = (t) => (t.t === 'num' ? t.v : t.t === 'un' && t.op === '-' && t.x.t === 'num' ? -t.x.v : null);
const isAgg = (e) => e.k === 'call' && (AGG[e.fn] || PERCENTILE.has(e.fn));
const key = (e) => JSON.stringify(e);
// does e contain a node matching test (not looking inside subqueries or windows' own calls)?
function has(e, test, intoWindows = false) {
  let found = false;
  const visit = (x) => {
    if (found || !x || typeof x !== 'object') return;
    if (test(x)) { found = true; return; }
    if (x.k === 'subq' || x.k === 'exists' || x.k === 'query') return;
    if (x.k === 'insub') { visit(x.x); return; }
    if (x.k === 'window' && !intoWindows) { for (const y of [...x.partition, ...x.order.map((o) => o.e), ...x.call.args]) visit(y); return; }
    for (const val of Object.values(x)) if (Array.isArray(val)) val.forEach(visit); else if (val && typeof val === 'object') visit(val);
  };
  visit(e);
  return found;
}
const hasAgg = (e) => has(e, isAgg);
const hasWindow = (e) => has(e, (x) => x.k === 'window');
const hasInterval = (e) => has(e, (x) => x.k === 'interval');
const colRefs = (e) => { const out = []; has(e, (x) => { if (x.k === 'col') out.push(x); return false; }); return out; };
const conjuncts = (e) => (!e ? [] : e.k === 'bin' && e.op === 'AND' ? [...conjuncts(e.l), ...conjuncts(e.r)] : [e]);
const andAll = (list) => (list.length ? list.reduce((a, b) => ({ k: 'bin', op: 'AND', l: a, r: b })) : null);

// sql(text, tables[, opts]) -> Query
//   opts.types: the Query gets .columns = [{ name, type: 'int' | 'float' | 'bool' | 'text' | 'date' | 'day' }]
//     (day: dates without a time), worked out from the tables' columns, not from the answer
//   opts.foldCase: unquoted names in lower case (PostgreSQL's way)
//   opts.session: values of functions such as version(), current_user ({ version: '...' })
//   opts.database: a schema name that means the tables themselves (as public does)
//   opts.functions: more functions, { name: { out: 'text' | 'number', f(...values) } }
//     (run once per distinct set of values; text missing as null)
// Tables may also be named "schema.table" (pg_catalog.pg_class): they are
// found by that name; an unqualified name also finds pg_catalog's.
export function sql(text, tables = {}, opts = {}) {
  const ast = parseStatement(String(text), !!opts.foldCase);
  const engine = Object.values(tables).find((t) => t?.engine)?.engine ?? null;
  const env = { tables, ctes: new Map(), engine, n: { hidden: 0 }, wantTypes: !!opts.types, pgNames: !!opts.foldCase, functions: opts.functions ?? null, session: opts.session ?? null, database: opts.database ?? null };
  const rel = compileQuery(ast, env, null);
  if (opts.types) rel.query.columns = rel.names.map((name) => ({ name, type: rel.typeOf?.(name) ?? 'float' }));
  return rel.query;
}

// A table by its (maybe schema-qualified) name, case aside; null if none.
function lookupTable(env, { name, schema }) {
  const { tables } = env, keys = Object.keys(tables);
  const get = (k) => tables[k] ?? tables[keys.find((x) => x.toLowerCase() === k.toLowerCase())];
  const sc = schema?.toLowerCase();
  if (sc && sc !== 'public' && sc !== env.database?.toLowerCase()) return get(`${schema}.${name}`) ?? null;
  return get(name) ?? (sc ? null : get(`pg_catalog.${name}`)) ?? null;
}
const userTables = (tables) => Object.keys(tables).filter((k) => !k.includes('.'));

// Column types for answers (see sql's opts.types), cached per table and column:
// whole numbers -> int, others -> float; dates all at midnight -> day.
const typeCache = new WeakMap();
const merge = (ts) => {
  const u = [...new Set(ts.filter(Boolean))];
  if (u.length <= 1) return u[0] ?? 'text';
  if (u.includes('text')) return 'text';
  if (u.every((t) => t === 'int' || t === 'bool')) return 'int';
  if (u.every((t) => t === 'day' || t === 'date')) return 'date';
  return 'float';
};
function columnType(t, name) {
  let m = typeCache.get(t);
  if (!m) typeCache.set(t, (m = new Map()));
  if (m.has(name)) return m.get(name);
  const c = t.cols?.get(name);
  let type = 'float';
  if (!c) type = 'float';
  else if (c.dict || c.strings) type = 'text';
  else if (t.isDataset) { // from the chunks' encodings: whole-number codes
    const enc = (t.meta?.chunks ?? []).map((ch) => ch.cols?.[name]);
    if (c.date) type = 'date';
    else type = enc.length && enc.every((z) => z && (z.missing === z.rows || (z.enc && !z.enc.div && Number.isInteger(z.enc.mul ?? 1) && Number.isInteger(z.enc.base ?? 0)))) ? 'int' : 'float';
  } else if (c.date) type = t.length && t.agg({ n: `count(${name} == ${name} && ${name} != dayStart(${name}))` }).n === 0 ? 'day' : 'date';
  else {
    const a = t.agg({ frac: `count(${name} == ${name} && ${name} != floor(${name}))`, hi: `max(abs(${name}))` });
    type = a.frac === 0 && !(a.hi >= 2 ** 53) ? 'int' : 'float';
  }
  m.set(name, type);
  return type;
}

// A relation: { query, names, kinds (name -> 'text' | 'date' | 'number') }.
const kindOfTable = (t, col) => { const c = t.cols?.get(col); return c?.dict || c?.strings ? 'text' : c?.date ? 'date' : 'number'; };
function tableRelation(t) {
  if (typeof t?.lazy !== 'function') { // (a split table, or something that isn't a table)
    const what = t?.constructor?.name === 'SplitTable' || t?.constructor?.name === 'StreamTable' ? `a ${t.constructor.name}: SQL runs on Table and Dataset, not on split tables (use its own methods, filter / groupBy / aggAsync, or copy its rows into a Table: splitEngine.local.table(await t.toColumnsAsync()))` : 'not a Vectro Table or Dataset';
    throw new TypeError(`vectro: sql() got ${what}`);
  }
  return { query: t.lazy(), names: t.columnNames, kinds: new Map(t.columnNames.map((n) => [n, kindOfTable(t, n)])), typeOf: (n) => columnType(t, n) };
}

function compileQuery(q, env, outer) {
  let scopeEnv = env;
  if (q.ctes.length) { // WITH: later ones and the body see earlier ones
    scopeEnv = { ...env, ctes: new Map(env.ctes) };
    for (const c of q.ctes) {
      let rel = compileQuery(c.q, scopeEnv, outer);
      if (c.cols) {
        if (c.cols.length !== rel.names.length) fail(`WITH ${c.name}(${c.cols.join(', ')}) names ${c.cols.length} columns, its query has ${rel.names.length}`);
        rel = renameRelation(rel, c.cols);
      }
      scopeEnv.ctes.set(c.name.toLowerCase(), rel);
    }
  }
  if (q.body.k === 'select') return compileSelect(q.body, scopeEnv, outer, q);
  // set operations: then ORDER BY / LIMIT on the result's columns
  let rel = compileSet(q.body, scopeEnv, outer);
  let out = rel.query;
  if (q.orderBy.length) {
    const by = [];
    for (const o of q.orderBy) {
      let name = null;
      if (o.e.k === 'num') name = rel.names[o.e.v - 1] ?? fail(`ORDER BY ${o.e.v}: there is no column ${o.e.v}`);
      else if (o.e.k === 'col' && !o.e.table) name = rel.names.find((n) => n.toLowerCase() === o.e.name.toLowerCase() || n === encodeName(o.e.name)) ?? fail(`ORDER BY ${o.e.name}: a UNION is ordered by its columns`);
      else fail('a UNION / INTERSECT / EXCEPT is ordered by its column names or numbers');
      if (o.nullsFirst) { const h = `__nf${env.n.hidden++}`; out = out.derive(h, rel.kinds.get(name) === 'text' ? `${name} == '' ? 0 : 1` : `${name} == ${name} ? 1 : 0`); by.push(h); }
      by.push(o.desc ? `-${name}` : name);
    }
    out = out.sort(by);
  }
  if (q.limit !== null) out = out.limit(q.limit, q.offset);
  else if (q.offset) fail('OFFSET needs LIMIT');
  return { ...rel, query: out.select(...rel.names) };
}

function renameRelation(rel, names) {
  let q = rel.query;
  const kinds = new Map();
  const tmp = rel.names.map((n, k) => `__r${k}_${n}`);
  rel.names.forEach((n, k) => { q = q.derive(tmp[k], n); });
  rel.names.forEach((n, k) => { kinds.set(names[k], rel.kinds.get(n)); });
  // (derive copies values; text and date columns keep their kind through a plain copy)
  let out = q.select(...tmp);
  tmp.forEach((t, k) => { out = out.derive(names[k], t); });
  return { query: out.select(...names), names, kinds, typeOf: (nm) => rel.typeOf?.(rel.names[names.indexOf(nm)]) ?? 'float' };
}

function compileSet(body, env, outer) {
  if (body.k === 'query') return compileQuery(body, env, outer);
  if (body.k === 'select') return compileSelect(body, env, outer, { orderBy: [], limit: null, offset: 0 });
  let L = compileSet(body.left, env, outer);
  const R = compileSet(body.right, env, outer);
  if (L.names.length !== R.names.length) fail(`${body.op.toUpperCase()} needs the same number of columns on each side (${L.names.length} and ${R.names.length})`);
  // NULL columns on one side become the other side's kind (text: '')
  const asText = (rel, k) => { const tmp = `__nt${env.n.hidden++}`; const names = rel.names.map((nm, j) => (j === k ? tmp : nm)); return { ...rel, query: rel.query.textmap(tmp, { fn: () => '', label: 'null' }).select(...names), names }; };
  let Lr = L, Rr = R;
  L.names.forEach((n, k) => {
    const a = L.kinds.get(n), b = R.kinds.get(R.names[k]);
    if (a === 'null' && b === 'text') Lr = asText(Lr, k);
    else if (b === 'null' && a === 'text') Rr = asText(Rr, k);
    else if (a !== 'null' && b !== 'null' && (a === 'text') !== (b === 'text')) fail(`${body.op.toUpperCase()} column ${k + 1} is ${a} on one side and ${b} on the other`);
  });
  const names = L.names;
  const kindsOut = new Map(L.names.map((n, k) => [n, L.kinds.get(n) === 'null' ? R.kinds.get(R.names[k]) : L.kinds.get(n)]));
  const right = Rr.query; // (columns by position; named as the left's)
  if (Lr.names !== L.names) { // (each NULL column made text: back under its name)
    let lq = Lr.query;
    Lr.names.forEach((x, j) => { if (x !== L.names[j]) lq = lq.derive(L.names[j], x); });
    L = { ...L, query: lq.select(...L.names) };
  }
  const typeOf = (nm) => { const k = names.indexOf(nm); return merge([L.typeOf?.(nm) ?? 'float', R.typeOf?.(R.names[k]) ?? 'float']); };
  if (body.op === 'union all') return { query: Query.union([L.query, right]), names, kinds: kindsOut, typeOf };
  if (body.op === 'union') return { query: Query.union([L.query, right]).groupBy(names).agg({ __u: 'count()' }).select(...names), names, kinds: kindsOut, typeOf };
  // INTERSECT / EXCEPT: both sides marked, grouped by every column
  const mark = (rel, m) => rel.query.derive('__src', String(m)).select(...rel.names, '__src');
  const grouped = Query.union([mark(L, 1), mark(Rr, 2)]).groupBy(names).agg({ __mn: 'min(__src)', __mx: 'max(__src)' });
  const kept = body.op === 'intersect' ? grouped.filter('__mn == 1 && __mx == 2') : grouped.filter('__mx == 1');
  return { query: kept.select(...names), names, kinds: kindsOut, typeOf };
}

// FROM item -> relation
function relationOf(item, env, outer) {
  if (item.q) return compileQuery(item.q, env, outer);
  const cte = item.schema ? null : env.ctes.get(item.name.toLowerCase());
  if (cte) return cte;
  const t = lookupTable(env, item);
  if (!t) fail(`no table "${item.schema ? `${item.schema}.` : ''}${item.name}" (tables: ${userTables(env.tables).join(', ') || 'none given'})`);
  return tableRelation(t);
}

// The names a SELECT's FROM can see, without compiling it: [{ alias, names }]
function scopeNamesOf(sel, env) {
  if (!sel.from) return [];
  const one = (item) => {
    if (item.q) {
      const body = item.q.body;
      if (body.k === 'select' && body.items.every((it) => it.alias || it.e.k === 'col')) return { alias: item.alias, names: body.items.map((it) => it.alias ?? it.e.name) };
      return { alias: item.alias, names: compileQuery(item.q, env, null).names };
    }
    const cte = item.schema ? null : env.ctes.get(item.name.toLowerCase());
    const t = cte ?? lookupTable(env, item);
    return { alias: item.alias, names: cte ? cte.names : t?.columnNames ?? [] };
  };
  return [one(sel.from), ...sel.joins.map(one)];
}

function compileSelect(ast, env, outer, tail) {
  const n = env.n;
  // FROM and JOINs: the query's columns, and how each relation's columns are named in it
  let first;
  if (ast.from) first = relationOf(ast.from, env, outer);
  else { // SELECT without FROM: one row
    if (!env.engine) fail('SELECT without FROM needs at least one table given (for its engine)');
    first = tableRelation(env.engine.table({ __one: [0] }));
  }
  let q = first.query;
  const scope = [{ alias: ast.from?.alias ?? '', rel: first, names: new Map(first.names.map((x) => [x, x])) }];
  const outNames = [...first.names];
  const kinds = new Map(first.names.map((x) => [x, first.kinds.get(x)]));
  const types = new Map(first.names.map((x) => [x, () => first.typeOf?.(x) ?? 'float'])); // (for opts.types: worked out when asked)

  // a column reference -> its output name (or, if outer resolves it, an outer marker)
  const find = (ref) => {
    if (!PLAIN.test(ref.name)) { // a quoted name with spaces or signs: as an output kept it, or as CSV loading wrote it
      const enc = { ...ref, name: encodeName(ref.name) }, und = { ...ref, name: ref.name.replace(/[^A-Za-z0-9_]/g, '_') };
      return find1(enc) ?? find1(und) ?? find1(ref);
    }
    return find1(ref);
  };
  const find1 = (ref) => {
    if (!ref.table && usingMap.has(ref.name.toLowerCase())) return usingMap.get(ref.name.toLowerCase());
    if (ref.table) {
      const s = scope.find((x) => x.alias.toLowerCase() === ref.table.toLowerCase());
      if (!s) return null;
      const out = s.names.get(ref.name) ?? [...s.names].find(([x]) => x.toLowerCase() === ref.name.toLowerCase())?.[1];
      if (!out) fail(`table "${ref.table}" has no column "${ref.name}"`);
      return out;
    }
    const hits = scope.filter((s) => s.names.has(ref.name));
    if (!hits.length) {
      const ci = [...new Set(scope.flatMap((s) => [...s.names].filter(([x]) => x.toLowerCase() === ref.name.toLowerCase()).map(([, o]) => o)))];
      if (ci.length === 1) return ci[0];
      if (ci.length > 1) fail(`column "${ref.name}" is ambiguous`);
      return null;
    }
    const outs = new Set(hits.map((s) => s.names.get(ref.name)));
    if (outs.size > 1) fail(`column "${ref.name}" is ambiguous: write ${hits.map((s) => `${s.alias}.${ref.name}`).join(' or ')}`);
    return [...outs][0];
  };
  const resolve = (ref) => {
    const out = find(ref);
    if (out !== null) return out;
    if (ref.table && !scope.some((s) => s.alias.toLowerCase() === ref.table.toLowerCase())) {
      if (outer?.canSee(ref)) fail(`"${ref.table}.${ref.name}" belongs to the outer query: a subquery can use outer columns only in WHERE equalities (WHERE inner.x = outer.y)`);
      fail(`no table or alias "${ref.table}"`);
    }
    if (outer?.canSee(ref)) fail(`"${ref.name}" belongs to the outer query: a subquery can use outer columns only in WHERE equalities (WHERE inner.x = outer.y)`);
    fail(`no column "${ref.name}"`);
  };
  const canSee = (ref) => { try { return find(ref) !== null; } catch { return true; } };

  // hidden columns: a name for an expression's AST (a column as it is)
  const hide = (t, prefix = '__h') => {
    if (t.t === 'var') return t.name;
    const name = `${prefix}${n.hidden++}`;
    q = q.derive(name, src(t));
    return name;
  };

  // USING (k) columns: an unqualified k reads the left's (LEFT), the right's (RIGHT) or either (FULL)
  const usingMap = new Map();
  // (the joins run after the translator below is set up: function declarations)
  function addJoin(j) {
    let right = relationOf(j, env, outer);
    if (j.on && (j.how === 'left' || j.how === 'right' || j.how === 'full')) { // ON conditions on the right table alone: it is filtered first
      const own = (c) => !has(c, (x) => x.k === 'subq' || x.k === 'exists' || x.k === 'insub') && colRefs(c).length > 0 && colRefs(c).every((ref) => (ref.table ? ref.table.toLowerCase() === j.alias.toLowerCase() : right.names.includes(ref.name) && !scope.some((s) => s.names.has(ref.name))));
      const isKey = (c) => c.k === 'bin' && c.op === '=' && c.l.k === 'col' && c.r.k === 'col' && !(own(c));
      const mine = conjuncts(j.on).filter((c) => !isKey(c) && own(c));
      if (mine.length && j.how === 'left') {
        const inner = { k: 'query', ctes: [], body: { k: 'select', distinct: false, items: [{ e: { k: 'star' }, alias: null }], from: { ...j, alias: j.alias }, joins: [], where: andAll(mine), groupBy: [], having: null }, orderBy: [], limit: null, offset: 0 };
        j = { ...j, q: inner, on: andAll(conjuncts(j.on).filter((c) => !mine.includes(c))) };
        right = relationOf(j, env, outer);
      }
    }
    const rs = { alias: j.alias, rel: right, names: new Map() };
    const rightHas = (name) => right.names.includes(name);
    const outerJoin = j.how === 'left' || j.how === 'right' || j.how === 'full';
    let on;
    const extra = []; // ON conditions that aren't key equalities (inner joins only)
    let rq = right.query;
    if (j.how === 'cross') { // every pair: both sides on one constant key
      const k = `__x${n.hidden++}`;
      q = q.derive(k, '0');
      rq = rq.derive(k, '0');
      on = { left: [k], right: [k] };
    } else if (j.using) {
      for (const c of j.using) if (!rightHas(c)) fail(`table "${j.name ?? j.alias}" has no column "${c}" (USING)`);
      on = { left: j.using.map((c) => resolve({ name: c })), right: j.using };
    } else {
      const lefts = [], rights = [];
      const conds = [];
      for (const c of conjuncts(j.on)) {
        const side = (ref) => (ref.k !== 'col' ? null : (ref.table ? ref.table.toLowerCase() === j.alias.toLowerCase() : rightHas(ref.name) && !scope.some((s) => s.names.has(ref.name))) ? 'right' : 'left');
        if (c.k === 'bin' && c.op === '=' && side(c.l) && side(c.r) && side(c.l) !== side(c.r)) {
          const [l, r] = side(c.l) === 'left' ? [c.l, c.r] : [c.r, c.l];
          if (!rightHas(r.name)) fail(`table "${j.name ?? j.alias}" has no column "${r.name}"`);
          lefts.push(resolve(l)); rights.push(r.name);
        } else conds.push(c);
      }
      if (!lefts.length) fail(`JOIN ${j.name ?? j.alias} needs ON with an equality between the two sides, e.g. ON a.id = b.id`);
      if (conds.length && outerJoin) fail(`${j.how.toUpperCase()} JOIN ... ON takes key equalities only (move other conditions to WHERE)`);
      on = { left: lefts, right: rights };
      extra.push(...conds);
    }
    // outer joins: a right key named like its left key gets a name of its own,
    // so it reads as missing where no right row matched (an inner join keeps one)
    const original = new Map(); // right column in rq -> its name in the right relation
    let rightCols = right.names;
    if (outerJoin) {
      const renamed = new Map();
      on = { left: on.left, right: on.right.map((r, k) => {
        if (on.left[k] !== r) return r;
        if (!renamed.has(r)) { const h = `__k${n.hidden++}`; rq = rq.derive(h, r); renamed.set(r, h); original.set(h, r); }
        return renamed.get(r);
      }) };
      if (renamed.size) { rightCols = [...right.names.filter((x) => !renamed.has(x)), ...renamed.values()]; rq = rq.select(...rightCols); }
    }
    // output names, as the join makes them: right key columns named like the left key go away
    const sameKey = new Set(on.right.filter((x, k) => on.left[k] === x));
    const leftNames = [...outNames], rightOut = [];
    for (const x of rightCols) {
      if (sameKey.has(x)) { rs.names.set(x, x); continue; }
      let out = x;
      while (outNames.includes(out)) out += '_right';
      outNames.push(out);
      rightOut.push(out);
      const orig = original.get(x) ?? x;
      rs.names.set(orig, out);
      kinds.set(out, right.kinds.get(orig));
      types.set(out, () => right.typeOf?.(orig) ?? 'float');
    }
    const keys = on.left.every((x, k) => x === on.right[k]) ? on.left : on;
    if (j.how === 'right' || j.how === 'full') q = outerJoinG(rq, on, j.how, leftNames, rightCols, rightOut);
    else q = q.join(rq, { on: keys, how: j.how === 'cross' ? 'inner' : j.how });
    scope.push(rs);
    if (j.using && outerJoin) {
      j.using.forEach((c, k) => {
        const lo = on.left[k], ro = rs.names.get(c);
        let out = j.how === 'left' ? lo : ro;
        if (j.how === 'full') { // either side's key
          out = `__u${n.hidden++}`;
          q = kinds.get(lo) === 'text' ? q.textmap(out, { inputs: [lo, ro], fn: (a, b) => (a !== '' ? a : b), label: 'coalesce' }) : q.derive(out, `coalesce(${lo}, ${ro})`);
          kinds.set(out, kinds.get(lo));
          types.set(out, types.get(lo));
        }
        usingMap.set(c.toLowerCase(), out);
      });
    }
    for (const c of extra) { const t = translate(c, plain); q = q.filter(src(t)); } // (translating may join: q after it)
  }

  // RIGHT / FULL JOIN: the matching pairs (FULL: and the left rows without a
  // match: a LEFT join), then the right rows whose keys no left row has (the
  // right side joined to the left's distinct keys), with the left's columns
  // missing; both in the same column order, one after the other.
  function outerJoinG(rq, on, how, leftNames, rightCols, rightOut) {
    const tag = n.hidden++;
    const leftQ = q.select(...leftNames);
    const keysOn = on.left.every((x, k) => x === on.right[k]) ? on.left : on;
    const order = [...leftNames, ...rightOut];
    if (!findDataset(leftQ.node) && !findDataset(rq.node)) { // in memory: the join's own probe says which right rows matched
      const state = {};
      const joined = leftQ.join(rq, { on: keysOn, how: how === 'full' ? 'left' : 'inner', view: true, unmatched: state });
      return Query.union([joined.select(...order), outerRest(Query.unmatched(state, rightCols), tag, leftNames, rightCols, rightOut)]);
    }
    const fresh = on.left.map((_, k) => `__a${tag}_${k}`), mark = `__am${tag}`;
    let lkeys = leftQ;
    on.left.forEach((x, k) => { lkeys = lkeys.derive(fresh[k], x); });
    lkeys = lkeys.select(...fresh).groupBy(fresh).agg({ [`__ac${tag}`]: 'count()' }).derive(mark, '1').select(...fresh, mark);
    const lone = rq.join(lkeys, { on: { left: on.right, right: fresh }, how: 'left', view: true }).filter(`!(${mark} == 1)`);
    const joined = leftQ.join(rq, { on: keysOn, how: how === 'full' ? 'left' : 'inner', view: true }); // (unique right keys: right columns looked up)
    return Query.union([joined.select(...order), outerRest(lone, tag, leftNames, rightCols, rightOut)]);
  }
  // the unmatched right rows (columns rightCols) with the left's columns missing, in the join's output order
  function outerRest(lone, tag, leftNames, rightCols, rightOut) {
    const tmp = rightCols.map((_, k) => `__p${tag}_${k}`);
    rightCols.forEach((x, k) => { lone = lone.derive(tmp[k], x); });
    lone = lone.select(...tmp);
    for (const L of leftNames) lone = kinds.get(L) === 'text' ? lone.textmap(L, { fn: () => '', label: 'null' }) : lone.derive(L, 'NaN');
    rightOut.forEach((o, k) => { lone = lone.derive(o, tmp[k]); });
    return lone.select(...leftNames, ...rightOut);
  }

  // ---- subqueries in expressions: joins that add a column ----------------------------------
  // The subquery's WHERE equalities with outer columns are its correlation
  // keys (taken out of it); anything else from outside is an error.
  const outerHere = { canSee: (ref) => canSee(ref) || !!outer?.canSee(ref) };
  function correlate(sub) {
    const body = sub.body;
    if (body.k !== 'select' || sub.ctes.length) return { inner: sub, pairs: [] }; // (only a plain SELECT can be correlated)
    const innerScope = scopeNamesOf(body, env);
    const isInner = (ref) => (ref.table ? innerScope.some((s) => s.alias.toLowerCase() === ref.table.toLowerCase()) : innerScope.some((s) => s.names.some((x) => x.toLowerCase() === ref.name.toLowerCase())));
    const isOuter = (ref) => !isInner(ref) && canSee(ref);
    const pairs = [], keep = [];
    for (const c of conjuncts(body.where)) {
      if (c.k === 'bin' && c.op === '=' && c.l.k === 'col' && c.r.k === 'col') {
        const lo = isOuter(c.l), ro = isOuter(c.r);
        if (lo !== ro) { pairs.push(lo ? { outer: c.l, inner: c.r } : { outer: c.r, inner: c.l }); continue; }
      }
      keep.push(c);
    }
    return { inner: { ...sub, body: { ...body, where: andAll(keep) } }, pairs };
  }
  // join rel (its columns `cols`) on outer key ASTs = rel key columns; the
  // rel's columns get fresh names; returns them
  // a subquery's columns looked up on the outer rows by key; in a grouped SELECT (ctx.step deferred),
  // done after grouping, so the columns are there for the grouped rows
  function attach(rel, outerKeys, relKeys, cols, ctx) {
    const tag = n.hidden++;
    const fresh = (c) => `__s${tag}_${c}`;
    const run = () => {
      let rq = rel.query;
      const all = [...new Set([...relKeys, ...cols])];
      all.forEach((c) => { rq = rq.derive(fresh(c), c); });
      rq = rq.select(...all.map(fresh));
      let lefts = outerKeys.map((t) => hide(t));
      let rights = relKeys.map(fresh);
      if (!lefts.length) { const k = `__x${n.hidden++}`; q = q.derive(k, '0'); rq = rq.derive(k, '0'); lefts = [k]; rights = [k]; }
      q = q.join(rq, { on: { left: lefts, right: rights }, how: 'left', view: true }); // (its keys are unique: right columns looked up, left rows not copied)
    };
    if (ctx?.step) ctx.step({ name: fresh(cols[0]), inputs: [], run }); else run();
    return cols.map(fresh);
  }
  // x NOT IN (subquery), as SQL has it: true where the subquery has no rows (x missing or not);
  // else true only where x is present, not among the values, and none of them is missing
  function notInValue(e, ctx, rel, vcol, mcol, relKeys, outerKeys) { // (rel: one row per value and key, mcol 1 on each)
    const missing = rel.kinds?.get(vcol) === 'text' ? `${vcol} == ''` : `${vcol} != ${vcol}`;
    let sq = rel.query;
    let keys = relKeys;
    if (!keys.length) { sq = sq.derive('__g', '0'); keys = ['__g']; }
    sq = sq.groupBy(keys).agg({ __n: 'count()', __nn: `count(${missing})` });
    const [cn, cnn] = attach({ ...rel, query: sq, names: [...keys, '__n', '__nn'] }, outerKeys, relKeys, ['__n', '__nn'], ctx);
    const [m] = attach(rel, [translate(e.x, ctx), ...outerKeys], [vcol, ...relKeys], [mcol], ctx);
    const isIn = bin('==', v(m), num(1));
    const none = bin('==', call('coalesce', v(cn), num(0)), num(0));
    return bin('||', none, bin('&&', bin('&&', present(e.x, ctx), { t: 'un', op: '!', x: isIn }), bin('==', call('coalesce', v(cnn), num(0)), num(0))));
  }
  function subqueryValue(e, ctx, negate = false) {
    if (e.k === 'exists') {
      const { inner, pairs } = correlate(e.q);
      const body = inner.body;
      const keyItems = pairs.map((pr, i) => ({ e: pr.inner, alias: `__k${i}` }));
      const sel = { ...body, distinct: true, items: [...keyItems, { e: { k: 'num', v: 1 }, alias: '__m' }], groupBy: [] };
      if (body.groupBy?.length || body.having) fail('EXISTS (SELECT ... GROUP BY ...) is not supported');
      const rel = compileQuery({ ...inner, body: sel, orderBy: [], limit: pairs.length ? null : 1, offset: 0 }, env, outerHere);
      const [m] = attach(rel, pairs.map((pr) => translate(pr.outer, ctx)), keyItems.map((it) => it.alias), ['__m'], ctx);
      return bin('==', v(m), num(1));
    }
    if (e.k === 'insub') {
      const { inner, pairs } = correlate(e.q);
      const body = inner.body;
      if (body.k !== 'select' || body.items.length !== 1 || body.items[0].e.k === 'star') {
        if (!pairs.length) { // a UNION or such: its one column
          const rel = compileQuery(inner, env, outerHere);
          if (rel.names.length !== 1) fail('IN (SELECT ...) needs exactly one column');
          const dist = { ...rel, query: rel.query.groupBy([rel.names[0]]).agg({ __m: 'count()' }).derive('__m1', '1') };
          if (negate) return notInValue(e, ctx, { ...dist, names: [rel.names[0], '__m', '__m1'] }, rel.names[0], '__m1', [], []);
          const [m] = attach({ ...dist, names: [rel.names[0], '__m1'] }, [translate(e.x, ctx)], [rel.names[0]], ['__m1'], ctx);
          return bin('==', v(m), num(1));
        }
        fail('IN (SELECT ...) needs exactly one column');
      }
      const keyItems = pairs.map((pr, i) => ({ e: pr.inner, alias: `__k${i}` }));
      const sel = { ...body, distinct: !body.groupBy?.length, items: [{ e: body.items[0].e, alias: '__v' }, ...keyItems, ...(body.groupBy?.length ? [] : [{ e: { k: 'num', v: 1 }, alias: '__m' }])] };
      let rel = compileQuery({ ...inner, body: sel, orderBy: [], limit: null, offset: 0 }, env, outerHere);
      if (body.groupBy?.length) { const keys = ['__v', ...keyItems.map((it) => it.alias)]; rel = { ...rel, query: rel.query.groupBy(keys).agg({ __c: 'count()' }).derive('__m', '1'), names: [...keys, '__m'] }; }
      if (negate) return notInValue(e, ctx, rel, '__v', '__m', keyItems.map((it) => it.alias), pairs.map((pr) => translate(pr.outer, ctx)));
      const [m] = attach(rel, [translate(e.x, ctx), ...pairs.map((pr) => translate(pr.outer, ctx))], ['__v', ...keyItems.map((it) => it.alias)], ['__m'], ctx);
      return bin('==', v(m), num(1));
    }
    // (SELECT one value)
    const { inner, pairs } = correlate(e.q);
    const body = inner.body;
    if (body.k === 'select' && body.items.length !== 1) fail('a subquery used as a value needs exactly one column');
    if (!pairs.length) {
      const rel = compileQuery(inner, env, outerHere);
      if (rel.names.length !== 1) fail('a subquery used as a value needs exactly one column');
      const [c] = attach(rel, [], [], [rel.names[0]], ctx);
      return v(c);
    }
    if (!(body.items.length === 1 && hasAgg(body.items[0].e)) || body.groupBy?.length) fail('a correlated subquery used as a value must compute one aggregate, e.g. (SELECT MAX(x) FROM t WHERE t.k = o.k)');
    const keyItems = pairs.map((pr, i) => ({ e: pr.inner, alias: `__k${i}` }));
    const sel = { ...body, items: [...keyItems, { e: body.items[0].e, alias: '__v' }], groupBy: keyItems.map((it) => it.e) };
    const rel = compileQuery({ ...inner, body: sel, orderBy: [], limit: null, offset: 0 }, env, outerHere);
    const [c] = attach(rel, pairs.map((pr) => translate(pr.outer, ctx)), keyItems.map((it) => it.alias), ['__v'], ctx);
    const it = body.items[0].e;
    return it.k === 'call' && it.fn === 'COUNT' ? call('coalesce', v(c), num(0)) : v(c); // (no rows: COUNT is 0)
  }

  // ---- translating expressions; `ctx.col(ref)` turns a column into an AST -----------------
  // (ctx.step: a new column the expression needs, made now, or after grouping for post)
  const plain = { col: (ref) => v(resolve(ref)), agg: () => fail('aggregates (SUM, COUNT...) belong in SELECT, HAVING or ORDER BY, not here'), window: () => fail('window functions (... OVER (...)) belong in SELECT or ORDER BY'), step: (st) => st.run() };
  const mostKind = (ks) => (ks.includes('text') ? 'text' : ks.includes('date') ? 'date' : 'number');
  function kindOf(e, ctx) {
    switch (e.k) {
      case 'col': { const sv = sessionRef(e); if (sv) return sv.kind; return ctx.kindOfCol?.(e) ?? kinds.get(e.resolved ? e.name : resolve(e)); }
      case 'date': return 'date';
      case 'str': return 'text';
      case 'window': return ctx.windowKind?.(e) ?? 'number';
      case 'cast': return TEXT_TYPES.has(e.type) ? 'text' : DATE_TYPES.has(e.type) ? 'date' : 'number';
      case 'bin': return (e.op === '+' || e.op === '-') && (kindOf(e.l, ctx) === 'date') !== (kindOf(e.r, ctx) === 'date') ? 'date' : 'number'; // (a date plus or minus days or an interval)
      case 'case': return mostKind([...e.whens.map((w) => kindOf(w.v, ctx)), kindOf(e.otherwise, ctx)]);
      case 'call':
        if (TEXT_FUNCTIONS[e.fn] && !isAgg(e)) return TEXT_FUNCTIONS[e.fn].out === 'text' ? 'text' : 'number';
        if (['COALESCE', 'IFNULL', 'NVL', 'NULLIF'].includes(e.fn)) return mostKind(e.args.map((a) => kindOf(a, ctx)));
        if (['MIN', 'MAX', 'MEDIAN', 'QUANTILE', 'QUANTILE_CONT', 'QUANTILE_DISC'].includes(e.fn) && e.args[0] && kindOf(e.args[0], ctx) === 'date') return 'date';
        if ((e.fn === 'PERCENTILE_CONT' || e.fn === 'PERCENTILE_DISC') && e.within && kindOf(e.within.e, ctx) === 'date') return 'date';
        if (e.fn === 'DATE_TRUNC') return 'date';
        if (sessionFn(e)) return sessionFn(e).kind;
        if (extraFn(e)) return extraFn(e).out === 'text' ? 'text' : 'number';
        return 'number';
      default: return 'number';
    }
  }
  // version(), current_user...: { value, kind } or null
  function sessionFn(e) {
    const k = e.fn?.toLowerCase();
    if (!env.session || !k || !Object.hasOwn(env.session, k) || isAgg(e)) return null;
    const value = env.session[k];
    return { value, kind: typeof value === 'string' ? 'text' : 'number' };
  }
  function extraFn(e) { const k = e.fn?.toLowerCase().replace(/^pg_catalog\./, ''); return env.functions && k && Object.hasOwn(env.functions, k) ? env.functions[k] : null; }
  function sessionRef(e) {
    if (e.table || !env.session || !SESSION_NAMES.has(e.name.toLowerCase()) || !Object.hasOwn(env.session, e.name.toLowerCase())) return null;
    let out = null;
    try { out = find(e); } catch { return null; }
    return out === null ? sessionFn({ fn: e.name, args: [] }) : null;
  }
  const constValue = (sv, ctx) => (sv.kind === 'text' ? valueColumn(ctx, [], () => sv.value, { label: 'session' }) : num(typeof sv.value === 'boolean' ? +sv.value : sv.value));

  // ---- the type of each answer column (opts.types) -----------------------------------
  function typeOfExpr(e, ctx) {
    const T = (x) => typeOfExpr(x, ctx);
    const kind = kindOf(e, ctx);
    switch (e.k) {
      case 'col': {
        const sv = sessionRef(e);
        if (sv) return sv.kind === 'text' ? 'text' : 'int';
        let out = null;
        try { out = e.resolved ? e.name : find(e); } catch { out = null; }
        const t = out !== null ? types.get(out) : null;
        return t ? t() : kind === 'text' ? 'text' : kind === 'date' ? 'date' : 'float';
      }
      case 'str': return 'text';
      case 'date': return e.v.trim().length <= 10 ? 'day' : 'date';
      case 'num': return e.bool ? 'bool' : Number.isInteger(e.v) ? 'int' : 'float';
      case 'null': return 'text';
      case 'neg': return T(e.x) === 'int' ? 'int' : 'float';
      case 'not': case 'isnull': case 'like': case 'in': case 'between': case 'exists': case 'insub': return 'bool';
      case 'bin':
        if (['AND', 'OR', 'IS', '=', '<>', '!=', '<', '<=', '>', '>=', '~', '~*', '!~', '!~*'].includes(e.op)) return 'bool';
        if (e.op === '/') return 'float';
        if (kind === 'date') { const [d, x] = kindOf(e.l, ctx) === 'date' ? [e.l, e.r] : [e.r, e.l]; return T(d) === 'day' && !hasInterval(x) && T(x) === 'int' ? 'day' : 'date'; }
        if ((e.op === '-') && kindOf(e.l, ctx) === 'date' && kindOf(e.r, ctx) === 'date') return isDay(e.l, ctx) && isDay(e.r, ctx) ? 'int' : 'float';
        return merge([T(e.l), T(e.r)]) === 'int' ? 'int' : 'float';
      case 'case': return merge([...e.whens.map((w) => T(w.v)), e.otherwise.k === 'null' ? null : T(e.otherwise)]);
      case 'cast': return TEXT_TYPES.has(e.type) ? 'text' : INT_TYPES.has(e.type) ? 'int' : BOOL_TYPES.has(e.type) ? 'bool' : e.type === 'DATE' ? 'day' : DATE_TYPES.has(e.type) ? 'date' : 'float';
      case 'subq': return kind === 'text' ? 'text' : 'float';
      case 'window': {
        const fn = WINDOW_FNS[e.call.fn], arg = e.call.args[0];
        if (['row_number', 'rank', 'dense_rank', 'ntile', 'count'].includes(fn)) return 'int';
        if (['percent_rank', 'cume_dist', 'avg'].includes(fn)) return 'float';
        if (fn === 'sum') return arg && T(arg) === 'int' ? 'int' : 'float';
        return arg ? T(arg) : 'float';
      }
      case 'call': {
        const arg = e.args[0];
        if (isAgg(e)) {
          if (e.fn === 'COUNT' || e.fn === 'APPROX_COUNT_DISTINCT') return 'int';
          if (e.fn === 'SUM') return arg && merge([T(arg)]) === 'int' || (arg && T(arg) === 'bool') ? 'int' : 'float';
          if (e.fn === 'MIN' || e.fn === 'MAX') return arg ? T(arg) : 'float';
          if (e.fn === 'PERCENTILE_DISC' || e.fn === 'QUANTILE_DISC' || e.fn === 'QUANTILE') return T(e.within?.e ?? arg);
          return kind === 'date' ? 'date' : 'float';
        }
        const sv = sessionFn(e);
        if (sv) return sv.kind === 'text' ? 'text' : typeof sv.value === 'boolean' ? 'bool' : Number.isInteger(sv.value) ? 'int' : 'float';
        if (extraFn(e)) return extraFn(e).out === 'text' ? 'text' : extraFn(e).out === 'bool' ? 'bool' : 'int';
        if (TEXT_FUNCTIONS[e.fn]) return TEXT_FUNCTIONS[e.fn].out === 'text' ? 'text' : ['STARTS_WITH', 'ENDS_WITH', 'CONTAINS', 'PREFIX', 'SUFFIX', 'REGEXP_MATCHES', 'REGEXP_LIKE'].includes(e.fn) ? 'bool' : 'int';
        if (DATE_PART[e.fn] || ['DOW', 'DAYOFWEEK', 'WEEKDAY', 'ISODOW', 'FLOOR', 'CEIL', 'CEILING'].includes(e.fn)) return 'int';
        if (e.fn === 'ROUND') return e.args.length > 1 && !(e.args[1].k === 'num' && e.args[1].v <= 0) ? 'float' : 'int';
        if (e.fn === 'ABS') return arg ? (T(arg) === 'int' ? 'int' : 'float') : 'float';
        if (['LEAST', 'GREATEST', 'COALESCE', 'IFNULL', 'NVL'].includes(e.fn)) return merge(e.args.map(T));
        if (e.fn === 'NULLIF') return arg ? T(arg) : 'float';
        if (e.fn === 'DATE_TRUNC') return ['day', 'week', 'month', 'year', 'quarter'].includes(String(e.args[0]?.v).toLowerCase()) ? 'day' : 'date';
        if (e.fn === 'NOW' || e.fn === 'CURRENT_TIMESTAMP') return 'date';
        return kind === 'text' ? 'text' : kind === 'date' ? 'date' : 'float';
      }
      default: return kind === 'text' ? 'text' : kind === 'date' ? 'date' : 'float';
    }
  }

  // a date without a time (a DATE): from the column's values, as opts.types has it
  function isDay(e, ctx) { try { return typeOfExpr(e, ctx) === 'day'; } catch { return false; } }

  // ---- text: columns computed once per distinct value (Table._textmap) ------------------
  // a column for an AST (an expression becomes a hidden derived column)
  function materialize(t, ctx) {
    if (t.t === 'var') return t.name;
    const name = `__h${n.hidden++}`;
    ctx.step({ name, inputs: astVars(t), run: () => { q = q.derive(name, src(t)); } });
    return name;
  }
  // an argument: a constant { value, kind } or a column { col, kind }
  function argOf(e, ctx) {
    if (e.k === 'str') return { value: e.v, kind: 'text' };
    if (e.k === 'num') return { value: e.v, kind: 'number' };
    if (e.k === 'null') return { value: null, kind: 'number' };
    if (e.k === 'neg' && e.x.k === 'num') return { value: -e.x.v, kind: 'number' };
    if (e.k === 'date') return { value: textToDate(e.v), kind: 'date' };
    const kind = kindOf(e, ctx) ?? 'number', t = translate(e, ctx);
    if (t.t === 'num') return { value: t.v, kind: 'number' };
    if (t.t === 'str') return { value: t.v, kind: 'text' };
    return { col: materialize(t, ctx), kind };
  }
  // a new column: f(values, kinds) of the arguments' values (null where missing,
  // and then the answer is missing unless skip)
  function valueColumn(ctx, args, f, { text = true, skip = false, date = false, label = 'fn' } = {}) {
    const inputs = [...new Set(args.filter((a) => a.col).map((a) => a.col))];
    const at = args.map((a) => (a.col ? inputs.indexOf(a.col) : -1)), ks = args.map((a) => a.kind);
    const fn = (...vals) => {
      const xs = args.map((a, i) => (at[i] >= 0 ? (isNull(vals[at[i]], a.kind) ? null : vals[at[i]]) : a.value));
      if (!skip && xs.includes(null)) return text ? '' : NaN;
      return f(xs, ks);
    };
    const name = `__t${n.hidden++}`;
    ctx.step({ name, inputs, run: () => { q = q.textmap(name, { inputs, fn, text, date, label }); } });
    return v(name);
  }
  const asType = (x, kind, type) => (x === null ? null : type === 's' ? toText(x, kind) : kind === 'text' ? (type === 't' ? textToDate(x) : textToNumber(x)) : x);
  function textCall(e, ctx) {
    const spec = TEXT_FUNCTIONS[e.fn], min = spec.min ?? spec.args.length;
    const LABEL_TESTS = { STARTS_WITH: 'startsWith', PREFIX: 'startsWith', ENDS_WITH: 'endsWith', SUFFIX: 'endsWith', CONTAINS: 'contains' };
    if (LABEL_TESTS[e.fn] && e.args.length === 2 && e.args[0].k === 'col' && e.args[1].k === 'str' && kindOf(e.args[0], ctx) === 'text')
    { const x = translate(e.args[0], ctx); return cond(bin('==', x, { t: 'str', v: '' }), num(NaN), cond(call(LABEL_TESTS[e.fn], x, { t: 'str', v: e.args[1].v }), num(1), num(0))); } // (a column: label by label, in the fused pass; missing stays missing)
    if (e.fn === 'STRFTIME' && e.args.length === 2 && e.args[1].k === 'str') { // times rounded to what the format shows: fewer distinct values to format
      spec.f(0, e.args[1].v); // (a specifier it doesn't know: an error now)
      const f = e.args[1].v.replace(/%%/g, '').replace(/%-/g, '%');
      const unit = /%[MSfgTcX]/.test(f) ? null : /%[HIp]/.test(f) ? 'hourStart' : /%[dejaAwuUWVGFx]/.test(f) ? 'dayStart' : /%[mbBh]/.test(f) ? 'monthStart' : /%[Yy]/.test(f) ? 'yearStart' : null;
      if (unit && kindOf(e.args[0], ctx) !== 'text') {
        const t = call(unit, translate(e.args[0], ctx));
        return valueColumn(ctx, [{ col: materialize(t, ctx), kind: 'date' }, { value: e.args[1].v, kind: 'text' }], ([x, fmt]) => spec.f(x, fmt), { label: 'strftime' });
      }
    }
    if (e.args.length < min || (!spec.rest && e.args.length > spec.args.length)) fail(`${e.fn}() takes ${min === spec.args.length && !spec.rest ? min : `${min}${spec.rest ? ' or more' : ` to ${spec.args.length}`}`} value(s)`);
    const args = e.args.map((a) => argOf(a, ctx)), type = (i) => spec.args[i] ?? spec.rest;
    return valueColumn(ctx, args, (xs, ks) => spec.f(...xs.map((x, i) => asType(x, ks[i], type(i)))), { text: spec.out === 'text', skip: spec.skip, label: e.fn.toLowerCase() });
  }
  // comparing text: as text; text with a number or a date: as numbers or dates
  function compareValues(op, e, ctx) {
    const args = [argOf(e.l, ctx), argOf(e.r, ctx)];
    const asOf = args.some((a) => a.kind === 'date') ? 'date' : args.some((a) => a.kind === 'number' && a.value !== null) ? 'number' : 'text';
    const conv = (x, k) => (asOf === 'text' ? toText(x, k) : k !== 'text' ? x : asOf === 'date' ? textToDate(x) : textToNumber(x));
    const test = { '==': (c) => c === 0, '!=': (c) => c !== 0, '<': (c) => c < 0, '<=': (c) => c <= 0, '>': (c) => c > 0, '>=': (c) => c >= 0 }[op];
    return valueColumn(ctx, args, ([a, b], [ka, kb]) => {
      const x = conv(a, ka), y = conv(b, kb);
      if (x !== x || y !== y) return NaN;
      return test(x < y ? -1 : x > y ? 1 : 0) ? 1 : 0;
    }, { text: false, label: op });
  }
  // true where a value is there (SQL's NULL is NaN for numbers, '' for text); null: always there (a literal)
  function present(e, ctx) {
    if (e.k === 'num' || e.k === 'str' || e.k === 'date' || e.k === 'interval') return num(1);
    if (isCond(e)) return bin('||', translate(e, ctx), negated(e, ctx)); // (a condition: known where it's true or false)
    const x = translate(e, ctx);
    return kindOf(e, ctx) === 'text' ? bin('!=', x, { t: 'str', v: '' }) : bin('==', x, x);
  }
  // a true / false / unknown condition (comparisons, AND, OR, NOT, IS, IN, LIKE...): translate()
  // gives where it's true, negated() where it's false; elsewhere it's unknown
  function isCond(e) {
    if (e.k === 'num') return !!e.bool;
    if (e.k === 'bin') return ['AND', 'OR', 'IS', '=', '<>', '!=', '<', '<=', '>', '>=', '~', '~*', '!~', '!~*'].includes(e.op);
    return ['not', 'isnull', 'like', 'in', 'between', 'exists', 'insub'].includes(e.k);
  }
  // a condition as a value: 1 true, 0 false, NaN unknown (SQL's NULL boolean)
  function condValue(e, ctx, t) { return cond(t, num(1), cond(negated(e, ctx), num(0), num(NaN))); }
  // NOT x with SQL's three values: where x is unknown (a missing value), so is NOT x, and the
  // row is dropped. Comparisons become their opposites (false on missing values already).
  const FLIP = { '=': '<>', '<>': '=', '!=': '=', '<': '>=', '<=': '>', '>': '<=', '>=': '<' };
  const REGEX_FLIP = { '~': '!~', '!~': '~', '~*': '!~*', '!~*': '~*' };
  function negated(x, ctx) {
    const T = (y) => translate(y, ctx);
    switch (x.k) {
      case 'not': return T(x.x);
      case 'bin':
        if (x.op === 'AND') return bin('||', negated(x.l, ctx), negated(x.r, ctx));
        if (x.op === 'OR') return bin('&&', negated(x.l, ctx), negated(x.r, ctx));
        if (FLIP[x.op]) return T({ ...x, op: FLIP[x.op] });
        if (REGEX_FLIP[x.op]) return T({ ...x, op: REGEX_FLIP[x.op] }); // (missing text: unknown either way)
        return { t: 'un', op: '!', x: T(x) }; // (IS: true or false, never unknown)
      case 'between': return T({ k: 'bin', op: 'OR', l: { k: 'bin', op: '<', l: x.x, r: x.lo }, r: { k: 'bin', op: '>', l: x.x, r: x.hi } });
      case 'in':
        if (kindOf(x.x, ctx) === 'text') return bin('&&', { t: 'un', op: '!', x: T(x) }, present(x.x, ctx));
        return x.list.map((i) => T({ k: 'bin', op: '<>', l: x.x, r: i })).reduce((a, b) => bin('&&', a, b));
      case 'like': return bin('&&', { t: 'un', op: '!', x: T(x) }, present(x.x, ctx));
      case 'insub': return subqueryValue(x, ctx, true);
      case 'exists': case 'isnull': return { t: 'un', op: '!', x: T(x) };
      default: { // a true / false value (1 / 0): NOT of a missing one stays missing
        const v = T(x);
        return bin('==', v, num(0));
      }
    }
  }
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
      case 'col': { const sv = sessionRef(e); return sv ? constValue(sv, ctx) : ctx.col(e); }
      case 'star': fail('* only works as a whole SELECT item (or COUNT(*))');
      case 'neg': return { t: 'un', op: '-', x: T(e.x) };
      case 'not': return negated(e.x, ctx);
      case 'exists': case 'insub': case 'subq': return subqueryValue(e, ctx);
      case 'window': return ctx.window(e);
      case 'bin': {
        const B = (x) => (x.k === 'null' ? num(0) : T(x)); // (a NULL condition is unknown: never true)
        if (e.op === 'AND') return bin('&&', B(e.l), B(e.r));
        if (e.op === 'OR') return bin('||', B(e.l), B(e.r));
        const op = { '=': '==', '<>': '!=' }[e.op] ?? e.op;
        if (op === 'IS' && e.r.k === 'num' && e.r.bool) { // x IS TRUE / IS FALSE: never unknown (unknown is neither)
          const x = e.l;
          if (isCond(x)) return e.r.v ? T(x) : negated(x, ctx);
          if (x.k === 'null') return num(0);
          return e.r.v ? bin('&&', bin('!=', T(x), num(0)), present(x, ctx)) : bin('==', T(x), num(0));
        }
        if (op === 'IS') { // a IS b: equal, or both missing
          const a = T(e.l), b = T(e.r);
          return bin('||', bin('==', a, b), bin('&&', call('isMissing', a), call('isMissing', b)));
        }
        if (op === '~' || op === '~*' || op === '!~' || op === '!~*') { // regular expressions
          const flags = op.endsWith('*') ? 'i' : '';
          const m = valueColumn(ctx, [argOf(e.l, ctx), argOf(e.r, ctx)], ([a, b], [ka, kb]) => (TEXT_FUNCTIONS.REGEXP_MATCHES.f(toText(a, ka), toText(b, kb), flags) ? 1 : 0), { text: false, label: op });
          return op.startsWith('!') ? bin('==', m, num(0)) : bin('==', m, num(1));
        }
        if (['==', '!=', '<', '<=', '>', '>='].includes(op)) {
          if (isCond(e.l) || isCond(e.r)) // (conditions compared: unknown if either one is)
            return bin('&&', bin(op, T(e.l), T(e.r)), bin('&&', present(e.l, ctx), present(e.r, ctx)));
          const lk = kindOf(e.l, ctx), rk = kindOf(e.r, ctx);
          if (lk === 'text' || rk === 'text') {
            if ((lk === 'date' && e.r.k === 'str') || (rk === 'date' && e.l.k === 'str')) // a date written as text
              return bin(op, e.l.k === 'str' ? T({ k: 'date', v: e.l.v }) : T(e.l), e.r.k === 'str' ? T({ k: 'date', v: e.r.v }) : T(e.r));
            const lit = e.l.k === 'str' ? e.r : e.r.k === 'str' ? e.l : null;
            if ((op === '==' || op === '!=') && lit && lit.k !== 'str' && kindOf(lit, ctx) === 'text') { // (a column and a label: by code)
              const c = bin(op, T(e.l), T(e.r));
              return op === '!=' ? bin('&&', c, present(lit, ctx)) : c;
            }
            return bin('==', compareValues(op, e, ctx), num(1));
          }
        }
        if ((op === '+' || op === '-') && (kindOf(e.l, ctx) === 'date' || kindOf(e.r, ctx) === 'date')) return dateArith(e, op, ctx);
        if (op === '!=') return bin('&&', bin('!=', T(e.l), T(e.r)), bin('&&', present(e.l, ctx), present(e.r, ctx))); // (missing on either side: unknown, so not kept)
        if (op === '%') { // SQL: the sign of the dividend (truncate the quotient toward 0)
          const a = T(e.l), b = T(e.r), qt = bin('/', a, b);
          return bin('-', a, bin('*', cond(bin('>=', qt, num(0)), call('floor', qt), call('ceil', qt)), b));
        }
        return bin(op, T(e.l), T(e.r));
      }
      case 'between': {
        if (kindOf(e.x, ctx) === 'text') return T({ k: 'bin', op: 'AND', l: { k: 'bin', op: '>=', l: e.x, r: e.lo }, r: { k: 'bin', op: '<=', l: e.x, r: e.hi } });
        const x = T(e.x);
        return bin('&&', bin('>=', x, T(e.lo)), bin('<=', x, T(e.hi)));
      }
      case 'in': {
        const x = T(e.x);
        if (kindOf(e.x, ctx) === 'text') {
          if (e.list.some((i) => i.k !== 'str')) fail('IN on a text column takes text values in quotes');
          return call('in', x, ...e.list.map((i) => ({ t: 'str', v: i.v })));
        }
        return e.list.map((i) => bin('==', x, T(i))).reduce((a, b) => bin('||', a, b));
      }
      case 'like': {
        if (e.pattern.k !== 'str') fail(`${e.ci ? 'ILIKE' : 'LIKE'} needs a pattern in quotes`);
        if (kindOf(e.x, ctx) !== 'text') fail(`${e.ci ? 'ILIKE' : 'LIKE'} works on text`);
        const there = present(e.x, ctx); // (missing text matches no pattern: unknown)
        if (e.ci && e.x.k === 'col') return bin('&&', call('ilike', T(e.x), { t: 'str', v: e.pattern.v }), there); // (a column: label by label)
        if (e.ci) return bin('&&', T({ k: 'like', x: { k: 'call', fn: 'LOWER', args: [e.x] }, pattern: { k: 'str', v: e.pattern.v.toLowerCase() } }), there);
        return bin('&&', call('like', T(e.x), { t: 'str', v: e.pattern.v }), there);
      }
      case 'isnull':
        if (e.x.k === 'str') return num(e.x.v === '' ? 1 : 0); // ('' is missing text)
        return kindOf(e.x, ctx) === 'text' ? bin('==', T(e.x), { t: 'str', v: '' }) : call('isMissing', T(e.x));
      case 'case': {
        if (kindOf(e, ctx) === 'text') { // which branch (a number), then that branch's value as text
          const branches = [...e.whens.map((w) => w.v), e.otherwise];
          if (branches.every((b) => b.k === 'str' || b.k === 'null')) { // labels in quotes only: each row's label code, on every thread
            const labels = [...new Set(branches.map((b) => (b.k === 'str' ? b.v : '')))], codeOf = (b) => num(labels.indexOf(b.k === 'str' ? b.v : ''));
            let code = codeOf(e.otherwise);
            for (let i = e.whens.length - 1; i >= 0; i--) code = cond(e.base ? T({ k: 'bin', op: '=', l: e.base, r: e.whens[i].c }) : T(e.whens[i].c), codeOf(e.whens[i].v), code);
            if (code.t !== 'num') {
              const col = materialize(code, ctx), name = `__t${n.hidden++}`;
              ctx.step({ name, inputs: [col], run: () => { q = q.textmap(name, { inputs: [col], labels, label: 'case' }); } });
              return v(name);
            }
          }
          let which = num(branches.length - 1);
          for (let i = e.whens.length - 1; i >= 0; i--) which = cond(e.base ? T({ k: 'bin', op: '=', l: e.base, r: e.whens[i].c }) : T(e.whens[i].c), num(i), which);
          const args = [which.t === 'num' ? { value: which.v, kind: 'number' } : { col: materialize(which, ctx), kind: 'number' }, ...branches.map((b) => argOf(b, ctx))];
          return valueColumn(ctx, args, (xs, ks) => { const x = xs[1 + xs[0]]; return x === null ? '' : toText(x, ks[1 + xs[0]]); }, { skip: true, label: 'case' });
        }
        let out = T(e.otherwise);
        for (let i = e.whens.length - 1; i >= 0; i--) {
          const c = e.base ? T({ k: 'bin', op: '=', l: e.base, r: e.whens[i].c }) : T(e.whens[i].c);
          out = cond(c, T(e.whens[i].v), out);
        }
        return out;
      }
      case 'cast': {
        const from = kindOf(e.x, ctx);
        if (TEXT_TYPES.has(e.type)) return from === 'text' ? T(e.x) : valueColumn(ctx, [argOf(e.x, ctx)], ([x], [k]) => toText(x, k), { label: 'text' });
        const asBool = (v) => (/^\s*(t|true|y|yes|on|1)\s*$/i.test(v) ? 1 : /^\s*(f|false|n|no|off|0)\s*$/i.test(v) ? 0 : NaN);
        if (e.x.k === 'str' && !DATE_TYPES.has(e.type)) {
          if (BOOL_TYPES.has(e.type)) return num(asBool(e.x.v));
          if (!INT_TYPES.has(e.type) && !FLOAT_TYPES.has(e.type)) fail(`CAST to ${e.type} is not supported`);
          const x = textToNumber(e.x.v);
          return num(INT_TYPES.has(e.type) ? Math.trunc(x) : x);
        }
        if (from === 'text' && e.x.k !== 'str') { // text to a number or a date (missing where it isn't one)
          const whole = INT_TYPES.has(e.type), date = DATE_TYPES.has(e.type), bool = BOOL_TYPES.has(e.type);
          if (!whole && !date && !bool && !FLOAT_TYPES.has(e.type)) fail(`CAST to ${e.type} is not supported`);
          return valueColumn(ctx, [argOf(e.x, ctx)], ([x]) => { if (bool) return asBool(x); if (date) { const t = textToDate(x); return e.type === 'DATE' ? Math.floor(t / 86_400_000) * 86_400_000 : t; } const v = textToNumber(x); return whole ? Math.trunc(v) : v; }, { text: false, date, label: e.type.toLowerCase() });
        }
        const x = T(e.x);
        if (INT_TYPES.has(e.type)) return cond(bin('>=', x, num(0)), call('floor', x), call('ceil', x));
        if (FLOAT_TYPES.has(e.type)) return x;
        if (BOOL_TYPES.has(e.type)) return cond(bin('==', x, x), cond(bin('!=', x, num(0)), num(1), num(0)), num(NaN));
        if (DATE_TYPES.has(e.type)) return e.x.k === 'str' ? translate({ k: 'date', v: e.x.v }, ctx) : e.type === 'DATE' ? call('dayStart', x) : x;
        fail(`CAST to ${e.type} is not supported`);
      }
      case 'call': {
        if (isAgg(e)) return ctx.agg(e);
        if (TEXT_FUNCTIONS[e.fn]) return textCall(e, ctx);
        if (['COALESCE', 'IFNULL', 'NVL', 'NULLIF'].includes(e.fn) && kindOf(e, ctx) === 'text') {
          if (e.args.length < 2 || (e.fn === 'NULLIF' && e.args.length !== 2)) fail(`${e.fn}() needs ${e.fn === 'NULLIF' ? 'two values' : 'two values or more'}`);
          const args = e.args.map((x) => argOf(x, ctx));
          if (e.fn === 'NULLIF') return valueColumn(ctx, args, ([a, b], [ka, kb]) => (a === null ? '' : b !== null && toText(a, ka) === toText(b, kb) ? '' : toText(a, ka)), { skip: true, label: 'nullif' });
          return valueColumn(ctx, args, (xs, ks) => { const i = xs.findIndex((x) => x !== null); return i < 0 ? '' : toText(xs[i], ks[i]); }, { skip: true, label: 'coalesce' });
        }
        const a = e.args.map(T), need = (k) => { if (a.length !== k) fail(`${e.fn}() takes ${k} value(s)`); };
        switch (e.fn) {
          case 'ABS': case 'SQRT': case 'FLOOR': need(1); return call(e.fn.toLowerCase(), a[0]);
          case 'CEIL': case 'CEILING': need(1); return call('ceil', a[0]);
          case 'ROUND': {
            const f = a.length > 1 ? 10 ** (e.args[1].k === 'num' ? e.args[1].v : fail('ROUND(x, digits) needs a number of digits')) : 1;
            const s = bin('*', a[0], num(f));
            return bin('/', cond(bin('>=', s, num(0)), call('floor', bin('+', s, num(0.5))), call('ceil', bin('-', s, num(0.5)))), num(f));
          }
          case 'LEAST': case 'GREATEST': { // missing values skipped, as in PostgreSQL and DuckDB (NULL only if all are)
            if (a.length < 2) fail(`${e.fn}() needs two values or more`);
            const f = e.fn === 'LEAST' ? 'min' : 'max';
            return a.reduce((x, y) => call(f, call('coalesce', x, y), call('coalesce', y, x)));
          }
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
          case 'EPOCH': need(1); return bin('/', a[0], num(1000)); // EXTRACT(EPOCH FROM ts): seconds since 1970
          case 'DATE_PART': { // date_part('year', ts)
            need(2);
            const part = e.args[0].k === 'str' ? e.args[0].v.toUpperCase() : null;
            if (part === 'EPOCH') return bin('/', a[1], num(1000));
            if (!DATE_PART[part]) fail(`DATE_PART('${e.args[0].v}', ...): use year, month, day, hour, minute, second or epoch`);
            return call(DATE_PART[part], a[1]);
          }
          default:
            if (sessionFn(e)) return constValue(sessionFn(e), ctx);
            if (extraFn(e)) { const fx = extraFn(e); return valueColumn(ctx, e.args.map((x) => argOf(x, ctx)), (xs) => fx.f(...xs), { text: fx.out === 'text', skip: true, label: e.fn.toLowerCase() }); }
            if (DATE_PART[e.fn]) { need(1); return call(DATE_PART[e.fn], a[0]); }
            if (WINDOW_FNS[e.fn] && !AGG[e.fn]) fail(`${e.fn}() needs OVER (...)`);
            fail(`unknown function ${e.fn}()`);
        }
      }
    }
    fail(`can't use ${e.k} here`);
  }
  const src = (a) => toSourceAst(a);
  // dates and numbers, as DuckDB and PostgreSQL have them: date ± n is n days later or earlier
  // (an INTERVAL: its milliseconds), a date again; DATE - DATE is a number of days (dates with
  // a time: the milliseconds between them, an interval)
  function dateArith(e, op, ctx) {
    const T = (x) => translate(x, ctx), ld = kindOf(e.l, ctx) === 'date', rd = kindOf(e.r, ctx) === 'date';
    if (ld && rd) {
      if (op === '+') fail('a date plus a date has no meaning (add a number of days or an INTERVAL)');
      const ms = bin('-', T(e.l), T(e.r));
      return isDay(e.l, ctx) && isDay(e.r, ctx) ? bin('/', ms, num(86_400_000)) : ms;
    }
    if (rd && op === '-') fail('a number minus a date has no meaning (write date - number)');
    const [d, x] = ld ? [e.l, e.r] : [e.r, e.l];
    const by = hasInterval(x) ? T(x) : bin('*', T(x), num(86_400_000));
    const t = bin(op, T(d), by);
    return valueColumn(ctx, [{ col: materialize(t, ctx), kind: 'date' }], ([ms]) => ms, { text: false, date: true, label: 'date' }); // (a date column)
  }

  for (const j of ast.joins) addJoin(j);

  if (ast.where) {
    if (hasAgg(ast.where)) fail('aggregates (SUM, COUNT...) can\'t be used in WHERE: use HAVING');
    if (hasWindow(ast.where)) fail('window functions can\'t be used in WHERE (filter on them in an outer query)');
    const t = ast.where.k === 'null' ? num(0) : translate(ast.where, plain); // (WHERE NULL: unknown, no row)
    q = q.filter(src(t));
  }

  // SELECT items (* expanded)
  const items = [];
  ast.items.forEach(({ e, alias }) => {
    if (e.k === 'star') {
      const from = e.table ? scope.filter((s) => s.alias.toLowerCase() === e.table.toLowerCase()) : scope;
      if (!from.length) fail(`no table or alias "${e.table}"`);
      const seen = new Set(items.map((i) => i.name));
      for (const s of from) {
        for (const [orig, out] of s.names) {
          const u = e.table ? null : usingMap.get(orig.toLowerCase()); // (a USING key once)
          const label = u ? orig : out;
          if (seen.has(label) || (!u && out.startsWith('__') && !out.startsWith('__q_'))) continue;
          seen.add(label);
          items.push({ e: { k: 'col', name: u ?? out, resolved: true }, name: label });
        }
      }
      return;
    }
    items.push({ e, alias, name: alias ?? null });
  });
  const used = new Set();
  items.forEach((it, i) => { // output names: alias, column, sum_price, col3...
    if (!it.name) {
      const c = it.e.k === 'window' ? it.e.call : it.e;
      if (it.e.k === 'col') it.name = it.e.name;
      else if (env.pgNames && (c.k === 'call' || it.e.k === 'window')) it.name = c.star && c.fn === 'COUNT' ? 'count' : c.fn.toLowerCase(); // (PostgreSQL's names: count, sum, version)
      else if (env.pgNames && it.e.k === 'cast') it.name = it.e.type.toLowerCase().replace(/ /g, '_');
      else if (isAgg(c) || it.e.k === 'window') it.name = `${c.star ? 'count' : AGG[c.fn] === 'mean' ? 'avg' : c.distinct ? 'count_distinct' : c.fn.toLowerCase()}${c.args[0]?.k === 'col' ? `_${c.args[0].name}` : ''}`;
      else it.name = `col${i + 1}`;
    }
    let nm = it.name, k = 2;
    while (used.has(nm)) nm = `${it.name}_${k++}`;
    nm = encodeName(nm); // ("sum:price:ok": kept under a plain name, shown as written)
    it.name = nm;
    used.add(nm);
  });
  const colOf = (e) => { if (e.resolved) return v(e.name); const sv = sessionRef(e); return sv ? constValue(sv, plain) : v(resolve(e)); };
  const orderBy = tail.orderBy ?? [];

  // ---- window functions: after grouping, before ORDER BY / DISTINCT / LIMIT ---------------
  // (their inputs are translated first, so aggregates inside them are known)
  const windowNodes = [];
  const collectWindows = (e) => { const visit = (x) => { if (!x || typeof x !== 'object') return; if (x.k === 'window') { if (!windowNodes.some((w) => key(w) === key(x))) windowNodes.push(x); return; } if (x.k === 'subq' || x.k === 'exists' || x.k === 'insub') return; for (const val of Object.values(x)) if (Array.isArray(val)) val.forEach(visit); else if (val && typeof val === 'object') visit(val); }; visit(e); };
  items.forEach((it) => collectWindows(it.e));
  orderBy.forEach((o) => collectWindows(o.e));
  if (ast.having && hasWindow(ast.having)) fail('window functions can\'t be used in HAVING (filter on them in an outer query)');
  let windowPlan = null; // [{ spec key, partition ASTs, order, items }]
  const windowVars = new Map(), windowKinds = new Map();
  function planWindows(tr, kindCtx) {
    const specs = new Map();
    for (const w of windowNodes) {
      const c = w.call, fn = WINDOW_FNS[c.fn] ?? (PERCENTILE.has(c.fn) ? null : null);
      if (!fn) fail(`${c.fn}() can't be used with OVER (window functions: ${Object.keys(WINDOW_FNS).join(', ')})`);
      if (c.distinct) fail(`${c.fn}(DISTINCT ...) OVER (...) is not supported`);
      const partition = w.partition.map(tr), order = w.order.map((o) => ({ ast: tr(o.e), desc: o.desc, nullsFirst: o.nullsFirst, text: o.nullsFirst && kindCtx(o.e) === 'text' }));
      const sk = key({ p: partition, o: order });
      if (!specs.has(sk)) specs.set(sk, { partition, order, items: [] });
      const constArg = (i, what) => { const x = c.args[i]; if (!x) return undefined; const k = constNum(tr(x)); if (k === null) fail(`${c.fn}(): the ${what} must be a number`); return k; };
      const item = { node: w, fn, name: `__w${n.hidden++}` };
      if (['lag', 'lead'].includes(fn)) { item.arg = tr(c.args[0] ?? fail(`${c.fn}() needs a value`)); item.offset = constArg(1, 'offset') ?? 1; item.def = constArg(2, 'default'); }
      else if (fn === 'ntile') { item.k = constArg(0, 'number of tiles'); if (!(item.k >= 1)) fail('NTILE(n) needs n >= 1'); }
      else if (fn === 'nth_value') { item.arg = tr(c.args[0]); item.k = constArg(1, 'position'); }
      else if (['first_value', 'last_value', 'sum', 'avg', 'min', 'max'].includes(fn)) {
        if ((fn === 'sum' || fn === 'avg') && kindCtx(c.args[0] ?? fail(`${c.fn}() needs a value`)) === 'text') fail(`${c.fn}() of text is not supported`);
        item.arg = tr(c.args[0] ?? fail(`${c.fn}() needs a value`));
      } else if (fn === 'count') { // (text: counts the values there, missing text '' left out)
        item.arg = c.star || !c.args.length ? null : tr(c.args[0]);
        if (item.arg && kindCtx(c.args[0]) === 'text') item.arg = cond(bin('&&', bin('!=', item.arg, { t: 'str', v: '' }), bin('==', item.arg, item.arg)), num(1), num(NaN));
      }
      if (w.frame) {
        const bound = (b) => (b.x === undefined ? { kind: b.kind } : { kind: b.kind, n: (() => { const t = tr(b.x); if (t.t !== 'num' || !(t.v >= 0)) fail('a window frame needs a number of rows (or a distance) like 3 PRECEDING'); return t.v; })() });
        item.frame = { unit: w.frame.unit, start: bound(w.frame.start), end: bound(w.frame.end) };
        if (item.frame.start.kind === 'unbounded' && w.frame.start.kind === 'unbounded' && false) item.frame.start.kind = 'unbounded';
      }
      specs.get(sk).items.push(item);
      windowVars.set(key(w), item.name);
      const argKind = c.args[0] ? kindCtx(c.args[0]) : 'number';
      windowKinds.set(key(w), KEEPS_KIND.has(fn) ? argKind : 'number');
    }
    windowPlan = [...specs.values()];
  }
  function applyWindows() {
    for (const spec of windowPlan ?? []) {
      const partition = spec.partition.map((t) => hide(t, '__wp'));
      const order = [];
      for (const o of spec.order) {
        const name = hide(o.ast, '__wo');
        if (o.nullsFirst) { const h = `__nf${n.hidden++}`; q = q.derive(h, o.text ? `${name} == '' ? 0 : 1` : `${name} == ${name} ? 1 : 0`); order.push({ name: h }); }
        order.push({ name, desc: o.desc });
      }
      const its = spec.items.map((it) => ({ name: it.name, fn: it.fn, arg: it.arg ? hide(it.arg, '__wa') : null, frame: it.frame, offset: it.offset, def: it.def, k: it.k }));
      q = q.window({ partition, order, items: its });
    }
  }
  const windowCtx = (base) => ({ ...base, window: (e) => v(windowVars.get(key(e)) ?? fail('internal: window not planned')), windowKind: (e) => windowKinds.get(key(e)) });

  // ---- percentiles: a window over each group's rows, then min() of it -----------------
  function percentileAgg(e, groupKeyNames, tr) {
    let p, arg;
    if (e.fn === 'MEDIAN') { if (e.args.length !== 1) fail('MEDIAN(x) takes one value'); p = 0.5; arg = e.args[0]; }
    else if (e.fn === 'PERCENTILE_CONT' || e.fn === 'PERCENTILE_DISC') {
      if (!e.within) fail(`${e.fn}(p) needs WITHIN GROUP (ORDER BY x)`);
      const t = tr(e.args[0] ?? fail(`${e.fn}(p) needs a fraction p`));
      if (t.t !== 'num' || !(t.v >= 0 && t.v <= 1)) fail(`${e.fn}(p): p must be a number from 0 to 1`);
      p = e.within.desc ? 1 - t.v : t.v;
      arg = e.within.e;
    } else { // QUANTILE_CONT(x, p) (DuckDB)
      const t = tr(e.args[1] ?? fail(`${e.fn}(x, p) needs a fraction p`));
      if (t.t !== 'num' || !(t.v >= 0 && t.v <= 1)) fail(`${e.fn}(x, p): p must be a number from 0 to 1`);
      p = t.v; arg = e.args[0];
    }
    const disc = e.fn === 'PERCENTILE_DISC' || e.fn === 'QUANTILE_DISC' || e.fn === 'QUANTILE'; // (DuckDB's QUANTILE is QUANTILE_DISC)
    const argName = hide(tr(arg), '__qa');
    const name = `__q${n.hidden++}`;
    q = q.window({ partition: groupKeyNames, order: [{ name: argName }], items: [{ name, fn: disc ? 'percentile_disc' : 'percentile_cont', arg: argName, p }] });
    return name;
  }

  const grouped = ast.groupBy.length > 0 || items.some((it) => hasAgg(it.e)) || (ast.having && hasAgg(ast.having));
  const sortKeys = [];
  if (grouped) {
    // group keys: ordinal, select alias or expression
    const keys = []; // { name, sqlKey, kind }
    for (const g of ast.groupBy) {
      let e = g;
      if (g.k === 'num') e = items[g.v - 1]?.e ?? fail(`GROUP BY ${g.v}: there is no select item ${g.v}`);
      else if (g.k === 'col' && !g.table && items.some((it) => it.alias === g.name) && !scope.some((s) => s.names.has(g.name))) e = items.find((it) => it.alias === g.name).e;
      if (hasAgg(e)) fail('GROUP BY can\'t use aggregates');
      if (hasWindow(e)) fail('GROUP BY can\'t use window functions');
      let t = e.k === 'col' ? colOf(e) : translate(e, plain);
      if (isCond(e) && e.k !== 'num') t = condValue(e, plain, t); // (GROUP BY a > 0: true, false and NULL groups)
      let name;
      if (t.t === 'var') name = t.name;
      else { name = `__g${keys.length}_${n.hidden++}`; q = q.derive(name, src(t)); }
      keys.push({ name, sqlKey: key(e), colName: t.t === 'var' ? t.name : null, kind: e.k === 'col' ? kinds.get(t.name) : kindOf(e, plain) });
    }
    // aggregates, each once
    const aggs = new Map(); // sqlKey -> name
    const spec = {}, extraKeys = [];
    const aggDone = new Map(); // (an aggregate used twice: the same expression, SUM's NULL check included)
    const aggOf = (e) => { const k = key(e); if (!aggDone.has(k)) aggDone.set(k, aggOnce(e)); return aggDone.get(k); };
    const aggOnce = (e) => {
      const k = key(e);
      if (aggs.has(k)) return v(aggs.get(k));
      const name = `__a${aggs.size}_${n.hidden++}`;
      aggs.set(k, name);
      if (e.args.some(hasAgg)) fail('aggregates can\'t contain aggregates');
      if (PERCENTILE.has(e.fn)) {
        const qn = percentileAgg(e, keys.map((x) => x.name), (x) => translate(x, plain));
        const arg = e.fn === 'MEDIAN' || e.fn.startsWith('QUANTILE') ? e.args[0] : e.within.e;
        if (kindOf(arg, plain) === 'text') { extraKeys.push(qn); aggs.set(k, qn); return v(qn); } // (one value per group: a key, so it stays text)
        spec[name] = `min(${qn})`;
        return kindOf(arg, plain) === 'date' ? asDate(name) : v(name);
      }
      if ((e.fn === 'MIN' || e.fn === 'MAX' || e.fn === 'SUM' || e.fn === 'AVG' || e.fn === 'MEAN') && e.args[0] && kindOf(e.args[0], plain) === 'text') fail(`${e.fn}() of text is not supported yet (COUNT and COUNT(DISTINCT) are)`);
      const fn = e.fn === 'COUNT' && e.star ? 'count' : e.fn === 'COUNT' && e.distinct ? 'countDistinct' : AGG[e.fn];
      if (e.distinct && e.fn !== 'COUNT') fail(`${e.fn}(DISTINCT ...) is not supported (COUNT(DISTINCT x) is)`);
      if (!e.star && e.args.length !== 1) fail(`${e.fn}() takes one value`);
      const arg = e.star ? null : src(translate(e.args[0], plain));
      spec[name] = e.star ? 'count()' : `${fn}(${arg})`;
      if (fn === 'sum') { spec[`${name}_n`] = `count(${arg})`; return cond(bin('>', v(`${name}_n`), num(0)), v(name), num(NaN)); } // (SUM of no values: NULL)
      if ((fn === 'min' || fn === 'max') && kindOf(e.args[0], plain) === 'date') return asDate(name);
      return v(name);
    };
    // an aggregate of dates (MIN, MAX, a percentile) as a date column again, after grouping
    const asDate = (name) => valueColumn(post, [{ col: name, kind: 'date' }], ([x]) => x, { text: false, date: true, label: 'date' });
    // after grouping: aggregates and keys by name; other columns must be keys
    const deferred = []; // columns made from grouped values: after grouping
    const post = windowCtx({
      step: (st) => deferred.push(st),
      agg: aggOf,
      col: (ref) => {
        const k = keys.find((x) => x.sqlKey === key(ref) || (x.colName && x.colName === (ref.resolved ? ref.name : resolve(ref))));
        if (!k) fail(`column "${ref.table ? `${ref.table}.` : ''}${ref.name}" must be in GROUP BY or inside an aggregate`);
        return v(k.name);
      },
      kindOfCol: (ref) => keys.find((x) => x.colName && x.colName === resolve(ref))?.kind,
    });
    const translatePost = (e) => {
      const k = keys.find((x) => x.sqlKey === key(e));
      if (k && e.k !== 'col') return v(k.name);
      return translate(e, post);
    };
    planWindows(translatePost, (x) => kindOf(x, post));
    const tri = (e, t) => { if (!isCond(e) || e.k === 'num' || t.t === 'var') return t; try { return condValue(e, post, t); } catch { return t; } }; // (built from GROUP BY expressions: as it is)
    const outs = withCols(items.map((it) => { const t = translatePost(it.e); return { ...it, ast: textConst(tri(it.e, t), post) }; }));
    const havingAst = ast.having ? (ast.having.k === 'null' ? num(0) : translatePost(ast.having)) : null;
    const orderAsts = orderBy.map((o) => ({ ...o, ast: orderTarget(o.e, outs, translatePost) }));
    q = q.groupBy([...keys.map((k) => k.name), ...extraKeys]).agg(Object.keys(spec).length ? spec : { __a_rows: 'count()' });
    const late = new Set(windowVars.values()), after = []; // (those using window values: after the windows)
    for (const st of deferred) if (st.inputs.some((x) => late.has(x))) { late.add(st.name); after.push(st); } else st.run();
    if (havingAst) q = q.filter(src(havingAst));
    applyWindows();
    for (const st of after) st.run();
    for (const o of outs) if (o.ast.t !== 'var') q = q.derive(o.col, src(o.ast));
    for (const o of orderAsts) sortKey(o, orderKind(o, outs, post));
    return finish(outs, (o) => (o.e.k === 'col' ? post.kindOfCol(o.e) ?? kinds.get(resolveSafe(o.e)) : kindOf(o.e, post)), post);
  }
  const pre = windowCtx(plain);
  planWindows((e) => (e.k === 'col' ? colOf(e) : translate(e, pre)), (x) => kindOf(x, pre));
  applyWindows();
  const outs = withCols(items.map((it) => { const t = it.e.k === 'col' ? colOf(it.e) : translate(it.e, pre); return { ...it, ast: textConst(isCond(it.e) && it.e.k !== 'num' ? condValue(it.e, pre, t) : t, pre) }; }));
  const orderAsts = orderBy.map((o) => ({ ...o, ast: orderTarget(o.e, outs, (e) => (e.k === 'col' ? colOf(e) : translate(e, pre))) }));
  for (const o of outs) if (o.ast.t !== 'var') q = q.derive(o.col, src(o.ast));
  for (const o of orderAsts) sortKey(o, orderKind(o, outs, pre));
  if (ast.distinct) { // (the sort's own columns, NULLS FIRST marks and such, made from the selected ones: kept as keys too)
    const cols = new Set(outs.map((o) => o.col));
    for (const o of orderAsts) if (!(o.ast.t === 'var' && cols.has(o.ast.name)) && !colRefs(o.e).every((ref) => outs.some((x) => (!ref.table && x.name === ref.name) || x.col === resolveSafe(ref)))) fail('SELECT DISTINCT ... ORDER BY: order by the selected columns (or expressions of them)');
    q = q.groupBy([...new Set([...cols, ...sortKeys.map((k) => k.replace(/^-/, ''))])]).agg({ __distinct_rows: 'count()' });
  }
  return finish(outs, (o) => (o.e.k === 'col' ? (sessionRef(o.e)?.kind ?? kinds.get(o.e.resolved ? o.e.name : resolve(o.e))) : kindOf(o.e, pre)), pre);

  // a text literal as an answer column: a column of that text
  function textConst(ast, ctx) { return ast.t === 'str' ? valueColumn(ctx, [], () => ast.v, { label: 'text' }) : ast; }
  // each output's column while the query runs: a column as it is, or a
  // hidden one (renamed at the end: an output may take a name in use)
  function withCols(outs) { for (const o of outs) o.col = o.ast.t === 'var' ? o.ast.name : `__s${n.hidden++}`; return outs; }
  function resolveSafe(e) { try { return e.resolved ? e.name : resolve(e); } catch { return null; } }
  // ORDER BY item -> an output name or an expression
  function orderTarget(e, outs, tr) {
    if (e.k === 'num') { const o = outs[e.v - 1]; if (!o) fail(`ORDER BY ${e.v}: there is no select item ${e.v}`); return v(o.col); }
    if (e.k === 'col' && !e.table) { const o = outs.find((x) => x.name === e.name); if (o) return v(o.col); }
    const t = tr(e), same = outs.find((o) => toSourceAst(o.ast) === toSourceAst(t));
    return same ? v(same.col) : t;
  }
  // the kind (text or not) of what an ORDER BY item sorts: a select item by number or name, or the expression
  function orderKind(o, outs, ctx) {
    const e = o.e.k === 'num' ? outs[o.e.v - 1]?.e : (o.e.k === 'col' && !o.e.table && outs.find((x) => x.name === o.e.name)?.e) || o.e;
    try { return kindOf(e, ctx); } catch { return null; }
  }
  function sortKey(o, kind) {
    let k;
    if (o.ast.t === 'var') k = o.ast.name;
    else { k = `__o${n.hidden++}`; q = q.derive(k, src(o.ast)); }
    // missing sorts last on its own (NaN, and '' for text); NULLS FIRST sorts present values after missing ones first
    if (o.nullsFirst) { const h = `__nf${n.hidden++}`; q = q.derive(h, kind === 'text' ? `${k} == '' ? 0 : 1` : `${k} == ${k} ? 1 : 0`); sortKeys.push(h); }
    sortKeys.push(o.desc ? `-${k}` : k);
  }
  function finish(outs, kindFn, ctx) {
    let out = q;
    if (sortKeys.length) out = out.sort(sortKeys);
    if (tail.limit !== null && tail.limit !== undefined) out = out.limit(tail.limit, tail.offset);
    else if (tail.offset) fail('OFFSET needs LIMIT');
    const names = outs.map((o) => o.name);
    const outKinds = new Map(outs.map((o) => [o.name, o.e.k === 'null' ? 'null' : kindFn(o) ?? 'number'])); // (a NULL column matches any kind in a UNION)
    if (outs.some((o) => o.col !== o.name)) { // (through fresh names: an output may be named like another's column)
      const tmp = new Map([...new Set(outs.map((o) => o.col))].map((c) => [c, `__f${n.hidden++}`]));
      for (const [c, t] of tmp) out = out.derive(t, c);
      out = out.select(...tmp.values());
      for (const o of outs) out = out.derive(o.name, tmp.get(o.col));
    }
    const outTypes = env.wantTypes ? new Map(outs.map((o) => [o.name, typeOfExpr(o.e, ctx)])) : null;
    return { query: out.select(...names), names, kinds: outKinds, typeOf: (nm) => outTypes?.get(nm) ?? 'float' };
  }
}

// AST -> expression text (as fuse.mjs toSource, with text quoted for the parser)
function toSourceAst(n) {
  switch (n.t) {
    case 'num': return Number.isNaN(n.v) ? 'NaN' : n.v === Infinity ? 'Infinity' : n.v === -Infinity ? '(-Infinity)' : n.v < 0 ? `(${n.v})` : String(n.v);
    case 'str': return quoteText(n.v);
    case 'var': return n.name;
    case 'un': return `${n.op}(${toSourceAst(n.x)})`;
    case 'bin': return `(${toSourceAst(n.l)} ${n.op} ${toSourceAst(n.r)})`;
    case 'call': return `${n.fn}(${n.args.map(toSourceAst).join(', ')})`;
    case 'cond': return `(${toSourceAst(n.c)} ? ${toSourceAst(n.x)} : ${toSourceAst(n.y)})`;
  }
  throw new Error(`bad node ${n.t}`);
}

export { Query, namesOf, columnType };
