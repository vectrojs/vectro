// OData v4 over Vectro tables (part of the HTTP server): Power BI's and
// Excel's "OData feed", Tableau's OData connector and OData client libraries
// read tables with no driver, with filters, sorting and paging done by Vectro.
//
//   GET /odata                     the service document (the tables)
//   GET /odata/$metadata           their columns and types (CSDL XML)
//   GET /odata/<table>?$filter=region eq 'Europe' and qty gt 2&$select=...&$orderby=ts desc&$top=100&$skip=0&$count=true
//   GET /odata/<table>?$apply=filter(qty gt 1)/groupby((region),aggregate(qty with sum as total,$count as n))
//   GET /odata/<table>/$count
//
// Every request becomes one Vectro SQL query. Answers come a page at a time
// (50,000 rows, or Prefer: odata.maxpagesize=n) with @odata.nextLink to the
// next page. A table's key is its first whole-number column ending in "id"
// (or its first whole-number column); requests by key aren't served.
import { sql, columnType, displayName } from '../sql.mjs';

export const PAGE = 50_000;
const NS = 'Vectro';
const EDM = { int: 'Edm.Int64', float: 'Edm.Double', bool: 'Edm.Boolean', text: 'Edm.String', date: 'Edm.DateTimeOffset', day: 'Edm.Date' };

export class ODataError extends Error { constructor(status, message) { super(message); this.status = status; } }
const bad = (m) => { throw new ODataError(400, m); };
const xml = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const qid = (name) => `"${String(name).replaceAll('"', '""')}"`;
const sqlText = (s) => `'${String(s).replaceAll("'", "''")}'`;
const odataName = (n) => (/^[A-Za-z_][A-Za-z0-9_]*$/.test(n) ? n : null); // (OData names: letters, digits, _)

function keyOf(table) {
  const ints = table.columnNames.filter((c) => columnType(table, c) === 'int');
  return ints.find((c) => /id$/i.test(c)) ?? ints[0] ?? null;
}

export function metadataXml(named) {
  const types = [], sets = [];
  for (const [name, t] of Object.entries(named)) {
    if (name.includes('.') || !odataName(name)) continue;
    const key = keyOf(t);
    const props = t.columnNames.filter(odataName).map((c) => {
      const type = EDM[columnType(t, c)] ?? 'Edm.Double';
      return `<Property Name="${xml(c)}" Type="${type}"${c === key ? ' Nullable="false"' : ''}${type === 'Edm.DateTimeOffset' ? ' Precision="3"' : ''}/>`;
    });
    types.push(`<EntityType Name="${xml(name)}">${key ? `<Key><PropertyRef Name="${xml(key)}"/></Key>` : ''}${props.join('')}</EntityType>`);
    sets.push(`<EntitySet Name="${xml(name)}" EntityType="${NS}.${xml(name)}"/>`);
  }
  return `<?xml version="1.0" encoding="utf-8"?><edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"><edmx:DataServices><Schema Namespace="${NS}" xmlns="http://docs.oasis-open.org/odata/ns/edm">${types.join('')}<EntityContainer Name="Container">${sets.join('')}</EntityContainer></Schema></edmx:DataServices></edmx:Edmx>`;
}

// ---- $filter expressions -> SQL ------------------------------------------------------------
function tokens(src) {
  const out = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (/\s/.test(c)) { i++; continue; }
    if (c === "'") { let v = ''; i++; for (;;) { if (i >= src.length) bad('a text in quotes is not closed'); if (src[i] === "'") { if (src[i + 1] === "'") { v += "'"; i += 2; continue; } i++; break; } v += src[i++]; } out.push({ t: 'str', v }); continue; }
    const dt = /^\d{4}-\d\d-\d\d(T\d\d:\d\d(:\d\d(\.\d+)?)?(Z|[+-]\d\d:\d\d)?)?/.exec(src.slice(i));
    if (dt) { out.push({ t: dt[1] ? 'datetime' : 'date', v: dt[0] }); i += dt[0].length; continue; }
    const num = /^-?(\d+\.?\d*(e[+-]?\d+)?|\.\d+)[mMdDlLfF]?(?![\w])/.exec(src.slice(i));
    if (num && (c !== '-' || !out.length || ['op', 'kw'].includes(out.at(-1).t) || out.at(-1).v === '(' || out.at(-1).v === ',')) { out.push({ t: 'num', v: num[0].replace(/[mMdDlLfF]$/, '') }); i += num[0].length; continue; }
    const id = /^[A-Za-z_$][\w$]*/.exec(src.slice(i));
    if (id) { out.push({ t: 'id', v: id[0] }); i += id[0].length; continue; }
    if ('(),/'.includes(c)) { out.push({ t: 'op', v: c }); i++; continue; }
    bad(`unexpected "${c}" in the expression`);
  }
  return out;
}
const CMP = { eq: '=', ne: '<>', gt: '>', ge: '>=', lt: '<', le: '<=' };
const ARITH = { add: '+', sub: '-', mul: '*', div: '/', divby: '/', mod: '%' };
// columns: names allowed (the table's, or the current $apply step's)
export function filterSql(src, columns) {
  const toks = tokens(src);
  let p = 0;
  const peek = () => toks[p], isId = (v) => peek()?.t === 'id' && peek().v === v;
  const need = (v) => { if (peek()?.v !== v) bad(`expected "${v}"${peek() ? ` near "${peek().v}"` : ' at the end'}`); p++; };
  const or = () => { let l = and(); while (isId('or')) { p++; l = `(${l} OR ${and()})`; } return l; };
  const and = () => { let l = not(); while (isId('and')) { p++; l = `(${l} AND ${not()})`; } return l; };
  const not = () => { if (isId('not')) { p++; return `(NOT ${not()})`; } return cmp(); };
  const cmp = () => {
    const l = add();
    const op = peek()?.t === 'id' ? peek().v : null;
    if (op && CMP[op]) {
      p++;
      const r = add();
      if (r === 'NULL') return op === 'eq' ? `(${l} IS NULL)` : op === 'ne' ? `(${l} IS NOT NULL)` : bad('null can only be compared with eq or ne');
      return `(${l} ${CMP[op]} ${r})`;
    }
    if (op === 'in') { p++; need('('); const list = [add()]; while (peek()?.v === ',') { p++; list.push(add()); } need(')'); return `(${l} IN (${list.join(', ')}))`; }
    return l;
  };
  const add = () => { let l = mul(); while (peek()?.t === 'id' && (peek().v === 'add' || peek().v === 'sub')) { const o = ARITH[toks[p++].v]; l = `(${l} ${o} ${mul()})`; } return l; };
  const mul = () => { let l = unary(); while (peek()?.t === 'id' && ['mul', 'div', 'divby', 'mod'].includes(peek().v)) { const o = ARITH[toks[p++].v]; l = `(${l} ${o} ${unary()})`; } return l; };
  const unary = () => (peek()?.t === 'id' && peek().v === '-' ? (p++, `(-${unary()})`) : primary());
  const primary = () => {
    const t = peek();
    if (!t) bad('the expression ends too early');
    p++;
    if (t.t === 'str') return sqlText(t.v);
    if (t.t === 'num') return `(${Number(t.v)})`;
    if (t.t === 'date') return `DATE ${sqlText(t.v)}`;
    if (t.t === 'datetime') { const ms = Date.parse(t.v); if (!Number.isFinite(ms)) bad(`bad date and time ${t.v}`); return `TIMESTAMP ${sqlText(new Date(ms).toISOString().replace('T', ' ').replace('Z', ''))}`; }
    if (t.v === '(') { const e = or(); need(')'); return e; }
    if (t.t !== 'id') bad(`unexpected "${t.v}"`);
    if (t.v === 'true') return 'TRUE';
    if (t.v === 'false') return 'FALSE';
    if (t.v === 'null') return 'NULL';
    if (peek()?.v === '(') { // a function
      p++;
      const args = [];
      if (peek()?.v !== ')') { args.push(or()); while (peek()?.v === ',') { p++; args.push(or()); } }
      need(')');
      return fn(t.v, args);
    }
    if (peek()?.v === '/') bad('paths (a/b) are not supported');
    if (!columns.includes(t.v)) bad(`no column "${t.v}" (columns: ${columns.join(', ')})`);
    return qid(t.v);
  };
  const fn = (name, a) => {
    const n = (k) => { if (a.length !== k) bad(`${name}() takes ${k} value(s)`); };
    switch (name) {
      case 'contains': n(2); return `(STRPOS(${a[0]}, ${a[1]}) > 0)`;
      case 'startswith': n(2); return `STARTS_WITH(${a[0]}, ${a[1]})`;
      case 'endswith': n(2); return `ENDS_WITH(${a[0]}, ${a[1]})`;
      case 'tolower': n(1); return `LOWER(${a[0]})`;
      case 'toupper': n(1); return `UPPER(${a[0]})`;
      case 'trim': n(1); return `TRIM(${a[0]})`;
      case 'length': n(1); return `LENGTH(${a[0]})`;
      case 'concat': n(2); return `(${a[0]} || ${a[1]})`;
      case 'indexof': n(2); return `(STRPOS(${a[0]}, ${a[1]}) - 1)`;
      case 'substring': if (a.length < 2 || a.length > 3) bad('substring() takes 2 or 3 values'); return `SUBSTRING(${a[0]}, ${a[1]} + 1${a[2] ? `, ${a[2]}` : ''})`;
      case 'year': case 'month': case 'day': case 'hour': case 'minute': case 'second': n(1); return `EXTRACT(${name.toUpperCase()} FROM ${a[0]})`;
      case 'date': n(1); return `CAST(${a[0]} AS DATE)`;
      case 'round': n(1); return `ROUND(${a[0]})`;
      case 'floor': n(1); return `FLOOR(${a[0]})`;
      case 'ceiling': n(1); return `CEIL(${a[0]})`;
      case 'now': n(0); return 'NOW()';
      default: bad(`the function ${name}() is not supported`);
    }
  };
  const out = or();
  if (p < toks.length) bad(`unexpected "${toks[p].v}"`);
  return out;
}

// ---- $apply -> nested SQL --------------------------------------------------------------------
// Returns { sql, columns } for the rows the transformations leave.
function splitTop(s, sep) { // split at sep outside parentheses and quotes
  const out = [];
  let depth = 0, q = false, cur = '';
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "'") q = !q;
    if (!q && c === '(') depth++;
    if (!q && c === ')') depth--;
    if (!q && depth === 0 && s.startsWith(sep, i)) { out.push(cur); cur = ''; i += sep.length - 1; continue; }
    cur += c;
  }
  out.push(cur);
  return out.map((x) => x.trim()).filter(Boolean);
}
const inner = (s, name) => { const m = new RegExp(`^${name}\\s*\\(([\\s\\S]*)\\)$`).exec(s.trim()); return m ? m[1] : null; };
const METHODS = { sum: 'SUM', min: 'MIN', max: 'MAX', average: 'AVG', countdistinct: 'COUNT' };
function aggregates(spec, columns) {
  return splitTop(spec, ',').map((a) => {
    let m = /^\$count\s+as\s+([A-Za-z_]\w*)$/.exec(a);
    if (m) return { sql: `COUNT(*) AS ${qid(m[1])}`, name: m[1] };
    m = /^([\s\S]+?)\s+with\s+(\w+)\s+as\s+([A-Za-z_]\w*)$/.exec(a);
    if (!m) bad(`aggregate(${a}): write "column with sum as name" or "$count as name"`);
    const method = METHODS[m[2]] ?? bad(`aggregate method ${m[2]} is not supported (sum, min, max, average, countdistinct)`);
    const e = filterSql(m[1], columns);
    return { sql: `${method}(${m[2] === 'countdistinct' ? 'DISTINCT ' : ''}${e}) AS ${qid(m[3])}`, name: m[3] };
  });
}
export function applySql(apply, table, columns) {
  let from = qid(table), cols = columns, k = 0;
  for (const step of splitTop(apply, '/')) {
    const alias = `_a${k++}`;
    let body;
    if ((body = inner(step, 'filter')) !== null) { from = `(SELECT * FROM ${from} ${alias === '_a0' ? '' : ''}WHERE ${filterSql(body, cols)})`; from = wrap(from, alias); continue; }
    if ((body = inner(step, 'aggregate')) !== null) { const ag = aggregates(body, cols); from = wrap(`(SELECT ${ag.map((x) => x.sql).join(', ')} FROM ${from})`, alias); cols = ag.map((x) => x.name); continue; }
    if ((body = inner(step, 'groupby')) !== null) {
      const parts = splitTop(body, ',');
      const keys = inner(`g${parts[0]}`, 'g');
      if (keys === null) bad('groupby((columns), aggregate(...))');
      const names = splitTop(keys, ',');
      for (const nm of names) if (!cols.includes(nm)) bad(`no column "${nm}"`);
      const ag = parts[1] ? aggregates(inner(parts[1], 'aggregate') ?? bad('groupby(..., aggregate(...))'), cols) : [];
      const select = [...names.map(qid), ...ag.map((x) => x.sql)].join(', ');
      from = wrap(`(SELECT ${select} FROM ${from} GROUP BY ${names.map(qid).join(', ')})`, alias);
      cols = [...names, ...ag.map((x) => x.name)];
      continue;
    }
    bad(`$apply: ${step.split('(')[0]} is not supported (filter, groupby, aggregate)`);
  }
  return { from, columns: cols };
}
const wrap = (sub, alias) => `${sub} ${alias}`;

// ---- a request -----------------------------------------------------------------------------------
// The SQL for a read of a table: { sql, countSql, columns, top, skip }
export function querySql(table, tableColumns, params, pageSize) {
  let from = qid(table), columns = tableColumns;
  const get = (k) => params.get(k) ?? params.get(k.slice(1)); // ($filter or filter)
  if (get('$apply')) ({ from, columns } = applySql(get('$apply'), table, tableColumns));
  const where = get('$filter') ? ` WHERE ${filterSql(get('$filter'), columns)}` : '';
  let select = columns;
  if (get('$select') && get('$select').trim() !== '*') {
    select = get('$select').split(',').map((s) => s.trim()).filter(Boolean);
    for (const c of select) if (!columns.includes(c)) bad(`no column "${c}" to select`);
  }
  const order = get('$orderby') ? ` ORDER BY ${get('$orderby').split(',').map((s) => { const [c, dir] = s.trim().split(/\s+/); if (!columns.includes(c)) bad(`no column "${c}" to order by`); if (dir && !/^(asc|desc)$/i.test(dir)) bad(`order "${dir}": asc or desc`); return `${qid(c)}${dir?.toLowerCase() === 'desc' ? ' DESC' : ''}`; }).join(', ')}` : '';
  const num = (k) => { const v = get(k); if (v === null || v === undefined) return null; if (!/^\d+$/.test(v)) bad(`${k} must be a whole number`); return Number(v); };
  const top = num('$top'), skip = num('$skip') ?? 0, token = params.get('$skiptoken');
  const start = skip + (token && /^\d+$/.test(token) ? Number(token) : 0);
  const want = top === null ? pageSize : Math.min(pageSize, Math.max(0, top - (start - skip)));
  const base = `FROM ${from}${where}`;
  return {
    sql: `SELECT ${select.map(qid).join(', ')} ${base}${order} LIMIT ${want + 1} OFFSET ${start}`, // (one more: is there a next page?)
    countSql: `SELECT COUNT(*) AS n ${base}`,
    columns: select, want, start, top, skip, count: /^true$/i.test(get('$count') ?? ''),
  };
}

// OData JSON for one value
function cell(type, v) {
  if (v === null || v === undefined || (typeof v === 'number' && v !== v)) return 'null';
  switch (type) {
    case 'text': return v === '' ? 'null' : JSON.stringify(v);
    case 'int': return String(Math.round(v));
    case 'bool': return v ? 'true' : 'false';
    case 'date': return `"${new Date(v).toISOString()}"`;
    case 'day': return `"${new Date(v).toISOString().slice(0, 10)}"`;
    default: return v === Infinity ? '"INF"' : v === -Infinity ? '"-INF"' : String(v);
  }
}

// Answer GET /odata... ; write(res, status, headers, body chunks)
export async function odata({ path, params, named, base, headers = {}, pageSize = PAGE }) {
  const H = { 'OData-Version': '4.0' };
  const json = (o, status = 200) => ({ status, headers: { ...H, 'content-type': 'application/json;odata.metadata=minimal' }, body: [JSON.stringify(o)] });
  const parts = path.split('/').filter(Boolean).map((p) => { try { return decodeURIComponent(p); } catch { return bad(`the path has a malformed %-escape: ${p}`); } }); // ['odata', ...]
  const tables = Object.fromEntries(Object.entries(named).filter(([n]) => !n.includes('.') && odataName(n)));
  if (parts.length === 1) return json({ '@odata.context': `${base}/$metadata`, value: Object.keys(tables).map((n) => ({ name: n, kind: 'EntitySet', url: n })) });
  if (parts[1] === '$metadata') return { status: 200, headers: { ...H, 'content-type': 'application/xml' }, body: [metadataXml(tables)] };
  const name = parts[1].replace(/\(.*\)$/, ''), t = tables[name];
  if (!t) throw new ODataError(404, `no table "${name}" (tables: ${Object.keys(tables).join(', ') || 'none'})`);
  if (parts[1] !== name) throw new ODataError(501, 'reading one row by its key is not supported; use $filter');
  const columns = t.columnNames.filter(odataName);
  if (parts[2] === '$count') {
    const where = params.get('$filter') ? ` WHERE ${filterSql(params.get('$filter'), columns)}` : '';
    const n = (await sql(`SELECT COUNT(*) AS n FROM ${qid(name)}${where}`, tables).toRowsAsync())[0].n;
    return { status: 200, headers: { ...H, 'content-type': 'text/plain' }, body: [String(n)] };
  }
  if (parts.length > 2) throw new ODataError(501, `/${parts.slice(2).join('/')} is not supported`);
  const pref = /odata\.maxpagesize=(\d+)/i.exec(headers.prefer ?? '');
  const page = pref ? Math.max(1, Math.min(Number(pref[1]), 1_000_000)) : pageSize;
  const q = querySql(name, columns, params, page);
  let query;
  try { query = sql(q.sql, tables, { types: true }); } catch (err) { bad(err.message.replace(/^vectro( SQL)?:\s*/, '')); }
  const types = query.columns.map((c) => c.type);
  const table = await query.collectAsync();
  const out = { status: 200, headers: { ...H, 'content-type': 'application/json;odata.metadata=minimal;odata.streaming=true', ...(pref ? { 'Preference-Applied': `odata.maxpagesize=${page}` } : {}) }, body: null };
  try {
    const n = Math.min(table.length, q.want), more = table.length > q.want && (q.top === null || q.start - q.skip + n < q.top);
    const data = await table.toColumnsAsync();
    const readers = query.columns.map((c) => { const d = data[c.name]; return d?.labels ? (i) => d.labels[d.codes[i]] : (i) => d[i]; });
    const keys = q.columns.map((c) => `${JSON.stringify(c)}:`);
    const select = params.get('$select') || params.get('$apply') ? `(${q.columns.join(',')})` : '';
    let head = `{"@odata.context":${JSON.stringify(`${base}/$metadata#${name}${select}`)}`;
    if (q.count) head += `,"@odata.count":${(await sql(q.countSql, tables).toRowsAsync())[0].n}`;
    if (more) { // the next page: the same request from the next row
      const next = new URLSearchParams(params);
      next.set('$skiptoken', String(q.start - q.skip + n));
      head += `,"@odata.nextLink":${JSON.stringify(`${base}/${encodeURIComponent(name)}?${next.toString().replace(/\+/g, '%20')}`)}`;
    }
    const body = [`${head},"value":[`];
    for (let a = 0; a < n; a += 16_384) {
      let s = '';
      for (let i = a; i < Math.min(n, a + 16_384); i++) {
        s += `${i ? ',' : ''}{`;
        for (let k = 0; k < keys.length; k++) s += `${k ? ',' : ''}${keys[k]}${cell(types[k], readers[k](i))}`;
        s += '}';
      }
      body.push(s);
    }
    body.push(']}');
    out.body = body;
  } finally { table.dispose(); }
  return out;
}

export { displayName };
